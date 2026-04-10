"""
Sentinel-2 NDVI raster download + local zonal statistics.

Downloads cloud-free NDVI composites from GEE as GeoTIFF (10m resolution),
then delegates zonal statistics to the shared zonal_stats module.

Approach:
- 5-day median composites (S2A + S2B revisit = 5 days)
- Cloud mask via SCL band (keep vegetation, bare soil, water, low cloud prob)
- NDVI = (B8 - B4) / (B8 + B4)
- computePixels() download (no extra IAM permissions needed)

Storage: /data/s2/{region_id}/{year}/
    s2_ndvi_{region_id}_{date_from}_{date_to}.tif

Tile strategy:
    At 10m a full Crimea composite is ~300 MB. For large regions the download
    is split into tiles of MAX_TILE_PX (~4000×4000 = 40×40 km).
"""
import logging
import math
import os
from datetime import date, timedelta
from pathlib import Path

import ee
from django.conf import settings

from .satellite_gee import GEEError, initialize

logger = logging.getLogger(__name__)

# Storage root
RASTER_DIR = os.environ.get(
    'S2_RASTER_DIR',
    getattr(settings, 'S2_RASTER_DIR', '/data/s2'),
)

SCALE_M = 10          # metres per pixel
SCALE_DEG = SCALE_M / 111320  # approximate degrees at mid-latitudes
COMPOSITE_DAYS = 5    # S2A+S2B revisit
MAX_TILE_PX = 25000   # GEE limit is 32768; use 25K for safety margin


# ---------------------------------------------------------------------------
# Temporal helpers
# ---------------------------------------------------------------------------

def s2_chunks(date_from, date_to, days=COMPOSITE_DAYS):
    """
    Split date range into N-day periods anchored to Jan 1.

    Anchoring guarantees stable chunk boundaries regardless of --date-from.
    """
    epoch = date(date_from.year, 1, 1)
    chunks = []
    cursor = epoch
    while cursor <= date_to:
        end = cursor + timedelta(days=days - 1)
        if end >= date_from:
            chunks.append((max(cursor, date_from), min(end, date_to)))
        cursor = end + timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Tiling
# ---------------------------------------------------------------------------

def _tile_extents(xmin, ymin, xmax, ymax, scale_deg, max_px):
    """
    Split a bounding box into tiles that fit GEE computePixels limits.

    Returns list of (tile_xmin, tile_ymin, tile_xmax, tile_ymax) tuples.
    """
    width_px = int((xmax - xmin) / scale_deg) + 1
    height_px = int((ymax - ymin) / scale_deg) + 1

    n_cols = math.ceil(width_px / max_px)
    n_rows = math.ceil(height_px / max_px)

    tile_w = (xmax - xmin) / n_cols
    tile_h = (ymax - ymin) / n_rows

    tiles = []
    for row in range(n_rows):
        for col in range(n_cols):
            tx0 = xmin + col * tile_w
            ty0 = ymin + row * tile_h
            tx1 = min(tx0 + tile_w, xmax)
            ty1 = min(ty0 + tile_h, ymax)
            tiles.append((tx0, ty0, tx1, ty1))

    return tiles


def _download_tile(composite, tx0, ty0, tx1, ty1, scale_deg):
    """Download a single tile from GEE as bytes."""
    w = int((tx1 - tx0) / scale_deg) + 1
    h = int((ty1 - ty0) / scale_deg) + 1
    return ee.data.computePixels({
        'expression': composite,
        'fileFormat': 'GEO_TIFF',
        'grid': {
            'crsCode': 'EPSG:4326',
            'affineTransform': {
                'scaleX': scale_deg,
                'shearX': 0,
                'translateX': tx0,
                'shearY': 0,
                'scaleY': -scale_deg,
                'translateY': ty1,
            },
            'dimensions': {'width': w, 'height': h},
        },
    })


def _merge_tiles(tile_paths, out_path):
    """Merge multiple GeoTIFF tiles into one file."""
    import rasterio
    from rasterio.merge import merge as rasterio_merge

    datasets = [rasterio.open(p) for p in tile_paths]
    try:
        mosaic, transform = rasterio_merge(datasets)
        profile = datasets[0].profile.copy()
        profile.update(
            width=mosaic.shape[2],
            height=mosaic.shape[1],
            transform=transform,
        )
        with rasterio.open(out_path, 'w', **profile) as dst:
            dst.write(mosaic)
    finally:
        for ds in datasets:
            ds.close()
        for p in tile_paths:
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _raster_path(region_id, date_from, date_to):
    d = Path(RASTER_DIR) / str(region_id) / str(date_from.year)
    d.mkdir(parents=True, exist_ok=True)
    fname = f's2_ndvi_{region_id}_{date_from.isoformat()}_{date_to.isoformat()}.tif'
    return str(d / fname)


def download_composite(region_geom_extent, region_id, date_from, date_to,
                       cloud_max=30, overwrite=False):
    """
    Download a cloud-free S2 median NDVI composite from GEE as GeoTIFF.

    Automatically tiles large regions that exceed GEE pixel limits.

    Args:
        region_geom_extent: (xmin, ymin, xmax, ymax) in EPSG:4326
        region_id: int, for file naming
        date_from, date_to: date objects
        cloud_max: max scene cloud cover %
        overwrite: re-download if exists

    Returns:
        str: path to GeoTIFF, or None if no data
    """
    initialize()

    out_path = _raster_path(region_id, date_from, date_to)
    if os.path.exists(out_path) and not overwrite:
        logger.info('Raster exists, skipping: %s', out_path)
        return out_path

    xmin, ymin, xmax, ymax = region_geom_extent
    aoi = ee.Geometry.Rectangle([xmin, ymin, xmax, ymax])

    df = date_from.isoformat()
    # GEE filterDate end is exclusive → add 1 day
    dt = (date_to + timedelta(days=1)).isoformat()

    try:
        s2 = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
              .filterDate(df, dt)
              .filterBounds(aoi)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_max)))

        n_images = s2.size().getInfo()
        if n_images == 0:
            logger.info('No S2 images for %s..%s', df, dt)
            return None

        # Cloud mask via SCL + NDVI
        def _add_ndvi(image):
            scl = image.select('SCL')
            clear = (scl.eq(4).Or(scl.eq(5))
                     .Or(scl.eq(6)).Or(scl.eq(7)))
            ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
            return ndvi.updateMask(clear)

        composite = s2.map(_add_ndvi).median().rename('NDVI').toFloat()

        # Check if tiling is needed
        tiles = _tile_extents(xmin, ymin, xmax, ymax, SCALE_DEG, MAX_TILE_PX)

        if len(tiles) == 1:
            # Single tile — download directly
            content = _download_tile(composite, xmin, ymin, xmax, ymax, SCALE_DEG)
            with open(out_path, 'wb') as f:
                f.write(content)
        else:
            # Multi-tile: download each, then merge
            logger.info('Region too large, splitting into %d tiles', len(tiles))
            tile_paths = []
            base = out_path.replace('.tif', '')
            for ti, (tx0, ty0, tx1, ty1) in enumerate(tiles):
                tile_path = f'{base}_tile{ti}.tif'
                content = _download_tile(composite, tx0, ty0, tx1, ty1, SCALE_DEG)
                with open(tile_path, 'wb') as f:
                    f.write(content)
                tile_paths.append(tile_path)
                logger.info('  Tile %d/%d downloaded', ti + 1, len(tiles))

            _merge_tiles(tile_paths, out_path)

        import rasterio
        with rasterio.open(out_path) as ds:
            logger.info(
                'Downloaded S2: %s (%d×%d, %.1f MB, %d images → median, %d tiles)',
                out_path, ds.width, ds.height,
                os.path.getsize(out_path) / 1e6, n_images, len(tiles),
            )

        return out_path

    except Exception as e:
        if os.path.exists(out_path):
            os.remove(out_path)
        raise GEEError(f'S2 download error: {e}')


def download_period(region_geom_extent, region_id, date_from, date_to,
                    cloud_max=30, overwrite=False):
    """
    Download all S2 composites for a date range.

    Returns:
        list of (date_from, date_to, tif_path) tuples
    """
    chunks = s2_chunks(date_from, date_to)
    results = []
    for i, (cf, ct) in enumerate(chunks):
        logger.info(
            'Downloading S2 composite %d/%d: %s..%s (region %d)',
            i + 1, len(chunks), cf, ct, region_id,
        )
        path = download_composite(
            region_geom_extent, region_id, cf, ct,
            cloud_max=cloud_max, overwrite=overwrite,
        )
        results.append((cf, ct, path))
    return results
