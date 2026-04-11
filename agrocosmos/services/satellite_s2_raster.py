"""
Sentinel-2 NDVI raster download + local zonal statistics.

Downloads cloud-free NDVI composites from GEE as GeoTIFF (10m resolution),
then delegates zonal statistics to the shared zonal_stats module.

Approach:
- 5-day median composites (S2A + S2B revisit = 5 days)
- Cloud mask via SCL band (keep vegetation, bare soil, water, low cloud prob)
- NDVI = (B8 - B4) / (B8 + B4)
- Download via getDownloadURL + requests (with timeout, no hangs)
- Auto-tiling for large regions (each tile ≤ 2500×2500 px)

Storage: /data/s2/{region_id}/{year}/
    s2_ndvi_{region_id}_{date_from}_{date_to}.tif
"""
import logging
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
COMPOSITE_DAYS = 5    # S2A+S2B revisit


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

    Automatically tiles large regions. Uses getDownloadURL + requests
    for reliable downloads with timeouts.

    Args:
        region_geom_extent: (xmin, ymin, xmax, ymax) in EPSG:4326
        region_id: int, for file naming
        date_from, date_to: date objects
        cloud_max: max scene cloud cover %
        overwrite: re-download if exists

    Returns:
        str: path to GeoTIFF, or None if no data
    """
    from .gee_download import download_tiled_composite

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

        download_tiled_composite(
            composite,
            extent=(xmin, ymin, xmax, ymax),
            scale_m=SCALE_M,
            out_path=out_path,
            n_images=n_images,
            sensor_label='S2',
        )
        return out_path

    except GEEError:
        if os.path.exists(out_path):
            os.remove(out_path)
        raise
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
