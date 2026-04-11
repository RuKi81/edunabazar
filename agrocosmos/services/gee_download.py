"""
Shared GEE raster download utilities with tiling and timeouts.

Uses getDownloadURL() + requests instead of computePixels() to avoid:
- 48 MB response size limit (still applies per tile, but we tile smartly)
- Indefinite hangs (requests has configurable timeouts)

GEE getDownloadURL limit: ~32 MB per band → ~8M pixels (float32).
MAX_TILE_PX = 2500 → 2500×2500 = 6.25M pixels ≈ 25 MB — safe margin.
"""
import logging
import math
import os
import tempfile

import ee
import requests as http_requests

logger = logging.getLogger(__name__)

MAX_TILE_PX = 2500
DOWNLOAD_TIMEOUT = 300  # seconds per tile


def tile_extents(xmin, ymin, xmax, ymax, scale_deg, max_px=MAX_TILE_PX):
    """
    Split a bounding box into tiles that fit GEE download limits.

    Returns list of (tile_xmin, tile_ymin, tile_xmax, tile_ymax) tuples.
    """
    width_px = int((xmax - xmin) / scale_deg) + 1
    height_px = int((ymax - ymin) / scale_deg) + 1

    n_cols = max(1, math.ceil(width_px / max_px))
    n_rows = max(1, math.ceil(height_px / max_px))

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

    logger.info(
        'Tiling: %d×%d px → %d×%d grid = %d tiles',
        width_px, height_px, n_cols, n_rows, len(tiles),
    )
    return tiles


def download_tile(composite, tx0, ty0, tx1, ty1, scale_m,
                  timeout=DOWNLOAD_TIMEOUT):
    """
    Download a single tile from GEE using getDownloadURL + requests.

    Returns bytes (GeoTIFF content).
    Raises GEEError on failure.
    """
    from .satellite_gee import GEEError

    region = ee.Geometry.Rectangle([tx0, ty0, tx1, ty1])

    try:
        url = composite.getDownloadURL({
            'scale': scale_m,
            'region': region,
            'format': 'GEO_TIFF',
            'crs': 'EPSG:4326',
        })
    except Exception as e:
        raise GEEError(f'getDownloadURL failed: {e}')

    try:
        resp = http_requests.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()

        content = resp.content
        # GEE returns JSON error for some failures
        if len(content) < 1000 and content[:1] == b'{':
            raise GEEError(f'GEE returned error: {content[:500]}')

        return content

    except http_requests.Timeout:
        raise GEEError(f'Download timeout ({timeout}s) for tile')
    except http_requests.RequestException as e:
        raise GEEError(f'Download failed: {e}')


def merge_tiles(tile_paths, out_path):
    """Merge multiple GeoTIFF tiles into one LZW-compressed file."""
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
            compress='lzw',
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )
        with rasterio.open(out_path, 'w', **profile) as dst:
            dst.write(mosaic)
    finally:
        for ds in datasets:
            ds.close()
        for p in tile_paths:
            if os.path.exists(p):
                os.remove(p)


def download_tiled_composite(composite, extent, scale_m, out_path,
                             n_images=0, sensor_label=''):
    """
    Download a GEE composite as GeoTIFF, automatically tiling if needed.

    Args:
        composite: ee.Image — the composite to download
        extent: (xmin, ymin, xmax, ymax) in EPSG:4326
        scale_m: pixel size in metres
        out_path: output file path
        n_images: number of source images (for logging)
        sensor_label: 'S2', 'Landsat', etc. (for logging)

    Returns:
        str: out_path on success
    """
    from .satellite_gee import GEEError

    xmin, ymin, xmax, ymax = extent
    scale_deg = scale_m / 111320

    tiles = tile_extents(xmin, ymin, xmax, ymax, scale_deg)

    if len(tiles) == 1:
        tx0, ty0, tx1, ty1 = tiles[0]
        content = download_tile(composite, tx0, ty0, tx1, ty1, scale_m)
        with open(out_path, 'wb') as f:
            f.write(content)
    else:
        logger.info('%s: downloading %d tiles…', sensor_label, len(tiles))
        tile_paths = []
        base = out_path.replace('.tif', '')

        for ti, (tx0, ty0, tx1, ty1) in enumerate(tiles):
            tile_path = f'{base}_tile{ti}.tif'
            content = download_tile(composite, tx0, ty0, tx1, ty1, scale_m)
            with open(tile_path, 'wb') as f:
                f.write(content)
            tile_paths.append(tile_path)
            logger.info('  Tile %d/%d OK (%.1f MB)',
                        ti + 1, len(tiles), len(content) / 1e6)

        merge_tiles(tile_paths, out_path)

    import rasterio
    with rasterio.open(out_path) as ds:
        logger.info(
            '%s: %s (%d×%d, %.1f MB, %d src images, %d tiles)',
            sensor_label, out_path, ds.width, ds.height,
            os.path.getsize(out_path) / 1e6, n_images, len(tiles),
        )

    return out_path
