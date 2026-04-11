"""
Shared GEE raster download utilities with tiling and timeouts.

Uses computePixels() with:
- Auto-tiling to fit 48 MB response limit (~12M pixels at float32)
- concurrent.futures timeout to prevent indefinite hangs

MAX_TILE_PX = 2000 → each tile ≈ 2000×2000 = 4M pixels ≈ 16 MB (< 48 MB).
"""
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import ee

logger = logging.getLogger(__name__)

MAX_TILE_PX = 2000    # 2000×2000 = 4M pixels × 4 = 16 MB (limit is 48 MB)
DOWNLOAD_TIMEOUT = 300  # seconds per tile
MAX_RESPONSE_BYTES = 50_331_648  # GEE computePixels hard limit


def tile_extents(xmin, ymin, xmax, ymax, scale_deg, max_px=MAX_TILE_PX):
    """
    Split a bounding box into tiles that fit GEE computePixels limits.

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

    msg = (f'Tiling: {width_px}×{height_px} px → '
           f'{n_cols}×{n_rows} grid = {len(tiles)} tiles')
    logger.info(msg)
    print(f'    [tile] {msg}')  # ensure visible in management command stdout
    return tiles


def _compute_pixels(params):
    """Wrapper for ee.data.computePixels (for use in thread pool)."""
    return ee.data.computePixels(params)


def download_tile(composite, tx0, ty0, tx1, ty1, scale_deg,
                  timeout=DOWNLOAD_TIMEOUT):
    """
    Download a single tile from GEE using computePixels with a timeout.

    Returns bytes (GeoTIFF content).
    Raises GEEError on failure.
    """
    from .satellite_gee import GEEError

    w = int((tx1 - tx0) / scale_deg) + 1
    h = int((ty1 - ty0) / scale_deg) + 1
    est_bytes = w * h * 4  # float32
    print(f'    [tile] downloading {w}×{h} = {w*h:,} px ({est_bytes/1e6:.1f} MB)')

    if est_bytes > MAX_RESPONSE_BYTES:
        raise GEEError(
            f'Tile too large: {w}×{h} = {est_bytes/1e6:.1f} MB > '
            f'{MAX_RESPONSE_BYTES/1e6:.1f} MB limit. '
            f'This should not happen with tiling enabled.'
        )

    params = {
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
    }

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_compute_pixels, params)
            return future.result(timeout=timeout)
    except TimeoutError:
        raise GEEError(
            f'computePixels timeout ({timeout}s) for tile {w}×{h}'
        )
    except Exception as e:
        raise GEEError(f'computePixels failed: {e}')


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
        content = download_tile(composite, tx0, ty0, tx1, ty1, scale_deg)
        with open(out_path, 'wb') as f:
            f.write(content)
    else:
        logger.info('%s: downloading %d tiles…', sensor_label, len(tiles))
        tile_paths = []
        base = out_path.replace('.tif', '')

        for ti, (tx0, ty0, tx1, ty1) in enumerate(tiles):
            tile_path = f'{base}_tile{ti}.tif'
            content = download_tile(composite, tx0, ty0, tx1, ty1, scale_deg)
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
