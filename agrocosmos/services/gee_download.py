"""
Shared GEE raster download utilities with tiling and timeouts.

Uses computePixels() with:
- Auto-tiling to fit 48 MB response limit (~12M pixels at float32)
- concurrent.futures timeout to prevent indefinite hangs
- Optional per-composite tile-level parallelism (env ``GEE_TILE_CONCURRENCY``)

MAX_TILE_PX = 2000 → each tile ≈ 2000×2000 = 4M pixels ≈ 16 MB (< 48 MB).
"""
import glob
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed


logger = logging.getLogger(__name__)

MAX_TILE_PX = 2000    # 2000×2000 = 4M pixels × 4 = 16 MB (limit is 48 MB)
DOWNLOAD_TIMEOUT = 300  # seconds per tile
MAX_RESPONSE_BYTES = 50_331_648  # GEE computePixels hard limit

# Number of tiles to download in parallel from a single composite window.
# GEE tolerates ~5-10 concurrent computePixels calls per project before it
# starts throwing RESOURCE_EXHAUSTED; 6 is a safe default. Retries on 429
# are handled inside services.gee_client.call_compute_pixels.
TILE_CONCURRENCY = max(1, int(os.environ.get('GEE_TILE_CONCURRENCY', '6')))

# A valid single-band GeoTIFF cannot be smaller than this (header alone is
# ~1 KB). GEE occasionally returns an empty/truncated body without raising;
# writing it to disk would leave a corrupt tile that breaks the merge.
MIN_TILE_BYTES = 512


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
    """Wrapper for ee.data.computePixels (for use in thread pool).

    Routed through ``services.gee_client.call_compute_pixels`` so that
    rate-limiting, retries on 429/quota errors, and daily metrics all
    happen transparently.
    """
    from .gee_client import call_compute_pixels
    return call_compute_pixels(params)


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
            content = future.result(timeout=timeout)
    except TimeoutError:
        raise GEEError(
            f'computePixels timeout ({timeout}s) for tile {w}×{h}'
        )
    except Exception as e:
        raise GEEError(f'computePixels failed: {e}')

    # GEE sometimes returns an empty/truncated body with HTTP 200 — treat
    # it as a failure here so a zero-size tile never reaches the disk.
    if not content or len(content) < MIN_TILE_BYTES:
        raise GEEError(
            f'computePixels returned {len(content) if content else 0} bytes '
            f'for tile {w}×{h} — empty/truncated response'
        )
    return content


def _atomic_write(path, content):
    """Write bytes to ``path`` atomically (tmp file + os.replace).

    If the process is killed mid-write, only the ``.part`` file is left
    behind — never a truncated file under the final name.
    """
    tmp = path + '.part'
    with open(tmp, 'wb') as f:
        f.write(content)
    os.replace(tmp, path)


def merge_tiles(tile_paths, out_path):
    """Merge multiple GeoTIFF tiles into one LZW-compressed file.

    Tiles are opened INSIDE the try block: if any tile is corrupt
    (e.g. zero-size leftover from a killed process), the finally-cleanup
    still removes every tile instead of leaving debris on disk.
    The mosaic is written to a temp name and moved into place atomically,
    so ``out_path`` either exists complete or not at all.
    """
    import rasterio
    from rasterio.merge import merge as rasterio_merge

    tmp_out = out_path + '.part'
    datasets = []
    try:
        for p in tile_paths:
            datasets.append(rasterio.open(p))
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
        # Region-wide mosaics (e.g. Moscow oblast S2 = 56297×30088 px,
        # ~6.8 GB raw) can exceed the classic TIFF 4 GB limit even with
        # LZW when the composite has little nodata — GDAL then aborts
        # mid-write with "Write failed". IF_SAFER switches to BigTIFF
        # only when the estimated size demands it.
        with rasterio.open(tmp_out, 'w', BIGTIFF='IF_SAFER', **profile) as dst:
            dst.write(mosaic)
        os.replace(tmp_out, out_path)
    finally:
        for ds in datasets:
            try:
                ds.close()
            except Exception:
                pass
        for p in list(tile_paths) + [tmp_out]:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


def _write_single_tile(composite, tile, scale_deg, out_path):
    """Download the only tile straight into ``out_path`` (atomically)."""
    tx0, ty0, tx1, ty1 = tile
    content = download_tile(composite, tx0, ty0, tx1, ty1, scale_deg)
    _atomic_write(out_path, content)


def _remove_partials(tile_paths):
    """Remove any partial tiles to keep the data dir clean."""
    for p in tile_paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


def _cleanup_stale_tiles(base):
    """Delete leftover ``<base>_tile*.tif`` / ``*.part`` from crashed runs."""
    stale = glob.glob(f'{base}_tile*.tif') + glob.glob(f'{base}*.part')
    for p in stale:
        try:
            os.remove(p)
            logger.info('Removed stale tile: %s', p)
        except OSError:
            pass


def _download_tiles_parallel(composite, tiles, scale_deg, base,
                             sensor_label) -> list[str]:
    """Download all tiles concurrently; fail fast on the first error."""
    conc = min(TILE_CONCURRENCY, len(tiles))
    msg = (f'{sensor_label}: downloading {len(tiles)} tiles '
           f'(concurrency={conc})…')
    logger.info(msg)
    print(f'    [tile] {msg}')

    tile_paths: list[str | None] = [None] * len(tiles)

    def _download_one(idx: int, bbox: tuple) -> tuple[int, str, int]:
        tx0, ty0, tx1, ty1 = bbox
        path = f'{base}_tile{idx}.tif'
        content = download_tile(composite, tx0, ty0, tx1, ty1, scale_deg)
        _atomic_write(path, content)
        return idx, path, len(content)

    with ThreadPoolExecutor(max_workers=conc) as pool:
        futures = {
            pool.submit(_download_one, ti, bbox): ti
            for ti, bbox in enumerate(tiles)
        }
        first_error: Exception | None = None
        done = 0
        for fut in as_completed(futures):
            try:
                ti, path, size = fut.result()
            except Exception as exc:
                # Fail fast: cancel the rest so we don't pile up
                # partial tile files and network traffic.
                if first_error is None:
                    first_error = exc
                for other in futures:
                    other.cancel()
                continue
            tile_paths[ti] = path
            done += 1
            logger.info('  Tile %d/%d OK (%.1f MB)',
                        done, len(tiles), size / 1e6)

        if first_error is not None:
            _remove_partials(tile_paths)
            raise first_error

    return [p for p in tile_paths if p]


def _log_downloaded(out_path, sensor_label, n_images, n_tiles):
    """Log final raster dimensions and size."""
    import rasterio
    with rasterio.open(out_path) as ds:
        logger.info(
            '%s: %s (%d×%d, %.1f MB, %d src images, %d tiles)',
            sensor_label, out_path, ds.width, ds.height,
            os.path.getsize(out_path) / 1e6, n_images, n_tiles,
        )


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

    xmin, ymin, xmax, ymax = extent
    scale_deg = scale_m / 111320

    base = out_path.replace('.tif', '')
    # Remove stale tiles / partials from previous crashed runs — they may
    # be zero-size or belong to a different tile grid and would poison
    # the merge below.
    _cleanup_stale_tiles(base)

    tiles = tile_extents(xmin, ymin, xmax, ymax, scale_deg)

    if len(tiles) == 1:
        _write_single_tile(composite, tiles[0], scale_deg, out_path)
    else:
        tile_paths = _download_tiles_parallel(
            composite, tiles, scale_deg, base, sensor_label,
        )
        merge_tiles(tile_paths, out_path)

    _log_downloaded(out_path, sensor_label, n_images, len(tiles))
    return out_path
