"""
NDVI raster tile renderer — serves GeoTIFF composites as pseudocolor PNG tiles.

Used by the raster dashboard to display S2/L8 NDVI overlays on Leaflet maps.
Reads from /data/s2/ and /data/landsat/ directories.

Tile URL pattern: /agrocosmos/api/raster-tile/{z}/{x}/{y}.png?sensor=s2&scope=d1&date=2025-06-05_2025-06-09

Pseudocolor palette:
    NDVI < 0.0  → transparent (water/bare)
    0.0 – 0.1   → #a50026  (bare soil / dead vegetation)
    0.1 – 0.2   → #d73027
    0.2 – 0.3   → #f46d43
    0.3 – 0.4   → #fdae61
    0.4 – 0.5   → #fee08b
    0.5 – 0.6   → #d9ef8b
    0.6 – 0.7   → #a6d96a
    0.7 – 0.8   → #66bd63
    0.8 – 0.9   → #1a9850
    0.9 – 1.0   → #006837  (dense vegetation)
"""
import io
import logging
import math
import os
from pathlib import Path

import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

# NDVI → RGBA pseudocolor palette (RdYlGn diverging)
_PALETTE = [
    # (min_ndvi, max_ndvi, R, G, B)
    (0.0, 0.1, 165, 0, 38),
    (0.1, 0.2, 215, 48, 39),
    (0.2, 0.3, 244, 109, 67),
    (0.3, 0.4, 253, 174, 97),
    (0.4, 0.5, 254, 224, 139),
    (0.5, 0.6, 217, 239, 139),
    (0.6, 0.7, 166, 217, 106),
    (0.7, 0.8, 102, 189, 99),
    (0.8, 0.9, 26, 152, 80),
    (0.9, 1.0, 0, 104, 55),
]

# Pre-build lookup table (LUT) for speed: 256 steps for NDVI 0..1
_LUT = np.zeros((256, 4), dtype=np.uint8)
for i in range(256):
    ndvi = i / 255.0
    r, g, b, a = 0, 0, 0, 0
    for lo, hi, pr, pg, pb in _PALETTE:
        if lo <= ndvi < hi:
            r, g, b, a = pr, pg, pb, 180
            break
    if ndvi >= 0.9:
        r, g, b, a = 0, 104, 55, 180
    _LUT[i] = [r, g, b, a]
# ndvi < 0 → transparent
_LUT[0] = [0, 0, 0, 0]

TILE_SIZE = 256


def _tile_bounds(z, x, y):
    """Convert XYZ tile coords to WGS84 bounding box (xmin, ymin, xmax, ymax)."""
    n = 2 ** z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon_min, lat_min, lon_max, lat_max


def render_tile(tif_path: str, z: int, x: int, y: int) -> bytes | None:
    """
    Render a 256×256 PNG tile from a GeoTIFF NDVI raster in pseudocolor.

    Returns PNG bytes or None if tile is outside raster extent or no valid data.
    """
    import rasterio
    from rasterio.windows import from_bounds

    if not tif_path or not os.path.exists(tif_path):
        return None

    tile_xmin, tile_ymin, tile_xmax, tile_ymax = _tile_bounds(z, x, y)

    try:
        with rasterio.open(tif_path) as ds:
            # Check if tile intersects raster
            r_bounds = ds.bounds
            if (tile_xmax <= r_bounds.left or tile_xmin >= r_bounds.right or
                    tile_ymax <= r_bounds.bottom or tile_ymin >= r_bounds.top):
                return None

            # Read the raster window that covers the tile
            window = from_bounds(
                tile_xmin, tile_ymin, tile_xmax, tile_ymax,
                transform=ds.transform,
            )

            # Read data at native resolution, then resize
            data = ds.read(
                1, window=window,
                out_shape=(TILE_SIZE, TILE_SIZE),
                resampling=rasterio.enums.Resampling.bilinear,
            )
            nodata = ds.nodata

    except Exception as e:
        logger.warning('Raster tile error %s z=%d x=%d y=%d: %s', tif_path, z, x, y, e)
        return None

    # Build valid mask
    if nodata is not None and not np.isnan(nodata):
        valid = data != nodata
    else:
        valid = ~np.isnan(data)

    if not valid.any():
        return None

    # Clip NDVI to 0..1, map to 0..255 index
    clipped = np.clip(data, 0, 1)
    indices = (clipped * 255).astype(np.uint8)
    indices[~valid] = 0  # transparent

    # Apply LUT
    rgba = _LUT[indices]  # shape: (256, 256, 4)

    img = Image.fromarray(rgba, 'RGBA')
    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


def find_raster_path(sensor: str, scope_id: str, date_range: str) -> str | None:
    """
    Find a raster file path given sensor, scope (region/district ID), and date range.

    Args:
        sensor: 's2' or 'l8'
        scope_id: e.g. 'd1' or '37'
        date_range: 'YYYY-MM-DD_YYYY-MM-DD'

    Returns:
        str path or None
    """
    from django.conf import settings

    if sensor == 's2':
        base = os.environ.get('S2_RASTER_DIR', getattr(settings, 'S2_RASTER_DIR', '/data/s2'))
        prefix = 's2_ndvi'
    elif sensor in ('l8', 'landsat'):
        base = os.environ.get('LANDSAT_RASTER_DIR', getattr(settings, 'LANDSAT_RASTER_DIR', '/data/landsat'))
        prefix = 'landsat_ndvi'
    else:
        return None

    parts = date_range.split('_')
    if len(parts) != 2:
        return None

    date_from, date_to = parts
    year = date_from[:4]

    fname = f'{prefix}_{scope_id}_{date_from}_{date_to}.tif'
    path = os.path.join(base, scope_id, year, fname)

    if os.path.exists(path):
        return path
    return None


def list_available_composites(sensor: str, scope_id: str, year: str) -> list[dict]:
    """
    List available raster composites for a sensor/scope/year.

    Returns list of {'date_from': '...', 'date_to': '...', 'size_mb': float}
    """
    from django.conf import settings

    if sensor == 's2':
        base = os.environ.get('S2_RASTER_DIR', getattr(settings, 'S2_RASTER_DIR', '/data/s2'))
        prefix = 's2_ndvi'
    elif sensor in ('l8', 'landsat'):
        base = os.environ.get('LANDSAT_RASTER_DIR', getattr(settings, 'LANDSAT_RASTER_DIR', '/data/landsat'))
        prefix = 'landsat_ndvi'
    else:
        return []

    directory = Path(base) / scope_id / year
    if not directory.exists():
        return []

    results = []
    pattern = f'{prefix}_{scope_id}_*.tif'
    for f in sorted(directory.glob(pattern)):
        # Parse dates from filename: prefix_scopeId_YYYY-MM-DD_YYYY-MM-DD.tif
        stem = f.stem
        parts = stem.split('_')
        # Last two parts before .tif are date_to and the one before is date_from
        if len(parts) >= 4:
            date_to = parts[-1]
            date_from = parts[-2]
            results.append({
                'date_from': date_from,
                'date_to': date_to,
                'size_mb': round(f.stat().st_size / 1e6, 1),
            })

    return results
