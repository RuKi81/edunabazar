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
            rb = ds.bounds
            if (tile_xmax <= rb.left or tile_xmin >= rb.right or
                    tile_ymax <= rb.bottom or tile_ymin >= rb.top):
                return None

            # Clip tile bounds to raster bounds (intersection)
            ix_min = max(tile_xmin, rb.left)
            iy_min = max(tile_ymin, rb.bottom)
            ix_max = min(tile_xmax, rb.right)
            iy_max = min(tile_ymax, rb.top)

            # Read only the intersecting window from the raster
            window = from_bounds(ix_min, iy_min, ix_max, iy_max, transform=ds.transform)

            # Determine how many pixels this intersection occupies in the 256px tile
            tile_w = tile_xmax - tile_xmin
            tile_h = tile_ymax - tile_ymin
            px_left = int(round((ix_min - tile_xmin) / tile_w * TILE_SIZE))
            px_top = int(round((tile_ymax - iy_max) / tile_h * TILE_SIZE))
            px_right = int(round((ix_max - tile_xmin) / tile_w * TILE_SIZE))
            px_bottom = int(round((tile_ymax - iy_min) / tile_h * TILE_SIZE))

            read_w = max(px_right - px_left, 1)
            read_h = max(px_bottom - px_top, 1)

            data = ds.read(
                1, window=window,
                out_shape=(read_h, read_w),
                resampling=rasterio.enums.Resampling.bilinear,
                boundless=True,
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

    # Apply LUT to the read portion
    rgba_part = _LUT[indices]  # shape: (read_h, read_w, 4)

    # Place into full transparent 256×256 tile
    tile_rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    tile_rgba[px_top:px_top + read_h, px_left:px_left + read_w] = rgba_part

    img = Image.fromarray(tile_rgba, 'RGBA')
    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


def _ndvi_rgba(data, nodata):
    """Pseudocolor RGBA array from an NDVI window; None when no valid data."""
    if nodata is not None and not np.isnan(nodata):
        valid = data != nodata
    else:
        valid = ~np.isnan(data)
    if not valid.any():
        return None
    indices = (np.clip(data, 0, 1) * 255).astype(np.uint8)
    indices[~valid] = 0
    rgba = _LUT[indices].copy()
    rgba[..., 3][valid & (indices > 0)] = 255  # thumbnails are opaque
    return rgba


def _draw_outline(img: 'Image.Image', outline: list, bbox: tuple) -> None:
    """Draw polygon rings ([[lon, lat], ...]) on top of a preview image."""
    from PIL import ImageDraw

    xmin, ymin, xmax, ymax = bbox
    out_w, out_h = img.size
    span_x = xmax - xmin
    span_y = ymax - ymin
    draw = ImageDraw.Draw(img)
    for ring in outline:
        pts = [
            ((lon - xmin) / span_x * (out_w - 1),
             (ymax - lat) / span_y * (out_h - 1))
            for lon, lat in ring
        ]
        if len(pts) >= 2:
            draw.line(pts + [pts[0]], fill=(27, 94, 32, 255), width=2)


def render_preview(tif_path: str, bbox: tuple, max_size: int = 256,
                   outline: list | None = None) -> bytes | None:
    """
    Render a small pseudocolor NDVI preview clipped to a WGS84 bbox.

    Used by the farmland passport to show the last composites as thumbnails.

    Args:
        tif_path: GeoTIFF composite path
        bbox: (xmin, ymin, xmax, ymax) in EPSG:4326
        max_size: longest output side, px
        outline: optional list of rings ([[lon, lat], ...]) drawn on top

    Returns:
        PNG bytes, or None when bbox is outside the raster / has no data.
    """
    import rasterio
    from rasterio.windows import from_bounds

    if not tif_path or not os.path.exists(tif_path):
        return None
    xmin, ymin, xmax, ymax = bbox
    if xmax <= xmin or ymax <= ymin:
        return None

    # Output aspect with longitude compression at mid-latitude.
    k = math.cos(math.radians((ymin + ymax) / 2.0))
    w_deg = (xmax - xmin) * k
    h_deg = ymax - ymin
    if w_deg >= h_deg:
        out_w = max_size
        out_h = max(int(round(max_size * h_deg / w_deg)), 1)
    else:
        out_h = max_size
        out_w = max(int(round(max_size * w_deg / h_deg)), 1)

    try:
        with rasterio.open(tif_path) as ds:
            rb = ds.bounds
            if (xmax <= rb.left or xmin >= rb.right or
                    ymax <= rb.bottom or ymin >= rb.top):
                return None
            window = from_bounds(xmin, ymin, xmax, ymax, transform=ds.transform)
            data = ds.read(
                1, window=window,
                out_shape=(out_h, out_w),
                resampling=rasterio.enums.Resampling.bilinear,
                boundless=True,
            )
            nodata = ds.nodata
    except Exception as e:
        logger.warning('Raster preview error %s: %s', tif_path, e)
        return None

    rgba = _ndvi_rgba(data, nodata)
    if rgba is None:
        return None

    img = Image.fromarray(rgba, 'RGBA')
    if outline:
        _draw_outline(img, outline, bbox)

    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


# Zone classification vs field median: <75% — problem, 75–90% — warning.
ZONE_PROBLEM_RATIO = 0.75
ZONE_WARN_RATIO = 0.9
# Zone dynamics: |Δ(NDVI/median)| >= 0.10 — significant change.
ZONE_DYNAMICS_DELTA = 0.10
_ZONE_RGBA = {
    'problem': (211, 47, 47, 235),
    'warn': (249, 168, 37, 220),
    'ok': (76, 175, 80, 150),
}
_DYN_RGBA = {
    'degraded': (211, 47, 47, 235),
    'improved': (25, 118, 210, 220),
    'stable': (158, 158, 158, 110),
}


def _preview_dims(bbox: tuple, max_size: int) -> tuple[int, int]:
    """(out_w, out_h) for a WGS84 bbox with mid-latitude lon compression."""
    xmin, ymin, xmax, ymax = bbox
    k = math.cos(math.radians((ymin + ymax) / 2.0))
    w_deg = (xmax - xmin) * k
    h_deg = ymax - ymin
    if w_deg >= h_deg:
        return max_size, max(int(round(max_size * h_deg / w_deg)), 1)
    return max(int(round(max_size * w_deg / h_deg)), 1), max_size


def _polygon_mask(outline: list, bbox: tuple, out_w: int, out_h: int):
    """Boolean array (out_h, out_w): True inside the field polygon rings."""
    from PIL import ImageDraw

    xmin, ymin, xmax, ymax = bbox
    span_x = xmax - xmin
    span_y = ymax - ymin
    mask_img = Image.new('L', (out_w, out_h), 0)
    draw = ImageDraw.Draw(mask_img)
    for ring in outline:
        pts = [
            ((lon - xmin) / span_x * (out_w - 1),
             (ymax - lat) / span_y * (out_h - 1))
            for lon, lat in ring
        ]
        if len(pts) >= 3:
            draw.polygon(pts, fill=255)
    return np.asarray(mask_img) > 0


def _read_field_ndvi(tif_path: str, bbox: tuple, out_w: int, out_h: int):
    """Read an NDVI window resampled to (out_h, out_w); (data, valid) or None."""
    import rasterio
    from rasterio.windows import from_bounds

    xmin, ymin, xmax, ymax = bbox
    try:
        with rasterio.open(tif_path) as ds:
            rb = ds.bounds
            if (xmax <= rb.left or xmin >= rb.right or
                    ymax <= rb.bottom or ymin >= rb.top):
                return None
            window = from_bounds(xmin, ymin, xmax, ymax, transform=ds.transform)
            data = ds.read(
                1, window=window,
                out_shape=(out_h, out_w),
                resampling=rasterio.enums.Resampling.bilinear,
                boundless=True,
            )
            nodata = ds.nodata
    except Exception as e:
        logger.warning('Raster zones error %s: %s', tif_path, e)
        return None

    if nodata is not None and not np.isnan(nodata):
        valid = data != nodata
    else:
        valid = ~np.isnan(data)
    return data, valid


def _save_png(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


def render_zones(tif_path: str, bbox: tuple, outline: list,
                 max_size: int = 384) -> tuple[bytes | None, dict | None]:
    """
    Render a within-field heterogeneity zone map from an NDVI composite.

    Pixels inside the field polygon are classified relative to the field
    median NDVI: <75% — problem (red), 75–90% — warning (yellow),
    >=90% — ok (green). Pixels outside the polygon stay transparent.

    Used by the farmland passport «Зоны неоднородности» section.

    Args:
        tif_path: GeoTIFF composite path
        bbox: (xmin, ymin, xmax, ymax) in EPSG:4326
        outline: field polygon rings ([[lon, lat], ...])
        max_size: longest output side, px

    Returns:
        (png_bytes, stats) or (None, None) when there is no usable data.
        stats: {'median', 'problem_pct', 'warn_pct', 'ok_pct', 'pixels'}
    """
    if not tif_path or not os.path.exists(tif_path) or not outline:
        return None, None
    xmin, ymin, xmax, ymax = bbox
    if xmax <= xmin or ymax <= ymin:
        return None, None

    out_w, out_h = _preview_dims(bbox, max_size)

    read = _read_field_ndvi(tif_path, bbox, out_w, out_h)
    if read is None:
        return None, None
    data, valid = read

    field = valid & _polygon_mask(outline, bbox, out_w, out_h)
    n_field = int(field.sum())
    if n_field < 4:
        return None, None

    median = float(np.median(data[field]))
    if median <= 0:
        return None, None

    ratio = np.zeros_like(data, dtype='float32')
    ratio[field] = data[field] / median
    problem = field & (ratio < ZONE_PROBLEM_RATIO)
    warn = field & (ratio >= ZONE_PROBLEM_RATIO) & (ratio < ZONE_WARN_RATIO)
    ok = field & (ratio >= ZONE_WARN_RATIO)

    rgba = np.zeros((out_h, out_w, 4), dtype=np.uint8)
    rgba[problem] = _ZONE_RGBA['problem']
    rgba[warn] = _ZONE_RGBA['warn']
    rgba[ok] = _ZONE_RGBA['ok']

    img = Image.fromarray(rgba, 'RGBA')
    _draw_outline(img, outline, bbox)

    stats = {
        'median': round(median, 3),
        'problem_pct': round(int(problem.sum()) / n_field * 100, 1),
        'warn_pct': round(int(warn.sum()) / n_field * 100, 1),
        'ok_pct': round(int(ok.sum()) / n_field * 100, 1),
        'pixels': n_field,
    }
    return _save_png(img), stats


def render_zone_dynamics(tif_now: str, tif_prev: str, bbox: tuple,
                         outline: list, max_size: int = 384,
                         ) -> tuple[bytes | None, dict | None]:
    """
    Render a zone-change map between two NDVI composites of the same field.

    Each composite is normalised by its own field median (removes seasonal
    trend), then per-pixel Δ = ratio_now − ratio_prev is classified:
    Δ <= −0.10 — degraded (red), Δ >= +0.10 — improved (blue),
    otherwise — stable (grey). Outside-field pixels stay transparent.

    Used by the farmland passport «Динамика зон» sub-section.

    Args:
        tif_now: newer GeoTIFF composite path
        tif_prev: older GeoTIFF composite path
        bbox: (xmin, ymin, xmax, ymax) in EPSG:4326
        outline: field polygon rings ([[lon, lat], ...])
        max_size: longest output side, px

    Returns:
        (png_bytes, stats) or (None, None) when data is unusable.
        stats: {'degraded_pct', 'improved_pct', 'stable_pct', 'pixels'}
    """
    if (not tif_now or not tif_prev or not os.path.exists(tif_now)
            or not os.path.exists(tif_prev) or not outline):
        return None, None
    xmin, ymin, xmax, ymax = bbox
    if xmax <= xmin or ymax <= ymin:
        return None, None

    out_w, out_h = _preview_dims(bbox, max_size)

    read_now = _read_field_ndvi(tif_now, bbox, out_w, out_h)
    read_prev = _read_field_ndvi(tif_prev, bbox, out_w, out_h)
    if read_now is None or read_prev is None:
        return None, None
    data_now, valid_now = read_now
    data_prev, valid_prev = read_prev

    mask = _polygon_mask(outline, bbox, out_w, out_h)
    field = valid_now & valid_prev & mask
    n_field = int(field.sum())
    if n_field < 4:
        return None, None

    median_now = float(np.median(data_now[field]))
    median_prev = float(np.median(data_prev[field]))
    if median_now <= 0 or median_prev <= 0:
        return None, None

    delta = np.zeros_like(data_now, dtype='float32')
    delta[field] = (data_now[field] / median_now
                    - data_prev[field] / median_prev)
    degraded = field & (delta <= -ZONE_DYNAMICS_DELTA)
    improved = field & (delta >= ZONE_DYNAMICS_DELTA)
    stable = field & ~degraded & ~improved

    rgba = np.zeros((out_h, out_w, 4), dtype=np.uint8)
    rgba[degraded] = _DYN_RGBA['degraded']
    rgba[improved] = _DYN_RGBA['improved']
    rgba[stable] = _DYN_RGBA['stable']

    img = Image.fromarray(rgba, 'RGBA')
    _draw_outline(img, outline, bbox)

    stats = {
        'degraded_pct': round(int(degraded.sum()) / n_field * 100, 1),
        'improved_pct': round(int(improved.sum()) / n_field * 100, 1),
        'stable_pct': round(int(stable.sum()) / n_field * 100, 1),
        'pixels': n_field,
    }
    return _save_png(img), stats


def zones_to_features(tif_path: str, bbox: tuple, outline: list,
                      max_size: int = 384, min_area_ha: float = 0.2,
                      ) -> list[dict] | None:
    """
    Vectorize within-field heterogeneity zones into polygon features.

    Same classification as :func:`render_zones` (vs field median NDVI),
    but the class grid is polygonized for export to UAV mission planners
    (KML for DJI Pilot/Fly, prescription maps).

    Args:
        tif_path: GeoTIFF composite path
        bbox: (xmin, ymin, xmax, ymax) in EPSG:4326
        outline: field polygon rings ([[lon, lat], ...])
        max_size: raster grid resolution used for polygonization
        min_area_ha: drop polygons smaller than this, ha

    Returns:
        List of {'zone': 'problem'|'warn'|'ok', 'area_ha': float,
        'geometry': GeoJSON polygon dict} sorted by zone severity then
        area desc, or None when there is no usable data.
    """
    import rasterio
    from rasterio import features as rio_features
    from rasterio.transform import from_bounds as tf_from_bounds

    if not tif_path or not os.path.exists(tif_path) or not outline:
        return None
    xmin, ymin, xmax, ymax = bbox
    if xmax <= xmin or ymax <= ymin:
        return None

    out_w, out_h = _preview_dims(bbox, max_size)

    read = _read_field_ndvi(tif_path, bbox, out_w, out_h)
    if read is None:
        return None
    data, valid = read

    field = valid & _polygon_mask(outline, bbox, out_w, out_h)
    n_field = int(field.sum())
    if n_field < 4:
        return None

    median = float(np.median(data[field]))
    if median <= 0:
        return None

    ratio = np.zeros_like(data, dtype='float32')
    ratio[field] = data[field] / median
    cls = np.zeros((out_h, out_w), dtype=np.uint8)
    cls[field & (ratio < ZONE_PROBLEM_RATIO)] = 1
    cls[field & (ratio >= ZONE_PROBLEM_RATIO) & (ratio < ZONE_WARN_RATIO)] = 2
    cls[field & (ratio >= ZONE_WARN_RATIO)] = 3

    transform = tf_from_bounds(xmin, ymin, xmax, ymax, out_w, out_h)
    # Приблизительный пересчёт deg² → га на средней широте поля.
    lat_mid = (ymin + ymax) / 2.0
    deg2_to_ha = 111_320.0 * 111_320.0 * math.cos(math.radians(lat_mid)) / 10_000.0
    zone_names = {1: 'problem', 2: 'warn', 3: 'ok'}
    # Упрощение контуров ~1 пиксель, чтобы KML не разбухал.
    tol = (xmax - xmin) / out_w

    feats = []
    try:
        with rasterio.Env():
            shapes_iter = rio_features.shapes(cls, mask=cls > 0,
                                              transform=transform)
            for geom, val in shapes_iter:
                area_ha = _geojson_polygon_area_deg2(geom) * deg2_to_ha
                if area_ha < min_area_ha:
                    continue
                feats.append({
                    'zone': zone_names[int(val)],
                    'area_ha': round(area_ha, 2),
                    'geometry': _simplify_geojson_polygon(geom, tol),
                })
    except Exception as e:
        logger.warning('Zone vectorize error %s: %s', tif_path, e)
        return None

    order = {'problem': 0, 'warn': 1, 'ok': 2}
    feats.sort(key=lambda f: (order[f['zone']], -f['area_ha']))
    return feats


def _geojson_polygon_area_deg2(geom: dict) -> float:
    """Shoelace area of a GeoJSON polygon (outer minus holes), deg²."""
    def ring_area(ring):
        s = 0.0
        for (x1, y1), (x2, y2) in zip(ring, ring[1:]):
            s += x1 * y2 - x2 * y1
        return abs(s) / 2.0

    rings = geom.get('coordinates', [])
    if not rings:
        return 0.0
    area = ring_area(rings[0])
    for hole in rings[1:]:
        area -= ring_area(hole)
    return max(area, 0.0)


# WGS84 .prj for shapefile exports (ESRI WKT).
_WGS84_PRJ = (
    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'
)


def zones_to_agras_shp_zip(feats: list, rates: dict, name: str) -> bytes:
    """
    Pack zone polygons into a zipped shapefile prescription map (DJI Agras VRA).

    Each polygon carries ZONE (problem/warn/ok), RATE (application rate,
    L/ha or kg/ha as entered by the agronomist) and AREA_HA attributes.
    CRS is EPSG:4326 (WGS84), which the DJI Agras RC expects on import
    («Map Source: Other», source unit — ha).

    Args:
        feats: output of :func:`zones_to_features`
        rates: {'problem': float, 'warn': float, 'ok': float}
        name: base file name inside the archive (no extension)

    Returns:
        ZIP bytes with name.shp/.shx/.dbf/.prj.
    """
    import zipfile

    import shapefile as pyshp

    shp_buf, shx_buf, dbf_buf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    with pyshp.Writer(shp=shp_buf, shx=shx_buf, dbf=dbf_buf,
                      shapeType=pyshp.POLYGON) as w:
        w.field('ZONE', 'C', size=10)
        w.field('RATE', 'N', decimal=2)
        w.field('AREA_HA', 'N', decimal=2)
        for f in feats:
            rings = [_orient_ring(r, clockwise=(i == 0))
                     for i, r in enumerate(f['geometry']['coordinates'])]
            w.poly(rings)
            w.record(f['zone'], float(rates.get(f['zone'], 0.0)),
                     f['area_ha'])

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr(f'{name}.shp', shp_buf.getvalue())
        z.writestr(f'{name}.shx', shx_buf.getvalue())
        z.writestr(f'{name}.dbf', dbf_buf.getvalue())
        z.writestr(f'{name}.prj', _WGS84_PRJ)
    return zip_buf.getvalue()


def _orient_ring(ring: list, clockwise: bool) -> list:
    """Shapefile ring order: outer — clockwise, holes — counter-clockwise."""
    s = 0.0
    for (x1, y1), (x2, y2) in zip(ring, ring[1:]):
        s += (x2 - x1) * (y2 + y1)
    is_cw = s > 0
    return list(ring) if is_cw == clockwise else list(reversed(ring))


# KML-цвета aabbggrr — в тон легенде паспорта (#d32f2f / #f9a825).
_KML_ZONE_STYLES = {
    'problem': ('Проблемная зона', 'b32f2fd3', 'ff2f2fd3'),
    'warn': ('Ниже нормы', '8025a8f9', 'ff25a8f9'),
}


def zones_to_kml_document(feats: list, field_id: int,
                          date_from: str, date_to: str) -> str:
    """KML: полигоны зон problem/warn + центроиды проблемных (waypoints).

    Экспорт зон неоднородности для ПО БПЛА DJI (Pilot 2 / Fly).
    Полигоны проблемных зон и зон «ниже нормы» + точки-центроиды
    проблемных зон как ориентиры точечного облёта (scouting).
    ``feats`` — результат :func:`zones_to_features`.
    """
    def coords(ring):
        return ' '.join(f'{x:.6f},{y:.6f},0' for x, y in ring)

    def centroid(ring):
        n = max(len(ring) - 1, 1)
        return (sum(p[0] for p in ring[:n]) / n,
                sum(p[1] for p in ring[:n]) / n)

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>',
        f'<name>Зоны поля {field_id} · {date_from} – {date_to}</name>',
    ]
    for key, (_, poly_color, line_color) in _KML_ZONE_STYLES.items():
        parts.append(
            f'<Style id="{key}"><LineStyle><color>{line_color}</color>'
            '<width>2</width></LineStyle>'
            f'<PolyStyle><color>{poly_color}</color></PolyStyle></Style>'
        )
    parts.append(
        '<Style id="wp"><IconStyle><color>ff2f2fd3</color></IconStyle></Style>'
    )

    counters = {'problem': 0, 'warn': 0}
    waypoints = []
    for f in feats:
        zone = f['zone']
        if zone not in _KML_ZONE_STYLES:
            continue  # «норма» для облёта не нужна
        counters[zone] += 1
        label, _, _ = _KML_ZONE_STYLES[zone]
        name = f"{label} {counters[zone]} ({f['area_ha']} га)"
        rings = f['geometry']['coordinates']
        boundaries = [
            f'<outerBoundaryIs><LinearRing><coordinates>{coords(rings[0])}'
            '</coordinates></LinearRing></outerBoundaryIs>'
        ]
        for hole in rings[1:]:
            boundaries.append(
                f'<innerBoundaryIs><LinearRing><coordinates>{coords(hole)}'
                '</coordinates></LinearRing></innerBoundaryIs>'
            )
        parts.append(
            f'<Placemark><name>{name}</name><styleUrl>#{zone}</styleUrl>'
            f"<Polygon>{''.join(boundaries)}</Polygon></Placemark>"
        )
        if zone == 'problem':
            cx, cy = centroid(rings[0])
            waypoints.append(
                f'<Placemark><name>Точка осмотра {counters[zone]}</name>'
                '<styleUrl>#wp</styleUrl>'
                f'<Point><coordinates>{cx:.6f},{cy:.6f},0</coordinates></Point>'
                '</Placemark>'
            )
    if waypoints:
        parts.append('<Folder><name>Точки осмотра</name>'
                     + ''.join(waypoints) + '</Folder>')
    parts.append('</Document></kml>')
    return '\n'.join(parts)


def _simplify_geojson_polygon(geom: dict, tol: float) -> dict:
    """Douglas–Peucker simplification via GEOS; falls back to original."""
    try:
        import json

        from django.contrib.gis.geos import GEOSGeometry

        g = GEOSGeometry(json.dumps(geom)).simplify(tol, preserve_topology=True)
        if not g.empty and g.geom_type == 'Polygon':
            return json.loads(g.geojson)
    except Exception:  # pragma: no cover
        pass
    return geom


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
