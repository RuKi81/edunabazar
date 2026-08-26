"""Рендер XYZ-тайлов (PNG) из Cloud-Optimized GeoTIFF (Фаза 4).

Открывает COG прямо из объектного хранилища по ``/vsis3/`` (range-запросы,
GDAL), перепроецирует в Web Mercator (EPSG:3857) через
:class:`rasterio.vrt.WarpedVRT` и вырезает тайл 256×256 под запрошенные
``z/x/y``. Поддерживает два режима отрисовки (``RasterLayer.style``):

* ``{'mode': 'singleband', 'colormap': 'ndvi'|'gray'|'viridis', 'min', 'max'}``
  — раскраска одного канала по палитре с авто-контрастом (min/max из стиля,
  иначе перцентили p2/p98 из ``layer.stats``);
* ``{'mode': 'rgb', 'bands': [r, g, b]}`` — «истинный цвет» из трёх каналов,
  каждый нормируется по своему p2/p98.

Стиль по умолчанию: ≥3 каналов → RGB [1,2,3], иначе одноканальный ``gray``.
Прозрачность слоя применяется на уровне MapLibre (``raster-opacity``), в тайле
альфа кодирует только валидность пикселя (nodata/вне охвата → прозрачно).

Чистые функции (``_mercator_tile_bounds``, ``_apply_singleband``,
``_apply_rgb``) тестируются без S3; :func:`render_layer_tile` умеет открывать
локальный файл (``source_path``) — это же используют тесты.
"""
from __future__ import annotations

import io
import logging
import math

import numpy as np
from PIL import Image

logger = logging.getLogger('my_fields')

TILE_SIZE = 256
# Полупериметр Web Mercator (EPSG:3857), м: πR, R=6378137.
_MERC_MAX = math.pi * 6378137.0

# Контрольные точки палитр (позиция 0..1 → RGB). Между ними — линейная
# интерполяция при построении LUT.
_COLORMAPS = {
    'gray': [(0.0, (0, 0, 0)), (1.0, (255, 255, 255))],
    'ndvi': [
        (0.0, (165, 0, 38)), (0.25, (253, 174, 97)), (0.5, (255, 255, 191)),
        (0.75, (166, 217, 106)), (1.0, (0, 104, 55)),
    ],
    'viridis': [
        (0.0, (68, 1, 84)), (0.25, (59, 82, 139)), (0.5, (33, 145, 140)),
        (0.75, (94, 201, 98)), (1.0, (253, 231, 37)),
    ],
}
_LUT_CACHE: dict[str, np.ndarray] = {}


def _colormap_lut(name: str) -> np.ndarray:
    """(256, 3) uint8 LUT для палитры ``name`` (кэшируется)."""
    key = name if name in _COLORMAPS else 'gray'
    lut = _LUT_CACHE.get(key)
    if lut is not None:
        return lut
    stops = _COLORMAPS[key]
    xs = np.array([s[0] for s in stops])
    idx = np.linspace(0.0, 1.0, 256)
    lut = np.zeros((256, 3), dtype=np.uint8)
    for ch in range(3):
        ys = np.array([s[1][ch] for s in stops], dtype='float64')
        lut[:, ch] = np.clip(np.interp(idx, xs, ys), 0, 255).astype(np.uint8)
    _LUT_CACHE[key] = lut
    return lut


def _mercator_tile_bounds(z: int, x: int, y: int):
    """XYZ-тайл → (xmin, ymin, xmax, ymax) в метрах EPSG:3857."""
    n = 2 ** z
    size = 2 * _MERC_MAX / n
    xmin = -_MERC_MAX + x * size
    xmax = xmin + size
    ymax = _MERC_MAX - y * size
    ymin = ymax - size
    return xmin, ymin, xmax, ymax


def _norm_index(data: np.ndarray, vmin: float, vmax: float) -> np.ndarray:
    """Нормировать в 0..255 (uint8-индекс LUT) по диапазону [vmin, vmax]."""
    if vmax <= vmin:
        vmax = vmin + 1e-6
    scaled = (data.astype('float64') - vmin) / (vmax - vmin)
    return (np.clip(scaled, 0.0, 1.0) * 255).astype(np.uint8)


def _apply_singleband(data, valid, vmin, vmax, colormap):
    """RGBA-тайл из одного канала по палитре ``colormap``."""
    lut = _colormap_lut(colormap)
    idx = _norm_index(data, vmin, vmax)
    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)
    rgba[..., :3] = lut[idx]
    rgba[..., 3] = np.where(valid, 255, 0).astype(np.uint8)
    return rgba


def _apply_rgb(bands, valid, ranges):
    """RGBA-тайл из трёх каналов (``bands`` — список из 3 массивов)."""
    h, w = bands[0].shape
    rgba = np.zeros((h, w, 4), dtype=np.uint8)
    for ch in range(3):
        vmin, vmax = ranges[ch]
        rgba[..., ch] = _norm_index(bands[ch], vmin, vmax)
    rgba[..., 3] = np.where(valid, 255, 0).astype(np.uint8)
    return rgba


def _band_range(layer, band_idx: int, style: dict):
    """(vmin, vmax) для канала: из стиля, иначе p2/p98 из stats, иначе 0..255."""
    if style.get('min') is not None and style.get('max') is not None:
        return float(style['min']), float(style['max'])
    stats = layer.stats or []
    if 0 <= band_idx - 1 < len(stats):
        s = stats[band_idx - 1] or {}
        lo = s.get('p2', s.get('min'))
        hi = s.get('p98', s.get('max'))
        if lo is not None and hi is not None:
            return float(lo), float(hi)
    return 0.0, 255.0


def _effective_style(layer) -> dict:
    """Стиль отрисовки с дефолтами: RGB для ≥3 каналов, иначе gray."""
    style = dict(layer.style or {})
    if not style.get('mode'):
        if (layer.band_count or 0) >= 3:
            style['mode'] = 'rgb'
        else:
            style['mode'] = 'singleband'
    return style


def _read_tile(source_path, env, z, x, y, indexes):
    """Прочитать окно тайла (256×256) для каналов ``indexes`` из COG.

    Возвращает ``(data, valid)`` где ``data`` — ``(len(indexes), 256, 256)``
    float64, ``valid`` — bool-маска ``(256, 256)``; либо ``None``, если тайл
    вне охвата растра / нет данных.
    """
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds as transform_from_bounds
    from rasterio.vrt import WarpedVRT
    from rasterio.warp import transform_bounds

    xmin, ymin, xmax, ymax = _mercator_tile_bounds(z, x, y)
    # WarpedVRT строим сразу на сетке тайла (256×256, transform в 3857) — тогда
    # обычный read() даёт выровненный тайл, а пиксели вне охвата растра
    # получают nodata (маска 0). Это корректный XYZ-подход без boundless.
    dst_transform = transform_from_bounds(
        xmin, ymin, xmax, ymax, TILE_SIZE, TILE_SIZE)
    with rasterio.Env(**(env or {})):
        with rasterio.open(source_path) as src:
            # Быстрый отсев: тайл вне охвата растра (в 3857) → пусто.
            sb = transform_bounds(src.crs, 'EPSG:3857', *src.bounds)
            if (xmax <= sb[0] or xmin >= sb[2]
                    or ymax <= sb[1] or ymin >= sb[3]):
                return None
            with WarpedVRT(src, crs='EPSG:3857',
                           transform=dst_transform,
                           width=TILE_SIZE, height=TILE_SIZE,
                           resampling=Resampling.bilinear) as vrt:
                data = vrt.read(indexes=indexes).astype('float64')
                masks = vrt.read_masks(indexes=indexes)
    valid = np.all(masks > 0, axis=0)
    if not valid.any():
        return None
    return data, valid


def _source_and_env(layer, source_path, env):
    """Путь к COG и GDAL-env: из аргументов (тесты) или из s3_storage."""
    if source_path is not None:
        return source_path, (env or {})
    from . import s3_storage
    return (s3_storage.vsis3_path(layer.cog_key),
            s3_storage.gdal_vsis3_env())


def render_layer_tile(layer, z: int, x: int, y: int, *,
                      source_path: str | None = None,
                      env: dict | None = None) -> bytes | None:
    """Отрендерить PNG-тайл слоя ``layer`` для ``z/x/y`` (или ``None``)."""
    if not (layer.cog_key or source_path):
        return None
    style = _effective_style(layer)
    path, gdal_env = _source_and_env(layer, source_path, env)

    if style.get('mode') == 'rgb':
        bands = style.get('bands') or [1, 2, 3]
        indexes = [int(b) for b in bands[:3]]
    else:
        indexes = [int(style.get('band', 1))]

    try:
        read = _read_tile(path, gdal_env, z, x, y, indexes)
    except Exception as e:  # noqa: BLE001 — тайл-эндпоинт не должен падать 500
        logger.warning('raster tile error layer=%s z=%d x=%d y=%d: %s',
                       getattr(layer, 'pk', '?'), z, x, y, e)
        return None
    if read is None:
        return None
    data, valid = read

    if style.get('mode') == 'rgb':
        ranges = [_band_range(layer, indexes[ch], style) for ch in range(3)]
        rgba = _apply_rgb([data[ch] for ch in range(3)], valid, ranges)
    else:
        vmin, vmax = _band_range(layer, indexes[0], style)
        rgba = _apply_singleband(
            data[0], valid, vmin, vmax, style.get('colormap', 'gray'))

    buf = io.BytesIO()
    Image.fromarray(rgba, 'RGBA').save(buf, format='PNG', optimize=True)
    return buf.getvalue()
