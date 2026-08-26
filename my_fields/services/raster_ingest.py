"""Конвейер конвертации растрового слоя в COG (Фаза 3).

Забирает оригинал GeoTIFF из бакета загрузок (``S3_BUCKET_UPLOADS``),
конвертирует его в Cloud-Optimized GeoTIFF (внутренние тайлы + оверзумы,
DEFLATE) и заливает в бакет COG (``S3_BUCKET_COG``). Попутно извлекает
метаданные (SRID, охват в 4326, число каналов, NoData, пер-канальную
статистику для авто-контраста) и заполняет поля :class:`RasterLayer`.

Тяжёлая часть (rasterio/GDAL) вынесена в чистые функции
:func:`convert_to_cog` и :func:`extract_metadata` — их можно тестировать на
локальном синтетическом GeoTIFF без объектного хранилища. Оркестратор
:func:`ingest_raster_layer` качает/заливает через :mod:`s3_storage` и
двигает статус слоя ``processing`` → ``ready``.

Запускается management-командой ``run_raster_ingest`` (её берёт воркер
``run_ndvi_worker`` по ``PipelineRun`` c ``task_type='raster_ingest'``).
"""
from __future__ import annotations

import math
import os
import shutil
import tempfile
from typing import Callable

from django.conf import settings

from ..models import RasterLayer

# Читаем не весь растр для статистики, а децимированную выборку до ~1 Мпикс:
# для авто-контраста (p2/p98) этого достаточно, а память/время ограничены.
_STATS_MAX_PIXELS = 1_000_000

Logger = Callable[[str], None]


class RasterIngestError(Exception):
    """Некорректный слой или сбой конвертации в COG."""


def _noop(_msg: str) -> None:
    pass


def convert_to_cog(src_path: str, dst_path: str, *, log: Logger = _noop) -> None:
    """Сконвертировать ``src_path`` в COG ``dst_path`` (драйвер GDAL COG).

    Драйвер сам строит внутренние тайлы и оверзумы (пирамиды), поэтому
    отдельный ``gdaladdo`` не нужен. ``BIGTIFF=IF_SAFER`` — безопасно для
    файлов >4 ГБ.
    """
    from rasterio.shutil import copy as rio_copy

    log(f'COG: {src_path} → {dst_path}')
    rio_copy(
        src_path, dst_path,
        driver='COG',
        compress='DEFLATE',
        predictor='YES',
        overview_resampling='average',
        BIGTIFF='IF_SAFER',
    )


def _band_stats(ds, bidx: int, nodata) -> dict:
    """Пер-канальная статистика по децимированной выборке канала ``bidx``.

    Возвращает ``{min, max, mean, p2, p98}`` (пусто, если валидных пикселей
    нет). ``p2``/``p98`` — перцентили для авто-контраста при рендере тайлов.
    """
    import numpy as np
    from rasterio.enums import Resampling

    h, w = ds.height, ds.width
    scale = max(1, int(math.sqrt(max(1, h * w) / _STATS_MAX_PIXELS)))
    out_h, out_w = max(1, h // scale), max(1, w // scale)
    arr = ds.read(
        bidx, out_shape=(out_h, out_w),
        resampling=Resampling.average,
    ).astype('float64')

    if nodata is not None and not (isinstance(nodata, float) and math.isnan(nodata)):
        mask = arr != nodata
    else:
        mask = ~np.isnan(arr)
    vals = arr[mask]
    if vals.size == 0:
        return {}
    return {
        'min': float(np.min(vals)),
        'max': float(np.max(vals)),
        'mean': float(np.mean(vals)),
        'p2': float(np.percentile(vals, 2)),
        'p98': float(np.percentile(vals, 98)),
    }


def extract_metadata(path: str) -> dict:
    """Прочитать метаданные растра ``path`` для реестра.

    Возвращает ``{srid, bounds, band_count, nodata, stats}``, где ``bounds``
    — ``[minx, miny, maxx, maxy]`` в EPSG:4326 (или в родных координатах, если
    проекция не определена), ``stats`` — список пер-канальной статистики.
    """
    import rasterio
    from rasterio.warp import transform_bounds

    with rasterio.open(path) as ds:
        srid = ds.crs.to_epsg() if ds.crs else 0
        if ds.crs:
            bounds = list(transform_bounds(
                ds.crs, 'EPSG:4326', *ds.bounds, densify_pts=21))
        else:
            bounds = [ds.bounds.left, ds.bounds.bottom,
                      ds.bounds.right, ds.bounds.top]
        nodata = ds.nodata
        band_count = ds.count
        stats = [_band_stats(ds, i, nodata) for i in range(1, band_count + 1)]

    return {
        'srid': srid or 0,
        'bounds': [float(b) for b in bounds],
        'band_count': int(band_count),
        'nodata': None if nodata is None else float(nodata),
        'stats': stats,
    }


def ingest_raster_layer(layer: RasterLayer, *, log: Logger = _noop) -> RasterLayer:
    """Полный конвейер для одного слоя: original → COG + метаданные.

    Двигает статус ``processing`` в начале и ``ready`` по завершении. При
    ошибке пробрасывает исключение (статус ``failed`` выставляет вызывающая
    команда). Временные файлы удаляются в ``finally``.
    """
    from . import s3_storage

    if not layer.upload_key:
        raise RasterIngestError('У слоя нет upload_key (оригинал не загружен).')

    layer.status = RasterLayer.Status.PROCESSING
    layer.error = ''
    layer.save(update_fields=['status', 'error', 'updated_at'])

    tmpdir = tempfile.mkdtemp(prefix='raster_ingest_')
    try:
        src = os.path.join(tmpdir, 'original.tif')
        log(f'Скачиваю оригинал {layer.upload_key} …')
        s3_storage.download_object(
            layer.upload_key, src, bucket=settings.S3_BUCKET_UPLOADS)

        dst = os.path.join(tmpdir, 'cog.tif')
        log('Конвертирую в COG …')
        convert_to_cog(src, dst, log=log)

        log('Извлекаю метаданные …')
        meta = extract_metadata(dst)

        cog_key = s3_storage.build_cog_key(layer.id)
        log(f'Заливаю COG {cog_key} …')
        s3_storage.upload_file(dst, cog_key, bucket=settings.S3_BUCKET_COG)

        layer.cog_key = cog_key
        layer.srid = meta['srid']
        layer.bounds = meta['bounds']
        layer.band_count = meta['band_count']
        layer.nodata = meta['nodata']
        layer.stats = meta['stats']
        layer.status = RasterLayer.Status.READY
        layer.error = ''
        layer.save(update_fields=[
            'cog_key', 'srid', 'bounds', 'band_count', 'nodata', 'stats',
            'status', 'error', 'updated_at',
        ])
        log(f'Готово: COG {cog_key}, каналов {meta["band_count"]}, '
            f'SRID {meta["srid"]}.')
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return layer
