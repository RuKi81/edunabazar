"""Паспорт пользовательского поля — NDVI-снимки и зоны неоднородности.

Переиспользует растровую инфраструктуру ``agrocosmos.services.raster_tiles``
поверх NDVI-композитов, скачанных «Спутниковым мониторингом» в scope
``f<id>`` (см. ``my_fields.services.monitoring``). В отличие от паспорта
угодья (``agrocosmos``), у пользовательского поля нет зональной статистики,
базлайнов и алертов — поэтому здесь только:

* список последних композитов (кадры-превью «Снимки NDVI»);
* PNG-превью композита, обрезанное по bbox поля;
* карта зон неоднородности + динамика к предыдущему композиту;
* векторные зоны для экспорта KML / SHP (карта-предписание DJI Agras).

Все растровые функции чистые (принимают ``tif_path``/``bbox``/``outline``),
так что связь с ``agrocosmos`` — только через сервис-слой (ленивые импорты,
чтобы не тянуть rasterio/PIL там, где они не нужны).
"""
from __future__ import annotations

from datetime import date


def scope_id(field) -> str:
    """Scope растров пользовательского поля в именах файлов (``f<id>``)."""
    return f'f{field.pk}'


def field_outline(field) -> list | None:
    """Упрощённые кольца ([[lon, lat], ...]) контура поля для превью/зон."""
    geom = field.geom
    if geom is None:
        return None
    try:
        simplified = geom.simplify(0.0001, preserve_topology=True)
        if not simplified.empty:
            geom = simplified
        coords = geom.coords
        if geom.geom_type == 'Polygon':
            coords = (coords,)
        rings = []
        for poly in coords:
            for ring in poly:
                rings.append([list(pt) for pt in ring])
        return rings or None
    except Exception:
        return None


def field_bbox(field, pad_frac: float = 0.15) -> tuple | None:
    """Bbox поля с отступом (доля от размера, минимум ~10 м) для контекста."""
    if field.geom is None:
        return None
    xmin, ymin, xmax, ymax = field.geom.extent
    pad_x = max((xmax - xmin) * pad_frac, 1e-4)
    pad_y = max((ymax - ymin) * pad_frac, 1e-4)
    return (xmin - pad_x, ymin - pad_y, xmax + pad_x, ymax + pad_y)


def resolve_composites(field, year: str) -> list:
    """Композиты по полю (scope ``f<id>``), S2+L8 объединены по периодам.

    Если за период есть оба сенсора — предпочитается Sentinel-2, пропуски
    закрываются Landsat. Каждый элемент несёт свой ``sensor``.
    Возвращает список, отсортированный по возрастанию даты (старые → свежие).
    """
    from agrocosmos.services.raster_tiles import list_available_composites

    scope = scope_id(field)
    merged = {}
    # S2 последним — перекрывает L8 при совпадении периода.
    for sensor in ('l8', 's2'):
        for c in list_available_composites(sensor, scope, year):
            merged[(c['date_from'], c['date_to'])] = dict(c, sensor=sensor)
    return [merged[k] for k in sorted(merged)]


def raster_frames(field, year: str, limit: int = 6) -> dict:
    """Последние ``limit`` композитов по полю (свежие первыми) для «Снимки NDVI»."""
    composites = resolve_composites(field, year)
    frames = [
        {
            'date_from': c['date_from'],
            'date_to': c['date_to'],
            'date': c['date_from'] + '_' + c['date_to'],
            'sensor': c['sensor'],
        }
        for c in reversed(composites[-limit:])  # latest first
    ]
    sensors = sorted({f['sensor'] for f in frames})
    return {
        'field_id': field.pk,
        'year': year,
        'scope': scope_id(field),
        'sensor': ('s2' if 's2' in sensors else (sensors[0] if sensors else None)),
        'sensors': sensors,
        'frames': frames,
    }


def preview_png(field, sensor: str, date_range: str) -> bytes | None:
    """PNG-превью NDVI-композита, обрезанное по bbox поля (или None)."""
    from agrocosmos.services.raster_tiles import find_raster_path, render_preview

    bbox = field_bbox(field, pad_frac=0.25)
    if bbox is None:
        return None
    tif_path = find_raster_path(sensor, scope_id(field), date_range)
    if not tif_path:
        return None
    return render_preview(
        tif_path, bbox, max_size=256, outline=field_outline(field),
    )


def _select_composites(composites: list, sensor: str, date_range: str) -> list:
    """Отфильтровать композиты по явному ``date``/``sensor`` (для конкретной даты).

    Явно заданный композит становится «текущим»: более свежие отбрасываются
    (динамика считается назад от него). Пустой ``sensor``/``date`` — без изменений.
    """
    if not (sensor and date_range):
        return composites
    target_from = date_range.split('_')[0]
    selected = [
        c for c in composites
        if c['date_from'] < target_from
        or (c['date_from'] == target_from and c['sensor'] == sensor)
    ]
    if not selected or selected[-1]['date_from'] != target_from:
        return []
    return selected


def zones(field, year: str, date_range: str = '', sensor: str = '') -> dict | None:
    """Карта зон неоднородности + динамика к предыдущему композиту.

    Идёт от свежих композитов к старым — первый с данными по полю.
    Возвращает ``None``, если растровых данных нет.
    """
    from agrocosmos.services.raster_tiles import find_raster_path, render_zones

    bbox = field_bbox(field)
    outline = field_outline(field)
    if bbox is None:
        return None

    composites = _select_composites(resolve_composites(field, year), sensor, date_range)
    scope = scope_id(field)

    for i, comp in enumerate(reversed(composites)):
        rng = comp['date_from'] + '_' + comp['date_to']
        tif_path = find_raster_path(comp['sensor'], scope, rng)
        if not tif_path:
            continue
        png_bytes, stats = render_zones(tif_path, bbox, outline)
        if png_bytes:
            older = composites[:len(composites) - 1 - i]
            return {
                'sensor': comp['sensor'],
                'scope': scope,
                'date_from': comp['date_from'],
                'date_to': comp['date_to'],
                'stats': stats,
                'image': _png_data_uri(png_bytes),
                'dynamics': _zones_dynamics(tif_path, older, scope, bbox, outline),
            }
    return None


def _zones_dynamics(tif_now: str, older: list, scope: str,
                    bbox: tuple, outline: list) -> dict | None:
    """Карта динамики к ближайшему предыдущему композиту с данными."""
    from agrocosmos.services.raster_tiles import (
        find_raster_path, render_zone_dynamics,
    )

    for comp in reversed(older):
        rng = comp['date_from'] + '_' + comp['date_to']
        tif_prev = find_raster_path(comp['sensor'], scope, rng)
        if not tif_prev:
            continue
        png_bytes, stats = render_zone_dynamics(tif_now, tif_prev, bbox, outline)
        if png_bytes:
            return {
                'prev_sensor': comp['sensor'],
                'prev_date_from': comp['date_from'],
                'prev_date_to': comp['date_to'],
                'stats': stats,
                'image': _png_data_uri(png_bytes),
            }
    return None


def zone_features(field, year: str, date_range: str = '', sensor: str = '') -> tuple:
    """Векторные зоны поля для экспорта (KML/SHP).

    Возвращает ``(feats, comp)`` — список фич и композит, по которому они
    построены; ``(None, None)`` — растровых данных нет.
    """
    from agrocosmos.services.raster_tiles import find_raster_path, zones_to_features

    bbox = field_bbox(field)
    outline = field_outline(field)
    if bbox is None:
        return None, None

    composites = _select_composites(resolve_composites(field, year), sensor, date_range)
    scope = scope_id(field)

    for comp in reversed(composites):
        rng = comp['date_from'] + '_' + comp['date_to']
        tif_path = find_raster_path(comp['sensor'], scope, rng)
        if not tif_path:
            continue
        feats = zones_to_features(tif_path, bbox, outline)
        if feats:
            return feats, comp
    return None, None


def _png_data_uri(png_bytes: bytes) -> str:
    import base64
    return 'data:image/png;base64,' + base64.b64encode(png_bytes).decode()


def default_year() -> str:
    return str(date.today().year)
