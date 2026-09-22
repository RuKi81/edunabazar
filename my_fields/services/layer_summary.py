"""Сводка по ГИС-слою: группировка по атрибуту + итоговые площади.

Питает страницу «Дашборды» (`/me/gis/dashboards/`): пайчарт с долями и
таблица итоговых площадей. Группировка — по любому атрибуту слоя
(например, подтип почвы ``soil_subt``), опциональный разрез ``split`` даёт
второе измерение (например, зоны ``zone_2``) и разворачивается в таблицу
кросс-табом на клиенте.

Площадь считается ГЕОДЕЗИЧЕСКИ — ``ST_Area(geom::geography) / 10000`` (га),
а не в градусах: слои хранятся в EPSG:4326, где площадь в единицах СК
физического смысла не имеет. Для точечных и линейных слоёв площади нет —
в ответе ``polygonal=False`` и доли считаются по числу объектов.

Фильтр по району — ``ST_Intersects`` с границей ``agro_district``, а площадь
берётся ПОЛНАЯ (полигон не обрезается по границе). Так сумма по сводке
совпадает с суммой площадей слоя, а не зависит от точности муниципальных
границ OSM; для слоя, целиком лежащего в одном районе (обычный случай)
разницы нет вообще.

Имена колонок в SQL подставляются только через ``psycopg.sql.Identifier`` и
только после проверки по ``layer.attributes`` — произвольный SQL от клиента
исключён.
"""
from __future__ import annotations

from django.db import connection
from psycopg import sql

from .shp_import import _attr_db_types


class LayerSummaryError(ValueError):
    """Некорректные параметры сводки (сообщение пригодно для показа в UI)."""


# Сводка считается синхронно в веб-запросе, а geography-площадь на больших
# слоях стоит дорого. Выше порога честно отказываем, вместо того чтобы
# упереться в statement_timeout на середине запроса.
MAX_FEATURES = 300_000
# Пайчарт и таблица бессмысленны при тысячах категорий, а groups × splits
# могут дать декартово произведение — режем и помечаем truncated.
MAX_GROUPS = 200
MAX_SPLITS = 40


def _area_expr(polygonal):
    """SQL-выражение площади группы в гектарах (0 для не-полигонов)."""
    if not polygonal:
        return sql.SQL('0')
    return sql.SQL('COALESCE(sum(ST_Area(l.geom::geography)), 0) / 10000.0')


def _fetch_rows(layer, group, split, district_id, polygonal, row_limit):
    """Выполнить GROUP BY и вернуть сырые строки ``(g[, s], count, area_ha)``."""
    select = [sql.SQL('l.{}::text AS g').format(sql.Identifier(group))]
    group_by = [sql.SQL('1')]
    if split:
        select.append(sql.SQL('l.{}::text AS s').format(sql.Identifier(split)))
        group_by.append(sql.SQL('2'))
    select.append(sql.SQL('count(*) AS n'))
    select.append(sql.SQL('{} AS area_ha').format(_area_expr(polygonal)))

    params = []
    join = sql.SQL('')
    if district_id:
        join = sql.SQL(
            ' JOIN agro_district d ON d.id = %s AND ST_Intersects(l.geom, d.geom)')
        params.append(int(district_id))

    query = sql.SQL(
        'SELECT {select} FROM {table} l{join} '
        'GROUP BY {group_by} ORDER BY area_ha DESC, n DESC LIMIT %s'
    ).format(
        select=sql.SQL(', ').join(select),
        table=sql.Identifier(layer.table_name),
        join=join,
        group_by=sql.SQL(', ').join(group_by),
    )
    params.append(row_limit + 1)

    with connection.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _accumulate(rows, split):
    """Свернуть сырые строки в ``{group_value: {count, area_ha, splits}}``."""
    groups = {}
    for row in rows:
        if split:
            gval, sval, count, area = row[0], row[1], row[2], row[3]
        else:
            gval, sval, count, area = row[0], None, row[1], row[2]
        item = groups.setdefault(
            gval, {'value': gval, 'count': 0, 'area_ha': 0.0, 'splits': {}})
        item['count'] += int(count or 0)
        item['area_ha'] += float(area or 0.0)
        if split:
            cell = item['splits'].setdefault(sval, {'count': 0, 'area_ha': 0.0})
            cell['count'] += int(count or 0)
            cell['area_ha'] += float(area or 0.0)
    return groups


def _split_totals(groups):
    """Итоги по значениям разреза (для порядка и заголовков колонок)."""
    totals = {}
    for item in groups.values():
        for sval, cell in item['splits'].items():
            acc = totals.setdefault(
                sval, {'value': sval, 'count': 0, 'area_ha': 0.0})
            acc['count'] += cell['count']
            acc['area_ha'] += cell['area_ha']
    return totals


def _sort_key(polygonal):
    """Сортировка категорий: по площади, а без площади — по числу объектов."""
    if polygonal:
        return lambda x: (-x['area_ha'], -x['count'], str(x['value'] or ''))
    return lambda x: (-x['count'], str(x['value'] or ''))


def summary_by_field(layer, group: str, split: str | None = None,
                     district_id: int | None = None) -> dict:
    """Сводка слоя: количество объектов и площадь по значениям ``group``.

    Args:
        layer: :class:`my_fields.models.GisLayer`.
        group: db-имя атрибута для группировки (обязателен).
        split: db-имя атрибута второго измерения или ``None``. Совпадение с
            ``group`` трактуется как отсутствие разреза.
        district_id: ``agro_district.id`` — оставить только объекты,
            пересекающие границу района (площади при этом ПОЛНЫЕ, см. модуль).

    Returns:
        ``{'group', 'split', 'polygonal', 'rows', 'splits', 'total',
        'truncated'}``. ``rows`` — список ``{value, count, area_ha, share,
        splits}`` (``share`` — доля от итога, 0..1), отсортированный по
        убыванию площади (или количества для не-полигональных слоёв).

    Raises:
        LayerSummaryError: поле не является атрибутом слоя либо слой слишком
            велик для синхронной сводки.
    """
    types = _attr_db_types(layer)
    if group not in types:
        raise LayerSummaryError(f'Недопустимое поле группировки: {group!r}.')
    if split == group:
        split = None
    if split and split not in types:
        raise LayerSummaryError(f'Недопустимое поле разреза: {split!r}.')
    if (layer.feature_count or 0) > MAX_FEATURES:
        raise LayerSummaryError(
            'Слой слишком большой для сводки '
            f'(объектов {layer.feature_count}, максимум {MAX_FEATURES}).')

    polygonal = layer.geom_kind == 'polygon'
    row_limit = MAX_GROUPS * MAX_SPLITS if split else MAX_GROUPS
    rows = _fetch_rows(layer, group, split, district_id, polygonal, row_limit)
    truncated = len(rows) > row_limit
    groups = _accumulate(rows[:row_limit], split)

    items = sorted(groups.values(), key=_sort_key(polygonal))
    if len(items) > MAX_GROUPS:
        items = items[:MAX_GROUPS]
        truncated = True

    splits = []
    if split:
        splits = sorted(_split_totals(groups).values(), key=_sort_key(polygonal))
        if len(splits) > MAX_SPLITS:
            splits = splits[:MAX_SPLITS]
            truncated = True

    total_count = sum(i['count'] for i in items)
    total_area = sum(i['area_ha'] for i in items)
    denom = (total_area if polygonal else total_count) or 0

    out_rows = []
    for item in items:
        measure = item['area_ha'] if polygonal else item['count']
        out_rows.append({
            'value': item['value'],
            'count': item['count'],
            'area_ha': round(item['area_ha'], 4),
            'share': (measure / denom) if denom else 0.0,
            # Ключ NULL-значения разреза — пустая строка (клиент так же
            # приводит value=null к ''), иначе получился бы ключ 'None'.
            'splits': {
                ('' if k is None else str(k)):
                    {'count': v['count'], 'area_ha': round(v['area_ha'], 4)}
                for k, v in item['splits'].items()
            },
        })

    return {
        'group': group,
        'split': split,
        'polygonal': polygonal,
        'district_id': int(district_id) if district_id else None,
        'rows': out_rows,
        'splits': [
            {'value': s['value'], 'count': s['count'],
             'area_ha': round(s['area_ha'], 4)}
            for s in splits
        ],
        'total': {'count': total_count, 'area_ha': round(total_area, 4)},
        'truncated': truncated,
    }
