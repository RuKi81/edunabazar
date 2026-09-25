"""Сводка по ГИС-слою: группировка по атрибуту + итоговые площади.

Питает страницу «Дашборды» (`/me/gis/dashboards/`): пайчарт с долями и
таблица итоговых площадей. Группировка — по любому атрибуту слоя
(например, подтип почвы ``soil_subt``), опциональный разрез ``split`` даёт
второе измерение (например, зоны ``zone_2``) и разворачивается в таблицу
кросс-табом на клиенте.

Группировок может быть ДВЕ (``group`` + ``group2``) — второй уровень
детализирует строки: категорией становится ПАРА значений, в таблице на
каждый уровень своя колонка. Больше двух уровней сознательно нет: число
строк растёт произведением, а диаграмма по комбинациям уже нечитаема
(секторы строятся по ПЕРВОМУ уровню).

Площадь считается ГЕОДЕЗИЧЕСКИ — ``ST_Area(geom::geography) / 10000`` (га),
а не в градусах: слои хранятся в EPSG:4326, где площадь в единицах СК
физического смысла не имеет. Для точечных и линейных слоёв площади нет —
в ответе ``polygonal=False`` и доли считаются по числу объектов.

Фильтр по району — ``ST_Intersects`` с границей ``agro_district``, а площадь
берётся ПОЛНАЯ (полигон не обрезается по границе). Так сумма по сводке
совпадает с суммой площадей слоя, а не зависит от точности муниципальных
границ OSM; для слоя, целиком лежащего в одном районе (обычный случай)
разницы нет вообще.

Значения атрибутов можно ограничить перечнем (``group_values`` /
``group2_values`` / ``split_values``) — это чекбоксы в UI под селектами
«Группировка» и «Разрез». Сравнение идёт ПО ТЕКСТОВОМУ представлению
(``col::text``) — так же, как строятся сами группы и как отдаёт значения
``/field-values/``, чтобы числовые коды и текст работали одинаково. SQL
``NULL`` в перечне обозначается токеном :data:`NULL_TOKEN` (пустая строка —
отдельное, непустое значение, и путать их нельзя).

Имена колонок в SQL подставляются только через ``psycopg.sql.Identifier`` и
только после проверки по ``layer.attributes``; сами значения — только
параметрами запроса (``= ANY(%s)``). Произвольный SQL от клиента исключён.
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
# Длина перечня значений в фильтре (чекбоксы): совпадает с лимитом
# ``/field-values/``, больше значений UI всё равно не покажет.
MAX_FILTER_VALUES = 500
# Токен SQL ``NULL`` в перечне значений фильтра.
NULL_TOKEN = '__null__'


def clean_values(raw, what: str):
    """Нормализовать перечень значений фильтра → ``list[str] | None``.

    ``None`` или пустой перечень — фильтра нет (берутся все значения). Дубли
    снимаются, порядок сохраняется (стабильные ссылки и пресеты).
    """
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple, set)):
        raise LayerSummaryError(f'{what}: ожидается список значений.')
    out, seen = [], set()
    for item in raw:
        val = NULL_TOKEN if item is None else str(item)
        if val in seen:
            continue
        seen.add(val)
        out.append(val)
    if len(out) > MAX_FILTER_VALUES:
        raise LayerSummaryError(
            f'{what}: слишком много значений (>{MAX_FILTER_VALUES}).')
    return out or None


def _value_filter(field, values):
    """Условие ``WHERE`` по перечню значений колонки → ``(SQL, params)``.

    Пустой перечень — ``(None, [])`` (фильтра нет). :data:`NULL_TOKEN`
    превращается в отдельное ``IS NULL``: ``= ANY`` NULL-ы не ловит.
    """
    if not values:
        return None, []
    col = sql.Identifier(field)
    plain = [v for v in values if v != NULL_TOKEN]
    parts, params = [], []
    if plain:
        parts.append(sql.SQL('l.{}::text = ANY(%s)').format(col))
        params.append(plain)
    if len(plain) != len(values):
        parts.append(sql.SQL('l.{} IS NULL').format(col))
    return sql.SQL('({})').format(sql.SQL(' OR ').join(parts)), params


def _area_expr(polygonal):
    """SQL-выражение площади группы в гектарах (0 для не-полигонов)."""
    if not polygonal:
        return sql.SQL('0')
    return sql.SQL('COALESCE(sum(ST_Area(l.geom::geography)), 0) / 10000.0')


def _fetch_rows(layer, fields, district_id, polygonal, row_limit,
                filters=()):
    """Выполнить GROUP BY по ``fields`` → строки ``(*ключи, count, area_ha)``.

    ``fields`` — кортеж db-имён в порядке ключей строки (уровни группировки,
    затем разрез); ``filters`` — пары ``(поле, перечень значений)``.
    """
    select, group_by = [], []
    for pos, field in enumerate(fields, start=1):
        select.append(sql.SQL('l.{}::text').format(sql.Identifier(field)))
        group_by.append(sql.SQL(str(pos)))
    select.append(sql.SQL('count(*) AS n'))
    select.append(sql.SQL('{} AS area_ha').format(_area_expr(polygonal)))

    params = []
    join = sql.SQL('')
    if district_id:
        join = sql.SQL(
            ' JOIN agro_district d ON d.id = %s AND ST_Intersects(l.geom, d.geom)')
        params.append(int(district_id))

    where_parts = []
    for field, values in filters:
        if not field:
            continue
        cond, cond_params = _value_filter(field, values)
        if cond is not None:
            where_parts.append(cond)
            params.extend(cond_params)
    where = sql.SQL('')
    if where_parts:
        where = sql.SQL(' WHERE {}').format(
            sql.SQL(' AND ').join(where_parts))

    query = sql.SQL(
        'SELECT {select} FROM {table} l{join}{where} '
        'GROUP BY {group_by} ORDER BY area_ha DESC, n DESC LIMIT %s'
    ).format(
        select=sql.SQL(', ').join(select),
        table=sql.Identifier(layer.table_name),
        join=join,
        where=where,
        group_by=sql.SQL(', ').join(group_by),
    )
    params.append(row_limit + 1)

    with connection.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _accumulate(rows, levels, split):
    """Свернуть сырые строки в ``{ключ-категория: {count, area_ha, splits}}``.

    ``levels`` — сколько первых колонок строки образуют категорию (1 или 2).
    Ключ словаря — кортеж значений уровней, чтобы одинаковые значения
    второго уровня под разными первыми не слипались.
    """
    groups = {}
    for row in rows:
        key = tuple(row[:levels])
        sval = row[levels] if split else None
        count, area = row[-2], row[-1]
        item = groups.setdefault(key, {
            'value': key[0],
            'value2': key[1] if levels > 1 else None,
            'count': 0, 'area_ha': 0.0, 'splits': {},
        })
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
    def label(x):
        return (str(x['value'] or ''), str(x.get('value2') or ''))
    if polygonal:
        return lambda x: (-x['area_ha'], -x['count'], label(x))
    return lambda x: (-x['count'], label(x))


def _resolve_fields(layer, group, group2, split):
    """Проверить поля сводки → ``(group, group2, split)``.

    Повторы снимаются (тот же атрибут во втором уровне или разрезе дал бы
    дубль-колонку без новой информации), неизвестное поле — ошибка.
    """
    types = _attr_db_types(layer)
    if group not in types:
        raise LayerSummaryError(f'Недопустимое поле группировки: {group!r}.')
    if group2 == group:
        group2 = None
    if group2 and group2 not in types:
        raise LayerSummaryError(
            f'Недопустимое поле второй группировки: {group2!r}.')
    if split in (group, group2):
        split = None
    if split and split not in types:
        raise LayerSummaryError(f'Недопустимое поле разреза: {split!r}.')
    return group, group2, split


def summary_by_field(layer, group: str, split: str | None = None,
                     district_id: int | None = None,
                     group_values=None, split_values=None,
                     group2: str | None = None, group2_values=None) -> dict:
    """Сводка слоя: количество объектов и площадь по значениям ``group``.

    Args:
        layer: :class:`my_fields.models.GisLayer`.
        group: db-имя атрибута для группировки (обязателен).
        split: db-имя атрибута разреза (колонки кросс-таба) или ``None``.
            Совпадение с уровнем группировки = отсутствие разреза.
        district_id: ``agro_district.id`` — оставить только объекты,
            пересекающие границу района (площади при этом ПОЛНЫЕ, см. модуль).
        group_values: перечень значений ``group`` (чекбоксы в UI) или ``None``
            — тогда берутся все. Сравнение по тексту, ``NULL`` —
            :data:`NULL_TOKEN`.
        split_values: то же для ``split``. Игнорируется без ``split``.
        group2: db-имя второго уровня группировки или ``None``:
            категорией становится пара ``(group, group2)``.
        group2_values: то же для ``group2``. Игнорируется без ``group2``.

    Returns:
        ``{'group', 'group2', 'split', 'polygonal', 'rows', 'splits', 'total',
        'truncated'}``. ``rows`` — список ``{value, value2, count, area_ha,
        share, splits}`` (``value2`` — ``None`` без ``group2``; ``share`` —
        доля от итога, 0..1), отсортированный по убыванию площади (или
        количества для не-полигональных слоёв).

    Raises:
        LayerSummaryError: поле не является атрибутом слоя либо слой слишком
            велик для синхронной сводки.
    """
    group, group2, split = _resolve_fields(layer, group, group2, split)
    if (layer.feature_count or 0) > MAX_FEATURES:
        raise LayerSummaryError(
            'Слой слишком большой для сводки '
            f'(объектов {layer.feature_count}, максимум {MAX_FEATURES}).')

    group_values = clean_values(group_values, 'Значения группировки')
    group2_values = clean_values(group2_values, 'Значения 2-й группировки')
    split_values = clean_values(split_values, 'Значения разреза')
    if not group2:
        group2_values = None
    if not split:
        split_values = None

    polygonal = layer.geom_kind == 'polygon'
    levels = 2 if group2 else 1
    fields = (group, group2, split) if group2 else (group, split)
    fields = tuple(f for f in fields if f)
    row_limit = MAX_GROUPS * MAX_SPLITS if split else MAX_GROUPS
    rows = _fetch_rows(
        layer, fields, district_id, polygonal, row_limit,
        filters=((group, group_values), (group2, group2_values),
                 (split, split_values)))
    truncated = len(rows) > row_limit
    groups = _accumulate(rows[:row_limit], levels, split)

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
            'value2': item['value2'],
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
        'group2': group2,
        'split': split,
        'polygonal': polygonal,
        'district_id': int(district_id) if district_id else None,
        # Эхо применённых фильтров: письмо и подпись сводки должны честно
        # показывать, что выборка неполная.
        'group_values': group_values,
        'group2_values': group2_values,
        'split_values': split_values,
        'rows': out_rows,
        'splits': [
            {'value': s['value'], 'count': s['count'],
             'area_ha': round(s['area_ha'], 4)}
            for s in splits
        ],
        'total': {'count': total_count, 'area_ha': round(total_area, 4)},
        'truncated': truncated,
    }
