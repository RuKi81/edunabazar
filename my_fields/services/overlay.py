"""Геообработка ГИС-слоёв (PostGIS): оверлеи, одно-слойные операции, spatial join.

Строит безопасный (``psycopg.sql``) ``SELECT`` под выбранную операцию и
материализует результат в новый слой через
:func:`my_fields.services.shp_import.create_layer_from_select`.

Двух-слойные (полигоны), :data:`OVERLAY_OPS`:

* ``intersection`` — попарное пересечение A×B (атрибуты A);
* ``difference``/``erase`` — A без объединённой геометрии B (атрибуты A);
* ``clip``         — обрезка A по объединению B, одна строка на A (атрибуты A);
* ``union``        — объединение (dissolve) всех геометрий A и B (без атрибутов);
* ``symmetric_difference`` — симметрическая разность dissolve(A)/dissolve(B).

Одно-слойные, :data:`SINGLE_OPS`:

* ``buffer``    — буфер в метрах (через geography), результат — полигоны;
* ``dissolve``  — объединение (опц. по полю), результат — полигоны;
* ``centroids`` — центроиды, результат — точки (атрибуты слоя);
* ``simplify``  — упрощение геометрии (допуск в метрах), тип геометрии сохраняется.

Соединение атрибутов:

* ``spatial_join`` — перенос атрибутов B в A по предикату
  (intersects/contains/within/covers) с агрегатами (count/sum/avg/min/max/first).

Тяжёлые операции выполняются асинхронно (см. management-команду
``run_gis_overlay`` и воркер ``run_ndvi_worker``): в ``create_layer_from_select``
снимается ``statement_timeout``.
"""
from __future__ import annotations

from psycopg import sql

from .shp_import import (
    MAX_IDENT, _attr_db_types, create_layer_from_select, slugify_identifier,
)

_NUMERIC_PG_TYPES = ('integer', 'bigint', 'smallint', 'double precision',
                     'real', 'numeric')


class OverlayError(Exception):
    """Некорректные входные данные для операции геообработки."""


# Двух-слойные операции. op → (метка, «сохраняются ли атрибуты слоя A»).
OVERLAY_OPS = {
    'intersection': ('Пересечение (A ∩ B)', True),
    'difference': ('Разность (A − B)', True),
    'erase': ('Стереть B из A (A − B)', True),
    'clip': ('Обрезка A по B (clip)', True),
    'union': ('Объединение (A ∪ B)', False),
    'symmetric_difference': ('Симметрическая разность (A △ B)', False),
}

# Одно-слойные операции. op → (метка, тип геометрии результата |
# None = как у исходного слоя).
SINGLE_OPS = {
    'buffer': ('Буфер (метры)', 'polygon'),
    'dissolve': ('Объединение (dissolve)', 'polygon'),
    'centroids': ('Центроиды', 'point'),
    'simplify': ('Упрощение геометрии', None),
}

# Пространственные предикаты spatial join (белый список → PostGIS-функция).
_JOIN_PREDICATES = {
    'intersects': 'ST_Intersects',
    'contains': 'ST_Contains',
    'within': 'ST_Within',
    'covers': 'ST_Covers',
    'covered_by': 'ST_CoveredBy',
}
_JOIN_AGGS = {'count', 'sum', 'avg', 'min', 'max', 'first'}
_NUMERIC_AGGS = {'sum', 'avg', 'min', 'max'}

# Все допустимые операции (для валидации в API).
ALL_OPS = set(OVERLAY_OPS) | set(SINGLE_OPS) | {'spatial_join'}


def op_label(op: str) -> str:
    if op in OVERLAY_OPS:
        return OVERLAY_OPS[op][0]
    if op in SINGLE_OPS:
        return SINGLE_OPS[op][0]
    if op == 'spatial_join':
        return 'Пространственное соединение'
    return op


def is_single_op(op: str) -> bool:
    return op in SINGLE_OPS


def _layer_cols(layer, alias=None):
    """``(prefix_sql, attr_meta)`` для атрибутов слоя.

    ``prefix_sql`` — ``[<alias>.]<col>, ...`` с завершающей запятой (пусто,
    если атрибутов нет). ``attr_meta`` — копия метаданных атрибутов.
    """
    cols = list(_attr_db_types(layer).keys())
    if alias:
        idents = [sql.SQL('{}.{}').format(sql.Identifier(alias), sql.Identifier(c))
                  for c in cols]
    else:
        idents = [sql.Identifier(c) for c in cols]
    prefix = sql.SQL(', ').join(idents)
    if idents:
        prefix = prefix + sql.SQL(', ')
    return prefix, [dict(a) for a in (layer.attributes or [])]


def _a_cols(layer_a):
    """Список ``a.<col>`` для атрибутов слоя A + сами метаданные атрибутов."""
    return _layer_cols(layer_a, 'a')


# Оставляем только полигональные части результата (пересечение по кромке может
# дать линии/точки) и приводим к Multi — колонка geom всё равно generic 4326.
def _poly(geom_expr: sql.Composable) -> sql.Composable:
    return sql.SQL('ST_Multi(ST_CollectionExtract({}, 3)) AS geom').format(geom_expr)


def _num_param(params, key, *, required=False, default=None):
    """Извлечь числовой параметр из ``params`` (или ошибка/дефолт)."""
    val = params.get(key, default)
    if val is None or val == '':
        if required:
            raise OverlayError(f'Не задан параметр «{key}».')
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        raise OverlayError(f'Параметр «{key}» должен быть числом.')


def build_overlay_select(layer_a, layer_b, op: str):
    """Собрать ``(attr_meta, select_sql, params)`` под операцию ``op``.

    Raises:
        OverlayError: неизвестная операция или неполигональные слои.
    """
    if op not in OVERLAY_OPS:
        raise OverlayError(f'Неизвестная операция: {op!r}.')
    if layer_a.geom_kind != 'polygon' or layer_b.geom_kind != 'polygon':
        raise OverlayError('Оверлеи поддерживаются только для полигональных слоёв.')

    A = sql.Identifier(layer_a.table_name)
    B = sql.Identifier(layer_b.table_name)

    if op == 'intersection':
        prefix, attr_meta = _a_cols(layer_a)
        select = sql.SQL(
            'SELECT {prefix}{geom} FROM {A} a JOIN {B} b '
            'ON ST_Intersects(a.geom, b.geom)'
        ).format(
            prefix=prefix,
            geom=_poly(sql.SQL('ST_Intersection(a.geom, b.geom)')),
            A=A, B=B,
        )
        return attr_meta, select, []

    if op == 'clip':
        # Обрезаем каждый объект A по объединению пересекающихся B — одна
        # строка на A (атрибуты A). JOIN (не LEFT) — A без пересечений отбрасываем.
        prefix, attr_meta = _a_cols(layer_a)
        select = sql.SQL(
            'SELECT {prefix}{geom} FROM {A} a '
            'JOIN LATERAL (SELECT ST_Union(b.geom) AS g FROM {B} b '
            'WHERE ST_Intersects(a.geom, b.geom)) bu ON true'
        ).format(
            prefix=prefix,
            geom=_poly(sql.SQL('ST_Intersection(a.geom, bu.g)')),
            A=A, B=B,
        )
        return attr_meta, select, []

    if op in ('difference', 'erase'):
        prefix, attr_meta = _a_cols(layer_a)
        # Для каждого объекта A вычитаем объединение пересекающихся объектов B.
        # Если пересечений нет (bu.g IS NULL) — объект A остаётся целиком.
        geom_case = sql.SQL(
            'CASE WHEN bu.g IS NULL THEN a.geom '
            'ELSE ST_Difference(a.geom, bu.g) END'
        )
        select = sql.SQL(
            'SELECT {prefix}{geom} FROM {A} a '
            'LEFT JOIN LATERAL (SELECT ST_Union(b.geom) AS g FROM {B} b '
            'WHERE ST_Intersects(a.geom, b.geom)) bu ON true'
        ).format(prefix=prefix, geom=_poly(geom_case), A=A, B=B)
        return attr_meta, select, []

    if op == 'union':
        select = sql.SQL(
            'SELECT {geom} FROM '
            '(SELECT geom AS g FROM {A} UNION ALL SELECT geom AS g FROM {B}) s'
        ).format(geom=_poly(sql.SQL('ST_Union(s.g)')), A=A, B=B)
        return [], select, []

    # symmetric_difference
    select = sql.SQL(
        'SELECT {geom} FROM '
        '(SELECT ST_Union(geom) AS g FROM {A}) ua, '
        '(SELECT ST_Union(geom) AS g FROM {B}) ub'
    ).format(geom=_poly(sql.SQL('ST_SymDifference(ua.g, ub.g)')), A=A, B=B)
    return [], select, []


def run_overlay(layer_a, layer_b, op: str, title: str, owner=None):
    """Выполнить оверлей ``op`` над A/B и материализовать в новый слой.

    Returns:
        Созданный :class:`my_fields.models.GisLayer`.

    Raises:
        OverlayError: некорректные входные данные.
    """
    attr_meta, select_sql, params = build_overlay_select(layer_a, layer_b, op)
    title = (title or '').strip()
    if not title:
        raise OverlayError('Укажите название слоя.')
    return create_layer_from_select(
        title, 'polygon', attr_meta, select_sql, params,
        owner=owner,
        source_note=f'overlay:{op}:{layer_a.table_name}+{layer_b.table_name}',
    )


# ── Одно-слойные операции ───────────────────────────────────────────────

def _require_kind(layer, allowed, op):
    if layer.geom_kind not in allowed:
        raise OverlayError(
            f'Операция «{op_label(op)}» неприменима к слою типа '
            f'«{layer.geom_kind}».')


def build_single_select(layer, op: str, params: dict):
    """``(attr_meta, result_kind, select_sql, sql_params)`` для одно-слойной операции."""
    if op not in SINGLE_OPS:
        raise OverlayError(f'Неизвестная операция: {op!r}.')
    T = sql.Identifier(layer.table_name)
    _, result_kind = SINGLE_OPS[op]

    if op == 'buffer':
        dist = _num_param(params, 'distance', required=True)
        if abs(dist) > 1_000_000:
            raise OverlayError('Радиус буфера слишком большой (макс. 1 000 000 м).')
        prefix, attr_meta = _layer_cols(layer)
        geom = sql.SQL('ST_Multi(ST_Buffer(geom::geography, %s)::geometry) AS geom')
        select = sql.SQL('SELECT {prefix}{geom} FROM {T}').format(
            prefix=prefix, geom=geom, T=T)
        return attr_meta, 'polygon', select, [dist]

    if op == 'centroids':
        prefix, attr_meta = _layer_cols(layer)
        geom = sql.SQL('ST_Centroid(geom) AS geom')
        select = sql.SQL('SELECT {prefix}{geom} FROM {T}').format(
            prefix=prefix, geom=geom, T=T)
        return attr_meta, 'point', select, []

    if op == 'simplify':
        _require_kind(layer, ('line', 'polygon'), op)
        tol = _num_param(params, 'tolerance', required=True)
        if tol < 0:
            raise OverlayError('Допуск упрощения не может быть отрицательным.')
        prefix, attr_meta = _layer_cols(layer)
        # Упрощение в метрах: переходим в метрическую проекцию (3857), упрощаем,
        # возвращаемся в 4326. PreserveTopology не «ломает» полигоны.
        geom = sql.SQL(
            'ST_Transform(ST_SimplifyPreserveTopology('
            'ST_Transform(geom, 3857), %s), 4326) AS geom')
        select = sql.SQL('SELECT {prefix}{geom} FROM {T}').format(
            prefix=prefix, geom=geom, T=T)
        return attr_meta, (result_kind or layer.geom_kind), select, [tol]

    # dissolve
    _require_kind(layer, ('polygon',), op)
    field = (params.get('field') or '').strip() if params.get('field') else ''
    if field:
        db_types = _attr_db_types(layer)
        if field not in db_types:
            raise OverlayError('Поле для объединения не найдено в слое.')
        kept = next((dict(a) for a in (layer.attributes or [])
                     if a.get('db') == field), None)
        col = sql.Identifier(field)
        select = sql.SQL(
            'SELECT {col}, ST_Multi(ST_Union(geom)) AS geom '
            'FROM {T} GROUP BY {col}'
        ).format(col=col, T=T)
        return ([kept] if kept else []), 'polygon', select, []
    select = sql.SQL('SELECT ST_Multi(ST_Union(geom)) AS geom FROM {T}').format(T=T)
    return [], 'polygon', select, []


def run_single(layer, op: str, title: str, owner=None, params=None):
    """Выполнить одно-слойную операцию и материализовать результат в новый слой."""
    attr_meta, result_kind, select_sql, sql_params = build_single_select(
        layer, op, params or {})
    title = (title or '').strip()
    if not title:
        raise OverlayError('Укажите название слоя.')
    return create_layer_from_select(
        title, result_kind, attr_meta, select_sql, sql_params,
        owner=owner, source_note=f'{op}:{layer.table_name}',
    )


# ── Пространственное соединение атрибутов (spatial join) ─────────────────

def _safe_col(raw, used, fallback):
    name = slugify_identifier(str(raw or ''), fallback=fallback)[:MAX_IDENT]
    base, i = name, 1
    while name in used:
        suffix = f'_{i}'
        name = f'{base[:MAX_IDENT - len(suffix)]}{suffix}'
        i += 1
    used.add(name)
    return name


def _join_agg_expr(spec, b_types, used, index):
    """``(expr_sql, attr_meta_dict)`` для одной спецификации агрегата spatial join."""
    agg = str(spec.get('agg') or '').lower()
    if agg not in _JOIN_AGGS:
        raise OverlayError(f'Недопустимый агрегат: {agg!r}.')
    field = spec.get('field')

    if agg == 'count':
        as_name = _safe_col(spec.get('as') or 'count', used, f'join_{index}')
        expr = sql.SQL('count(*)')
        col_type = 'integer'
    else:
        if not field or field not in b_types:
            raise OverlayError('Поле слоя B для соединения не найдено.')
        col = sql.SQL('b.{}').format(sql.Identifier(field))
        default_name = spec.get('as') or f'{agg}_{field}'
        as_name = _safe_col(default_name, used, f'join_{index}')
        if agg in _NUMERIC_AGGS:
            if b_types[field] not in _NUMERIC_PG_TYPES:
                raise OverlayError(
                    f'Агрегат «{agg}» применим только к числовым полям.')
            expr = sql.SQL('{fn}({col})').format(fn=sql.SQL(agg), col=col)
            col_type = 'double precision'
        else:  # first — первое значение среди совпадений
            expr = sql.SQL('(array_agg({col}))[1]').format(col=col)
            bt = b_types[field]
            col_type = bt if bt in ('integer', 'double precision', 'date') else 'text'

    label = spec.get('label') or as_name
    return (sql.SQL('{e} AS {n}').format(e=expr, n=sql.Identifier(as_name)),
            {'name': str(label)[:200], 'db': as_name, 'type': col_type})


def build_spatial_join_select(layer_a, layer_b, params: dict):
    """``(attr_meta, result_kind, select_sql, sql_params)`` для spatial join.

    Одна строка на объект A (геометрия A сохраняется); к атрибутам A
    добавляются агрегаты по пересекающимся объектам B.
    """
    pred = str(params.get('predicate') or 'intersects')
    if pred not in _JOIN_PREDICATES:
        raise OverlayError('Неизвестный пространственный предикат.')
    pred_fn = _JOIN_PREDICATES[pred]

    a_prefix, a_attrs = _a_cols(layer_a)
    b_types = _attr_db_types(layer_b)
    specs = params.get('joins')
    if not isinstance(specs, list) or not specs:
        raise OverlayError('Укажите хотя бы одно переносимое поле или агрегат.')

    used = {a.get('db') for a in a_attrs if a.get('db')}
    agg_exprs, new_attrs = [], []
    for i, spec in enumerate(specs):
        if not isinstance(spec, dict):
            raise OverlayError('Некорректная спецификация соединения.')
        expr, meta = _join_agg_expr(spec, b_types, used, i + 1)
        agg_exprs.append(expr)
        new_attrs.append(meta)

    j_cols = sql.SQL(', ').join(
        sql.SQL('j.{}').format(sql.Identifier(m['db'])) for m in new_attrs)
    lateral = sql.SQL(
        'LEFT JOIN LATERAL (SELECT {aggs} FROM {B} b '
        'WHERE {pred}(a.geom, b.geom)) j ON true'
    ).format(
        aggs=sql.SQL(', ').join(agg_exprs),
        B=sql.Identifier(layer_b.table_name),
        pred=sql.SQL(pred_fn),
    )
    select = sql.SQL(
        'SELECT {aprefix}{jcols}, a.geom AS geom FROM {A} a {lateral}'
    ).format(
        aprefix=a_prefix, jcols=j_cols,
        A=sql.Identifier(layer_a.table_name), lateral=lateral,
    )
    return a_attrs + new_attrs, layer_a.geom_kind, select, []


def run_spatial_join(layer_a, layer_b, title: str, owner=None, params=None):
    """Выполнить spatial join и материализовать результат в новый слой."""
    attr_meta, result_kind, select_sql, sql_params = build_spatial_join_select(
        layer_a, layer_b, params or {})
    title = (title or '').strip()
    if not title:
        raise OverlayError('Укажите название слоя.')
    return create_layer_from_select(
        title, result_kind, attr_meta, select_sql, sql_params,
        owner=owner,
        source_note=f'spatial_join:{layer_a.table_name}+{layer_b.table_name}',
    )


# ── Единый диспетчер (используется командой/эндпоинтом) ──────────────────

def run_from_params(op: str, *, layer_a, layer_b=None, title, owner=None, params=None):
    """Выполнить операцию ``op`` по её категории и вернуть созданный слой."""
    params = params or {}
    if op in SINGLE_OPS:
        return run_single(layer_a, op, title, owner=owner, params=params)
    if op == 'spatial_join':
        if layer_b is None:
            raise OverlayError('Не задан второй слой (B).')
        return run_spatial_join(layer_a, layer_b, title, owner=owner, params=params)
    if op in OVERLAY_OPS:
        if layer_b is None:
            raise OverlayError('Не задан второй слой (B).')
        return run_overlay(layer_a, layer_b, op, title, owner=owner)
    raise OverlayError(f'Неизвестная операция: {op!r}.')
