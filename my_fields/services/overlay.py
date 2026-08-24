"""Оверлейные операции между двумя ГИС-слоями (PostGIS).

Строит безопасный (``psycopg.sql``) ``SELECT`` под выбранную операцию и
материализует результат в новый слой через
:func:`my_fields.services.shp_import.create_layer_from_select`.

Операции (v1 — полигональные слои):

* ``intersection`` — попарное пересечение A×B (атрибуты слоя A);
* ``difference``   — A без объединённой геометрии B (атрибуты слоя A);
* ``union``        — объединение (dissolve) всех геометрий A и B (без атрибутов);
* ``symmetric_difference`` — симметрическая разность dissolve(A) и dissolve(B)
  (без атрибутов).

Тяжёлые операции выполняются асинхронно (см. management-команду
``run_gis_overlay`` и воркер ``run_ndvi_worker``): в ``create_layer_from_select``
снимается ``statement_timeout``.
"""
from __future__ import annotations

from psycopg import sql

from .shp_import import _attr_db_types, create_layer_from_select


class OverlayError(Exception):
    """Некорректные входные данные для оверлейной операции."""


# op → (человекочитаемая метка, «сохраняются ли атрибуты слоя A»).
OVERLAY_OPS = {
    'intersection': ('Пересечение (A ∩ B)', True),
    'difference': ('Разность (A − B)', True),
    'union': ('Объединение (A ∪ B)', False),
    'symmetric_difference': ('Симметрическая разность (A △ B)', False),
}


def op_label(op: str) -> str:
    entry = OVERLAY_OPS.get(op)
    return entry[0] if entry else op


def _a_cols(layer_a):
    """Список ``a.<col>`` для атрибутов слоя A + сами метаданные атрибутов."""
    cols = list(_attr_db_types(layer_a).keys())
    idents = [sql.SQL('a.{}').format(sql.Identifier(c)) for c in cols]
    prefix = sql.SQL(', ').join(idents)
    if idents:
        prefix = prefix + sql.SQL(', ')
    return prefix, list(layer_a.attributes or [])


# Оставляем только полигональные части результата (пересечение по кромке может
# дать линии/точки) и приводим к Multi — колонка geom всё равно generic 4326.
def _poly(geom_expr: sql.Composable) -> sql.Composable:
    return sql.SQL('ST_Multi(ST_CollectionExtract({}, 3)) AS geom').format(geom_expr)


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

    if op == 'difference':
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
