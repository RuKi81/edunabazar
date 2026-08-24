"""Безопасный конструктор ``WHERE`` для визуального фильтра слоя (SQL-выборка).

Пользователь собирает условия в UI (поле · оператор · значение + ALL/ANY), а
здесь они компилируются в параметризованный ``WHERE`` через ``psycopg.sql`` —
без сырого SQL от клиента. Разрешены только колонки из ``layer.attributes`` и
операторы из белого списка :data:`OPERATORS`, значения приводятся к типу
колонки. Это исключает SQL-инъекции и делает выборку предсказуемой.

Спецификация фильтра (JSON от фронта)::

    {
      "match": "all" | "any",          # AND / OR между правилами
      "rules": [
        {"field": "<db>", "op": "eq", "value": ...},
        ...
      ]
    }
"""
from psycopg import sql


class LayerQueryError(ValueError):
    """Некорректный фильтр (сообщение пригодно для показа в UI)."""


# Операторы: op -> нужно ли значение (0 — без значения, 1 — одно, 2 — два,
# 'list' — список). Текстовые операторы работают по ``col::text``.
OPERATORS = {
    'eq': 1, 'neq': 1,
    'gt': 1, 'gte': 1, 'lt': 1, 'lte': 1,
    'contains': 1, 'starts': 1, 'ends': 1,
    'in': 'list', 'between': 2,
    'is_null': 0, 'not_null': 0,
}

_CMP_SQL = {'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}


def _column_types(layer) -> dict:
    """``{db_col: pg_type}`` для всех фильтруемых колонок (+ id)."""
    from .shp_import import _ALLOWED_CAST_TYPES, _attr_db_types
    types = {'id': 'integer'}
    for db, pg_type in _attr_db_types(layer).items():
        types[db] = pg_type if pg_type in _ALLOWED_CAST_TYPES else 'text'
    return types


def _rule_sql(field_ident, pg_type, op, value):
    """Собрать ``(sql, params)`` для одного правила."""
    col = field_ident
    typed = sql.SQL('{}::{}').format(col, sql.SQL(pg_type))

    if op in ('is_null', 'not_null'):
        return (sql.SQL('{} IS NULL').format(col) if op == 'is_null'
                else sql.SQL('{} IS NOT NULL').format(col)), []

    if op == 'eq':
        return sql.SQL('{} = %s::{}').format(col, sql.SQL(pg_type)), [value]
    if op == 'neq':
        # IS DISTINCT FROM — чтобы NULL корректно отличался от значения.
        return sql.SQL('{} IS DISTINCT FROM %s::{}').format(
            col, sql.SQL(pg_type)), [value]
    if op in _CMP_SQL:
        return sql.SQL('{} {} %s::{}').format(
            col, sql.SQL(_CMP_SQL[op]), sql.SQL(pg_type)), [value]

    if op in ('contains', 'starts', 'ends'):
        s = str(value)
        pattern = {'contains': f'%{s}%', 'starts': f'{s}%',
                   'ends': f'%{s}'}[op]
        return sql.SQL('{}::text ILIKE %s').format(col), [pattern]

    if op == 'between':
        lo, hi = value
        return sql.SQL('{} BETWEEN %s::{} AND %s::{}').format(
            col, sql.SQL(pg_type), sql.SQL(pg_type)), [lo, hi]

    if op == 'in':
        placeholders = sql.SQL(', ').join(
            sql.SQL('%s::{}').format(sql.SQL(pg_type)) for _ in value)
        return sql.SQL('{} IN ({})').format(col, placeholders), list(value)

    raise LayerQueryError(f'Неизвестный оператор: {op!r}.')


def build_filter(layer, filter_spec):
    """Скомпилировать структурный фильтр в ``(sql, params)`` или ``(None, [])``.

    ``sql`` — условие без обёртки ``WHERE`` (или ``None``, если правил нет).
    ``LayerQueryError`` — неизвестное поле/оператор или неверное значение.
    """
    if not filter_spec:
        return None, []
    if not isinstance(filter_spec, dict):
        raise LayerQueryError('Фильтр должен быть объектом.')

    match = str(filter_spec.get('match', 'all')).lower()
    if match not in ('all', 'any'):
        raise LayerQueryError('match должен быть "all" или "any".')
    rules = filter_spec.get('rules') or []
    if not isinstance(rules, list):
        raise LayerQueryError('rules должен быть списком.')

    types = _column_types(layer)
    parts, params = [], []
    for rule in rules:
        if not isinstance(rule, dict):
            raise LayerQueryError('Правило должно быть объектом.')
        field = rule.get('field')
        op = rule.get('op')
        if field not in types:
            raise LayerQueryError(f'Недопустимое поле: {field!r}.')
        if op not in OPERATORS:
            raise LayerQueryError(f'Недопустимый оператор: {op!r}.')

        arity = OPERATORS[op]
        value = rule.get('value')
        if arity == 0:
            value = None
        elif arity == 2:
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise LayerQueryError(
                    'Оператор "между" требует два значения.')
        elif arity == 'list':
            if not isinstance(value, (list, tuple)) or not value:
                raise LayerQueryError(
                    'Оператор "в списке" требует непустой список.')
        else:  # arity == 1
            if value is None or (isinstance(value, str) and value == ''):
                raise LayerQueryError('Укажите значение условия.')

        rule_sql, rule_params = _rule_sql(
            sql.Identifier(field), types[field], op, value)
        parts.append(rule_sql)
        params.extend(rule_params)

    if not parts:
        return None, []
    joiner = sql.SQL(' AND ') if match == 'all' else sql.SQL(' OR ')
    return sql.SQL('({})').format(joiner.join(parts)), params


def _search_sql(cols, query_text):
    """Подстрочный поиск (ILIKE) по всем колонкам + id → ``(sql, params)``."""
    q = (query_text or '').strip()
    if not q:
        return None, []
    pattern = f'%{q}%'
    search_cols = [sql.Identifier('id')] + [sql.Identifier(c) for c in cols]
    ors = sql.SQL(' OR ').join(
        sql.SQL('{}::text ILIKE %s').format(c) for c in search_cols)
    return sql.SQL('({})').format(ors), [pattern] * len(search_cols)


def build_where(layer, cols, filter_spec=None, query_text=''):
    """Собрать полный ``WHERE`` из структурного фильтра И подстрочного поиска.

    Args:
        layer: :class:`my_fields.models.GisLayer`.
        cols: список db-колонок слоя (для поиска по всем полям).
        filter_spec: спецификация визуального фильтра (см. модуль) или None.
        query_text: подстрока быстрого поиска (комбинируется через AND).

    Returns:
        ``(where_sql, params)`` — ``where_sql`` это ``sql.SQL('')`` (нет
        условий) либо ``sql.SQL(' WHERE (...)')``.
    """
    parts, params = [], []
    filt_sql, filt_params = build_filter(layer, filter_spec)
    if filt_sql is not None:
        parts.append(filt_sql)
        params.extend(filt_params)
    search_sql, search_params = _search_sql(cols, query_text)
    if search_sql is not None:
        parts.append(search_sql)
        params.extend(search_params)

    if not parts:
        return sql.SQL(''), []
    where = sql.SQL(' WHERE {}').format(sql.SQL(' AND ').join(parts))
    return where, params
