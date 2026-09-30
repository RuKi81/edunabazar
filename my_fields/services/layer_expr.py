"""Безопасный компилятор SQL-ФОРМУЛЫ для заполнения атрибута слоя.

Массовое заполнение столбца одним значением (см.
:func:`my_fields.services.shp_import.fill_column`) не покрывает частые задачи
вида «площадь в гектарах», «код = район + номер», «привести название к
верхнему регистру». Для них в панели «✎ Заполнить» есть отдельная строка
ФОРМУЛЫ: текст компилируется здесь в SQL-выражение правой части ``UPDATE
... SET col = (<выражение>)``.

Сырой SQL от клиента НЕ исполняется: текст разбирается на токены и
собирается заново через ``psycopg.sql``, причём разрешены только

* колонки текущего слоя (плюс служебные ``id`` и ``geom``) —
  подставляются как ``sql.Identifier``;
* числовые и строковые литералы (строки — через ``sql.Literal``);
* операторы из :data:`OPERATORS` и ключевые слова из :data:`KEYWORDS`
  (``CASE/WHEN/THEN/ELSE/END``, ``AND/OR/NOT``, ``IS NULL`` и т. п.);
* функции из белого списка :data:`FUNCTIONS` (строковые, числовые, дата,
  плюс базовые PostGIS);
* приведение типов ``::`` к типам из :data:`CAST_TYPES`.

Всё остальное (точка с запятой, подзапросы, неизвестные имена и функции)
отвергается с понятным сообщением для UI. Подзапрос невозможен по
построению: ``select`` не входит ни в ключевые слова, ни в функции.

Примеры формул::

    ST_Area(geom::geography) / 10000          -- площадь, га
    upper(trim(name))
    coalesce(code, '') || '-' || id
    CASE WHEN area > 100 THEN 'крупное' ELSE 'мелкое' END
"""
from __future__ import annotations

import re

from psycopg import sql


class LayerExprError(ValueError):
    """Некорректная формула (сообщение пригодно для показа в UI)."""


# Предел длины формулы — защита от мусорного ввода и тяжёлых выражений.
MAX_EXPR_LEN = 2000
# Предел глубины скобок.
MAX_DEPTH = 20

# Ключевые слова (без них не собрать CASE/логику). Регистр не важен.
KEYWORDS = {
    'and', 'or', 'not', 'is', 'null', 'true', 'false',
    'case', 'when', 'then', 'else', 'end',
    'between', 'in', 'like', 'ilike', 'distinct', 'from',
}

# Функции, разрешённые в формуле (нижний регистр). Только детерминированные
# вычисления над строками/числами/датами и базовая геометрия PostGIS —
# ничего, что читает другие таблицы или меняет состояние.
FUNCTIONS = {
    # строки
    'upper', 'lower', 'initcap', 'trim', 'btrim', 'ltrim', 'rtrim',
    'length', 'char_length', 'substr', 'substring', 'left', 'right',
    'replace', 'concat', 'concat_ws', 'lpad', 'rpad', 'split_part',
    'position', 'strpos', 'md5', 'to_char', 'format', 'regexp_replace',
    # числа
    'abs', 'round', 'ceil', 'ceiling', 'floor', 'trunc', 'sqrt', 'power',
    'mod', 'greatest', 'least', 'sign', 'exp', 'ln', 'log', 'random',
    # NULL/условия
    'coalesce', 'nullif',
    # дата/время
    'now', 'age', 'date_part', 'date_trunc', 'extract', 'to_date',
    'to_number', 'to_timestamp',
    # геометрия (PostGIS) — площади/длины/координаты текущего объекта
    'st_area', 'st_perimeter', 'st_length', 'st_x', 'st_y',
    'st_centroid', 'st_pointonsurface', 'st_astext', 'st_geometrytype',
    'st_npoints', 'st_numgeometries', 'st_srid', 'st_isvalid',
    'st_distance', 'st_transform', 'st_buffer', 'st_envelope',
    'st_xmin', 'st_xmax', 'st_ymin', 'st_ymax',
}

# Типы для ``::`` (подмножество, совпадающее с типами колонок слоя + то,
# что нужно для геодезических расчётов).
CAST_TYPES = {
    'text', 'varchar', 'char', 'integer', 'int', 'int4', 'int8', 'bigint',
    'smallint', 'numeric', 'decimal', 'real', 'float', 'float8',
    'double precision', 'boolean', 'bool', 'date', 'timestamp',
    'timestamptz', 'geometry', 'geography', 'json', 'jsonb',
}

# Операторы (многосимвольные — первыми, иначе '<=' распадётся на '<' и '=').
OPERATORS = ('::', '||', '<>', '!=', '<=', '>=', '+', '-', '*', '/', '%',
             '<', '>', '=', '(', ')', ',')

_TOKEN_RE = re.compile(
    r"""
      (?P<ws>\s+)
    | (?P<num>\d+(?:\.\d+)?)
    | '(?P<str>(?:[^']|'')*)'
    | (?P<ident>[A-Za-z_\u0400-\u04FF][A-Za-z_0-9\u0400-\u04FF]*)
    | (?P<op>::|\|\||<>|!=|<=|>=|[-+*/%<>=(),])
    """,
    re.VERBOSE,
)


def _tokenize(text: str):
    """Разбить формулу на токены ``(kind, value)``.

    Raises:
        LayerExprError: недопустимый символ (в т. ч. ``;`` и кавычка ``"``).
    """
    tokens = []
    pos = 0
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if m is None:
            raise LayerExprError(
                f'Недопустимый символ в формуле: {text[pos]!r}.')
        pos = m.end()
        if m.lastgroup == 'ws':
            continue
        if m.lastgroup == 'str':
            # '' внутри литерала — экранированная одинарная кавычка.
            tokens.append(('str', (m.group('str') or '').replace("''", "'")))
        else:
            tokens.append((m.lastgroup, m.group(m.lastgroup)))
    if not tokens:
        raise LayerExprError('Формула пустая.')
    return tokens


def _columns(layer) -> dict:
    """``{db_имя_в_нижнем_регистре: db_имя}`` — колонки, доступные в формуле."""
    from .shp_import import _attr_db_types

    cols = {'id': 'id', 'geom': 'geom'}
    for db in _attr_db_types(layer):
        cols[db.lower()] = db
    return cols


def _cast_type(tokens, i):
    """Тип после ``::`` → ``(тип, следующий индекс)``.

    Отдельно обрабатывается двухсловный ``double precision``.
    """
    if i >= len(tokens) or tokens[i][0] != 'ident':
        raise LayerExprError('После :: должен идти тип (например, ::numeric).')
    name = tokens[i][1].lower()
    nxt = i + 1
    if (name == 'double' and nxt < len(tokens)
            and tokens[nxt][0] == 'ident'
            and tokens[nxt][1].lower() == 'precision'):
        name, nxt = 'double precision', nxt + 1
    if name not in CAST_TYPES:
        raise LayerExprError(f'Недопустимый тип приведения: {name!r}.')
    return name, nxt


def _ident_sql(tokens, i, columns):
    """Скомпилировать идентификатор (функция / ключевое слово / колонка)."""
    raw = tokens[i][1]
    low = raw.lower()
    is_call = (i + 1 < len(tokens) and tokens[i + 1] == ('op', '('))
    if is_call:
        if low not in FUNCTIONS:
            raise LayerExprError(f'Функция {raw!r} не разрешена в формуле.')
        return sql.SQL(low)
    if low in KEYWORDS:
        return sql.SQL(low.upper())
    if low in columns:
        return sql.Identifier(columns[low])
    raise LayerExprError(
        f'Неизвестное имя {raw!r}: разрешены только столбцы слоя, '
        f'geom, id и функции из списка.')


def _op_sql(value: str, depth: int):
    """Оператор/скобка → ``(sql, новая глубина вложенности)``.

    Raises:
        LayerExprError: лишняя закрывающая скобка / слишком глубокая
            вложенность.
    """
    if value == '(':
        depth += 1
        if depth > MAX_DEPTH:
            raise LayerExprError('Слишком глубокая вложенность скобок.')
    elif value == ')':
        depth -= 1
        if depth < 0:
            raise LayerExprError('Лишняя закрывающая скобка.')
    return sql.SQL(value), depth


def _compile_tokens(tokens, columns) -> list:
    """Токены → список Composable-частей выражения (с проверкой скобок)."""
    parts = []
    depth = 0
    i = 0
    while i < len(tokens):
        kind, value = tokens[i]
        if kind == 'num':
            parts.append(sql.SQL(value))
            i += 1
        elif kind == 'str':
            parts.append(sql.Literal(value))
            i += 1
        elif kind == 'ident':
            parts.append(_ident_sql(tokens, i, columns))
            i += 1
        elif value == '::':
            cast, i = _cast_type(tokens, i + 1)
            parts.append(sql.SQL('::' + cast))
        else:
            part, depth = _op_sql(value, depth)
            parts.append(part)
            i += 1
    if depth:
        raise LayerExprError('Не закрыта скобка.')
    return parts


def compile_expression(layer, text: str):
    """Скомпилировать формулу в SQL-выражение (``psycopg.sql.Composed``).

    Args:
        layer: :class:`my_fields.models.GisLayer` — источник списка колонок.
        text: текст формулы из UI.

    Returns:
        Composable вида ``(<выражение>)`` — готов к подстановке в ``SET``.

    Raises:
        LayerExprError: пустая/слишком длинная формула, недопустимый символ,
            неизвестное имя или функция, несбалансированные скобки.
    """
    text = (text or '').strip()
    if not text:
        raise LayerExprError('Формула пустая.')
    if len(text) > MAX_EXPR_LEN:
        raise LayerExprError(
            f'Формула слишком длинная (> {MAX_EXPR_LEN} символов).')

    parts = _compile_tokens(_tokenize(text), _columns(layer))
    return sql.SQL('({})').format(sql.SQL(' ').join(parts))
