"""Тесты компилятора SQL-ФОРМУЛЫ для заполнения атрибута слоя.

Проверяем два контура:

* :func:`my_fields.services.layer_expr.compile_expression` — что разрешённые
  формулы реально вычисляются Postgres-ом (через ``fill_column(expr=...)``),
  а всё подозрительное (``;``, подзапрос, кавычки, неизвестные имена и
  функции) отвергается ДО обращения к БД;
* область применения формулы — та же, что у заполнения значением (фильтр,
  список id, «только пустые»).

Требуют PostGIS. Локально: $env:PROJ_LIB='' (конфликт PROJ/GDAL).
"""
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from psycopg import sql

from my_fields.services.layer_expr import LayerExprError, compile_expression
from my_fields.services.shp_import import (
    ShapefileImportError, create_empty_layer, fill_column,
)

User = get_user_model()

# (name, num, x, y) — квадрат ~0.05° от (x, y).
_ENV = (
    ('  Alpha ', 10, 34.10, 45.10),
    ('Beta', 20, 34.20, 45.20),
    (None, None, 34.30, 45.30),
)


class LayerExprTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('alice', password='x')
        self.layer = create_empty_layer(
            'Участки', 'polygon',
            attributes=[
                {'name': 'name', 'type': 'text'},
                {'name': 'num', 'type': 'integer'},
                {'name': 'code', 'type': 'text'},
                {'name': 'area', 'type': 'double precision'},
            ],
            owner=self.user,
        )
        t = sql.Identifier(self.layer.table_name)
        self.ids = []
        with connection.cursor() as cur:
            for name, num, x, y in _ENV:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (name, num, geom) VALUES '
                    '(%s, %s, ST_SetSRID(ST_MakeEnvelope(%s, %s, %s, %s), '
                    '4326)) RETURNING id'
                ).format(t=t), [name, num, x, y, x + 0.05, y + 0.05])
                self.ids.append(cur.fetchone()[0])

    def _values(self, db):
        with connection.cursor() as cur:
            cur.execute(sql.SQL('SELECT id, {c} FROM {t} ORDER BY id').format(
                c=sql.Identifier(db), t=sql.Identifier(self.layer.table_name)))
            return {row[0]: row[1] for row in cur.fetchall()}


class CompileExpressionTests(LayerExprTestCase):
    """Только компиляция — без обращения к таблице."""

    def _sql(self, text):
        with connection.cursor() as cur:
            return compile_expression(self.layer, text).as_string(
                cur.cursor.connection)

    def test_column_is_quoted_identifier(self):
        self.assertIn('"name"', self._sql('upper(name)'))

    def test_string_literal_is_escaped(self):
        # Литерал уходит через sql.Literal — кавычка удваивается, а не
        # закрывает строку.
        out = self._sql("'a''b'")
        self.assertIn("'a''b'", out)

    def test_service_columns_allowed(self):
        self.assertIn('"geom"', self._sql('ST_Area(geom::geography)'))
        self.assertIn('"id"', self._sql('id + 1'))

    def test_case_when_keywords(self):
        out = self._sql("CASE WHEN num > 10 THEN 'big' ELSE 'small' END").upper()
        self.assertIn('CASE', out)
        self.assertIn('END', out)

    def test_double_precision_cast(self):
        self.assertIn('double precision', self._sql('num::double precision'))

    # ── отказы ──
    def test_empty_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, '   ')

    def test_semicolon_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, "1; DROP TABLE users")

    def test_double_quote_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'upper("name")')

    def test_subquery_rejected(self):
        # 'select' нет ни в ключевых словах, ни в функциях → неизвестное имя.
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, '(select max(num) from pg_class)')

    def test_unknown_column_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'upper(nope)')

    def test_unknown_function_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'pg_sleep(10)')

    def test_unknown_cast_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'num::regclass')

    def test_cast_without_type_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'num::')

    def test_unbalanced_parens_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'upper(name')
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, 'upper(name))')

    def test_too_long_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, '1 + ' * 900 + '1')

    def test_too_deep_rejected(self):
        with self.assertRaises(LayerExprError):
            compile_expression(self.layer, '(' * 21 + '1' + ')' * 21)


class FillByExpressionTests(LayerExprTestCase):
    """``fill_column(expr=...)`` — значение вычисляется для каждого объекта."""

    def test_area_in_hectares(self):
        n = fill_column(self.layer, 'area', None,
                        expr='ST_Area(geom::geography) / 10000')
        self.assertEqual(n, 3)
        for v in self._values('area').values():
            self.assertGreater(v, 0)

    def test_string_functions(self):
        fill_column(self.layer, 'code', None, expr='upper(trim(name))')
        vals = self._values('code')
        self.assertEqual(vals[self.ids[0]], 'ALPHA')
        self.assertEqual(vals[self.ids[1]], 'BETA')
        self.assertIsNone(vals[self.ids[2]])   # name IS NULL → NULL

    def test_concat_with_id(self):
        fill_column(self.layer, 'code', None,
                    expr="coalesce(name, '') || '-' || id")
        self.assertEqual(self._values('code')[self.ids[1]],
                         f'Beta-{self.ids[1]}')

    def test_case_expression(self):
        fill_column(self.layer, 'code', None,
                    expr="CASE WHEN num >= 20 THEN 'big' ELSE 'small' END")
        vals = self._values('code')
        self.assertEqual(vals[self.ids[0]], 'small')
        self.assertEqual(vals[self.ids[1]], 'big')

    def test_expr_wins_over_value(self):
        fill_column(self.layer, 'code', 'ЗНАЧЕНИЕ', expr="'ФОРМУЛА'")
        self.assertEqual(set(self._values('code').values()), {'ФОРМУЛА'})

    def test_expr_result_cast_to_column_type(self):
        # Результат формулы — double, столбец integer: приведение к типу
        # столбца делает сам UPDATE (round).
        fill_column(self.layer, 'num', None, expr='2.6')
        self.assertEqual(set(self._values('num').values()), {3})

    # ── область применения такая же, как у заполнения значением ──
    def test_expr_only_selected_ids(self):
        n = fill_column(self.layer, 'code', None, expr='upper(name)',
                        ids=[self.ids[0]])
        self.assertEqual(n, 1)
        self.assertIsNone(self._values('code')[self.ids[1]])

    def test_expr_with_filter_spec(self):
        n = fill_column(
            self.layer, 'code', None, expr="'X'",
            filter_spec={'match': 'all',
                         'rules': [{'field': 'num', 'op': 'gte', 'value': 20}]})
        self.assertEqual(n, 1)
        self.assertEqual(self._values('code')[self.ids[1]], 'X')

    def test_expr_only_empty(self):
        fill_column(self.layer, 'code', 'занято', ids=[self.ids[0]])
        n = fill_column(self.layer, 'code', None, expr="'новое'",
                        only_empty=True)
        self.assertEqual(n, 2)
        self.assertEqual(self._values('code')[self.ids[0]], 'занято')

    # ── ошибки ──
    def test_invalid_expr_raises_layer_expr_error(self):
        with self.assertRaises(LayerExprError):
            fill_column(self.layer, 'code', None, expr='pg_sleep(1)')

    def test_expr_rejected_by_postgres_is_400_error(self):
        # Формула разобрана нами, но Postgres отвергает типы аргументов —
        # это ошибка пользователя (400), а не 500.
        with self.assertRaises(ShapefileImportError):
            fill_column(self.layer, 'code', None, expr='upper(num, name)')

    def test_blank_expr_falls_back_to_value(self):
        fill_column(self.layer, 'code', 'знач', expr='   ')
        self.assertEqual(set(self._values('code').values()), {'знач'})
