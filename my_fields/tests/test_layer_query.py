"""Тесты визуального конструктора SQL-выборки (services/layer_query.py).

Проверяем безопасную компиляцию структурного фильтра в параметризованный
WHERE (через list_features), комбинацию с подстрочным поиском, валидацию
(защита от инъекций/неизвестных полей и операторов) и материализацию
результата выборки в новый слой (create_layer_from_query).

Требуют PostGIS. Локально: $env:PROJ_LIB='' (конфликт PROJ/GDAL).
"""
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from psycopg import sql

from my_fields.models import GisLayer
from my_fields.services.layer_query import LayerQueryError, build_filter
from my_fields.services.shp_import import (
    create_empty_layer, create_layer_from_query, distinct_values, list_features,
)

User = get_user_model()

_ENV = (
    ('Alpha', 10, 1.5, 34.10, 45.10),
    ('Beta', 20, 2.5, 34.20, 45.20),
    ('Gamma', 30, None, 34.30, 45.30),
)


class LayerQueryTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('alice', password='x')
        self.layer = create_empty_layer(
            'Участки', 'polygon',
            attributes=[
                {'name': 'name', 'type': 'text'},
                {'name': 'num', 'type': 'integer'},
                {'name': 'area', 'type': 'double precision'},
            ],
            owner=self.user,
        )
        t = sql.Identifier(self.layer.table_name)
        with connection.cursor() as cur:
            for name, num, area, x, y in _ENV:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (name, num, area, geom) VALUES '
                    '(%s, %s, %s, ST_SetSRID('
                    'ST_MakeEnvelope(%s, %s, %s, %s), 4326))'
                ).format(t=t),
                    [name, num, area, x, y, x + 0.05, y + 0.05])

    def _count(self, filter_spec=None, q=''):
        return list_features(
            self.layer, query_text=q, filter_spec=filter_spec)['total']

    def _names(self, filter_spec=None, q=''):
        data = list_features(self.layer, query_text=q, filter_spec=filter_spec)
        return sorted(r['props']['name'] for r in data['results'])

    # ── операторы ──
    def test_eq(self):
        self.assertEqual(
            self._names({'rules': [{'field': 'num', 'op': 'eq', 'value': 20}]}),
            ['Beta'])

    def test_neq_includes_null_rows(self):
        # IS DISTINCT FROM: строка с area=NULL отличается от 1.5.
        self.assertEqual(
            self._names({'rules': [
                {'field': 'area', 'op': 'neq', 'value': 1.5}]}),
            ['Beta', 'Gamma'])

    def test_gt(self):
        self.assertEqual(
            self._names({'rules': [{'field': 'num', 'op': 'gt', 'value': 15}]}),
            ['Beta', 'Gamma'])

    def test_between(self):
        self.assertEqual(
            self._names({'rules': [
                {'field': 'num', 'op': 'between', 'value': [15, 25]}]}),
            ['Beta'])

    def test_in(self):
        self.assertEqual(
            self._names({'rules': [
                {'field': 'num', 'op': 'in', 'value': [10, 30]}]}),
            ['Alpha', 'Gamma'])

    def test_contains_ilike(self):
        self.assertEqual(
            self._names({'rules': [
                {'field': 'name', 'op': 'contains', 'value': 'mm'}]}),
            ['Gamma'])

    def test_starts_ends(self):
        self.assertEqual(
            self._names({'rules': [
                {'field': 'name', 'op': 'starts', 'value': 'Al'}]}),
            ['Alpha'])
        self.assertEqual(
            self._names({'rules': [
                {'field': 'name', 'op': 'ends', 'value': 'ta'}]}),
            ['Beta'])

    def test_is_null_not_null(self):
        self.assertEqual(
            self._names({'rules': [{'field': 'area', 'op': 'is_null'}]}),
            ['Gamma'])
        self.assertEqual(
            self._names({'rules': [{'field': 'area', 'op': 'not_null'}]}),
            ['Alpha', 'Beta'])

    def test_match_any(self):
        self.assertEqual(
            self._names({'match': 'any', 'rules': [
                {'field': 'num', 'op': 'eq', 'value': 10},
                {'field': 'num', 'op': 'eq', 'value': 30}]}),
            ['Alpha', 'Gamma'])

    def test_filter_combined_with_search(self):
        # filter (num>0) AND q='Beta'
        self.assertEqual(
            self._names({'rules': [{'field': 'num', 'op': 'gt', 'value': 0}]},
                        q='Beta'),
            ['Beta'])

    def test_empty_filter_returns_all(self):
        self.assertEqual(self._count(None), 3)
        self.assertEqual(self._count({'rules': []}), 3)

    # ── валидация / безопасность ──
    def test_unknown_field_rejected(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'rules': [
                {'field': 'nope', 'op': 'eq', 'value': 1}]})

    def test_injection_field_rejected(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'rules': [
                {'field': 'num; DROP TABLE x', 'op': 'eq', 'value': 1}]})

    def test_unknown_op_rejected(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'rules': [
                {'field': 'num', 'op': 'regex', 'value': 1}]})

    def test_between_requires_two_values(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'rules': [
                {'field': 'num', 'op': 'between', 'value': [1]}]})

    def test_in_requires_nonempty_list(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'rules': [
                {'field': 'num', 'op': 'in', 'value': []}]})

    def test_missing_value_rejected(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'rules': [
                {'field': 'num', 'op': 'eq', 'value': ''}]})

    def test_bad_match_rejected(self):
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, {'match': 'xor', 'rules': []})

    # ── вложенные группы (value-фильтр по столбцам ∧ конструктор) ──
    def test_nested_group_and_of_any(self):
        # (num IN 10,20,30) AND (name = Alpha OR name = Beta) → Alpha, Beta.
        spec = {'match': 'all', 'rules': [
            {'field': 'num', 'op': 'in', 'value': [10, 20, 30]},
            {'match': 'any', 'rules': [
                {'field': 'name', 'op': 'eq', 'value': 'Alpha'},
                {'field': 'name', 'op': 'eq', 'value': 'Beta'},
            ]},
        ]}
        self.assertEqual(self._names(spec), ['Alpha', 'Beta'])

    def test_nested_group_null_or_in(self):
        # value-фильтр по столбцу с выбором «(пусто)»: (area IN 1.5) OR NULL.
        spec = {'match': 'all', 'rules': [
            {'match': 'any', 'rules': [
                {'field': 'area', 'op': 'in', 'value': [1.5]},
                {'field': 'area', 'op': 'is_null'},
            ]},
        ]}
        self.assertEqual(self._names(spec), ['Alpha', 'Gamma'])

    def test_nested_empty_group_ignored(self):
        # Пустая под-группа не добавляет условий (эквивалент отсутствия фильтра).
        self.assertEqual(self._count({'rules': [{'rules': []}]}), 3)

    def test_nested_depth_limit_rejected(self):
        spec = {'rules': []}
        node = spec
        for _ in range(7):
            child = {'rules': []}
            node['rules'].append(child)
            node = child
        node['rules'].append({'field': 'num', 'op': 'eq', 'value': 10})
        with self.assertRaises(LayerQueryError):
            build_filter(self.layer, spec)

    # ── distinct_values (перечень значений для кебаб-фильтра) ──
    def test_distinct_values_any_type(self):
        info = distinct_values(self.layer, 'num')
        self.assertEqual(
            sorted(v['value'] for v in info['values']), ['10', '20', '30'])
        self.assertFalse(info['has_null'])

    def test_distinct_values_reports_null(self):
        info = distinct_values(self.layer, 'area')
        self.assertTrue(info['has_null'])
        self.assertIn(None, [v['value'] for v in info['values']])

    def test_distinct_values_unknown_field(self):
        self.assertIsNone(distinct_values(self.layer, 'nope'))

    # ── материализация выборки в новый слой ──
    def test_create_layer_from_query(self):
        new_layer = create_layer_from_query(
            self.layer, 'num>15',
            filter_spec={'rules': [{'field': 'num', 'op': 'gt', 'value': 15}]},
            owner=self.user)
        self.assertIsInstance(new_layer, GisLayer)
        self.assertEqual(new_layer.feature_count, 2)
        self.assertEqual(new_layer.geom_kind, 'polygon')
        self.assertEqual(new_layer.attributes, self.layer.attributes)
        self.assertNotEqual(new_layer.table_name, self.layer.table_name)
        data = list_features(new_layer)
        self.assertEqual(sorted(r['props']['name'] for r in data['results']),
                         ['Beta', 'Gamma'])
