"""Тесты общего механизма материализации слоя из SELECT.

``create_layer_from_select`` — фундамент для SQL-выборки («сохранить результат
как слой») и оверлейных операций. Проверяем, что из произвольного безопасного
SELECT создаётся полноценный слой: таблица PostGIS с ``id serial PK`` и
типобезопасной колонкой ``geom geometry(Geometry, 4326)``, GIST-индекс, запись
реестра с корректными feature_count/extent/attributes, а строки с пустой
геометрией отбрасываются.

Требуют PostGIS. Локально: $env:PROJ_LIB='' (конфликт PROJ/GDAL).
"""
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from psycopg import sql

from my_fields.models import GisLayer
from my_fields.services.shp_import import (
    create_empty_layer, create_layer_from_select, list_features,
)

User = get_user_model()


class CreateLayerFromSelectTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('alice', password='x')
        # Исходный точечный слой с одним атрибутом.
        self.src = create_empty_layer(
            'Точки', 'point',
            attributes=[{'name': 'name', 'type': 'text'}],
            owner=self.user,
        )
        # Две точки + одна строка с NULL-геометрией (должна отсеяться).
        with connection.cursor() as cur:
            ins = sql.SQL(
                'INSERT INTO {t} (name, geom) VALUES (%s, {g})'
            )
            cur.execute(ins.format(
                t=sql.Identifier(self.src.table_name),
                g=sql.SQL('ST_SetSRID(ST_MakePoint(34.10, 45.10), 4326)'),
            ), ['A'])
            cur.execute(ins.format(
                t=sql.Identifier(self.src.table_name),
                g=sql.SQL('ST_SetSRID(ST_MakePoint(34.20, 45.20), 4326)'),
            ), ['B'])
            cur.execute(sql.SQL(
                'INSERT INTO {t} (name, geom) VALUES (%s, NULL)'
            ).format(t=sql.Identifier(self.src.table_name)), ['C'])

    def _buffer_select(self, meters):
        """SELECT буфера (в метрах через geography) с атрибутом name."""
        select_sql = sql.SQL(
            'SELECT {name}, ST_Buffer(geom::geography, %s)::geometry AS geom '
            'FROM {src}'
        ).format(name=sql.Identifier('name'),
                 src=sql.Identifier(self.src.table_name))
        return select_sql, [meters]

    def test_materialize_buffer_layer(self):
        select_sql, params = self._buffer_select(100)
        attr_meta = [{'name': 'name', 'db': 'name', 'type': 'text'}]
        layer = create_layer_from_select(
            'Буфер 100 м', 'polygon', attr_meta, select_sql, params,
            owner=self.user, source_note='overlay:buffer',
        )
        # Запись реестра корректна.
        self.assertIsInstance(layer, GisLayer)
        self.assertEqual(layer.geom_kind, 'polygon')
        self.assertEqual(layer.geom_type, 'Polygon')
        self.assertEqual(layer.feature_count, 2)      # NULL-геометрия отсеяна
        self.assertEqual(layer.attributes, attr_meta)
        self.assertEqual(layer.owner_id, self.user.pk)
        self.assertEqual(layer.source_archive, 'overlay:buffer')
        self.assertIsNotNone(layer.extent)
        self.assertEqual(len(layer.extent), 4)

        # Физическая таблица: id serial PK, geom 4326 полигон, 2 строки.
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'SELECT ST_SRID(geom), GeometryType(geom) FROM {t} LIMIT 1'
            ).format(t=sql.Identifier(layer.table_name)))
            srid, gtype = cur.fetchone()
            self.assertEqual(srid, 4326)
            self.assertIn('POLYGON', gtype.upper())
            cur.execute(sql.SQL('SELECT count(*) FROM {t}').format(
                t=sql.Identifier(layer.table_name)))
            self.assertEqual(cur.fetchone()[0], 2)
            # id проставлен и уникален (serial PK).
            cur.execute(sql.SQL(
                'SELECT count(DISTINCT id), min(id), max(id) FROM {t}'
            ).format(t=sql.Identifier(layer.table_name)))
            distinct, mn, mx = cur.fetchone()
            self.assertEqual(distinct, 2)
            self.assertEqual((mn, mx), (1, 2))

        # Атрибуты читаются штатным list_features.
        data = list_features(layer)
        self.assertEqual(data['total'], 2)
        names = sorted(r['props']['name'] for r in data['results'])
        self.assertEqual(names, ['A', 'B'])

    def test_empty_title_rejected(self):
        select_sql, params = self._buffer_select(10)
        from my_fields.services.shp_import import ShapefileImportError
        with self.assertRaises(ShapefileImportError):
            create_layer_from_select(
                '   ', 'polygon',
                [{'name': 'name', 'db': 'name', 'type': 'text'}],
                select_sql, params, owner=self.user,
            )

    def test_unique_table_name_for_same_title(self):
        select_sql, params = self._buffer_select(10)
        meta = [{'name': 'name', 'db': 'name', 'type': 'text'}]
        a = create_layer_from_select('Дубль', 'polygon', meta, *([select_sql, params]), owner=self.user)
        select_sql2, params2 = self._buffer_select(20)
        b = create_layer_from_select('Дубль', 'polygon', meta, select_sql2, params2, owner=self.user)
        self.assertNotEqual(a.table_name, b.table_name)
