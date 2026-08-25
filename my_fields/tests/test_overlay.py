"""Тесты оверлейных операций между слоями (services/overlay.py).

Геометрия задаётся квадратами в EPSG:4326 (координаты около 0 — площади в
градусах², этого достаточно для относительных проверок). Фиксируем состав
результата (feature_count), сохранение атрибутов слоя A и суммарную площадь.

Требуют PostGIS.
"""
import json
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.db import connection
from django.test import TestCase
from django.utils import timezone
from psycopg import sql

from access.models import ResourceGrant
from agrocosmos.models import PipelineRun
from legacy.models import LegacyUser
from my_fields.models import GisLayer
from my_fields.services.overlay import (
    OVERLAY_OPS, SINGLE_OPS, OverlayError, run_overlay, run_single,
    run_spatial_join,
)
from my_fields.services.shp_import import create_empty_layer

User = get_user_model()

GL = ResourceGrant.ResourceType.GIS_LAYER


def _mk_legacy(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RUB', name=username,
        address='', phone='', inn='', status=10,
        created_at=now, updated_at=now, contacts='',
    )


def _square(x0, y0, x1, y1):
    return (f'POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, '
            f'{x0} {y1}, {x0} {y0}))')


class OverlayTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('alice', password='x')
        # Слой A: два непересекающихся квадрата 2×2 (площадь 4 каждый).
        self.a = create_empty_layer(
            'A', 'polygon',
            attributes=[{'name': 'name', 'type': 'text'}], owner=self.user)
        self._insert(self.a, [('a1', _square(0, 0, 2, 2)),
                              ('a2', _square(10, 10, 12, 12))])
        # Слой B: один квадрат 2×2, перекрывает A1 на 1×1.
        self.b = create_empty_layer(
            'B', 'polygon',
            attributes=[{'name': 'label', 'type': 'text'}], owner=self.user)
        self._insert(self.b, [('b1', _square(1, 1, 3, 3))], col='label')

    def _insert(self, layer, rows, col='name'):
        t = sql.Identifier(layer.table_name)
        c = sql.Identifier(col)
        with connection.cursor() as cur:
            for val, wkt in rows:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} ({c}, geom) VALUES '
                    '(%s, ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t, c=c), [val, wkt])

    def _total_area(self, layer):
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'SELECT COALESCE(SUM(ST_Area(geom)), 0) FROM {t}'
            ).format(t=sql.Identifier(layer.table_name)))
            return float(cur.fetchone()[0])

    # ── операции ──
    def test_intersection(self):
        out = run_overlay(self.a, self.b, 'intersection', 'A∩B', owner=self.user)
        self.assertIsInstance(out, GisLayer)
        self.assertEqual(out.geom_kind, 'polygon')
        self.assertEqual(out.feature_count, 1)          # только A1×B1
        self.assertEqual(out.attributes, self.a.attributes)  # атрибуты A
        self.assertAlmostEqual(self._total_area(out), 1.0, places=6)

    def test_difference(self):
        out = run_overlay(self.a, self.b, 'difference', 'A−B', owner=self.user)
        self.assertEqual(out.feature_count, 2)          # A1 (обрезан) + A2
        self.assertEqual(out.attributes, self.a.attributes)
        self.assertAlmostEqual(self._total_area(out), 7.0, places=6)  # 3 + 4

    def test_union(self):
        out = run_overlay(self.a, self.b, 'union', 'A∪B', owner=self.user)
        self.assertEqual(out.feature_count, 1)          # один (multi)polygon
        self.assertEqual(out.attributes, [])            # dissolve — без атрибутов
        self.assertAlmostEqual(self._total_area(out), 11.0, places=6)  # 7 + 4

    def test_symmetric_difference(self):
        out = run_overlay(self.a, self.b, 'symmetric_difference', 'A△B',
                          owner=self.user)
        self.assertEqual(out.feature_count, 1)
        self.assertEqual(out.attributes, [])
        self.assertAlmostEqual(self._total_area(out), 10.0, places=6)

    def test_intersection_keeps_attribute_values(self):
        out = run_overlay(self.a, self.b, 'intersection', 'A∩B', owner=self.user)
        with connection.cursor() as cur:
            cur.execute(sql.SQL('SELECT name FROM {t}').format(
                t=sql.Identifier(out.table_name)))
            names = [r[0] for r in cur.fetchall()]
        self.assertEqual(names, ['a1'])

    # ── валидация ──
    def test_unknown_op(self):
        with self.assertRaises(OverlayError):
            run_overlay(self.a, self.b, 'nope', 'X', owner=self.user)

    def test_non_polygon_rejected(self):
        line = create_empty_layer('L', 'line', owner=self.user)
        with self.assertRaises(OverlayError):
            run_overlay(self.a, line, 'intersection', 'X', owner=self.user)

    def test_empty_title_rejected(self):
        with self.assertRaises(OverlayError):
            run_overlay(self.a, self.b, 'union', '   ', owner=self.user)

    def test_ops_registry_labels(self):
        for op in ('intersection', 'difference', 'union', 'symmetric_difference'):
            self.assertIn(op, OVERLAY_OPS)


class OverlayEndpointTests(TestCase):
    """POST /overlay/ ставит задачу; воркер/команда её выполняет; GET отдаёт слой."""

    def setUp(self):
        self.dj = User.objects.create_user('mgr', password='x')
        self.lu = _mk_legacy('mgr')
        # whole-class manage + view — достаточно для создания и чтения слоёв.
        ResourceGrant.objects.create(
            legacy_user=self.lu, resource_type=GL, resource_id=None, level='manage')
        ResourceGrant.objects.create(
            legacy_user=self.lu, resource_type=GL, resource_id=None, level='view')

        self.a = create_empty_layer(
            'A', 'polygon', attributes=[{'name': 'name', 'type': 'text'}],
            owner=self.dj)
        self.b = create_empty_layer(
            'B', 'polygon', attributes=[{'name': 'name', 'type': 'text'}],
            owner=self.dj)
        self._insert(self.a, [('a1', _square(0, 0, 2, 2))])
        self._insert(self.b, [('b1', _square(1, 1, 3, 3))])

        self.client.force_login(self.dj)
        session = self.client.session
        session['legacy_user_id'] = self.lu.pk
        session.save()

    def _insert(self, layer, rows):
        t = sql.Identifier(layer.table_name)
        with connection.cursor() as cur:
            for val, wkt in rows:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (name, geom) VALUES '
                    '(%s, ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t), [val, wkt])

    def _create(self, payload):
        return self.client.post(
            '/me/gis/api/overlay/', data=json.dumps(payload),
            content_type='application/json')

    # ── постановка задачи ──
    def test_create_enqueues_run(self):
        r = self._create({'layer_a_id': self.a.pk, 'layer_b_id': self.b.pk,
                          'op': 'intersection', 'title': 'A∩B'})
        self.assertEqual(r.status_code, 202, r.content)
        run_id = r.json()['run_id']
        run = PipelineRun.objects.get(pk=run_id)
        self.assertEqual(run.task_type, PipelineRun.TaskType.GIS_OVERLAY)
        self.assertEqual(run.status, PipelineRun.Status.QUEUED)
        self.assertEqual(run.launch_args['op'], 'intersection')

    # ── полный цикл: постановка → команда → статус ──
    def test_full_cycle_via_command(self):
        run_id = self._create(
            {'layer_a_id': self.a.pk, 'layer_b_id': self.b.pk,
             'op': 'intersection', 'title': 'Пересечение'}).json()['run_id']
        call_command('run_gis_overlay', run_id=run_id)

        r = self.client.get(f'/me/gis/api/overlay/{run_id}/')
        self.assertEqual(r.status_code, 200, r.content)
        body = r.json()
        self.assertEqual(body['status'], 'completed')
        self.assertIsNotNone(body['layer'])
        self.assertEqual(body['layer']['feature_count'], 1)
        self.assertEqual(body['records_count'], 1)
        # Слой действительно создан в реестре.
        self.assertTrue(GisLayer.objects.filter(pk=body['layer']['id']).exists())

    # ── полный цикл через воркер (--once) ──
    def test_full_cycle_via_worker(self):
        run_id = self._create(
            {'layer_a_id': self.a.pk, 'layer_b_id': self.b.pk,
             'op': 'union', 'title': 'Объединение'}).json()['run_id']
        # Воркер вызывает close_old_connections(); при CONN_MAX_AGE=0 это
        # закрыло бы соединение, обёрнутое транзакцией TestCase. Глушим на
        # время теста — диспетчеризация задачи проверяется, соединение живо.
        with mock.patch(
            'agrocosmos.management.commands.run_ndvi_worker.close_old_connections'
        ):
            call_command('run_ndvi_worker', once=True)
        run = PipelineRun.objects.get(pk=run_id)
        self.assertEqual(run.status, PipelineRun.Status.COMPLETED)
        self.assertEqual(run.records_count, 1)

    # ── валидация ──
    def test_same_layer_rejected(self):
        r = self._create({'layer_a_id': self.a.pk, 'layer_b_id': self.a.pk,
                          'op': 'union', 'title': 'X'})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'same_layer')

    def test_invalid_op_rejected(self):
        r = self._create({'layer_a_id': self.a.pk, 'layer_b_id': self.b.pk,
                          'op': 'nope', 'title': 'X'})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'invalid_op')

    def test_empty_title_rejected(self):
        r = self._create({'layer_a_id': self.a.pk, 'layer_b_id': self.b.pk,
                          'op': 'union', 'title': '  '})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'empty_title')

    def test_non_polygon_rejected(self):
        line = create_empty_layer('L', 'line', owner=self.dj)
        r = self._create({'layer_a_id': self.a.pk, 'layer_b_id': line.pk,
                          'op': 'intersection', 'title': 'X'})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'not_polygon')

    def test_anonymous_forbidden(self):
        self.client.logout()
        r = self._create({'layer_a_id': self.a.pk, 'layer_b_id': self.b.pk,
                          'op': 'union', 'title': 'X'})
        self.assertIn(r.status_code, (401, 403))

    def test_status_not_found(self):
        r = self.client.get('/me/gis/api/overlay/999999/')
        self.assertEqual(r.status_code, 404)


class SingleOpTests(TestCase):
    """Одно-слойные операции: buffer / dissolve / centroids / simplify."""

    def setUp(self):
        self.user = User.objects.create_user('sam', password='x')
        # Два непересекающихся квадрата 2×2 с атрибутом name.
        self.a = create_empty_layer(
            'A', 'polygon',
            attributes=[{'name': 'name', 'type': 'text'}], owner=self.user)
        self._insert(self.a, [('a1', _square(0, 0, 2, 2)),
                              ('a2', _square(10, 10, 12, 12))])

    def _insert(self, layer, rows, col='name'):
        t = sql.Identifier(layer.table_name)
        c = sql.Identifier(col)
        with connection.cursor() as cur:
            for val, wkt in rows:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} ({c}, geom) VALUES '
                    '(%s, ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t, c=c), [val, wkt])

    def _total_area(self, layer):
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'SELECT COALESCE(SUM(ST_Area(geom)), 0) FROM {t}'
            ).format(t=sql.Identifier(layer.table_name)))
            return float(cur.fetchone()[0])

    def test_buffer_grows_area_keeps_attrs(self):
        out = run_single(self.a, 'buffer', 'Буфер', owner=self.user,
                         params={'distance': 10000})
        self.assertEqual(out.geom_kind, 'polygon')
        self.assertEqual(out.feature_count, 2)               # объекты сохранены
        self.assertEqual(out.attributes, self.a.attributes)  # атрибуты сохранены
        self.assertGreater(self._total_area(out), self._total_area(self.a))

    def test_buffer_requires_distance(self):
        with self.assertRaises(OverlayError):
            run_single(self.a, 'buffer', 'X', owner=self.user, params={})

    def test_centroids_produces_points(self):
        out = run_single(self.a, 'centroids', 'Центроиды', owner=self.user)
        self.assertEqual(out.geom_kind, 'point')
        self.assertEqual(out.feature_count, 2)
        self.assertEqual(out.attributes, self.a.attributes)
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'SELECT DISTINCT GeometryType(geom) FROM {t}'
            ).format(t=sql.Identifier(out.table_name)))
            self.assertEqual(cur.fetchone()[0], 'POINT')

    def test_simplify_keeps_kind_and_count(self):
        out = run_single(self.a, 'simplify', 'Упрощение', owner=self.user,
                         params={'tolerance': 1})
        self.assertEqual(out.geom_kind, 'polygon')
        self.assertEqual(out.feature_count, 2)

    def test_dissolve_all(self):
        out = run_single(self.a, 'dissolve', 'Dissolve', owner=self.user)
        self.assertEqual(out.geom_kind, 'polygon')
        self.assertEqual(out.feature_count, 1)          # одна (multi)геометрия
        self.assertEqual(out.attributes, [])
        self.assertAlmostEqual(self._total_area(out), 8.0, places=6)

    def test_dissolve_by_field(self):
        out = run_single(self.a, 'dissolve', 'DissolveBy', owner=self.user,
                         params={'field': 'name'})
        self.assertEqual(out.feature_count, 2)          # две группы a1/a2
        self.assertEqual([a['db'] for a in out.attributes], ['name'])

    def test_dissolve_non_polygon_rejected(self):
        line = create_empty_layer('L', 'line', owner=self.user)
        with self.assertRaises(OverlayError):
            run_single(line, 'dissolve', 'X', owner=self.user)

    def test_ops_registry(self):
        for op in ('buffer', 'dissolve', 'centroids', 'simplify'):
            self.assertIn(op, SINGLE_OPS)


class ClipEraseTests(TestCase):
    """Двух-слойные clip / erase."""

    def setUp(self):
        self.user = User.objects.create_user('cle', password='x')
        self.a = create_empty_layer(
            'A', 'polygon',
            attributes=[{'name': 'name', 'type': 'text'}], owner=self.user)
        self._insert(self.a, [('a1', _square(0, 0, 2, 2)),
                              ('a2', _square(10, 10, 12, 12))])
        self.b = create_empty_layer('B', 'polygon', owner=self.user)
        self._insert_geom(self.b, [_square(1, 1, 3, 3)])

    def _insert(self, layer, rows, col='name'):
        t = sql.Identifier(layer.table_name)
        c = sql.Identifier(col)
        with connection.cursor() as cur:
            for val, wkt in rows:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} ({c}, geom) VALUES '
                    '(%s, ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t, c=c), [val, wkt])

    def _insert_geom(self, layer, wkts):
        t = sql.Identifier(layer.table_name)
        with connection.cursor() as cur:
            for wkt in wkts:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (geom) VALUES '
                    '(ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t), [wkt])

    def _total_area(self, layer):
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'SELECT COALESCE(SUM(ST_Area(geom)), 0) FROM {t}'
            ).format(t=sql.Identifier(layer.table_name)))
            return float(cur.fetchone()[0])

    def test_clip_keeps_only_overlapping_a(self):
        out = run_overlay(self.a, self.b, 'clip', 'Clip', owner=self.user)
        self.assertEqual(out.feature_count, 1)          # только a1 (a2 не пересекает B)
        self.assertEqual(out.attributes, self.a.attributes)
        self.assertAlmostEqual(self._total_area(out), 1.0, places=6)

    def test_erase_equals_difference(self):
        out = run_overlay(self.a, self.b, 'erase', 'Erase', owner=self.user)
        self.assertEqual(out.feature_count, 2)          # a1 (обрезан) + a2
        self.assertEqual(out.attributes, self.a.attributes)
        self.assertAlmostEqual(self._total_area(out), 7.0, places=6)

    def test_clip_erase_in_registry(self):
        self.assertIn('clip', OVERLAY_OPS)
        self.assertIn('erase', OVERLAY_OPS)


class SpatialJoinTests(TestCase):
    """Пространственное соединение атрибутов (перенос B → A + агрегаты)."""

    def setUp(self):
        self.user = User.objects.create_user('spj', password='x')
        self.a = create_empty_layer(
            'A', 'polygon',
            attributes=[{'name': 'name', 'type': 'text'}], owner=self.user)
        with connection.cursor() as cur:
            t = sql.Identifier(self.a.table_name)
            for val, wkt in [('a1', _square(0, 0, 2, 2)),
                             ('a2', _square(10, 10, 12, 12))]:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (name, geom) VALUES '
                    '(%s, ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t), [val, wkt])
        # Точки с числовым атрибутом val: 2 внутри a1, 1 внутри a2.
        self.b = create_empty_layer(
            'B', 'point',
            attributes=[{'name': 'val', 'type': 'double precision'}],
            owner=self.user)
        with connection.cursor() as cur:
            t = sql.Identifier(self.b.table_name)
            for val, wkt in [(10, 'POINT(0.5 0.5)'), (20, 'POINT(1.5 1.5)'),
                             (5, 'POINT(11 11)')]:
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (val, geom) VALUES '
                    '(%s, ST_SetSRID(ST_GeomFromText(%s), 4326))'
                ).format(t=t), [val, wkt])

    def _rows(self, layer, cols):
        with connection.cursor() as cur:
            cur.execute(sql.SQL('SELECT {c} FROM {t} ORDER BY name').format(
                c=sql.SQL(', ').join(sql.Identifier(x) for x in cols),
                t=sql.Identifier(layer.table_name)))
            return cur.fetchall()

    def test_count_and_sum(self):
        out = run_spatial_join(
            self.a, self.b, 'Join', owner=self.user,
            params={'predicate': 'intersects', 'joins': [
                {'agg': 'count', 'as': 'cnt'},
                {'agg': 'sum', 'field': 'val', 'as': 'sum_val'},
            ]})
        self.assertEqual(out.geom_kind, 'polygon')
        self.assertEqual(out.feature_count, 2)          # одна строка на A
        dbs = [a['db'] for a in out.attributes]
        self.assertEqual(dbs, ['name', 'cnt', 'sum_val'])
        rows = self._rows(out, ['name', 'cnt', 'sum_val'])
        self.assertEqual(rows[0][0], 'a1')
        self.assertEqual(rows[0][1], 2)
        self.assertAlmostEqual(rows[0][2], 30.0)
        self.assertEqual(rows[1][0], 'a2')
        self.assertEqual(rows[1][1], 1)
        self.assertAlmostEqual(rows[1][2], 5.0)

    def test_empty_joins_rejected(self):
        with self.assertRaises(OverlayError):
            run_spatial_join(self.a, self.b, 'X', owner=self.user,
                             params={'predicate': 'intersects', 'joins': []})

    def test_bad_predicate_rejected(self):
        with self.assertRaises(OverlayError):
            run_spatial_join(self.a, self.b, 'X', owner=self.user,
                             params={'predicate': 'nope', 'joins': [
                                 {'agg': 'count'}]})

    def test_numeric_agg_on_text_rejected(self):
        with self.assertRaises(OverlayError):
            # sum по текстовому полю name слоя A недоступно — но поле берётся из B;
            # здесь B.val числовое, а проверяем защиту на несуществующем поле.
            run_spatial_join(self.a, self.b, 'X', owner=self.user,
                             params={'joins': [
                                 {'agg': 'sum', 'field': 'missing'}]})


class SingleOpEndpointTests(TestCase):
    """POST /overlay/ для одно-слойной операции и spatial join (полный цикл)."""

    def setUp(self):
        self.dj = User.objects.create_user('mgr2', password='x')
        self.lu = _mk_legacy('mgr2')
        ResourceGrant.objects.create(
            legacy_user=self.lu, resource_type=GL, resource_id=None, level='manage')
        ResourceGrant.objects.create(
            legacy_user=self.lu, resource_type=GL, resource_id=None, level='view')
        self.a = create_empty_layer(
            'A', 'polygon', attributes=[{'name': 'name', 'type': 'text'}],
            owner=self.dj)
        with connection.cursor() as cur:
            t = sql.Identifier(self.a.table_name)
            cur.execute(sql.SQL(
                'INSERT INTO {t} (name, geom) VALUES '
                "('a1', ST_SetSRID(ST_GeomFromText(%s), 4326))"
            ).format(t=t), [_square(0, 0, 2, 2)])
        self.client.force_login(self.dj)
        session = self.client.session
        session['legacy_user_id'] = self.lu.pk
        session.save()

    def _create(self, payload):
        return self.client.post(
            '/me/gis/api/overlay/', data=json.dumps(payload),
            content_type='application/json')

    def test_single_op_enqueues_without_layer_b(self):
        r = self._create({'layer_a_id': self.a.pk, 'op': 'buffer',
                          'title': 'Буфер', 'params': {'distance': 5000}})
        self.assertEqual(r.status_code, 202, r.content)
        run = PipelineRun.objects.get(pk=r.json()['run_id'])
        self.assertEqual(run.launch_args['op'], 'buffer')
        self.assertIsNone(run.launch_args['layer_b_id'])
        self.assertEqual(run.launch_args['params'], {'distance': 5000})

    def test_single_op_full_cycle(self):
        run_id = self._create(
            {'layer_a_id': self.a.pk, 'op': 'centroids',
             'title': 'Центроиды'}).json()['run_id']
        call_command('run_gis_overlay', run_id=run_id)
        body = self.client.get(f'/me/gis/api/overlay/{run_id}/').json()
        self.assertEqual(body['status'], 'completed')
        self.assertEqual(body['layer']['geom_kind'], 'point')
        self.assertEqual(body['layer']['feature_count'], 1)
