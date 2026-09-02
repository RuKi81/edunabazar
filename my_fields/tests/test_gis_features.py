"""Тесты API объектов ГИС-слоёв: чтение атрибутов (GET) и правка (PATCH).

Проверяем гейты доступа (view читает, edit правит, manage — тоже) и то, что
:func:`my_fields.services.shp_import.update_feature` реально пишет в таблицу
PostGIS с корректным приведением типов (text/int/double).

Как и в ``test_gis_access``, гранты вешаются на ``LegacyUser``, а физические
таблицы слоёв создаём сырым DDL в ``setUpTestData`` (PostgreSQL DDL
транзакционен и откатывается вместе с тест-классом).
"""
import json

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.utils import timezone

from access.models import ResourceGrant
from legacy.models import LegacyUser
from my_fields.models import GisLayer

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


class GisFeaturesTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.roles = {}
        for name in ('admin', 'viewer', 'editor', 'nobody'):
            dj = User.objects.create_user(name, password='x')
            cls.roles[name] = (dj, _mk_legacy(name))

        cls.layer = GisLayer.objects.create(
            title='Поля', table_name='gis_up_feat', original_filename='f.shp',
            geom_kind='polygon', feature_count=2, color='#333333', sort_order=0,
            attributes=[
                {'name': 'Название', 'db': 'name', 'type': 'text'},
                {'name': 'Площадь', 'db': 'area', 'type': 'double precision'},
                {'name': 'Кол-во', 'db': 'cnt', 'type': 'integer'},
            ],
        )

        with connection.cursor() as cur:
            cur.execute(
                'CREATE TABLE gis_up_feat ('
                'id serial PRIMARY KEY, geom geometry(Geometry, 4326), '
                'name text, area double precision, cnt integer)'
            )
            cur.execute(
                "INSERT INTO gis_up_feat (id, name, area, cnt) VALUES "
                "(1, 'Первое', 10.5, 3), (2, 'Второе', 20.0, 7)"
            )
            # Явные id не сдвигают serial-последовательность — выставляем её
            # вручную, иначе INSERT ... RETURNING id (create_feature) даст id=1.
            cur.execute(
                "SELECT setval(pg_get_serial_sequence('gis_up_feat', 'id'), 2)"
            )

        _, viewer_lu = cls.roles['viewer']
        _, editor_lu = cls.roles['editor']
        ResourceGrant.objects.create(
            legacy_user=viewer_lu, resource_type=GL, resource_id=None, level='view')
        ResourceGrant.objects.create(
            legacy_user=editor_lu, resource_type=GL, resource_id=None, level='edit')

    def _login(self, role):
        dj, lu = self.roles[role]
        self.client.force_login(dj)
        session = self.client.session
        session['legacy_user_id'] = lu.pk
        session.save()

    def _features_url(self):
        return f'/me/gis/api/layers/{self.layer.pk}/features/'

    def _feature_url(self, fid):
        return f'/me/gis/api/layers/{self.layer.pk}/features/{fid}/'

    def _layer_url(self):
        return f'/me/gis/api/layers/{self.layer.pk}/'

    def _patch_layer(self, payload):
        return self.client.patch(
            self._layer_url(), data=json.dumps(payload),
            content_type='application/json')

    def _patch(self, fid, props):
        return self.client.patch(
            self._feature_url(fid), data=json.dumps({'props': props}),
            content_type='application/json')

    def _db_value(self, fid, col):
        with connection.cursor() as cur:
            cur.execute(f'SELECT {col} FROM gis_up_feat WHERE id = %s', [fid])
            return cur.fetchone()[0]

    # ── доступ ──────────────────────────────────────────────────────────
    def test_anonymous_features_401(self):
        self.assertEqual(self.client.get(self._features_url()).status_code, 401)

    def test_nobody_features_403(self):
        self._login('nobody')
        self.assertEqual(self.client.get(self._features_url()).status_code, 403)
        self.assertEqual(self._patch(1, {'name': 'x'}).status_code, 403)

    def test_viewer_can_read_not_write(self):
        self._login('viewer')
        resp = self.client.get(self._features_url())
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body['total'], 2)
        self.assertEqual(len(body['results']), 2)
        first = next(r for r in body['results'] if r['id'] == 1)
        self.assertEqual(first['props']['name'], 'Первое')
        self.assertEqual(first['props']['cnt'], 3)
        # view не даёт править
        self.assertEqual(self._patch(1, {'name': 'x'}).status_code, 403)

    # ── правка ──────────────────────────────────────────────────────────
    def test_editor_updates_text(self):
        self._login('editor')
        r = self._patch(1, {'name': 'Обновлённое'})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self._db_value(1, 'name'), 'Обновлённое')

    def test_editor_updates_numeric_cast(self):
        self._login('editor')
        r = self._patch(2, {'area': '99.25', 'cnt': '42'})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertAlmostEqual(self._db_value(2, 'area'), 99.25)
        self.assertEqual(self._db_value(2, 'cnt'), 42)

    def test_blank_numeric_becomes_null(self):
        self._login('editor')
        r = self._patch(1, {'cnt': ''})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertIsNone(self._db_value(1, 'cnt'))

    def test_unknown_column_ignored_returns_404(self):
        self._login('editor')
        # только неизвестная колонка → нет валидных полей → 404 (noop)
        r = self._patch(1, {'geom': 'x', 'not_a_col': 1})
        self.assertEqual(r.status_code, 404)
        # исходное имя не тронуто
        self.assertEqual(self._db_value(1, 'name'), 'Первое')

    def test_missing_props_400(self):
        self._login('editor')
        r = self.client.patch(
            self._feature_url(1), data=json.dumps({}),
            content_type='application/json')
        self.assertEqual(r.status_code, 400)

    def test_admin_full_access(self):
        self._login('admin')
        self.assertEqual(self.client.get(self._features_url()).json()['total'], 2)
        self.assertEqual(self._patch(1, {'name': 'A'}).status_code, 200)

    # ── смена цвета слоя (PATCH color) ───────────────────────────────────
    def test_editor_updates_color(self):
        self._login('editor')
        r = self._patch_layer({'color': '#FF8800'})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()['layer']['color'], '#ff8800')
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.color, '#ff8800')

    def test_color_short_form_expands(self):
        self._login('editor')
        r = self._patch_layer({'color': '#0f0'})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.color, '#00ff00')

    def test_invalid_color_400(self):
        self._login('editor')
        r = self._patch_layer({'color': 'red'})
        self.assertEqual(r.status_code, 400)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.color, '#333333')

    def test_viewer_cannot_change_color(self):
        self._login('viewer')
        r = self._patch_layer({'color': '#123456'})
        self.assertEqual(r.status_code, 403)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.color, '#333333')

    def test_patch_title_and_color_together(self):
        self._login('editor')
        r = self._patch_layer({'title': 'Новое имя', 'color': '#abcdef'})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.title, 'Новое имя')
        self.assertEqual(self.layer.color, '#abcdef')

    def test_patch_empty_body_400(self):
        self._login('editor')
        r = self._patch_layer({})
        self.assertEqual(r.status_code, 400)

    # ── тематическая раскраска (PATCH style) ─────────────────────────────
    def _stats_url(self, field):
        return f'/me/gis/api/layers/{self.layer.pk}/field-stats/?field={field}'

    def test_style_categorical_valid(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'categorical', 'field': 'name',
            'categories': [{'value': 'Первое', 'color': '#FF0000'},
                           {'value': 'Второе', 'color': '#0f0'}],
        }})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        st = self.layer.style
        self.assertEqual(st['mode'], 'categorical')
        self.assertEqual(st['field'], 'name')
        self.assertEqual(st['categories'][0]['color'], '#ff0000')
        self.assertEqual(st['categories'][1]['color'], '#00ff00')  # #0f0 → расширено
        self.assertEqual(st['other_color'], '#cccccc')  # дефолт

    def test_style_graduated_sorts_stops(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'graduated', 'field': 'area',
            'stops': [{'value': 20, 'color': '#d7191c'},
                      {'value': 10, 'color': '#2b83ba'}],
        }})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        stops = self.layer.style['stops']
        self.assertEqual([s['value'] for s in stops], [10.0, 20.0])  # отсортировано

    def test_style_single_resets(self):
        self._login('editor')
        self.layer.style = {'mode': 'categorical', 'field': 'name',
                            'categories': [{'value': 'x', 'color': '#111111'}],
                            'other_color': '#cccccc'}
        self.layer.save(update_fields=['style'])
        r = self._patch_layer({'style': {'mode': 'single'}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style, {'mode': 'single'})

    def test_style_unknown_field_400(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'categorical', 'field': 'not_a_col',
            'categories': [{'value': 'x', 'color': '#111111'}]}})
        self.assertEqual(r.status_code, 400)

    def test_style_graduated_needs_two_stops_400(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'graduated', 'field': 'area',
            'stops': [{'value': 10, 'color': '#2b83ba'}]}})
        self.assertEqual(r.status_code, 400)

    def test_style_bad_color_400(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'categorical', 'field': 'name',
            'categories': [{'value': 'Первое', 'color': 'red'}]}})
        self.assertEqual(r.status_code, 400)

    def test_style_viewer_forbidden(self):
        self._login('viewer')
        r = self._patch_layer({'style': {'mode': 'single'}})
        self.assertEqual(r.status_code, 403)

    def test_style_opacity_stored_and_clamped(self):
        self._login('editor')
        r = self._patch_layer({'style': {'mode': 'single', 'opacity': 0.5}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style['opacity'], 0.5)
        # Вне [0, 1] — клипуется.
        self._patch_layer({'style': {'mode': 'single', 'opacity': 5}})
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style['opacity'], 1.0)

    def test_style_opacity_with_categorical(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'categorical', 'field': 'name', 'opacity': 0.2,
            'categories': [{'value': 'Первое', 'color': '#FF0000'}]}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style['mode'], 'categorical')
        self.assertEqual(self.layer.style['opacity'], 0.2)

    def test_style_invalid_opacity_ignored(self):
        self._login('editor')
        r = self._patch_layer({'style': {'mode': 'single', 'opacity': 'nope'}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertNotIn('opacity', self.layer.style)

    def test_style_locked_stored(self):
        self._login('editor')
        r = self._patch_layer({'style': {'mode': 'single', 'locked': True}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertTrue(self.layer.style['locked'])
        # Снятие блокировки сохраняется как False.
        self._patch_layer({'style': {'mode': 'single', 'locked': False}})
        self.layer.refresh_from_db()
        self.assertFalse(self.layer.style['locked'])

    def test_style_outline_only_stored(self):
        self._login('editor')
        r = self._patch_layer({'style': {'mode': 'single', 'outline_only': True}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertTrue(self.layer.style['outline_only'])
        # Сбрасывается в False.
        self._patch_layer({'style': {'mode': 'single', 'outline_only': False}})
        self.layer.refresh_from_db()
        self.assertFalse(self.layer.style['outline_only'])

    def test_style_line_width_stored_and_clamped(self):
        self._login('editor')
        r = self._patch_layer({'style': {'mode': 'single', 'line_width': 3.5}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style['line_width'], 3.5)
        # Вне [0.1, 20] — клипуется.
        self._patch_layer({'style': {'mode': 'single', 'line_width': 999}})
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style['line_width'], 20.0)

    def test_style_invalid_line_width_ignored(self):
        self._login('editor')
        r = self._patch_layer({'style': {'mode': 'single', 'line_width': 'nope'}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertNotIn('line_width', self.layer.style)

    def test_style_outline_with_categorical(self):
        self._login('editor')
        r = self._patch_layer({'style': {
            'mode': 'categorical', 'field': 'name',
            'outline_only': True, 'line_width': 2,
            'categories': [{'value': 'Первое', 'color': '#FF0000'}]}})
        self.assertEqual(r.status_code, 200, r.content)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style['mode'], 'categorical')
        self.assertTrue(self.layer.style['outline_only'])
        self.assertEqual(self.layer.style['line_width'], 2.0)

    # ── field-stats endpoint ─────────────────────────────────────────────
    def test_field_stats_numeric(self):
        self._login('viewer')
        r = self.client.get(self._stats_url('area'))
        self.assertEqual(r.status_code, 200, r.content)
        st = r.json()['stats']
        self.assertTrue(st['numeric'])
        self.assertAlmostEqual(st['min'], 10.5)
        self.assertAlmostEqual(st['max'], 20.0)

    def test_field_stats_text_distinct(self):
        self._login('viewer')
        r = self.client.get(self._stats_url('name'))
        self.assertEqual(r.status_code, 200, r.content)
        st = r.json()['stats']
        self.assertFalse(st['numeric'])
        values = sorted(v['value'] for v in st['values'])
        self.assertEqual(values, ['Второе', 'Первое'])

    def test_field_stats_unknown_field_404(self):
        self._login('viewer')
        r = self.client.get(self._stats_url('not_a_col'))
        self.assertEqual(r.status_code, 404)

    def test_field_stats_no_field_400(self):
        self._login('viewer')
        r = self.client.get(f'/me/gis/api/layers/{self.layer.pk}/field-stats/')
        self.assertEqual(r.status_code, 400)

    def test_field_stats_anonymous_401(self):
        r = self.client.get(self._stats_url('name'))
        self.assertEqual(r.status_code, 401)

    # ── правка ГЕОМЕТРИИ объектов ────────────────────────────────────────
    POLY = {'type': 'Polygon', 'coordinates': [[
        [34.0, 45.0], [34.1, 45.0], [34.1, 45.1], [34.0, 45.1], [34.0, 45.0]]]}
    POINT = {'type': 'Point', 'coordinates': [34.05, 45.05]}

    def _post_feature(self, geometry):
        return self.client.post(
            self._features_url(), data=json.dumps({'geometry': geometry}),
            content_type='application/json')

    def _patch_geom(self, fid, geometry):
        return self.client.patch(
            self._feature_url(fid), data=json.dumps({'geometry': geometry}),
            content_type='application/json')

    def _geom_type(self, fid):
        with connection.cursor() as cur:
            cur.execute(
                'SELECT ST_GeometryType(geom) FROM gis_up_feat WHERE id = %s',
                [fid])
            row = cur.fetchone()
            return row[0] if row else None

    def test_editor_creates_feature(self):
        self._login('editor')
        r = self._post_feature(self.POLY)
        self.assertEqual(r.status_code, 201, r.content)
        new_id = r.json()['id']
        self.assertEqual(self._geom_type(new_id), 'ST_Polygon')
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.feature_count, 3)  # было 2

    def test_editor_updates_geometry(self):
        self._login('editor')
        r = self._patch_geom(1, self.POLY)
        self.assertEqual(r.status_code, 200, r.content)
        self.assertTrue(r.json()['geometry_updated'])
        self.assertEqual(self._geom_type(1), 'ST_Polygon')

    def test_patch_geometry_and_props_together(self):
        self._login('editor')
        r = self.client.patch(
            self._feature_url(1),
            data=json.dumps({'geometry': self.POLY, 'props': {'name': 'X'}}),
            content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self._db_value(1, 'name'), 'X')
        self.assertEqual(self._geom_type(1), 'ST_Polygon')

    def test_create_type_mismatch_400(self):
        self._login('editor')
        r = self._post_feature(self.POINT)  # слой polygon
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'invalid_geometry')

    def test_update_type_mismatch_400(self):
        self._login('editor')
        r = self._patch_geom(1, self.POINT)
        self.assertEqual(r.status_code, 400)

    def test_create_invalid_geometry_400(self):
        self._login('editor')
        r = self._post_feature({'type': 'Polygon', 'coordinates': []})
        self.assertEqual(r.status_code, 400)

    def test_delete_feature(self):
        self._login('editor')
        r = self.client.delete(self._feature_url(1))
        self.assertEqual(r.status_code, 200, r.content)
        with connection.cursor() as cur:
            cur.execute('SELECT count(*) FROM gis_up_feat WHERE id = 1')
            self.assertEqual(cur.fetchone()[0], 0)
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.feature_count, 1)

    def test_delete_missing_404(self):
        self._login('editor')
        r = self.client.delete(self._feature_url(999))
        self.assertEqual(r.status_code, 404)

    def test_features_geojson(self):
        self._login('editor')
        self._patch_geom(1, self.POLY)
        r = self.client.get(self._features_url() + '?geometry=1')
        self.assertEqual(r.status_code, 200, r.content)
        body = r.json()
        self.assertEqual(body['geom_kind'], 'polygon')
        feats = body['featurecollection']['features']
        self.assertEqual(len(feats), 1)  # только объект с геометрией
        self.assertEqual(feats[0]['id'], 1)
        self.assertEqual(feats[0]['geometry']['type'], 'Polygon')

    def test_viewer_cannot_create_403(self):
        self._login('viewer')
        self.assertEqual(self._post_feature(self.POLY).status_code, 403)

    def test_viewer_cannot_delete_403(self):
        self._login('viewer')
        self.assertEqual(self.client.delete(self._feature_url(1)).status_code, 403)

    def test_anonymous_create_401(self):
        self.assertEqual(self._post_feature(self.POLY).status_code, 401)

    # ── rank_of: позиция объекта для перехода таблицы на его страницу ─────
    def test_rank_of_default_sort(self):
        # По умолчанию сортировка по id ASC → id=1 на позиции 0, id=2 — 1.
        self._login('viewer')
        r = self.client.get(self._features_url() + '?rank_of=1')
        self.assertEqual(r.status_code, 200, r.content)
        body = r.json()
        self.assertEqual(body['rank'], 0)
        self.assertEqual(body['total'], 2)
        r2 = self.client.get(self._features_url() + '?rank_of=2')
        self.assertEqual(r2.json()['rank'], 1)

    def test_rank_of_respects_sort_dir(self):
        # id DESC → id=2 первым (rank 0), id=1 вторым (rank 1).
        self._login('viewer')
        r = self.client.get(self._features_url() + '?rank_of=2&sort=id&dir=desc')
        self.assertEqual(r.json()['rank'], 0)
        r2 = self.client.get(self._features_url() + '?rank_of=1&sort=id&dir=desc')
        self.assertEqual(r2.json()['rank'], 1)

    def test_rank_of_respects_column_sort(self):
        # Сортировка по cnt ASC: id=1 (cnt=3) перед id=2 (cnt=7).
        self._login('viewer')
        r = self.client.get(self._features_url() + '?rank_of=2&sort=cnt&dir=asc')
        self.assertEqual(r.json()['rank'], 1)

    def test_rank_of_filtered_out_returns_null(self):
        # Поиск, под который объект не попадает → rank=None, total=1.
        self._login('viewer')
        r = self.client.get(self._features_url() + '?rank_of=2&q=Первое')
        body = r.json()
        self.assertIsNone(body['rank'])
        self.assertEqual(body['total'], 1)

    def test_rank_of_missing_id_returns_null(self):
        self._login('viewer')
        r = self.client.get(self._features_url() + '?rank_of=999999')
        self.assertIsNone(r.json()['rank'])

    def test_rank_of_invalid_400(self):
        self._login('viewer')
        r = self.client.get(self._features_url() + '?rank_of=abc')
        self.assertEqual(r.status_code, 400)

    def test_rank_of_requires_view(self):
        self._login('nobody')
        r = self.client.get(self._features_url() + '?rank_of=1')
        self.assertEqual(r.status_code, 403)


class GisLayerCreateTestCase(TestCase):
    """Создание нового пустого слоя: тип геометрии + атрибуты (POST create/)."""

    CREATE = '/me/gis/api/layers/create/'

    @classmethod
    def setUpTestData(cls):
        cls.roles = {}
        for name in ('admin', 'manager', 'viewer'):
            dj = User.objects.create_user(name, password='x')
            cls.roles[name] = (dj, _mk_legacy(name))
        _, manager_lu = cls.roles['manager']
        _, viewer_lu = cls.roles['viewer']
        ResourceGrant.objects.create(
            legacy_user=manager_lu, resource_type=GL, resource_id=None, level='manage')
        ResourceGrant.objects.create(
            legacy_user=viewer_lu, resource_type=GL, resource_id=None, level='view')

    def _login(self, role):
        dj, lu = self.roles[role]
        self.client.force_login(dj)
        session = self.client.session
        session['legacy_user_id'] = lu.pk
        session.save()

    def _create(self, payload):
        return self.client.post(
            self.CREATE, data=json.dumps(payload),
            content_type='application/json')

    @staticmethod
    def _table_columns(table_name):
        with connection.cursor() as cur:
            cur.execute(
                'SELECT column_name, data_type FROM information_schema.columns '
                'WHERE table_name = %s', [table_name])
            return {r[0]: r[1] for r in cur.fetchall()}

    def test_create_point_layer_with_attributes(self):
        self._login('manager')
        r = self._create({
            'title': 'Точки осмотра', 'geom_kind': 'point',
            'attributes': [
                {'name': 'Комментарий', 'type': 'text'},
                {'name': 'Балл', 'type': 'integer'},
            ],
        })
        self.assertEqual(r.status_code, 201, r.content)
        lyr = r.json()['layer']
        self.assertEqual(lyr['geom_kind'], 'point')
        self.assertEqual(lyr['feature_count'], 0)
        self.assertEqual(len(lyr['attributes']), 2)

        layer = GisLayer.objects.get(pk=lyr['id'])
        cols = self._table_columns(layer.table_name)
        self.assertIn('id', cols)
        self.assertIn('geom', cols)
        # Атрибуты приведены к безопасным именам с нужными pg-типами.
        dbs = {a['db']: a['type'] for a in layer.attributes}
        self.assertEqual(set(dbs.values()), {'text', 'integer'})
        for db, t in dbs.items():
            self.assertIn(db, cols)

    def test_create_polygon_no_attributes(self):
        self._login('manager')
        r = self._create({'title': 'Контуры', 'geom_kind': 'polygon'})
        self.assertEqual(r.status_code, 201, r.content)
        layer = GisLayer.objects.get(pk=r.json()['layer']['id'])
        self.assertEqual(layer.geom_kind, 'polygon')
        self.assertEqual(layer.attributes, [])

    def test_admin_can_create(self):
        self._login('admin')
        r = self._create({'title': 'Линии', 'geom_kind': 'line'})
        self.assertEqual(r.status_code, 201, r.content)

    def test_empty_title_400(self):
        self._login('manager')
        r = self._create({'title': '   ', 'geom_kind': 'point'})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'empty_title')

    def test_invalid_geom_kind_400(self):
        self._login('manager')
        r = self._create({'title': 'X', 'geom_kind': 'circle'})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'invalid_geom_kind')

    def test_invalid_attr_type_400(self):
        self._login('manager')
        r = self._create({
            'title': 'X', 'geom_kind': 'point',
            'attributes': [{'name': 'A', 'type': 'boolean'}]})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error'], 'create_failed')

    def test_blank_attr_name_skipped(self):
        self._login('manager')
        r = self._create({
            'title': 'X', 'geom_kind': 'point',
            'attributes': [{'name': '  ', 'type': 'text'},
                           {'name': 'Годен', 'type': 'text'}]})
        self.assertEqual(r.status_code, 201, r.content)
        layer = GisLayer.objects.get(pk=r.json()['layer']['id'])
        self.assertEqual(len(layer.attributes), 1)

    def test_viewer_forbidden_403(self):
        self._login('viewer')
        r = self._create({'title': 'X', 'geom_kind': 'point'})
        self.assertEqual(r.status_code, 403)

    def test_anonymous_401(self):
        r = self._create({'title': 'X', 'geom_kind': 'point'})
        self.assertEqual(r.status_code, 401)

    def test_created_layer_accepts_feature(self):
        # Новый слой сразу поддерживает добавление объекта своей геометрии.
        self._login('manager')
        r = self._create({'title': 'Точки', 'geom_kind': 'point'})
        pk = r.json()['layer']['id']
        r2 = self.client.post(
            f'/me/gis/api/layers/{pk}/features/',
            data=json.dumps({'geometry': {'type': 'Point', 'coordinates': [34.0, 45.0]}}),
            content_type='application/json')
        self.assertEqual(r2.status_code, 201, r2.content)
