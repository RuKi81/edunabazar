"""Тесты ГИС-загрузчика SHP (ZIP) → таблица PostGIS на каждый .shp.

Фиксируем:
* доступ: 401 без логина, 403 для не-админа, 200 для админа (гейт как у
  всей страницы /me/gis — LegacyUser с username из ADMIN_USERNAMES);
* импорт: ZIP c .shp/.shx/.dbf → создаётся отдельная таблица PostGIS,
  запись в реестре GisLayer, корректный feature_count и охват (extent);
* список слоёв (GET) и универсальный MVT-эндпоинт (protobuf);
* удаление слоя дропает и физическую таблицу, и запись реестра;
* архив без .shp даёт понятную ошибку (400), а не 500.

Требуют PostGIS + GDAL (есть в CI). Шейп-файлы генерим на лету через pyshp.
"""
import io
import json
import math
import os
import tempfile
import zipfile

import shapefile
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TestCase
from django.utils import timezone

from legacy.models import LegacyUser
from my_fields.models import GisFolder, GisLayer

User = get_user_model()

# Небольшой полигон около Крыма (lon/lat, EPSG:4326).
RING = [[34.10, 45.10], [34.10, 45.12], [34.12, 45.12], [34.12, 45.10], [34.10, 45.10]]


def _make_shp_zip(shp_name='fields', with_shp=True):
    """Собрать ZIP с одним полигональным шейп-файлом (без .prj → SRID 0)."""
    tmp = tempfile.mkdtemp(prefix='gis_test_')
    base = os.path.join(tmp, shp_name)
    if with_shp:
        w = shapefile.Writer(base)
        w.field('name', 'C', size=40)
        w.field('num', 'N', size=10)
        w.poly([RING])
        w.record('поле-A', 42)
        w.close()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        if with_shp:
            for ext in ('shp', 'shx', 'dbf'):
                zf.write(base + '.' + ext, arcname=shp_name + '.' + ext)
        else:
            zf.writestr('readme.txt', 'no shapefile here')
    buf.seek(0)
    return buf.read()


def _deg2tile(lon, lat, z):
    """lon/lat → (x, y) номера XYZ-тайла на зуме z (Slippy Map)."""
    n = 2.0 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_r = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_r)) / math.pi) / 2.0 * n)
    return x, y


def _table_exists(name):
    with connection.cursor() as cur:
        cur.execute('SELECT to_regclass(%s)', [f'public.{name}'])
        return cur.fetchone()[0] is not None


class GisLayersTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        now = timezone.now()
        cls.admin_user = User.objects.create_user('admin', password='x')
        cls.plain_user = User.objects.create_user('bob', password='x')
        # username 'admin' входит в дефолтный ADMIN_USERNAMES → админ-гейт.
        cls.legacy_admin = LegacyUser.objects.create(
            type=0, username='admin', auth_key='', password_hash='',
            email='admin@test.com', currency='RUB', name='Admin',
            address='', phone='', inn='', status=10,
            created_at=now, updated_at=now, contacts='',
        )
        cls.legacy_plain = LegacyUser.objects.create(
            type=0, username='bob', auth_key='', password_hash='',
            email='bob@test.com', currency='RUB', name='Bob',
            address='', phone='', inn='', status=10,
            created_at=now, updated_at=now, contacts='',
        )

    def _login_admin(self):
        self.client.force_login(self.admin_user)
        session = self.client.session
        session['legacy_user_id'] = self.legacy_admin.pk
        session.save()

    def _login_plain(self):
        self.client.force_login(self.plain_user)
        session = self.client.session
        session['legacy_user_id'] = self.legacy_plain.pk
        session.save()

    def _upload(self, zip_bytes, filename='fields.zip'):
        upload = SimpleUploadedFile(filename, zip_bytes, content_type='application/zip')
        return self.client.post('/me/gis/api/layers/', {'files': upload})


class AuthTests(GisLayersTestCase):
    def test_anonymous_gets_401(self):
        self.assertEqual(self.client.get('/me/gis/api/layers/').status_code, 401)

    def test_non_admin_gets_403(self):
        self._login_plain()
        self.assertEqual(self.client.get('/me/gis/api/layers/').status_code, 403)
        resp = self._upload(_make_shp_zip())
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(GisLayer.objects.count(), 0)


class ImportTests(GisLayersTestCase):
    def setUp(self):
        self._login_admin()

    def test_upload_creates_table_and_registry(self):
        resp = self._upload(_make_shp_zip(shp_name='fields'))
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(len(body['created']), 1)
        self.assertEqual(body['errors'], [])

        layer = GisLayer.objects.get()
        self.assertEqual(layer.feature_count, 1)
        self.assertEqual(layer.geom_kind, 'polygon')
        self.assertTrue(layer.table_name.startswith('gis_up_'))
        # Атрибуты сохранены (name/num).
        attr_names = {a['name'] for a in layer.attributes}
        self.assertEqual(attr_names, {'name', 'num'})
        # Охват посчитан и попадает в район полигона.
        self.assertIsNotNone(layer.extent)
        self.assertAlmostEqual(layer.extent[0], 34.10, places=2)
        # Физическая таблица создана и в ней ровно 1 объект.
        self.assertTrue(_table_exists(layer.table_name))
        with connection.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{layer.table_name}"')
            self.assertEqual(cur.fetchone()[0], 1)

    def test_two_shapefiles_two_tables(self):
        # Два .shp в одном архиве → две таблицы.
        tmp = tempfile.mkdtemp()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            for nm in ('layer_a', 'layer_b'):
                base = os.path.join(tmp, nm)
                w = shapefile.Writer(base)
                w.field('name', 'C', size=20)
                w.poly([RING])
                w.record(nm)
                w.close()
                for ext in ('shp', 'shx', 'dbf'):
                    zf.write(base + '.' + ext, arcname=nm + '.' + ext)
        buf.seek(0)
        resp = self._upload(buf.read())
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(GisLayer.objects.count(), 2)
        self.assertEqual(len({x.table_name for x in GisLayer.objects.all()}), 2)

    def test_zip_without_shp_is_400(self):
        resp = self._upload(_make_shp_zip(with_shp=False))
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json()['ok'])
        self.assertTrue(resp.json()['errors'])
        self.assertEqual(GisLayer.objects.count(), 0)


class ListAndTilesTests(GisLayersTestCase):
    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='fields'))
        self.layer = GisLayer.objects.get()

    def test_list(self):
        resp = self.client.get('/me/gis/api/layers/')
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body['count'], 1)
        self.assertEqual(body['results'][0]['table_name'], self.layer.table_name)

    def test_mvt_tile_returns_protobuf(self):
        # Берём тайл, реально накрывающий полигон (на z0 объект схлопнулся бы
        # ниже разрешения MVT). Центр полигона ≈ (34.11, 45.11).
        z = 14
        x, y = _deg2tile(34.11, 45.11, z)
        url = f'/me/gis/api/layers/{self.layer.pk}/tiles/{z}/{x}/{y}.pbf'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'], 'application/x-protobuf')
        self.assertGreater(len(resp.content), 0)

    def test_mvt_tile_denied_for_anonymous(self):
        self.client.logout()
        z = 14
        x, y = _deg2tile(34.11, 45.11, z)
        resp = self.client.get(f'/me/gis/api/layers/{self.layer.pk}/tiles/{z}/{x}/{y}.pbf')
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp['Content-Type'], 'application/x-protobuf')

    def test_delete_drops_table_and_row(self):
        table = self.layer.table_name
        resp = self.client.delete(f'/me/gis/api/layers/{self.layer.pk}/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(GisLayer.objects.count(), 0)
        self.assertFalse(_table_exists(table))

    def test_geometry_all_without_bbox(self):
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/?geometry=1')
        self.assertEqual(resp.status_code, 200, resp.content)
        feats = resp.json()['featurecollection']['features']
        self.assertEqual(len(feats), 1)   # единственный полигон слоя

    def test_geometry_bbox_covering_returns_feature(self):
        # Полигон около (34.1..34.12, 45.1..45.12) — bbox накрывает его.
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/'
            '?geometry=1&bbox=34.0,45.0,34.2,45.2')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(len(resp.json()['featurecollection']['features']), 1)

    def test_geometry_bbox_elsewhere_returns_empty(self):
        # Экстент вдали от полигона — объектов быть не должно.
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/'
            '?geometry=1&bbox=0.0,0.0,1.0,1.0')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(len(resp.json()['featurecollection']['features']), 0)

    def test_geometry_malformed_bbox_ignored(self):
        # Битый bbox → фильтр не применяется, отдаём как без bbox.
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/'
            '?geometry=1&bbox=foo,bar')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(len(resp.json()['featurecollection']['features']), 1)

    def test_feature_extent_get(self):
        # id=1 — единственный объект (serial PK). Охват должен накрывать
        # полигон около (34.1..34.12, 45.1..45.12).
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/1/')
        self.assertEqual(resp.status_code, 200, resp.content)
        ext = resp.json()['extent']
        self.assertEqual(len(ext), 4)
        self.assertAlmostEqual(ext[0], 34.10, places=2)
        self.assertAlmostEqual(ext[3], 45.12, places=2)

    def test_feature_extent_missing_is_404(self):
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/999999/')
        self.assertEqual(resp.status_code, 404)

    def test_feature_extent_requires_auth(self):
        self.client.logout()
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/1/')
        self.assertEqual(resp.status_code, 401)


class ExportTests(GisLayersTestCase):
    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='fields'))
        self.layer = GisLayer.objects.get()

    def _export(self, fmt):
        return self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/export/?format={fmt}')

    def test_export_shp_zip(self):
        resp = self._export('shp')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(resp['Content-Type'], 'application/zip')
        self.assertIn('attachment', resp['Content-Disposition'])
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            names = z.namelist()
            exts = {n.rsplit('.', 1)[-1] for n in names}
            self.assertTrue({'shp', 'shx', 'dbf', 'prj'}.issubset(exts))
            base = next(n[:-4] for n in names if n.endswith('.shp'))
            r = shapefile.Reader(
                shp=io.BytesIO(z.read(base + '.shp')),
                shx=io.BytesIO(z.read(base + '.shx')),
                dbf=io.BytesIO(z.read(base + '.dbf')),
            )
            self.assertEqual(len(r), 1)         # один полигон слоя
            fields = [f[0] for f in r.fields if f[0] != 'DeletionFlag']
            self.assertIn('id', fields)

    def test_export_geojson_zip(self):
        resp = self._export('geojson')
        self.assertEqual(resp.status_code, 200, resp.content)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            gj_name = next(n for n in z.namelist() if n.endswith('.geojson'))
            fc = json.loads(z.read(gj_name).decode('utf-8'))
        self.assertEqual(fc['type'], 'FeatureCollection')
        self.assertEqual(len(fc['features']), 1)
        self.assertEqual(fc['features'][0]['geometry']['type'], 'Polygon')
        self.assertEqual(fc['features'][0]['properties']['num'], 42)

    def test_export_xlsx_zip(self):
        import openpyxl
        resp = self._export('xlsx')
        self.assertEqual(resp.status_code, 200, resp.content)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            xlsx_name = next(n for n in z.namelist() if n.endswith('.xlsx'))
            wb = openpyxl.load_workbook(io.BytesIO(z.read(xlsx_name)))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        self.assertEqual(rows[0][0], 'id')
        self.assertIn('name', rows[0])
        self.assertIn('wkt', rows[0])
        self.assertEqual(len(rows), 2)          # заголовок + 1 объект

    def test_export_unknown_format_is_400(self):
        resp = self._export('kml')
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json()['ok'])

    def test_export_requires_auth(self):
        self.client.logout()
        self.assertEqual(self._export('shp').status_code, 401)


class ParseBboxTests(TestCase):
    def test_valid(self):
        from my_fields.api import _parse_bbox
        self.assertEqual(_parse_bbox('1,2,3,4'), (1.0, 2.0, 3.0, 4.0))

    def test_none_and_empty(self):
        from my_fields.api import _parse_bbox
        self.assertIsNone(_parse_bbox(None))
        self.assertIsNone(_parse_bbox(''))

    def test_wrong_count(self):
        from my_fields.api import _parse_bbox
        self.assertIsNone(_parse_bbox('1,2,3'))

    def test_non_numeric(self):
        from my_fields.api import _parse_bbox
        self.assertIsNone(_parse_bbox('a,b,c,d'))

    def test_degenerate(self):
        from my_fields.api import _parse_bbox
        self.assertIsNone(_parse_bbox('3,2,1,4'))   # maxx <= minx
        self.assertIsNone(_parse_bbox('1,2,3,2'))   # maxy <= miny


class RenameAndReorderTests(GisLayersTestCase):
    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='layer_a'))
        self._upload(_make_shp_zip(shp_name='layer_b'))
        self.a, self.b = list(GisLayer.objects.order_by('sort_order', 'id'))

    def test_import_assigns_increasing_sort_order(self):
        self.assertLess(self.a.sort_order, self.b.sort_order)

    def test_patch_renames_layer(self):
        resp = self.client.patch(
            f'/me/gis/api/layers/{self.a.pk}/',
            data=json.dumps({'title': 'Мой слой'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 200, resp.content)
        self.a.refresh_from_db()
        self.assertEqual(self.a.title, 'Мой слой')

    def test_patch_empty_title_is_400(self):
        resp = self.client.patch(
            f'/me/gis/api/layers/{self.a.pk}/',
            data=json.dumps({'title': '   '}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 400)
        self.a.refresh_from_db()
        self.assertNotEqual(self.a.title, '')

    def test_reorder_updates_sort_order(self):
        # Отправляем b первым → b получает sort_order=0, a=1.
        resp = self.client.post(
            '/me/gis/api/layers/reorder/',
            data=json.dumps({'order': [self.b.pk, self.a.pk]}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 200, resp.content)
        self.a.refresh_from_db()
        self.b.refresh_from_db()
        self.assertEqual(self.b.sort_order, 0)
        self.assertEqual(self.a.sort_order, 1)

    def test_reorder_requires_admin(self):
        self.client.logout()
        self._login_plain()
        resp = self.client.post(
            '/me/gis/api/layers/reorder/',
            data=json.dumps({'order': [self.b.pk, self.a.pk]}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 403)

    def test_rename_requires_admin(self):
        self.client.logout()
        resp = self.client.patch(
            f'/me/gis/api/layers/{self.a.pk}/',
            data=json.dumps({'title': 'X'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 401)


class FolderTests(GisLayersTestCase):
    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='layer_a'))
        self._upload(_make_shp_zip(shp_name='layer_b'))
        self.a, self.b = list(GisLayer.objects.order_by('sort_order', 'id'))

    def test_create_folder(self):
        resp = self.client.post(
            '/me/gis/api/folders/',
            data=json.dumps({'name': 'Поля 2025'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 201, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['folder']['name'], 'Поля 2025')
        self.assertEqual(GisFolder.objects.count(), 1)

    def test_create_folder_default_name(self):
        resp = self.client.post(
            '/me/gis/api/folders/', data='{}',
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.json()['folder']['name'], 'Новая папка')

    def test_folders_listed_in_layers_collection(self):
        GisFolder.objects.create(name='F1')
        body = self.client.get('/me/gis/api/layers/').json()
        self.assertIn('folders', body)
        self.assertEqual(len(body['folders']), 1)
        self.assertEqual(body['folders'][0]['name'], 'F1')

    def test_layer_dict_exposes_folder(self):
        f = GisFolder.objects.create(name='F1')
        self.a.folder = f
        self.a.save(update_fields=['folder'])
        body = self.client.get('/me/gis/api/layers/').json()
        row = next(x for x in body['results'] if x['id'] == self.a.pk)
        self.assertEqual(row['folder'], f.pk)

    def test_patch_folder_name(self):
        f = GisFolder.objects.create(name='old')
        resp = self.client.patch(
            f'/me/gis/api/folders/{f.pk}/',
            data=json.dumps({'name': 'new'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 200, resp.content)
        f.refresh_from_db()
        self.assertEqual(f.name, 'new')

    def test_patch_folder_empty_name_is_400(self):
        f = GisFolder.objects.create(name='keep')
        resp = self.client.patch(
            f'/me/gis/api/folders/{f.pk}/',
            data=json.dumps({'name': '  '}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 400)
        f.refresh_from_db()
        self.assertEqual(f.name, 'keep')

    def test_patch_folder_collapsed_and_visible(self):
        f = GisFolder.objects.create(name='F1')
        resp = self.client.patch(
            f'/me/gis/api/folders/{f.pk}/',
            data=json.dumps({'collapsed': True, 'visible': False}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 200, resp.content)
        f.refresh_from_db()
        self.assertTrue(f.collapsed)
        self.assertFalse(f.visible)

    def test_delete_folder_keeps_layers(self):
        f = GisFolder.objects.create(name='F1')
        self.a.folder = f
        self.a.save(update_fields=['folder'])
        resp = self.client.delete(f'/me/gis/api/folders/{f.pk}/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(GisFolder.objects.count(), 0)
        self.a.refresh_from_db()
        self.assertIsNone(self.a.folder)   # SET_NULL → слой уходит в корень
        self.assertTrue(GisLayer.objects.filter(pk=self.a.pk).exists())

    def test_layout_save_assigns_folder_and_order(self):
        f = GisFolder.objects.create(name='F1', sort_order=5)
        resp = self.client.post(
            '/me/gis/api/layers/reorder/',
            data=json.dumps({
                'folders': [{'id': f.pk}],
                'layers': [
                    {'id': self.b.pk, 'folder': f.pk},
                    {'id': self.a.pk, 'folder': None},
                ],
            }),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 200, resp.content)
        f.refresh_from_db()
        self.a.refresh_from_db()
        self.b.refresh_from_db()
        self.assertEqual(f.sort_order, 0)
        self.assertEqual(self.b.folder_id, f.pk)
        self.assertIsNone(self.a.folder_id)
        self.assertEqual(self.b.sort_order, 0)
        self.assertEqual(self.a.sort_order, 1)

    def test_layout_save_ignores_invalid_folder(self):
        resp = self.client.post(
            '/me/gis/api/layers/reorder/',
            data=json.dumps({
                'folders': [],
                'layers': [{'id': self.a.pk, 'folder': 999999}],
            }),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 200, resp.content)
        self.a.refresh_from_db()
        self.assertIsNone(self.a.folder_id)   # несуществующая папка → NULL

    def test_folder_create_requires_admin(self):
        self.client.logout()
        self._login_plain()
        resp = self.client.post(
            '/me/gis/api/folders/',
            data=json.dumps({'name': 'X'}),
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 403)

    def test_folder_delete_requires_auth(self):
        f = GisFolder.objects.create(name='F1')
        self.client.logout()
        resp = self.client.delete(f'/me/gis/api/folders/{f.pk}/')
        self.assertEqual(resp.status_code, 401)
