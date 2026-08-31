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
from django.db import connection, transaction
from django.test import TestCase
from django.utils import timezone
from psycopg import sql

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

    def test_cyrillic_field_name_truncated_in_dbf(self):
        # Имя поля в DBF ограничено 10 байтами. Сторонние выгрузки (ArcGIS,
        # Росреестр) обрезают кириллическое имя (UTF-8, 2 байта/символ) ПОСРЕДИ
        # многобайтового символа, из-за чего штатный ``layer.fields`` падает
        # UnicodeDecodeError ещё до импорта. Импорт должен это переживать.
        # pyshp обрезает по границе символа (безопасно), поэтому портим
        # 11-байтовый слот имени первого поля в .dbf вручную — так же, как это
        # делают чужие инструменты.
        tmp = tempfile.mkdtemp(prefix='gis_test_cyr_')
        base = os.path.join(tmp, 'cyr')
        w = shapefile.Writer(base, encoding='utf-8')
        w.field('name', 'C', size=40)
        w.field('num', 'N', size=10)
        w.poly([RING])
        w.record('поле-A', 7)
        w.close()

        # Патчим первое поле-дескриптор (offset 32, слот имени = байты 0..10).
        # 'наименова'[:9] байт → обрыв посреди последнего символа.
        with open(base + '.dbf', 'r+b') as fh:
            data = bytearray(fh.read())
            bad = 'наименование'.encode('utf-8')[:9]
            name_slot = bad + b'\x00' * (11 - len(bad))
            data[32:32 + 11] = name_slot
            fh.seek(0)
            fh.write(data)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            for ext in ('shp', 'shx', 'dbf'):
                zf.write(base + '.' + ext, arcname='cyr.' + ext)
        buf.seek(0)

        resp = self._upload(buf.read())
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(GisLayer.objects.count(), 1)
        layer = GisLayer.objects.get()
        self.assertEqual(layer.feature_count, 1)
        # Значения атрибутов прочитаны по индексу (устойчиво к битому имени).
        with connection.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{layer.table_name}"')
            self.assertEqual(cur.fetchone()[0], 1)

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

    def test_export_filename_matches_layer_title(self):
        # Имя ZIP и вложенных файлов = название слоя (как в плашке), кириллица.
        self.layer.title = 'Поля 2024/тест'   # со «слэшем» — проверим санитайз
        self.layer.save(update_fields=['title'])
        from urllib.parse import quote
        expected = 'Поля 2024_тест'           # '/' → '_'
        # shp: вложенные файлы носят имя слоя
        resp = self._export('shp')
        self.assertEqual(resp.status_code, 200, resp.content)
        cd = resp['Content-Disposition']
        self.assertIn("filename*=UTF-8''" + quote(expected + '.zip'), cd)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            names = z.namelist()
            self.assertIn(expected + '.shp', names)
            self.assertIn(expected + '.dbf', names)
        # geojson / xlsx: вложенный файл тоже носит имя слоя
        with zipfile.ZipFile(io.BytesIO(self._export('geojson').content)) as z:
            self.assertIn(expected + '.geojson', z.namelist())
        with zipfile.ZipFile(io.BytesIO(self._export('xlsx').content)) as z:
            self.assertIn(expected + '.xlsx', z.namelist())

    def test_export_survives_low_statement_timeout(self):
        # Регрессия: на слоях с большим количеством объектов тяжёлый скан
        # ST_AsGeoJSON+ST_AsText упирался в глобальный statement_timeout
        # Postgres и отменялся (QueryCanceled → «export_failed»). Экспорт
        # должен снимать лимит на этот read и отдавать архив.
        table = self.layer.table_name
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'INSERT INTO {t} (name, num, geom) '
                "SELECT 'poly-' || g, g, "
                'ST_SetSRID(ST_MakeEnvelope(34.1, 45.1, 34.12, 45.12), 4326) '
                'FROM generate_series(1, 5000) g'
            ).format(t=sql.Identifier(table)))
            # эмулируем прод: агрессивный лимит на сессии
            cur.execute("SET statement_timeout = '2ms'")
        try:
            # sanity: лимит на сессии реально действует (детерминированно —
            # через pg_sleep, а не завися от времени скана); savepoint откатит
            # аборт транзакции, чтобы не сломать последующие запросы.
            with self.assertRaises(Exception):
                with transaction.atomic(), connection.cursor() as cur:
                    cur.execute('SELECT pg_sleep(1)')   # 1с ≫ 2мс лимита
            # с фиксом экспорт снимает лимит и успешно формирует архив
            for fmt in ('shp', 'geojson', 'xlsx'):
                resp = self._export(fmt)
                self.assertEqual(resp.status_code, 200, (fmt, resp.content))
        finally:
            with connection.cursor() as cur:
                cur.execute('SET statement_timeout = DEFAULT')

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


class LayerQueryEndpointTests(GisLayersTestCase):
    """POST /me/gis/api/layers/<pk>/query/ — визуальный конструктор выборки."""

    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='fields'))   # 1 объект: num=42
        self.layer = GisLayer.objects.get()

    def _query(self, body):
        return self.client.post(
            f'/me/gis/api/layers/{self.layer.pk}/query/',
            data=json.dumps(body), content_type='application/json')

    def test_filter_matches(self):
        resp = self._query({'filter': {'rules': [
            {'field': 'num', 'op': 'eq', 'value': 42}]}})
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['total'], 1)

    def test_filter_no_match(self):
        resp = self._query({'filter': {'rules': [
            {'field': 'num', 'op': 'eq', 'value': 99}]}})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['total'], 0)

    def test_invalid_filter_is_400(self):
        resp = self._query({'filter': {'rules': [
            {'field': 'nope', 'op': 'eq', 'value': 1}]}})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_filter')

    def test_save_as_creates_layer(self):
        resp = self._query({
            'filter': {'rules': [{'field': 'num', 'op': 'gt', 'value': 0}]},
            'save_as': 'Выборка num>0'})
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(GisLayer.objects.count(), 2)
        new_layer = GisLayer.objects.get(pk=body['layer']['id'])
        self.assertEqual(new_layer.feature_count, 1)
        self.assertEqual(new_layer.title, 'Выборка num>0')

    def test_save_as_empty_title_is_400(self):
        resp = self._query({'filter': {'rules': []}, 'save_as': '   '})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'empty_title')

    def test_ids_only_query_returns_matching_ids(self):
        # Матч (num=42 > 0) → все id; без пагинации.
        resp = self._query({
            'ids_only': True,
            'filter': {'rules': [{'field': 'num', 'op': 'gt', 'value': 0}]}})
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['total'], 1)
        self.assertEqual(len(body['ids']), 1)

    def test_ids_only_query_no_match(self):
        resp = self._query({
            'ids_only': True,
            'filter': {'rules': [{'field': 'num', 'op': 'gt', 'value': 100}]}})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['ids'], [])

    def test_ids_only_invalid_filter_is_400(self):
        resp = self._query({
            'ids_only': True,
            'filter': {'rules': [{'field': 'nope', 'op': 'eq', 'value': 1}]}})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_filter')

    def test_ids_only_features_get(self):
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/?ids_only=1')
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['total'], 1)
        self.assertEqual(len(body['ids']), 1)

    def test_ids_only_features_get_search(self):
        # Поиск по несуществующей подстроке → пустой набор id.
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/'
            '?ids_only=1&q=zzzнеттакого')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['ids'], [])

    def test_anonymous_denied(self):
        self.client.logout()
        resp = self._query({'filter': {'rules': []}})
        self.assertEqual(resp.status_code, 401)

    def test_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        resp = self._query({'filter': {'rules': []}})
        self.assertEqual(resp.status_code, 403)


class BulkDeleteEndpointTests(GisLayersTestCase):
    """DELETE /me/gis/api/layers/<pk>/features/ — пакетное удаление по ids."""

    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='fields'))   # 1 объект: id=1
        self.layer = GisLayer.objects.get()

    def _delete(self, body):
        return self.client.delete(
            f'/me/gis/api/layers/{self.layer.pk}/features/',
            data=json.dumps(body), content_type='application/json')

    def _feature_total(self):
        resp = self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/features/')
        return resp.json()['total']

    def test_bulk_delete_removes_features(self):
        self.assertEqual(self._feature_total(), 1)
        resp = self._delete({'ids': [1]})
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['deleted'], 1)
        self.assertEqual(self._feature_total(), 0)

    def test_bulk_delete_empty_ids_noop(self):
        resp = self._delete({'ids': []})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['deleted'], 0)
        self.assertEqual(self._feature_total(), 1)

    def test_bulk_delete_unknown_id_deletes_zero(self):
        resp = self._delete({'ids': [999999]})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['deleted'], 0)
        self.assertEqual(self._feature_total(), 1)

    def test_bulk_delete_missing_ids_is_400(self):
        resp = self._delete({})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'no_ids')

    def test_bulk_delete_requires_auth(self):
        self.client.logout()
        resp = self._delete({'ids': [1]})
        self.assertEqual(resp.status_code, 401)

    def test_bulk_delete_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        resp = self._delete({'ids': [1]})
        self.assertEqual(resp.status_code, 403)


class LayerDuplicateEndpointTests(GisLayersTestCase):
    """POST /me/gis/api/layers/<pk>/duplicate/ — полная копия слоя."""

    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='fields'))   # 1 объект: num=42
        self.layer = GisLayer.objects.get()

    def _dup(self, body=None):
        return self.client.post(
            f'/me/gis/api/layers/{self.layer.pk}/duplicate/',
            data=json.dumps(body or {}), content_type='application/json')

    def test_duplicate_creates_copy(self):
        resp = self._dup()
        self.assertEqual(resp.status_code, 201, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(GisLayer.objects.count(), 2)
        new_layer = GisLayer.objects.get(pk=body['layer']['id'])
        self.assertNotEqual(new_layer.pk, self.layer.pk)
        self.assertEqual(new_layer.title, f'копия_{self.layer.title}')
        self.assertEqual(new_layer.feature_count, self.layer.feature_count)
        self.assertEqual(new_layer.geom_kind, self.layer.geom_kind)
        self.assertTrue(_table_exists(new_layer.table_name))
        self.assertNotEqual(new_layer.table_name, self.layer.table_name)

    def test_duplicate_copies_style_and_color(self):
        self.layer.color = '#123456'
        self.layer.style = {'mode': 'single', 'locked': True}
        self.layer.save(update_fields=['color', 'style'])
        resp = self._dup()
        self.assertEqual(resp.status_code, 201, resp.content)
        new_layer = GisLayer.objects.get(pk=resp.json()['layer']['id'])
        self.assertEqual(new_layer.color, '#123456')
        self.assertEqual(new_layer.style.get('locked'), True)

    def test_duplicate_custom_title(self):
        resp = self._dup({'title': 'Моя копия'})
        self.assertEqual(resp.status_code, 201, resp.content)
        new_layer = GisLayer.objects.get(pk=resp.json()['layer']['id'])
        self.assertEqual(new_layer.title, 'Моя копия')

    def test_anonymous_denied(self):
        self.client.logout()
        self.assertEqual(self._dup().status_code, 401)

    def test_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        self.assertEqual(self._dup().status_code, 403)


def _table_columns(name):
    """Множество имён колонок физической таблицы слоя."""
    with connection.cursor() as cur:
        cur.execute(
            'SELECT column_name FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s',
            ['public', name])
        return {row[0] for row in cur.fetchall()}


class ColumnManagementTests(GisLayersTestCase):
    """Добавление / переименование / удаление атрибутивных столбцов слоя."""

    def setUp(self):
        self._login_admin()
        self._upload(_make_shp_zip(shp_name='fields'))   # атрибуты name, num
        self.layer = GisLayer.objects.get()

    def _add(self, body):
        return self.client.post(
            f'/me/gis/api/layers/{self.layer.pk}/columns/',
            data=json.dumps(body), content_type='application/json')

    def _col_url(self, db):
        return f'/me/gis/api/layers/{self.layer.pk}/columns/{db}/'

    def _attr_db(self, name):
        self.layer.refresh_from_db()
        for a in self.layer.attributes:
            if a['name'] == name:
                return a['db']
        return None

    # ── Добавление ──
    def test_add_column_creates_meta_and_physical_column(self):
        resp = self._add({'name': 'Примечание', 'type': 'text'})
        self.assertEqual(resp.status_code, 201, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        db = body['column']['db']
        self.assertEqual(body['column']['type'], 'text')
        self.layer.refresh_from_db()
        names = [a['name'] for a in self.layer.attributes]
        self.assertIn('Примечание', names)
        self.assertIn(db, _table_columns(self.layer.table_name))

    def test_add_column_numeric_type(self):
        resp = self._add({'name': 'Урожай', 'type': 'double precision'})
        self.assertEqual(resp.status_code, 201, resp.content)
        self.assertEqual(resp.json()['column']['type'], 'double precision')

    def test_add_column_dedup_db_name(self):
        # Повторное добавление того же отображаемого имени → уникальный db.
        db1 = self._add({'name': 'note', 'type': 'text'}).json()['column']['db']
        db2 = self._add({'name': 'note', 'type': 'text'}).json()['column']['db']
        self.assertNotEqual(db1, db2)
        cols = _table_columns(self.layer.table_name)
        self.assertIn(db1, cols)
        self.assertIn(db2, cols)

    def test_add_column_empty_name_400(self):
        self.assertEqual(self._add({'name': '  ', 'type': 'text'}).status_code, 400)

    def test_add_column_invalid_type_400(self):
        self.assertEqual(self._add({'name': 'x', 'type': 'bogus'}).status_code, 400)

    # ── Переименование (имя + физическая колонка) ──
    def test_rename_column_renames_physical_column(self):
        old_db = self._attr_db('num')
        resp = self.client.patch(
            self._col_url(old_db), data=json.dumps({'name': 'quantity'}),
            content_type='application/json')
        self.assertEqual(resp.status_code, 200, resp.content)
        new_db = resp.json()['layer']['attributes']
        new_db = next(a['db'] for a in new_db if a['name'] == 'quantity')
        self.assertNotEqual(new_db, old_db)
        cols = _table_columns(self.layer.table_name)
        self.assertIn(new_db, cols)
        self.assertNotIn(old_db, cols)
        self.layer.refresh_from_db()
        attr = next(a for a in self.layer.attributes if a['db'] == new_db)
        self.assertEqual(attr['name'], 'quantity')

    def test_rename_cyrillic_name_slugs_physical_column(self):
        old_db = self._attr_db('num')
        resp = self.client.patch(
            self._col_url(old_db), data=json.dumps({'name': 'Номер'}),
            content_type='application/json')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.layer.refresh_from_db()
        attr = next(a for a in self.layer.attributes if a['name'] == 'Номер')
        self.assertNotEqual(attr['db'], old_db)   # 'num' → slug (кириллица)
        cols = _table_columns(self.layer.table_name)
        self.assertIn(attr['db'], cols)
        self.assertNotIn(old_db, cols)

    def test_rename_updates_style_field(self):
        old_db = self._attr_db('num')
        self.layer.style = {'mode': 'graduated', 'field': old_db, 'stops': []}
        self.layer.save(update_fields=['style'])
        resp = self.client.patch(
            self._col_url(old_db), data=json.dumps({'name': 'quantity'}),
            content_type='application/json')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.layer.refresh_from_db()
        new_db = next(a['db'] for a in self.layer.attributes if a['name'] == 'quantity')
        self.assertEqual(self.layer.style.get('field'), new_db)

    def test_rename_empty_name_400(self):
        db = self._attr_db('num')
        resp = self.client.patch(
            self._col_url(db), data=json.dumps({'name': ''}),
            content_type='application/json')
        self.assertEqual(resp.status_code, 400)

    def test_rename_unknown_column_404(self):
        resp = self.client.patch(
            self._col_url('nope'), data=json.dumps({'name': 'X'}),
            content_type='application/json')
        self.assertEqual(resp.status_code, 404)

    # ── Удаление ──
    def test_delete_column_drops_meta_and_physical_column(self):
        db = self._attr_db('num')
        resp = self.client.delete(self._col_url(db))
        self.assertEqual(resp.status_code, 200, resp.content)
        self.layer.refresh_from_db()
        self.assertNotIn(db, [a['db'] for a in self.layer.attributes])
        self.assertNotIn(db, _table_columns(self.layer.table_name))

    def test_delete_column_resets_style_using_it(self):
        db = self._attr_db('num')
        self.layer.style = {'mode': 'graduated', 'field': db, 'stops': []}
        self.layer.save(update_fields=['style'])
        self.client.delete(self._col_url(db))
        self.layer.refresh_from_db()
        self.assertEqual(self.layer.style, {})

    def test_delete_unknown_column_404(self):
        self.assertEqual(self.client.delete(self._col_url('nope')).status_code, 404)

    # ── Доступ ──
    def test_add_requires_login(self):
        self.client.logout()
        self.assertEqual(self._add({'name': 'x', 'type': 'text'}).status_code, 401)

    def test_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        self.assertEqual(self._add({'name': 'x', 'type': 'text'}).status_code, 403)
        db = self._attr_db('num')
        self.assertEqual(self.client.delete(self._col_url(db)).status_code, 403)
