"""Тесты Фазы 1 растрового модуля (my_fields ГИС).

Покрывают три независимых слоя:

* :mod:`my_fields.services.s3_storage` — абстракция объектного хранилища
  (генерация ключей, /vsis3/ путь, GDAL-env, multipart/presign/head/delete
  на моке boto3-клиента);
* модель :class:`my_fields.models.RasterLayer` — дефолты/статусы;
* пер-ресурсный доступ ``resource_type='raster_layer'`` через
  ``access.services`` (грант конкретного слоя / whole-class, страница /me/gis).

Живой MinIO не требуется — boto3-клиент мокается.
"""
import os as _os
import shutil as _shutil
import tempfile as _tempfile
import unittest
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

try:  # rasterio/numpy для тестов конвейера ingest (Фаза 3)
    import numpy as _np
    import rasterio as _rio
    from rasterio.transform import from_origin as _from_origin
    _HAS_RASTERIO = True
except Exception:  # pragma: no cover - окружения без rasterio
    _HAS_RASTERIO = False

from access.models import ResourceGrant
from access.services import (
    accessible_raster_layer_ids,
    can_open_gis_page,
    has_resource_access,
)
from legacy.models import LegacyUser
from my_fields.models import RasterLayer
from my_fields.services import s3_storage

User = get_user_model()

_S3_SETTINGS = dict(
    S3_ENDPOINT_URL='http://10.0.0.11:9000',
    S3_PUBLIC_ENDPOINT_URL='https://s3.edunabazar.ru',
    S3_ACCESS_KEY='key',
    S3_SECRET_KEY='secret',
    S3_REGION='us-east-1',
    S3_BUCKET_UPLOADS='raster-uploads',
    S3_BUCKET_COG='raster-cog',
)


def _mk_legacy(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RUB', name=username,
        address='', phone='', inn='', status=10,
        created_at=now, updated_at=now, contacts='',
    )


# ─────────────────────────────────────────────────────────────────────
# s3_storage — чистые функции (без сети)
# ─────────────────────────────────────────────────────────────────────
class S3StorageHelpersTest(TestCase):
    @override_settings(**_S3_SETTINGS)
    def test_is_configured_true(self):
        self.assertTrue(s3_storage.is_configured())

    @override_settings(S3_ENDPOINT_URL='', S3_ACCESS_KEY='', S3_SECRET_KEY='')
    def test_is_configured_false(self):
        self.assertFalse(s3_storage.is_configured())

    def test_build_upload_key_shape(self):
        key = s3_storage.build_upload_key(8, 'MyRaster.TIF')
        self.assertTrue(key.startswith('8/'))
        self.assertTrue(key.endswith('/original.tif'))
        # uuid-папка между owner и файлом уникальна
        other = s3_storage.build_upload_key(8, 'MyRaster.TIF')
        self.assertNotEqual(key, other)

    def test_build_upload_key_owner_none_and_no_ext(self):
        key = s3_storage.build_upload_key(None, 'noext')
        self.assertTrue(key.startswith('0/'))
        self.assertTrue(key.endswith('/original'))

    def test_build_cog_key(self):
        self.assertEqual(s3_storage.build_cog_key(42), '42/cog.tif')

    @override_settings(**_S3_SETTINGS)
    def test_vsis3_path_defaults_to_cog_bucket(self):
        self.assertEqual(
            s3_storage.vsis3_path('42/cog.tif'),
            '/vsis3/raster-cog/42/cog.tif',
        )

    @override_settings(**_S3_SETTINGS)
    def test_gdal_vsis3_env(self):
        env = s3_storage.gdal_vsis3_env()
        self.assertEqual(env['AWS_S3_ENDPOINT'], '10.0.0.11:9000')
        self.assertEqual(env['AWS_HTTPS'], 'NO')
        self.assertEqual(env['AWS_VIRTUAL_HOSTING'], 'FALSE')
        self.assertEqual(env['AWS_ACCESS_KEY_ID'], 'key')
        self.assertEqual(env['GDAL_DISABLE_READDIR_ON_OPEN'], 'EMPTY_DIR')


# ─────────────────────────────────────────────────────────────────────
# s3_storage — операции с хранилищем на моке boto3-клиента
# ─────────────────────────────────────────────────────────────────────
@override_settings(**_S3_SETTINGS)
class S3StorageOpsTest(TestCase):
    def setUp(self):
        self.client = MagicMock()
        patcher = patch.object(s3_storage, '_client', return_value=self.client)
        self.addCleanup(patcher.stop)
        self.mock_client = patcher.start()

    def test_create_multipart_upload(self):
        self.client.create_multipart_upload.return_value = {'UploadId': 'uid-1'}
        uid = s3_storage.create_multipart_upload('k/original.tif')
        self.assertEqual(uid, 'uid-1')
        self.client.create_multipart_upload.assert_called_once()
        kwargs = self.client.create_multipart_upload.call_args.kwargs
        self.assertEqual(kwargs['Bucket'], 'raster-uploads')
        self.assertEqual(kwargs['Key'], 'k/original.tif')

    def test_presign_part_url_uses_public_client(self):
        self.client.generate_presigned_url.return_value = 'https://signed/part'
        url = s3_storage.presign_part_url('k', 'uid', 3)
        self.assertEqual(url, 'https://signed/part')
        # presign частей должен идти на публичный эндпоинт
        self.mock_client.assert_called_with(public=True)
        args = self.client.generate_presigned_url.call_args
        self.assertEqual(args.args[0], 'upload_part')
        self.assertEqual(args.kwargs['Params']['PartNumber'], 3)

    def test_complete_multipart_sorts_parts(self):
        parts = [
            {'PartNumber': 2, 'ETag': 'b'},
            {'PartNumber': 1, 'ETag': 'a'},
        ]
        s3_storage.complete_multipart_upload('k', 'uid', parts)
        kwargs = self.client.complete_multipart_upload.call_args.kwargs
        ordered = kwargs['MultipartUpload']['Parts']
        self.assertEqual([p['PartNumber'] for p in ordered], [1, 2])

    def test_abort_multipart_upload(self):
        s3_storage.abort_multipart_upload('k', 'uid')
        self.client.abort_multipart_upload.assert_called_once_with(
            Bucket='raster-uploads', Key='k', UploadId='uid')

    def test_object_size(self):
        self.client.head_object.return_value = {'ContentLength': 123}
        self.assertEqual(s3_storage.object_size('k', bucket='raster-uploads'), 123)

    def test_delete_object_noop_on_empty_key(self):
        s3_storage.delete_object('')
        self.client.delete_object.assert_not_called()

    def test_delete_object(self):
        s3_storage.delete_object('k', bucket='raster-cog')
        self.client.delete_object.assert_called_once_with(
            Bucket='raster-cog', Key='k')


# ─────────────────────────────────────────────────────────────────────
# Модель RasterLayer
# ─────────────────────────────────────────────────────────────────────
class RasterLayerModelTest(TestCase):
    def test_defaults(self):
        r = RasterLayer.objects.create(title='Ортофото')
        self.assertEqual(r.status, RasterLayer.Status.UPLOADING)
        self.assertEqual(r.size_bytes, 0)
        self.assertEqual(r.band_count, 0)
        self.assertEqual(r.stats, [])
        self.assertEqual(r.style, {})
        self.assertEqual(r.opacity, 1.0)
        self.assertIn('Загрузка', str(r))


# ─────────────────────────────────────────────────────────────────────
# Доступ: resource_type='raster_layer'
# ─────────────────────────────────────────────────────────────────────
class RasterAccessTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.RL = ResourceGrant.ResourceType.RASTER_LAYER
        cls.layer_a = RasterLayer.objects.create(title='A')
        cls.layer_b = RasterLayer.objects.create(title='B')
        cls.viewer = _mk_legacy('r_viewer')     # whole-class view
        cls.perlayer = _mk_legacy('r_perlayer')  # только layer_a
        cls.nobody = _mk_legacy('r_nobody')
        ResourceGrant.objects.create(
            legacy_user=cls.viewer, resource_type=cls.RL,
            resource_id=None, level='view')
        ResourceGrant.objects.create(
            legacy_user=cls.perlayer, resource_type=cls.RL,
            resource_id=cls.layer_a.pk, level='view')

    def test_whole_class_view(self):
        self.assertTrue(has_resource_access(
            self.viewer, self.RL, self.layer_b.pk, 'view'))
        self.assertIsNone(accessible_raster_layer_ids(self.viewer))
        self.assertTrue(can_open_gis_page(self.viewer))

    def test_per_layer_scope(self):
        self.assertTrue(has_resource_access(
            self.perlayer, self.RL, self.layer_a.pk, 'view'))
        self.assertFalse(has_resource_access(
            self.perlayer, self.RL, self.layer_b.pk, 'view'))
        self.assertEqual(
            accessible_raster_layer_ids(self.perlayer), {self.layer_a.pk})
        self.assertTrue(can_open_gis_page(self.perlayer))

    def test_view_grant_insufficient_for_edit(self):
        self.assertFalse(has_resource_access(
            self.viewer, self.RL, self.layer_a.pk, 'edit'))

    def test_nobody_denied(self):
        self.assertFalse(has_resource_access(
            self.nobody, self.RL, self.layer_a.pk, 'view'))
        self.assertEqual(accessible_raster_layer_ids(self.nobody), set())
        self.assertFalse(can_open_gis_page(self.nobody))


# ─────────────────────────────────────────────────────────────────────
# Эндпоинты загрузки: init → sign → complete / abort / detail
# boto3 не нужен — функции s3_storage мокаются целиком.
# ─────────────────────────────────────────────────────────────────────
@override_settings(**_S3_SETTINGS)
class RasterUploadEndpointsTest(TestCase):
    INIT = '/me/gis/api/rasters/upload/init/'
    SIGN = '/me/gis/api/rasters/upload/sign/'
    COMPLETE = '/me/gis/api/rasters/upload/complete/'
    ABORT = '/me/gis/api/rasters/upload/abort/'
    LIST = '/me/gis/api/rasters/'

    @classmethod
    def setUpTestData(cls):
        RL = ResourceGrant.ResourceType.RASTER_LAYER
        cls.manager_dj = User.objects.create_user('r_mgr', password='x')
        cls.manager_lu = _mk_legacy('r_mgr')
        ResourceGrant.objects.create(
            legacy_user=cls.manager_lu, resource_type=RL,
            resource_id=None, level='manage')
        cls.nobody_dj = User.objects.create_user('r_none', password='x')
        cls.nobody_lu = _mk_legacy('r_none')

    def _login(self, dj, lu):
        self.client.force_login(dj)
        session = self.client.session
        session['legacy_user_id'] = lu.pk
        session.save()

    def _login_manager(self):
        self._login(self.manager_dj, self.manager_lu)

    def _post(self, url, payload):
        import json
        return self.client.post(
            url, data=json.dumps(payload), content_type='application/json')

    def _detail(self, pk):
        return f'/me/gis/api/rasters/{pk}/'

    # ── init ──
    def test_init_creates_layer_and_multipart(self):
        self._login_manager()
        with patch.object(s3_storage, 'create_multipart_upload',
                          return_value='uid-1') as mock_create:
            resp = self._post(self.INIT,
                              {'filename': 'field.tif', 'size': 100 * 1024 * 1024})
        self.assertEqual(resp.status_code, 201, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['upload_id'], 'uid-1')
        self.assertEqual(body['part_count'], 2)  # 100 МБ / 64 МБ → 2 части
        mock_create.assert_called_once()
        layer = RasterLayer.objects.get(pk=body['layer_id'])
        self.assertEqual(layer.status, RasterLayer.Status.UPLOADING)
        self.assertEqual(layer.upload_id, 'uid-1')
        self.assertEqual(layer.original_filename, 'field.tif')
        self.assertEqual(layer.title, 'field')

    def test_init_rejects_non_tiff(self):
        self._login_manager()
        resp = self._post(self.INIT, {'filename': 'x.png', 'size': 10})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_filename')

    def test_init_rejects_bad_size(self):
        self._login_manager()
        resp = self._post(self.INIT, {'filename': 'x.tif', 'size': 0})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_size')

    def test_init_forbidden_without_manage(self):
        self._login(self.nobody_dj, self.nobody_lu)
        resp = self._post(self.INIT, {'filename': 'x.tif', 'size': 10})
        self.assertEqual(resp.status_code, 403)

    @override_settings(S3_ENDPOINT_URL='', S3_ACCESS_KEY='', S3_SECRET_KEY='')
    def test_init_storage_disabled_503(self):
        s3_storage.reset_clients()
        self._login_manager()
        resp = self._post(self.INIT, {'filename': 'x.tif', 'size': 10})
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()['error'], 'storage_disabled')

    # ── sign ──
    def _make_uploading_layer(self):
        return RasterLayer.objects.create(
            title='f', status=RasterLayer.Status.UPLOADING,
            original_filename='f.tif', upload_key='0/x/original.tif',
            upload_id='uid-1', size_bytes=100, owner=self.manager_dj,
        )

    def test_sign_returns_urls(self):
        self._login_manager()
        layer = self._make_uploading_layer()
        with patch.object(s3_storage, 'presign_part_url',
                          side_effect=lambda k, u, n, **kw: f'https://s/{n}'):
            resp = self._post(self.SIGN,
                              {'layer_id': layer.pk, 'part_numbers': [1, 2]})
        self.assertEqual(resp.status_code, 200, resp.content)
        urls = resp.json()['urls']
        self.assertEqual(urls['1'], 'https://s/1')
        self.assertEqual(urls['2'], 'https://s/2')

    def test_sign_rejects_bad_part_numbers(self):
        self._login_manager()
        layer = self._make_uploading_layer()
        resp = self._post(self.SIGN, {'layer_id': layer.pk, 'part_numbers': []})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_part_numbers')

    def test_sign_not_uploading_conflict(self):
        self._login_manager()
        layer = RasterLayer.objects.create(
            title='f', status=RasterLayer.Status.QUEUED, upload_id='')
        resp = self._post(self.SIGN, {'layer_id': layer.pk, 'part_numbers': [1]})
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.json()['error'], 'not_uploading')

    # ── complete ──
    def test_complete_finalizes_and_queues(self):
        from agrocosmos.models import PipelineRun
        self._login_manager()
        layer = self._make_uploading_layer()
        with patch.object(s3_storage, 'complete_multipart_upload') as mock_c, \
                patch.object(s3_storage, 'object_size', return_value=12345):
            resp = self._post(self.COMPLETE, {
                'layer_id': layer.pk,
                'parts': [{'PartNumber': 1, 'ETag': 'a'},
                          {'PartNumber': 2, 'ETag': 'b'}],
            })
        self.assertEqual(resp.status_code, 200, resp.content)
        mock_c.assert_called_once()
        layer.refresh_from_db()
        self.assertEqual(layer.status, RasterLayer.Status.QUEUED)
        self.assertEqual(layer.upload_id, '')
        self.assertEqual(layer.size_bytes, 12345)
        # Поставлена задача конвертации в COG для этого слоя.
        run = PipelineRun.objects.filter(
            task_type=PipelineRun.TaskType.RASTER_INGEST,
            status=PipelineRun.Status.QUEUED).latest('pk')
        self.assertEqual(run.launch_args.get('layer_id'), layer.pk)

    def test_complete_rejects_invalid_parts(self):
        self._login_manager()
        layer = self._make_uploading_layer()
        resp = self._post(self.COMPLETE, {'layer_id': layer.pk, 'parts': []})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_parts')

    # ── abort ──
    def test_abort_deletes_layer(self):
        self._login_manager()
        layer = self._make_uploading_layer()
        with patch.object(s3_storage, 'abort_multipart_upload') as mock_a:
            resp = self._post(self.ABORT, {'layer_id': layer.pk})
        self.assertEqual(resp.status_code, 200, resp.content)
        mock_a.assert_called_once()
        self.assertFalse(RasterLayer.objects.filter(pk=layer.pk).exists())

    # ── detail PATCH / DELETE ──
    def test_patch_title_and_opacity(self):
        self._login_manager()
        layer = RasterLayer.objects.create(
            title='old', status=RasterLayer.Status.READY, opacity=1.0)
        resp = self.client.patch(
            self._detail(layer.pk),
            data='{"title": "Новый", "opacity": 0.4}',
            content_type='application/json')
        self.assertEqual(resp.status_code, 200, resp.content)
        layer.refresh_from_db()
        self.assertEqual(layer.title, 'Новый')
        self.assertAlmostEqual(layer.opacity, 0.4)

    def test_delete_removes_layer_and_objects(self):
        self._login_manager()
        layer = RasterLayer.objects.create(
            title='r', status=RasterLayer.Status.READY,
            upload_key='0/x/original.tif', cog_key='9/cog.tif')
        with patch.object(s3_storage, 'delete_object') as mock_del:
            resp = self.client.delete(self._detail(layer.pk))
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertFalse(RasterLayer.objects.filter(pk=layer.pk).exists())
        self.assertEqual(mock_del.call_count, 2)  # upload_key + cog_key


# ─────────────────────────────────────────────────────────────────────
# Конвейер ingest (Фаза 3): original → COG + метаданные.
# Работает на локальном синтетическом GeoTIFF; S3 (download/upload) мокается.
# ─────────────────────────────────────────────────────────────────────
def _write_synthetic_tif(path, *, crs='EPSG:32637', nodata=0.0):
    """32×32, 1 канал, UTM-37N; левый-верхний угол ~ (500000, 5500000)."""
    data = _np.arange(1, 32 * 32 + 1, dtype='float32').reshape(32, 32)
    data[0, 0] = nodata  # хотя бы один nodata-пиксель
    transform = _from_origin(500000, 5500000, 10, 10)  # 10 м/пиксель
    with _rio.open(
        path, 'w', driver='GTiff', height=32, width=32, count=1,
        dtype='float32', crs=crs, transform=transform, nodata=nodata,
    ) as ds:
        ds.write(data, 1)


@unittest.skipUnless(_HAS_RASTERIO, 'rasterio недоступен')
class RasterIngestServiceTest(TestCase):
    def setUp(self):
        self.tmp = _tempfile.mkdtemp(prefix='raster_ingest_test_')
        self.addCleanup(lambda: _shutil.rmtree(self.tmp, ignore_errors=True))

    def test_extract_metadata(self):
        from my_fields.services import raster_ingest
        src = _os.path.join(self.tmp, 'src.tif')
        _write_synthetic_tif(src)
        meta = raster_ingest.extract_metadata(src)
        self.assertEqual(meta['srid'], 32637)
        self.assertEqual(meta['band_count'], 1)
        self.assertEqual(meta['nodata'], 0.0)
        # bounds в 4326: долгота ~39°E, широта ~49°N (UTM 37N).
        minx, miny, maxx, maxy = meta['bounds']
        self.assertTrue(30 < minx < 45 and 30 < maxx < 45)
        self.assertTrue(45 < miny < 55 and 45 < maxy < 55)
        st = meta['stats'][0]
        self.assertIn('p2', st)
        self.assertIn('p98', st)
        self.assertGreater(st['max'], st['min'])

    def test_convert_to_cog_is_readable(self):
        from my_fields.services import raster_ingest
        src = _os.path.join(self.tmp, 'src.tif')
        dst = _os.path.join(self.tmp, 'cog.tif')
        _write_synthetic_tif(src)
        raster_ingest.convert_to_cog(src, dst)
        self.assertTrue(_os.path.exists(dst))
        with _rio.open(dst) as ds:
            self.assertEqual(ds.count, 1)
            self.assertEqual(ds.crs.to_epsg(), 32637)

    @override_settings(**_S3_SETTINGS)
    def test_ingest_raster_layer_full(self):
        from my_fields.services import raster_ingest
        src = _os.path.join(self.tmp, 'src.tif')
        _write_synthetic_tif(src)

        layer = RasterLayer.objects.create(
            title='ingest me', status=RasterLayer.Status.QUEUED,
            upload_key='0/x/original.tif')

        def fake_download(key, dest, bucket=None):
            _shutil.copy(src, dest)

        with patch.object(s3_storage, 'download_object',
                          side_effect=fake_download), \
                patch.object(s3_storage, 'upload_file') as mock_up:
            raster_ingest.ingest_raster_layer(layer)

        mock_up.assert_called_once()
        layer.refresh_from_db()
        self.assertEqual(layer.status, RasterLayer.Status.READY)
        self.assertEqual(layer.cog_key, f'{layer.pk}/cog.tif')
        self.assertEqual(layer.srid, 32637)
        self.assertEqual(layer.band_count, 1)
        self.assertEqual(len(layer.stats), 1)
        self.assertEqual(layer.error, '')

    def test_ingest_requires_upload_key(self):
        from my_fields.services import raster_ingest
        layer = RasterLayer.objects.create(
            title='no key', status=RasterLayer.Status.QUEUED)
        with self.assertRaises(raster_ingest.RasterIngestError):
            raster_ingest.ingest_raster_layer(layer)
