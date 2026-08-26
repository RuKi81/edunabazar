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
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

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
