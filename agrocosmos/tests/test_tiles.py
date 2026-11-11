"""Тесты MVT-эндпоинта угодий (``agrocosmos/views/tiles.py::api_tile``).

Регрессия на 504-таймаут карты: без фильтра region/district запрос
общероссийский, и на низких зумах bbox тайла охватывает огромную площадь
— ST_AsMVT по миллионам полигонов не укладывается в таймаут шлюза.
Поэтому ниже ``MIN_TILE_ZOOM_UNFILTERED`` сервер обязан отдавать пустой
тайл БЕЗ обращения к БД. Фильтрованный запрос ограничен индексом FK и
сохраняет низкий порог ``MIN_TILE_ZOOM_FILTERED``.
"""
from types import SimpleNamespace

from django.test import TestCase, override_settings
from django.test.client import RequestFactory
from django.utils import timezone

from access.models import ResourceGrant
from agrocosmos.views import tiles
from legacy.models import LegacyUser

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


@override_settings(CACHES=_DUMMY_CACHE)
class TileZoomFloorTests(TestCase):
    """Порог зума короткозамыкает дорогой запрос до PostGIS."""

    def setUp(self):
        self.rf = RequestFactory()
        self.admin = SimpleNamespace(is_superuser=True, username='admin')

    def _get(self, z, x, y, query=''):
        req = self.rf.get(f'/agrocosmos/api/tiles/{z}/{x}/{y}.pbf' + query)
        req.legacy_user = self.admin
        return tiles.api_tile(req, z, x, y)

    def test_unfiltered_below_floor_is_empty_without_db(self):
        with self.assertNumQueries(0):
            resp = self._get(6, 41, 20)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, b'')
        self.assertEqual(resp['Content-Type'], 'application/x-protobuf')

    def test_filtered_keeps_lower_floor(self):
        # region-фильтр опускает порог до MIN_TILE_ZOOM_FILTERED (5),
        # поэтому z=6 уже НЕ короткозамыкается и уходит в БД.
        with self.assertNumQueries(1):
            resp = self._get(6, 41, 20, query='?region=1')
        self.assertEqual(resp.status_code, 200)

    def test_unfiltered_below_filtered_floor_is_empty(self):
        with self.assertNumQueries(0):
            resp = self._get(4, 10, 5)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, b'')

    def test_non_admin_gets_403_without_db(self):
        req = self.rf.get('/agrocosmos/api/tiles/6/41/20.pbf')
        req.legacy_user = None
        with self.assertNumQueries(0):
            resp = tiles.api_tile(req, 6, 41, 20)
        self.assertEqual(resp.status_code, 403)


def _mk_legacy(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RUB', name=username,
        address='', phone='', inn='', status=10,
        created_at=now, updated_at=now, contacts='',
    )


@override_settings(CACHES=_DUMMY_CACHE)
class TileFarmlandGrantTests(TestCase):
    """Слой «Границы угодий» открывается грантом ``farmland``, не только админу."""

    def setUp(self):
        self.rf = RequestFactory()
        self.user = _mk_legacy('sorokina')

    def _get(self):
        req = self.rf.get('/agrocosmos/api/tiles/6/41/20.pbf')
        req.legacy_user = self.user
        return tiles.api_tile(req, 6, 41, 20)

    def test_plain_user_gets_403(self):
        self.assertEqual(self._get().status_code, 403)

    def test_gis_layer_grant_does_not_open_farmlands(self):
        ResourceGrant.objects.create(
            legacy_user=self.user, level='manage',
            resource_type=ResourceGrant.ResourceType.GIS_LAYER)
        self.assertEqual(self._get().status_code, 403)

    def test_farmland_grant_opens_tiles(self):
        ResourceGrant.objects.create(
            legacy_user=self.user, level='view',
            resource_type=ResourceGrant.ResourceType.FARMLAND)
        self.assertEqual(self._get().status_code, 200)

    def test_dashboard_toggle_follows_grant(self):
        from django.test import Client
        c = Client()
        s = c.session
        s['legacy_user_id'] = self.user.id
        s.save()
        toggle = 'layers</i> Границы угодий'
        html = c.get('/agrocosmos/').content.decode()
        self.assertNotIn(toggle, html)
        ResourceGrant.objects.create(
            legacy_user=self.user, level='view',
            resource_type=ResourceGrant.ResourceType.FARMLAND)
        html = c.get('/agrocosmos/').content.decode()
        self.assertIn(toggle, html)
