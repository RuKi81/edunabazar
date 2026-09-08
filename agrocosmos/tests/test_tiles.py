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

from agrocosmos.views import tiles

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
