"""Тесты MVT-эндпоинта угодий (``agrocosmos/views/tiles.py::api_tile``).

Регрессия на 504-таймаут карты: без фильтра region/district запрос
общероссийский, и на низких зумах bbox тайла охватывает огромную площадь
— ST_AsMVT по миллионам полигонов не укладывается в таймаут шлюза.
Поэтому ниже ``MIN_TILE_ZOOM_UNFILTERED`` сервер обязан отдавать пустой
тайл БЕЗ обращения к БД. Фильтрованный запрос ограничен индексом FK и
сохраняет низкий порог ``MIN_TILE_ZOOM_FILTERED``.
"""
import math
from types import SimpleNamespace

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings
from django.test.client import RequestFactory
from django.utils import timezone

from access.models import ResourceGrant
from agrocosmos.models import District, Farmland, FarmlandCropSeason, Region
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


def _square(x, y, size):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _tile_xy(lon, lat, z):
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_r = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_r)) / math.pi) / 2.0 * n)
    return x, y


@override_settings(CACHES=_DUMMY_CACHE)
class TileSeasonTests(TestCase):
    """``season_year`` добавляет в тайл класс сезона и признак уборки."""

    Z = 12

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50, 0.5),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50, 0.5),
        )
        cls.farmland = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(30.10, 50.10, 0.02),
        )
        # raster говорит «яровые», fused — «озимые, убрано»: побеждает fused.
        FarmlandCropSeason.objects.create(
            farmland=cls.farmland, year=2026, source='raster',
            season_class='spring',
        )
        FarmlandCropSeason.objects.create(
            farmland=cls.farmland, year=2026, source='fused',
            season_class='winter', is_harvested=True,
        )

    def _get(self, query):
        x, y = _tile_xy(30.11, 50.11, self.Z)
        req = RequestFactory().get(
            f'/agrocosmos/api/tiles/{self.Z}/{x}/{y}.pbf?region={self.region.id}'
            + query)
        req.legacy_user = SimpleNamespace(is_superuser=True, username='admin')
        with self.assertNoLogs('agrocosmos', level='ERROR'):
            return tiles.api_tile(req, self.Z, x, y)

    def test_plain_tile_has_no_season_props(self):
        body = self._get('').content
        self.assertIn(b'crop_type', body)
        self.assertNotIn(b'season_class', body)

    def test_season_year_adds_class_preferring_fused(self):
        body = self._get('&season_year=2026').content
        self.assertIn(b'season_class', body)
        self.assertIn(b'harvested', body)
        self.assertIn(b'winter', body)
        self.assertNotIn(b'spring', body)

    def test_year_without_classification_keeps_farmland(self):
        body = self._get('&season_year=2020').content
        self.assertIn(b'crop_type', body)
        self.assertNotIn(b'winter', body)

    def test_invalid_season_year_is_ignored(self):
        for raw in ('abc', '1', '2026;DROP'):
            self.assertEqual(tiles._season_join(raw), ('', ''))
        body = self._get('&season_year=abc').content
        self.assertNotIn(b'season_class', body)

    def test_dashboard_has_season_filters(self):
        html = self.client.get('/agrocosmos/').content.decode()
        for value in ('winter', 'spring', 'harvested', 'unused'):
            self.assertIn(
                f'class="agro-season-cb" value="{value}"', html)


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

    def test_raster_dashboard_has_season_filters(self):
        from django.test import Client
        ResourceGrant.objects.create(
            legacy_user=self.user, level='view',
            resource_type=ResourceGrant.ResourceType.FARMLAND)
        c = Client()
        s = c.session
        s['legacy_user_id'] = self.user.id
        s.save()
        html = c.get('/agrocosmos/raster/').content.decode()
        for value in ('winter', 'spring', 'harvested', 'unused'):
            self.assertIn(
                f'class="agro-season-cb" value="{value}"', html)

    def test_report_cards_link_to_raster_dashboard(self):
        html = self.client.get(
            '/agrocosmos/report/region-detailed/').content.decode()
        self.assertIn("var MAP_URL = '/agrocosmos/raster/';", html)

    def test_map_reports_tab_matches_map_kind(self):
        tab = '<a href="{}" class="">\n          <i class="material-icons"'
        tab += ' style="font-size:14px; vertical-align:-2px;">assessment</i>'
        html = self.client.get('/agrocosmos/').content.decode()
        self.assertIn(tab.format('/agrocosmos/report/region/'), html)
        html = self.client.get('/agrocosmos/raster/').content.decode()
        self.assertIn(tab.format('/agrocosmos/report/region-detailed/'), html)
