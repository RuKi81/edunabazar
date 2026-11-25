"""
Тесты многострановости Агрокосмоса (``Region.country``).

Главный инвариант: всё, что не задаёт страну явно, работает по России —
субъекты других стран не попадают ни в селекты, ни в хороплет, ни в
страновой отчёт, ни в автопостановку на мониторинг.
"""
import io
import json
from pathlib import Path
from datetime import date
from unittest import mock

from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Point, Polygon
from django.core.management import call_command
from django.test import SimpleTestCase, TestCase, override_settings

from agrocosmos.models import (
    District, DistrictNdviSeries, Farmland, MonitoringTask, Region,
)
from agrocosmos.services import districts_status_geojson
from agrocosmos.services.countries import (
    COUNTRIES, MONITORED_COUNTRIES, get_country, normalize_country,
)
from agrocosmos.views.reports import _rf_total_farmland_area_ha

YEAR = date.today().year
D1 = date(YEAR, 6, 10)

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}
_LOCMEM_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'},
}


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class CountryRegistryTests(SimpleTestCase):
    def test_downloaded_country_outlines(self):
        path = (Path(__file__).resolve().parents[1] / 'static'
                / 'agrocosmos' / 'country_outlines.geojson')
        payload = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(payload['type'], 'FeatureCollection')
        features = payload['features']
        self.assertEqual([f['properties']['country'] for f in features],
                         ['KZ', 'BY', 'TR', 'IR', 'EG'])
        capitals = {
            'KZ': Point(71.43, 51.13), 'BY': Point(27.56, 53.9),
            'TR': Point(32.85, 39.93), 'IR': Point(51.39, 35.69),
            'EG': Point(31.24, 30.04),
        }
        for feature in features:
            geom = GEOSGeometry(json.dumps(feature['geometry']))
            self.assertIn(geom.geom_type, ('Polygon', 'MultiPolygon'))
            self.assertTrue(geom.valid)
            self.assertFalse(geom.empty)
            self.assertTrue(geom.contains(capitals[feature['properties']['country']]))

    def test_normalize(self):
        self.assertEqual(normalize_country('kz'), 'KZ')
        self.assertEqual(normalize_country(' BY '), 'BY')
        self.assertEqual(normalize_country('all'), 'ALL')
        self.assertIsNone(normalize_country('XX'))
        self.assertIsNone(normalize_country(''))
        self.assertIsNone(normalize_country(None))

    def test_unknown_falls_back_to_russia(self):
        self.assertEqual(get_country(None).code, 'RU')
        self.assertEqual(get_country('nope').code, 'RU')
        self.assertEqual(get_country('in').code, 'IN')

    def test_all_countries_pseudo(self):
        """«Все страны» — псевдокод с экстентом, покрывающим реестр."""
        c = get_country('ALL')
        self.assertEqual(c.code, 'ALL')
        self.assertEqual(c.name, 'Все страны')
        (south, west), (north, east) = c.bounds
        for other in COUNTRIES:
            (os_, ow), (on, oe) = other.bounds
            self.assertLessEqual(south, os_)
            self.assertLessEqual(west, ow)
            self.assertGreaterEqual(north, on)
            self.assertGreaterEqual(east, oe)

    def test_registry(self):
        self.assertEqual(
            [c.code for c in COUNTRIES],
            ['RU', 'KZ', 'BY', 'TR', 'IR', 'EG', 'IN', 'CN', 'BR'],
        )
        self.assertEqual(
            MONITORED_COUNTRIES, ('RU', 'KZ', 'BY', 'TR', 'IR', 'EG'))
        # Районы есть только там, где страна стоит на мониторинге.
        for c in COUNTRIES:
            self.assertEqual(c.has_districts, c.monitored)

    def test_region_as_district(self):
        """Египет: районов в OSM нет — район равен субъекту."""
        eg = get_country('EG')
        self.assertTrue(eg.region_as_district)
        self.assertIsNone(eg.district_admin_level)
        self.assertTrue(eg.has_districts)
        self.assertEqual(get_country('IR').district_admin_level, 5)
        self.assertEqual(get_country('TR').district_admin_level, 6)
        self.assertFalse(get_country('IN').has_districts)


class _CountriesData(TestCase):
    """РФ и Казахстан с районами и угодьями, Индия — без районов."""

    @classmethod
    def setUpTestData(cls):
        cls.ru = Region.objects.create(
            name='Регион РФ', code='ru-1', geom=_square(30, 50),
        )
        cls.kz = Region.objects.create(
            name='Область КЗ', code='KZ-1', country='KZ', geom=_square(70, 50),
        )
        cls.ind = Region.objects.create(
            name='Штат', code='IN-1', country='IN', geom=_square(78, 20),
        )
        cls.d_ru = District.objects.create(
            region=cls.ru, name='Район РФ', geom=_square(30, 50),
        )
        cls.d_kz = District.objects.create(
            region=cls.kz, name='Район КЗ', geom=_square(70, 50),
        )

        def farmland(region, district, area):
            return Farmland.objects.create(
                region=region, district=district, crop_type='arable',
                area_ha=area, geom=_square(30.1, 50.1, 0.05),
            )

        farmland(cls.ru, cls.d_ru, 100)
        farmland(cls.kz, cls.d_kz, 300)
        farmland(cls.ind, None, 500)

        for district, ndvi in ((cls.d_ru, 0.6), (cls.d_kz, 0.4)):
            DistrictNdviSeries.objects.create(
                district=district, acquired_date=D1, crop_type='arable',
                source=DistrictNdviSeries.Source.MODIS,
                sum_ndvi_area=ndvi * 100, sum_area=100,
            )


@override_settings(CACHES=_DUMMY_CACHE)
class CountryPagesTests(_CountriesData):
    def _region_names(self, url, **params):
        resp = self.client.get(url, params)
        self.assertEqual(resp.status_code, 200)
        return resp, [r.name for r in resp.context['regions']]

    def test_dashboard_defaults_to_russia(self):
        resp, names = self._region_names('/agrocosmos/')
        self.assertEqual(names, ['Регион РФ'])
        self.assertEqual(resp.context['country'].code, 'RU')
        self.assertContains(resp, 'id="agro-country"')

    def test_dashboard_country_param(self):
        resp, names = self._region_names('/agrocosmos/', country='kz')
        self.assertEqual(names, ['Область КЗ'])
        self.assertEqual(resp.context['country'].code, 'KZ')
        self.assertEqual(resp.context['region_id'], 'all')
        self.assertContains(resp, "var COUNTRY = 'KZ';")
        self.assertContains(
            resp, 'L.latLngBounds([[40.5, 46.5], [55.5, 87.5]])',
        )

    def test_select_offers_only_loaded_countries(self):
        resp = self.client.get('/agrocosmos/')
        self.assertEqual(
            [c.code for c in resp.context['countries']],
            ['ALL', 'RU', 'KZ', 'IN'],
        )
        # Страна без загруженных субъектов видна, только пока она открыта.
        resp = self.client.get('/agrocosmos/', {'country': 'BR'})
        self.assertEqual(
            [c.code for c in resp.context['countries']],
            ['ALL', 'RU', 'KZ', 'IN', 'BR'],
        )
        self.assertEqual(list(resp.context['regions']), [])

    def test_dashboard_all_countries(self):
        """«Все страны»: все загруженные субъекты и объединённый экстент."""
        resp, names = self._region_names('/agrocosmos/', country='ALL')
        self.assertEqual(resp.context['country'].code, 'ALL')
        self.assertEqual(sorted(names), ['Область КЗ', 'Регион РФ', 'Штат'])
        self.assertContains(resp, "var COUNTRY = 'ALL';")

    def test_dashboard_country_from_region(self):
        """Ссылка только с ``?region=<id>`` открывает страну региона."""
        resp, names = self._region_names('/agrocosmos/', region=self.kz.pk)
        self.assertEqual(resp.context['country'].code, 'KZ')
        self.assertEqual(names, ['Область КЗ'])

    def test_dashboard_unknown_country_is_russia(self):
        resp, names = self._region_names('/agrocosmos/', country='zz')
        self.assertEqual(resp.context['country'].code, 'RU')
        self.assertEqual(names, ['Регион РФ'])

    def test_report_region_country(self):
        url = '/agrocosmos/report/region/'
        resp, names = self._region_names(url)
        self.assertEqual(names, ['Регион РФ'])
        resp, names = self._region_names(url, country='IN', region='country')
        self.assertEqual(names, ['Штат'])
        self.assertContains(resp, 'id="rpt-country"')
        self.assertContains(resp, '— Индия (свод по субъектам) —')

    def test_other_pages_stay_russian(self):
        for url in ('/agrocosmos/raster/', '/agrocosmos/report/screening/',
                    '/agrocosmos/report/region-detailed/',
                    '/agrocosmos/report/unused/'):
            resp, names = self._region_names(url, country='KZ')
            self.assertEqual(names, ['Регион РФ'], url)


@override_settings(CACHES=_DUMMY_CACHE)
class CountryReportApiTests(_CountriesData):
    URL = '/agrocosmos/api/report/country/'

    def _get(self, **params):
        resp = self.client.get(self.URL, {'year': YEAR, **params}).json()
        self.assertTrue(resp['ok'])
        return resp

    def test_default_is_russia_only(self):
        resp = self._get()
        self.assertEqual(resp['country']['code'], 'RU')
        self.assertEqual(
            [r['region_name'] for r in resp['regions']], ['Регион РФ'],
        )
        self.assertAlmostEqual(
            resp['country_overall_series'][0]['mean_ndvi'], 0.6, places=3,
        )
        shares = resp['farmland_shares']
        self.assertEqual(
            [(r['region_name'], r['area_ha']) for r in shares['regions']],
            [('Регион РФ', 100.0)],
        )

    def test_other_country(self):
        resp = self._get(country='KZ')
        self.assertEqual(resp['country']['name_gen'], 'Казахстана')
        self.assertEqual(
            [r['region_name'] for r in resp['regions']], ['Область КЗ'],
        )
        self.assertAlmostEqual(
            resp['country_overall_series'][0]['mean_ndvi'], 0.4, places=3,
        )
        self.assertEqual(resp['farmland_shares']['total_area_ha'], 300.0)

    def test_country_without_districts_groups_by_region(self):
        """В Индии районов нет: угодья привязаны только к субъекту."""
        resp = self._get(country='IN')
        self.assertEqual(resp['regions'], [])
        self.assertEqual(
            [(r['region_id'], r['area_ha'])
             for r in resp['farmland_shares']['regions']],
            [(self.ind.pk, 500.0)],
        )

    def test_all_countries_report(self):
        """Отчёт по «Все страны» агрегирует ряды и доли угодий всех стран."""
        resp = self._get(country='ALL')
        self.assertEqual(resp['country']['code'], 'ALL')
        self.assertEqual(
            sorted(r['region_name'] for r in resp['regions']),
            ['Область КЗ', 'Регион РФ'],
        )
        # 0.6·100 га РФ + 0.4·100 га КЗ → средневзвешенное 0.5.
        self.assertAlmostEqual(
            resp['country_overall_series'][0]['mean_ndvi'], 0.5, places=3,
        )
        self.assertEqual(resp['farmland_shares']['total_area_ha'], 900.0)

    def test_rf_total_area_excludes_other_countries(self):
        Farmland.objects.create(
            region=None, district=None, crop_type='arable', area_ha=7,
            geom=_square(30.1, 50.1, 0.05),
        )
        # 100 га РФ + 7 га без региона; 300 (КЗ) и 500 (Индия) не входят.
        self.assertEqual(_rf_total_farmland_area_ha(), 107.0)


@override_settings(CACHES=_LOCMEM_CACHE)
class CountryGeojsonApiTests(_CountriesData):
    def setUp(self):
        from django.core.cache import cache
        cache.clear()

    def _names(self, url, **params):
        resp = self.client.get(url, params)
        self.assertEqual(resp.status_code, 200)
        return sorted(
            f['properties']['name'] for f in resp.json()['features']
        )

    def test_regions_default_is_russia(self):
        url = '/agrocosmos/api/regions/'
        self.assertEqual(self._names(url), ['Регион РФ'])
        self.assertEqual(self._names(url, country='KZ'), ['Область КЗ'])
        self.assertEqual(self._names(url, country='BR'), [])
        self.assertEqual(
            self._names(url, country='ALL'),
            ['Область КЗ', 'Регион РФ', 'Штат'],
        )

    def test_region_by_id_ignores_country(self):
        """``?id=`` зарубежного региона работает и без ``?country=``."""
        url = '/agrocosmos/api/regions/'
        self.assertEqual(self._names(url, id=self.kz.pk), ['Область КЗ'])
        self.assertEqual(self._names(url, id=self.ru.pk), ['Регион РФ'])

    def test_districts_status_per_country(self):
        url = '/agrocosmos/api/districts/status/'
        self.assertEqual(self._names(url), ['Район РФ'])
        self.assertEqual(self._names(url, country='KZ'), ['Район КЗ'])
        self.assertEqual(self._names(url, country='IN'), [])
        self.assertEqual(self._names(url, region=self.kz.pk), ['Район КЗ'])
        self.assertEqual(self._names(url, region=self.ru.pk), ['Район РФ'])
        self.assertEqual(
            self._names(url, country='ALL'), ['Район КЗ', 'Район РФ'],
        )

    def test_districts_status_etag_differs_by_country(self):
        url = '/agrocosmos/api/districts/status/'
        ru = self.client.get(url)['ETag']
        kz = self.client.get(url, {'country': 'KZ'})['ETag']
        self.assertNotEqual(ru, kz)
        self.assertEqual(
            self.client.get(url, HTTP_IF_NONE_MATCH=ru).status_code, 304,
        )

    def test_refresh_cache_rebuilds_every_country_with_districts(self):
        payload = districts_status_geojson.refresh_cache()
        self.assertEqual(
            [f['properties']['name'] for f in payload['features']],
            ['Район РФ'],
        )
        with mock.patch.object(
            districts_status_geojson, 'build_geojson_payload',
        ) as build:
            # Оба кэша тёплые — запросы обходятся без пересборки.
            districts_status_geojson.get_fast_blob()
            districts_status_geojson.get_or_build('KZ')
            districts_status_geojson.get_fast_blob('ALL')
            build.assert_not_called()


class CountryMonitoringTests(_CountriesData):
    def test_only_monitored_countries_enrolled(self):
        """Страна без мониторинга не зачисляется, даже с районами и угодьями."""
        d_in = District.objects.create(
            region=self.ind, name='Округ', geom=_square(78, 20),
        )
        Farmland.objects.filter(region=self.ind).update(district=d_in)

        out = io.StringIO()
        call_command('ensure_all_regions_monitored', stdout=out)
        enrolled = set(
            MonitoringTask.objects.filter(year=YEAR, status='active')
            .values_list('region__code', flat=True)
        )
        self.assertEqual(enrolled, {'ru-1', 'KZ-1'})
        self.assertIn('created=2', out.getvalue())
        self.assertIn('skipped_no_farmlands=0', out.getvalue())

    def test_status_recompute_skips_unmonitored_countries(self):
        from agrocosmos.management.commands.recompute_district_ndvi_status import (
            Command,
        )
        names = [name for _, name in Command._resolve_regions(None)]
        self.assertEqual(names, ['Область КЗ', 'Регион РФ'])
        # Явно заданный регион пересчитывается независимо от страны.
        self.assertEqual(
            Command._resolve_regions(self.ind.pk), [(self.ind.pk, 'Штат')],
        )


@override_settings(CACHES=_LOCMEM_CACHE)
class ImportInvalidatesBoundaryCachesTests(_CountriesData):
    """Кэш границ живёт неделю: список, запрошенный до импорта, не должен
    пережить импорт (так пустой список областей Казахстана завис на проде)."""

    REGIONS_MOD = 'agrocosmos.management.commands.import_russia_regions'
    DISTRICTS_MOD = 'agrocosmos.management.commands.import_russia_districts'
    POLY = {'type': 'Polygon', 'coordinates': [[
        [71, 50], [71.4, 50], [71.4, 50.4], [71, 50.4], [71, 50],
    ]]}

    def setUp(self):
        from django.core.cache import cache
        cache.clear()

    def _names(self, url, **params):
        return sorted(
            f['properties']['name']
            for f in self.client.get(url, params).json()['features']
        )

    def test_region_import_refreshes_regions_list(self):
        url = '/agrocosmos/api/regions/'
        self.assertEqual(self._names(url, country='KZ'), ['Область КЗ'])
        self.assertEqual(self._names(url), ['Регион РФ'])
        rels = [{'osm_id': 215743, 'name': 'Акмолинская область',
                 'tags': {'ISO3166-2': 'KZ-11'}}]
        with mock.patch(f'{self.REGIONS_MOD}.fetch_country_admin_relations',
                        return_value=rels), \
             mock.patch(f'{self.REGIONS_MOD}.fetch_polygon_geojson',
                        return_value=self.POLY), \
             mock.patch(f'{self.REGIONS_MOD}.time.sleep'):
            call_command('import_russia_regions', '--country', 'KZ',
                         stdout=io.StringIO())
        self.assertEqual(
            self._names(url, country='KZ'),
            ['Акмолинская область', 'Область КЗ'],
        )

    def test_district_import_refreshes_districts_and_choropleth(self):
        Region.objects.filter(pk=self.kz.pk).update(osm_id=215743)
        districts_url = '/agrocosmos/api/districts/'
        status_url = '/agrocosmos/api/districts/status/'
        self.assertEqual(
            self._names(districts_url, region=self.kz.pk), ['Район КЗ'])
        self.assertEqual(self._names(status_url, country='KZ'), ['Район КЗ'])

        poly = {'type': 'Polygon', 'coordinates': [[
            [70.1, 50.1], [70.2, 50.1], [70.2, 50.2], [70.1, 50.2], [70.1, 50.1],
        ]]}
        rels = [{'osm_id': 301, 'name': 'Новый район', 'tags': {}}]
        with mock.patch(f'{self.DISTRICTS_MOD}.fetch_admin_relations_in',
                        return_value=rels), \
             mock.patch(f'{self.DISTRICTS_MOD}.fetch_polygon_geojson',
                        return_value=poly), \
             mock.patch(f'{self.DISTRICTS_MOD}.time.sleep'):
            call_command('import_russia_districts', '--country', 'KZ',
                         stdout=io.StringIO(), stderr=io.StringIO())

        expected = ['Новый район', 'Район КЗ']
        self.assertEqual(
            self._names(districts_url, region=self.kz.pk), expected)
        self.assertEqual(self._names(status_url, country='KZ'), expected)
        # Россию импорт Казахстана не трогает.
        self.assertEqual(self._names(status_url), ['Район РФ'])
