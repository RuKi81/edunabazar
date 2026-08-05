"""
Тесты дашборда Agrocosmos (``agrocosmos/views/pages.py``) — страховочная
сетка перед рефакторингом ``dashboard`` (C=16).

Покрывается: дефолтный scope 'all' без параметра region, фильтрация
сводки по региону/району, устойчивость к мусорным параметрам, парсинг
``?year=``, кеширование глобальной сводки, список районов для селекта.
"""
import io
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.cache import cache
from django.core.management import call_command
from django.test import TestCase, override_settings

from agrocosmos.models import District, Farmland, Region
from agrocosmos.views.pages import refresh_farmland_stats

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}
_LOCMEM_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'},
}

URL = '/agrocosmos/'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class _BaseDashboardData(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.region_a = Region.objects.create(
            name='Регион А', code='ra-1', geom=_square(30, 50),
        )
        cls.region_b = Region.objects.create(
            name='Регион Б', code='rb-1', geom=_square(40, 50),
        )
        cls.district_a1 = District.objects.create(
            region=cls.region_a, name='Район А1', geom=_square(30, 50),
        )
        cls.district_a2 = District.objects.create(
            region=cls.region_a, name='Район А2', geom=_square(30.5, 50),
        )
        cls.district_b1 = District.objects.create(
            region=cls.region_b, name='Район Б1', geom=_square(40, 50),
        )
        Farmland.objects.create(
            district=cls.district_a1, crop_type='arable', area_ha=100,
            geom=_square(30.0, 50.0, 0.01),
        )
        Farmland.objects.create(
            district=cls.district_a2, crop_type='pasture', area_ha=50,
            geom=_square(30.5, 50.0, 0.01),
        )
        Farmland.objects.create(
            district=cls.district_b1, crop_type='arable', area_ha=200,
            geom=_square(40.0, 50.0, 0.01),
        )


@override_settings(CACHES=_DUMMY_CACHE)
class DashboardTests(_BaseDashboardData):

    def test_default_region_is_all(self):
        resp = self.client.get(URL)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context['region_id'], 'all')
        self.assertEqual(list(resp.context['districts']), [])
        self.assertEqual(resp.context['summary']['total_count'], 3)
        self.assertEqual(resp.context['summary']['total_area'], 350)

    def test_explicit_empty_region_kept(self):
        resp = self.client.get(URL, {'region': ''})
        self.assertEqual(resp.context['region_id'], '')
        # пустой регион = глобальная сводка
        self.assertEqual(resp.context['summary']['total_count'], 3)

    def test_region_filters_summary_and_districts(self):
        resp = self.client.get(URL, {'region': str(self.region_a.pk)})
        self.assertEqual(resp.context['summary']['total_count'], 2)
        self.assertEqual(resp.context['summary']['total_area'], 150)
        self.assertEqual(
            [d.name for d in resp.context['districts']],
            ['Район А1', 'Район А2'],
        )

    def test_district_filter_wins_over_region(self):
        resp = self.client.get(URL, {
            'region': str(self.region_a.pk),
            'district': str(self.district_a1.pk),
        })
        self.assertEqual(resp.context['summary']['total_count'], 1)
        self.assertEqual(resp.context['summary']['total_area'], 100)

    def test_garbage_params_fall_back_to_global(self):
        resp = self.client.get(URL, {'region': 'garbage', 'district': 'x'})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context['summary']['total_count'], 3)
        self.assertEqual(list(resp.context['districts']), [])

    def test_crop_stats_ordered_by_area_desc(self):
        resp = self.client.get(URL)
        stats = resp.context['crop_stats']
        self.assertEqual([s['crop_type'] for s in stats], ['arable', 'pasture'])
        self.assertEqual(stats[0]['cnt'], 2)
        self.assertEqual(stats[0]['area'], 300)

    def test_year_parsing_robust(self):
        resp = self.client.get(URL, {'year': '2024,2025,abc,'})
        self.assertEqual(resp.context['selected_years'], {2024, 2025})

    def test_farmland_id_passthrough(self):
        resp = self.client.get(URL, {'farmland': '77'})
        self.assertEqual(resp.context['farmland_id'], '77')


@override_settings(CACHES=_LOCMEM_CACHE)
class DashboardCacheTests(_BaseDashboardData):

    def setUp(self):
        cache.clear()

    def test_global_summary_cached(self):
        self.client.get(URL)
        cached = cache.get('agrocosmos:farmland_stats:global')
        self.assertIsNotNone(cached)
        self.assertEqual(cached['summary']['total_count'], 3)

    def test_filtered_summary_not_cached_and_fresh(self):
        # прогреваем глобальный кеш, затем убеждаемся, что фильтрованный
        # запрос не подхватывает его
        self.client.get(URL)
        resp = self.client.get(URL, {'region': str(self.region_b.pk)})
        self.assertEqual(resp.context['summary']['total_count'], 1)
        self.assertEqual(resp.context['summary']['total_area'], 200)

    def test_global_summary_cached_without_ttl(self):
        # Вечный кэш: рендер не должен пересчитывать агрегат повторно.
        self.client.get(URL)
        with mock.patch(
            'agrocosmos.views.pages._compute_farmland_stats',
        ) as compute:
            resp = self.client.get(URL)
        compute.assert_not_called()
        self.assertEqual(resp.context['summary']['total_count'], 3)

    def test_refresh_farmland_stats_rotates_cache(self):
        stale = {'summary': {'total_count': 0, 'total_area': 0},
                 'crop_stats': []}
        cache.set('agrocosmos:farmland_stats:global', stale, timeout=None)
        payload = refresh_farmland_stats()
        self.assertEqual(payload['summary']['total_count'], 3)
        cached = cache.get('agrocosmos:farmland_stats:global')
        self.assertEqual(cached['summary']['total_count'], 3)

    def test_dogpile_follower_polls_cache(self):
        # Лок занят «победителем»; follower дожидается появления payload
        # в кэше вместо запуска собственного агрегата.
        cache.add('agrocosmos:farmland_stats:global:build_lock', '1',
                  timeout=300)
        payload = {'summary': {'total_count': 42, 'total_area': 1.0},
                   'crop_stats': []}

        def _winner_publishes(_):
            cache.set('agrocosmos:farmland_stats:global', payload,
                      timeout=None)

        with mock.patch('agrocosmos.views.pages.time.sleep',
                        side_effect=_winner_publishes) as slept:
            resp = self.client.get(URL)
        slept.assert_called_once()
        self.assertEqual(resp.context['summary']['total_count'], 42)


@override_settings(CACHES=_LOCMEM_CACHE)
class PrewarmFarmlandStatsTests(_BaseDashboardData):

    def setUp(self):
        cache.clear()

    def test_prewarm_command_warms_farmland_stats(self):
        out = io.StringIO()
        with mock.patch(
            'agrocosmos.services.districts_status_geojson.refresh_cache',
            return_value={'features': []},
        ):
            call_command('prewarm_agro_caches', stdout=out)
        self.assertIn('farmland_stats: 3 farmlands summarised', out.getvalue())
        cached = cache.get('agrocosmos:farmland_stats:global')
        self.assertEqual(cached['summary']['total_count'], 3)
