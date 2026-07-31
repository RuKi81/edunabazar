"""
Страховочные тесты ``/agrocosmos/api/ndvi-stats/`` перед рефакторингом.

Фиксируют оба пути агрегации: быстрый (предагрегат DistrictNdviSeries,
``source=modis`` без fact_isp) и медленный (сырые VegetationIndex —
fallback при fact_isp или пустом предагрегате), а также baseline/z-score,
сводку и per-crop breakdown.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import SimpleTestCase, TestCase, override_settings

from agrocosmos.models import (
    District, DistrictNdviSeries, Farmland, NdviBaseline, Region,
    SatelliteScene, VegetationIndex,
)
from agrocosmos.services.ndvi_stats import doy_to_mmdd

YEAR = 2025
D1 = date(YEAR, 6, 10)
D2 = date(YEAR, 6, 26)

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


@override_settings(CACHES=_DUMMY_CACHE)
class NdviStatsApiTests(TestCase):
    URL = '/agrocosmos/api/ndvi-stats/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион Статы', code='rs-1', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район Статы', geom=_square(30, 50),
        )
        cls.fl_arable = Farmland.objects.create(
            district=cls.district, crop_type='arable', area_ha=100,
            geom=_square(30.0, 50.0, 0.01),
            properties={'Fact_isp': 'Используется'},
        )
        cls.fl_pasture = Farmland.objects.create(
            district=cls.district, crop_type='pasture', area_ha=50,
            geom=_square(30.2, 50.0, 0.01),
        )

        # Предагрегат (быстрый путь: source=modis)
        DistrictNdviSeries.objects.create(
            district=cls.district, acquired_date=D1, crop_type='arable',
            source=DistrictNdviSeries.Source.MODIS,
            sum_ndvi_area=0.50 * 100, sum_area=100, obs_count=1,
        )
        DistrictNdviSeries.objects.create(
            district=cls.district, acquired_date=D2, crop_type='arable',
            source=DistrictNdviSeries.Source.MODIS,
            sum_ndvi_area=0.60 * 100, sum_area=100, obs_count=1,
        )
        DistrictNdviSeries.objects.create(
            district=cls.district, acquired_date=D2, crop_type='pasture',
            source=DistrictNdviSeries.Source.MODIS,
            sum_ndvi_area=0.80 * 50, sum_area=50, obs_count=1,
        )

        NdviBaseline.objects.create(
            district=cls.district, day_of_year=D2.timetuple().tm_yday,
            mean_ndvi=0.60, std_ndvi=0.1, crop_type='',
        )

        # Сырые VI (медленный путь: fact_isp)
        scene = SatelliteScene.objects.create(
            satellite='modis_terra', scene_id='test-scene-1',
            acquired_date=D2,
        )
        VegetationIndex.objects.create(
            farmland=cls.fl_arable, scene=scene, index_type='ndvi',
            acquired_date=D2, mean=0.70,
        )
        VegetationIndex.objects.create(
            farmland=cls.fl_pasture, scene=scene, index_type='ndvi',
            acquired_date=D2, mean=0.90,
        )

    def _get(self, **params):
        params.setdefault('region', self.region.pk)
        return self.client.get(self.URL, params)

    def test_region_required(self):
        self.assertEqual(self.client.get(self.URL).status_code, 400)
        self.assertEqual(
            self.client.get(self.URL, {'region': 'abc'}).status_code, 400,
        )

    def test_by_period_area_weighted(self):
        resp = self._get(source='modis', year=YEAR).json()
        self.assertTrue(resp['ok'])
        by_period = {p['date']: p for p in resp['stats']['by_period']}
        self.assertAlmostEqual(by_period[str(D1)]['mean_ndvi'], 0.5, places=3)
        # D2: (0.6*100 + 0.8*50) / 150 = 0.6667
        self.assertAlmostEqual(by_period[str(D2)]['mean_ndvi'], 0.6667, places=3)

    def test_by_crop_type_sorted_desc(self):
        resp = self._get(source='modis', year=YEAR).json()
        by_crop = resp['stats']['by_crop_type']
        self.assertEqual(by_crop[0]['crop_type'], 'pasture')
        self.assertAlmostEqual(by_crop[0]['mean_ndvi'], 0.8, places=3)
        arable = next(c for c in by_crop if c['crop_type'] == 'arable')
        # (0.5*100 + 0.6*100) / 200 = 0.55
        self.assertAlmostEqual(arable['mean_ndvi'], 0.55, places=3)
        self.assertEqual(arable['count'], 1)

    def test_summary(self):
        resp = self._get(source='modis', year=YEAR).json()
        summary = resp['stats']['summary']
        self.assertEqual(summary['total_farmlands'], 2)
        self.assertEqual(summary['with_ndvi'], 2)
        # (50 + 60 + 40) / 250 = 0.6
        self.assertAlmostEqual(summary['mean_ndvi'], 0.6, places=3)

    def test_baseline_and_z_score(self):
        resp = self._get(source='modis', year=YEAR).json()
        bl = resp['stats']['baseline']
        self.assertEqual(len(bl), 1)
        # Метка baseline считается в календаре года запроса и совпадает
        # с реальной датой ряда (doy 177 в 2025 → «06-26»).
        self.assertEqual(bl[0]['date'], D2.strftime('%m-%d'))
        self.assertAlmostEqual(bl[0]['mean_ndvi'], 0.6, places=3)

        by_period = {p['date']: p for p in resp['stats']['by_period']}
        # D2: (0.6667 - 0.6) / 0.1 = 0.67
        self.assertAlmostEqual(by_period[str(D2)]['z_score'], 0.6667, places=2)
        self.assertIsNone(by_period[str(D1)]['z_score'])

    def test_last_period_end_modis_only(self):
        resp = self._get(source='modis', year=YEAR).json()
        self.assertEqual(resp['stats']['last_period_end'], '2025-07-04')

    def test_crop_types_filter(self):
        resp = self._get(source='modis', year=YEAR, crop_types='pasture').json()
        by_crop = resp['stats']['by_crop_type']
        self.assertEqual([c['crop_type'] for c in by_crop], ['pasture'])

    def test_date_range_filter(self):
        resp = self._get(
            source='modis', year=YEAR,
            date_from=str(D2), date_to=str(D2),
        ).json()
        dates = [p['date'] for p in resp['stats']['by_period']]
        self.assertEqual(dates, [str(D2)])

    def test_crop_breakdown(self):
        resp = self._get(source='modis', year=YEAR, breakdown='crop').json()
        breakdown = resp['stats']['crop_breakdown']
        # Фиксированный порядок: пашня раньше пастбища
        self.assertEqual(
            [c['crop_type'] for c in breakdown], ['arable', 'pasture'],
        )
        arable = breakdown[0]
        self.assertEqual(len(arable['by_period']), 2)
        # Без per-crop baseline — fallback на общий
        self.assertAlmostEqual(arable['baseline'][0]['mean_ndvi'], 0.6, places=3)

    def test_farmland_and_usage_summary(self):
        resp = self._get(source='modis', year=YEAR).json()
        fl_summary = {r['crop_type']: r for r in resp['stats']['farmland_summary']}
        self.assertEqual(fl_summary['arable']['count'], 1)
        self.assertAlmostEqual(fl_summary['arable']['area_ha'], 100.0, places=1)
        usage = {r['fact_isp']: r for r in resp['stats']['usage_summary']}
        self.assertIn('Используется', usage)

    def test_fact_isp_falls_back_to_raw_vi(self):
        resp = self._get(source='modis', year=YEAR, fact_isp='used').json()
        self.assertTrue(resp['ok'])
        by_period = {p['date']: p for p in resp['stats']['by_period']}
        # Только arable-угодье «Используется»: сырой VI mean=0.7
        self.assertAlmostEqual(by_period[str(D2)]['mean_ndvi'], 0.7, places=3)
        self.assertEqual(resp['stats']['summary']['total_farmlands'], 1)

    def test_empty_preaggregate_falls_back_to_raw_vi(self):
        DistrictNdviSeries.objects.all().delete()
        resp = self._get(source='modis', year=YEAR).json()
        self.assertTrue(resp['ok'])
        by_period = {p['date']: p for p in resp['stats']['by_period']}
        # Сырые VI: (0.7*100 + 0.9*50) / 150 = 0.7667
        self.assertAlmostEqual(by_period[str(D2)]['mean_ndvi'], 0.7667, places=3)

    def test_district_filter(self):
        other = District.objects.create(
            region=self.region, name='Другой район', geom=_square(31, 50),
        )
        DistrictNdviSeries.objects.create(
            district=other, acquired_date=D2, crop_type='arable',
            source=DistrictNdviSeries.Source.MODIS,
            sum_ndvi_area=0.20 * 300, sum_area=300, obs_count=1,
        )
        resp = self._get(
            source='modis', year=YEAR, district=self.district.pk,
        ).json()
        by_period = {p['date']: p for p in resp['stats']['by_period']}
        # Без чужого района: (0.6*100 + 0.8*50) / 150, а не с 0.2*300
        self.assertAlmostEqual(by_period[str(D2)]['mean_ndvi'], 0.6667, places=3)


class DoyToMmddTests(SimpleTestCase):
    """Календарь конвертации doy → 'MM-DD' (фикс leap-year off-by-one)."""

    def test_non_leap_year_after_february(self):
        # doy 177 в 2025 — 26 июня (раньше через 2024 выходило «06-25»)
        self.assertEqual(doy_to_mmdd(177, 2025), '06-26')

    def test_leap_year(self):
        self.assertEqual(doy_to_mmdd(177, 2024), '06-25')
        self.assertEqual(doy_to_mmdd(60, 2024), '02-29')

    def test_before_march_same_in_both(self):
        self.assertEqual(doy_to_mmdd(59, 2024), '02-28')
        self.assertEqual(doy_to_mmdd(59, 2025), '02-28')

    def test_doy_366_clamped_in_non_leap(self):
        self.assertEqual(doy_to_mmdd(366, 2025), '12-31')
        self.assertEqual(doy_to_mmdd(366, 2024), '12-31')

    def test_default_year_is_non_leap(self):
        self.assertEqual(doy_to_mmdd(177), '06-26')
