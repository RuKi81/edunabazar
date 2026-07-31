"""
Страховочные тесты region- и district-отчётов перед рефакторингом
(``/agrocosmos/api/report/region/`` и ``/agrocosmos/api/report/district/``).

Фиксируют текущий контракт ответа: area-weighted агрегацию из
предагрегата DistrictNdviSeries, z-score против baseline, состав полей.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, DistrictNdviSeries, Farmland, FarmlandPhenology,
    NdviBaseline, Region,
)

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


def _series(district, acq_date, mean_ndvi, area, crop_type='arable'):
    return DistrictNdviSeries.objects.create(
        district=district, acquired_date=acq_date, crop_type=crop_type,
        source=DistrictNdviSeries.Source.MODIS,
        sum_ndvi_area=mean_ndvi * area, sum_area=area,
    )


def _baseline(district, doy, mean, std=0.1, crop_type=''):
    return NdviBaseline.objects.create(
        district=district, day_of_year=doy, mean_ndvi=mean, std_ndvi=std,
        crop_type=crop_type,
    )


@override_settings(CACHES=_DUMMY_CACHE)
class ReportRegionApiTests(TestCase):
    URL = '/agrocosmos/api/report/region/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Тестовый регион', code='rr-1', geom=_square(30, 50),
        )
        cls.d_ok = District.objects.create(
            region=cls.region, name='Район Норма', geom=_square(30, 50),
        )
        cls.d_bad = District.objects.create(
            region=cls.region, name='Район Спад', geom=_square(31, 50),
        )
        cls.d_empty = District.objects.create(
            region=cls.region, name='Район Пустой', geom=_square(32, 50),
        )

        _series(cls.d_ok, D1, 0.55, 100)
        _series(cls.d_ok, D2, 0.60, 100)
        _baseline(cls.d_ok, D2.timetuple().tm_yday, 0.60)

        _series(cls.d_bad, D2, 0.30, 200)
        _baseline(cls.d_bad, D2.timetuple().tm_yday, 0.60)

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_params_required(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(region='1').status_code, 400)
        self.assertEqual(self._get(region='x', year='y').status_code, 400)

    def test_unknown_region_404(self):
        self.assertEqual(self._get(region='999999', year=YEAR).status_code, 404)

    def test_all_districts_present_even_empty(self):
        resp = self._get(region=self.region.pk, year=YEAR).json()
        self.assertTrue(resp['ok'])
        names = [d['district_name'] for d in resp['districts']]
        self.assertEqual(
            names, ['Район Норма', 'Район Пустой', 'Район Спад'],
        )
        empty = next(d for d in resp['districts'] if d['district_name'] == 'Район Пустой')
        self.assertEqual(empty['series'], [])
        self.assertIsNone(empty['latest_ndvi'])
        self.assertEqual(empty['assessment'], 'Нет данных')

    def test_z_score_against_own_baseline(self):
        resp = self._get(region=self.region.pk, year=YEAR).json()
        by_name = {d['district_name']: d for d in resp['districts']}

        ok = by_name['Район Норма']
        self.assertAlmostEqual(ok['latest_ndvi'], 0.6, places=3)
        self.assertAlmostEqual(ok['latest_z_score'], 0.0, places=2)
        self.assertEqual(ok['latest_date'], str(D2))

        bad = by_name['Район Спад']
        self.assertAlmostEqual(bad['latest_z_score'], -3.0, places=2)
        self.assertEqual(bad['assessment'], 'Критическое снижение вегетации')

    def test_region_overall_is_area_weighted(self):
        resp = self._get(region=self.region.pk, year=YEAR).json()
        series = {s['date']: s['mean_ndvi'] for s in resp['region_overall_series']}
        # D2: (0.6*100 + 0.3*200) / 300 = 0.4
        self.assertAlmostEqual(series[str(D2)], 0.4, places=3)
        self.assertAlmostEqual(series[str(D1)], 0.55, places=3)

    def test_region_baseline_is_district_average(self):
        resp = self._get(region=self.region.pk, year=YEAR).json()
        bl = resp['region_baseline']
        self.assertEqual(len(bl), 1)
        self.assertEqual(bl[0]['date'], str(D2))
        self.assertAlmostEqual(bl[0]['mean_ndvi'], 0.6, places=3)

    def test_district_baseline_series(self):
        resp = self._get(region=self.region.pk, year=YEAR).json()
        ok = next(d for d in resp['districts'] if d['district_name'] == 'Район Норма')
        self.assertEqual(len(ok['baseline']), 1)
        self.assertEqual(ok['baseline'][0]['date'], str(D2))

    def test_crop_types_collapsed(self):
        _series(self.d_ok, D2, 0.80, 100, crop_type='pasture')
        resp = self._get(region=self.region.pk, year=YEAR).json()
        ok = next(d for d in resp['districts'] if d['district_name'] == 'Район Норма')
        # (0.6*100 + 0.8*100) / 200 = 0.7
        self.assertAlmostEqual(ok['latest_ndvi'], 0.7, places=3)

    def test_last_period_end(self):
        resp = self._get(region=self.region.pk, year=YEAR).json()
        self.assertEqual(resp['last_period_end'], '2025-07-04')


@override_settings(CACHES=_DUMMY_CACHE)
class ReportDistrictApiTests(TestCase):
    URL = '/agrocosmos/api/report/district/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион Д', code='rd-1', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Целевой район', geom=_square(30, 50),
        )
        cls.other_district = District.objects.create(
            region=cls.region, name='Соседний район', geom=_square(31, 50),
        )

        cls.fl_arable_1 = Farmland.objects.create(
            district=cls.district, crop_type='arable', area_ha=60,
            geom=_square(30.0, 50.0, 0.01),
        )
        cls.fl_arable_2 = Farmland.objects.create(
            district=cls.district, crop_type='arable', area_ha=40,
            geom=_square(30.1, 50.0, 0.01),
        )
        cls.fl_pasture = Farmland.objects.create(
            district=cls.district, crop_type='pasture', area_ha=50,
            geom=_square(30.2, 50.0, 0.01),
        )

        _series(cls.district, D1, 0.50, 100, crop_type='arable')
        _series(cls.district, D2, 0.60, 100, crop_type='arable')
        _series(cls.district, D2, 0.80, 50, crop_type='pasture')
        # Соседний район — только для region_overall_series
        _series(cls.other_district, D2, 0.40, 150, crop_type='arable')

        doy2 = D2.timetuple().tm_yday
        _baseline(cls.district, doy2, 0.60)                       # overall
        _baseline(cls.district, doy2, 0.55, crop_type='arable')   # per-crop

        FarmlandPhenology.objects.create(
            farmland=cls.fl_arable_1, year=YEAR, source='modis',
            sos_date=date(YEAR, 4, 1), eos_date=date(YEAR, 9, 15),
            pos_date=date(YEAR, 7, 1), max_ndvi=0.8, mean_ndvi=0.6,
            los_days=120, total_ndvi=80,
        )

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_params_required(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(district='x', year='y').status_code, 400)

    def test_unknown_district_404(self):
        self.assertEqual(self._get(district='999999', year=YEAR).status_code, 404)

    def test_overall_series_area_weighted(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertTrue(resp['ok'])
        series = {s['date']: s['mean_ndvi'] for s in resp['overall_series']}
        self.assertAlmostEqual(series[str(D1)], 0.5, places=3)
        # D2: (0.6*100 + 0.8*50) / 150 = 0.6667
        self.assertAlmostEqual(series[str(D2)], 0.6667, places=3)

    def test_crop_types_with_farmlands_only(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        codes = [c['crop_type'] for c in resp['crop_types']]
        self.assertIn('arable', codes)
        self.assertIn('pasture', codes)
        self.assertNotIn('fallow', codes)  # нет угодий этой категории

    def test_crop_counts_and_area(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        arable = next(c for c in resp['crop_types'] if c['crop_type'] == 'arable')
        self.assertEqual(arable['count'], 2)
        self.assertAlmostEqual(arable['area_ha'], 100.0, places=1)

    def test_z_score_uses_overall_baseline(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        arable = next(c for c in resp['crop_types'] if c['crop_type'] == 'arable')
        # latest 0.6 vs overall baseline (0.6, 0.1) → z = 0
        self.assertAlmostEqual(arable['latest_z_score'], 0.0, places=2)

        pasture = next(c for c in resp['crop_types'] if c['crop_type'] == 'pasture')
        # latest 0.8 vs (0.6, 0.1) → z = 2 → «выше нормы»
        self.assertAlmostEqual(pasture['latest_z_score'], 2.0, places=2)
        self.assertEqual(pasture['assessment'], 'Вегетация выше нормы')

    def test_per_crop_baseline_with_overall_fallback(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        arable = next(c for c in resp['crop_types'] if c['crop_type'] == 'arable')
        self.assertAlmostEqual(arable['baseline'][0]['mean_ndvi'], 0.55, places=3)
        pasture = next(c for c in resp['crop_types'] if c['crop_type'] == 'pasture')
        self.assertAlmostEqual(pasture['baseline'][0]['mean_ndvi'], 0.60, places=3)

    def test_phenology_attached(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        arable = next(c for c in resp['crop_types'] if c['crop_type'] == 'arable')
        ph = arable['phenology']
        self.assertEqual(ph['count'], 1)
        self.assertEqual(ph['avg_los'], 120)
        self.assertEqual(ph['avg_sos'], '01.04')
        self.assertEqual(ph['avg_pos'], '01.07')
        pasture = next(c for c in resp['crop_types'] if c['crop_type'] == 'pasture')
        self.assertIsNone(pasture['phenology'])

    def test_region_overall_includes_neighbours(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        series = {s['date']: s['mean_ndvi'] for s in resp['region_overall_series']}
        # D2 по региону: (0.6*100 + 0.8*50 + 0.4*150) / 300 = 0.5333
        self.assertAlmostEqual(series[str(D2)], 0.5333, places=3)

    def test_meta_and_last_period_end(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertEqual(resp['district']['name'], 'Целевой район')
        self.assertEqual(resp['region']['name'], 'Регион Д')
        self.assertEqual(resp['last_period_end'], '2025-07-04')
