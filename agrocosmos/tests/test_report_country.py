"""
Тесты country-level NDVI отчёта (``/agrocosmos/api/report/country/``).

Endpoint читает предагрегат DistrictNdviSeries с суммированием до уровня
(регион, дата) и категоризирует каждый регион по z-score его последней
точки против СОБСТВЕННОГО baseline региона (усреднённые baseline районов).
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, DistrictNdviSeries, NdviBaseline, Region,
)
from agrocosmos.views.reports import _country_category

YEAR = 2025
D1 = date(YEAR, 6, 10)   # doy 161
D2 = date(YEAR, 6, 26)   # doy 177 (последняя точка)


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


def _baseline(district, doy, mean, std=0.1):
    return NdviBaseline.objects.create(
        district=district, day_of_year=doy, mean_ndvi=mean, std_ndvi=std,
        crop_type='',
    )


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportCountryApiTests(TestCase):
    URL = '/agrocosmos/api/report/country/'

    @classmethod
    def setUpTestData(cls):
        cls.region_ok = Region.objects.create(
            name='Регион В норме', code='r-ok', geom=_square(30, 50),
        )
        cls.region_bad = Region.objects.create(
            name='Регион Аномалия', code='r-bad', geom=_square(40, 50),
        )
        cls.region_empty = Region.objects.create(
            name='Регион Без данных', code='r-empty', geom=_square(50, 50),
        )
        cls.d_ok = District.objects.create(
            region=cls.region_ok, name='Район ОК', geom=_square(30, 50),
        )
        cls.d_bad = District.objects.create(
            region=cls.region_bad, name='Район Плохо', geom=_square(40, 50),
        )

        # region_ok: NDVI 0.6 при baseline 0.6 → z = 0 → normal.
        _series(cls.d_ok, D1, 0.55, 100)
        _series(cls.d_ok, D2, 0.60, 100)
        _baseline(cls.d_ok, D2.timetuple().tm_yday, 0.60)

        # region_bad: NDVI 0.30 при baseline 0.60, std 0.1 → z = -3 → anomaly.
        _series(cls.d_bad, D1, 0.50, 200)
        _series(cls.d_bad, D2, 0.30, 200)
        _baseline(cls.d_bad, D2.timetuple().tm_yday, 0.60)

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_year_required(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(year='oops').status_code, 400)

    def test_regions_with_data_only(self):
        resp = self._get(year=YEAR).json()
        self.assertTrue(resp['ok'])
        names = [r['region_name'] for r in resp['regions']]
        self.assertIn('Регион В норме', names)
        self.assertIn('Регион Аномалия', names)
        self.assertNotIn('Регион Без данных', names)

    def test_categorisation_by_own_baseline(self):
        resp = self._get(year=YEAR).json()
        by_name = {r['region_name']: r for r in resp['regions']}

        ok = by_name['Регион В норме']
        self.assertEqual(ok['category'], 'normal')
        self.assertAlmostEqual(ok['latest_z_score'], 0.0, places=2)

        bad = by_name['Регион Аномалия']
        self.assertEqual(bad['category'], 'anomaly')
        self.assertAlmostEqual(bad['latest_z_score'], -3.0, places=2)
        self.assertEqual(bad['latest_date'], str(D2))

    def test_area_weighted_country_series(self):
        """Страна на D2: (0.6*100 + 0.3*200) / 300 = 0.4."""
        resp = self._get(year=YEAR).json()
        series = {s['date']: s['mean_ndvi'] for s in resp['country_overall_series']}
        self.assertAlmostEqual(series[str(D2)], 0.4, places=3)
        # D1: (0.55*100 + 0.5*200) / 300 = 0.51(6)
        self.assertAlmostEqual(series[str(D1)], 0.517, places=3)

    def test_crop_types_collapsed(self):
        """Строки разных культур одного (район, дата) суммируются."""
        _series(self.d_ok, D2, 0.80, 100, crop_type='pasture')
        resp = self._get(year=YEAR).json()
        by_name = {r['region_name']: r for r in resp['regions']}
        # region_ok D2: (0.6*100 + 0.8*100) / 200 = 0.7
        self.assertAlmostEqual(by_name['Регион В норме']['latest_ndvi'], 0.7, places=3)

    def test_country_baseline_present(self):
        resp = self._get(year=YEAR).json()
        bl = resp['country_baseline']
        self.assertEqual(len(bl), 1)
        # Среднее двух региональных baseline: (0.6 + 0.6) / 2
        self.assertAlmostEqual(bl[0]['mean_ndvi'], 0.6, places=3)
        self.assertEqual(bl[0]['date'], str(D2))

    def test_other_year_excluded(self):
        resp = self._get(year=YEAR - 1).json()
        self.assertTrue(resp['ok'])
        self.assertEqual(resp['regions'], [])
        self.assertEqual(resp['country_overall_series'], [])

    def test_last_period_end(self):
        resp = self._get(year=YEAR).json()
        self.assertEqual(resp['last_period_end'], '2025-07-04')  # D2 + 8 дней


class CountryCategoryTests(TestCase):
    """Границы бакетов категоризации регионов."""

    def test_buckets(self):
        self.assertEqual(_country_category(None), 'nodata')
        self.assertEqual(_country_category(-3.0), 'anomaly')
        self.assertEqual(_country_category(-1.5), 'anomaly')
        self.assertEqual(_country_category(-1.49), 'below')
        self.assertEqual(_country_category(-0.51), 'below')
        self.assertEqual(_country_category(-0.5), 'normal')
        self.assertEqual(_country_category(0.0), 'normal')
        self.assertEqual(_country_category(2.0), 'normal')
