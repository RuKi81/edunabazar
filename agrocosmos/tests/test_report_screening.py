"""
Тесты скрининга проблемных полей (``/agrocosmos/api/report/screening/``).

Endpoint ранжирует угодья района по баллу неблагополучия: z-score
последнего детального NDVI против baseline, доля пикселей < 0.4 в
гистограмме и неразрешённые алерты.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, Farmland, NdviBaseline, Region, SatelliteScene,
    VegetationAlert, VegetationIndex,
)
from agrocosmos.views.reports import (
    _histogram_low_pct, _screening_category, _screening_score,
)

YEAR = 2025
D1 = date(YEAR, 6, 10)
D2 = date(YEAR, 6, 26)


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _scene(satellite, acq_date, suffix=''):
    return SatelliteScene.objects.create(
        satellite=satellite,
        scene_id=f'{satellite}_{acq_date}{suffix}',
        acquired_date=acq_date,
    )


def _vi(farmland, scene, mean, **kwargs):
    return VegetationIndex.objects.create(
        farmland=farmland, scene=scene, index_type='ndvi',
        acquired_date=scene.acquired_date, mean=mean, **kwargs,
    )


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportScreeningApiTests(TestCase):
    URL = '/agrocosmos/api/report/screening/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50),
        )

        def farmland(x):
            return Farmland.objects.create(
                region=cls.region, district=cls.district,
                crop_type=Farmland.CropType.ARABLE, area_ha=100,
                geom=_square(x, 50.1, 0.05),
            )

        cls.fl_ok = farmland(30.1)       # z=0, здоровое поле
        cls.fl_bad = farmland(30.2)      # z=-3, аномалия
        cls.fl_hetero = farmland(30.3)   # средний NDVI в норме, но 60% < 0.4
        cls.fl_nodata = farmland(30.4)   # без наблюдений — не в отчёте

        s2_d1 = _scene('sentinel2', D1)
        s2_d2 = _scene('sentinel2', D2)

        # fl_ok: последнее 0.60 при baseline 0.60 → z=0.
        _vi(cls.fl_ok, s2_d1, 0.55)
        _vi(cls.fl_ok, s2_d2, 0.60, histogram=[0, 0, 20, 60, 20])

        # fl_bad: 0.30 при baseline 0.60/0.1 → z=-3.
        _vi(cls.fl_bad, s2_d1, 0.50)
        _vi(cls.fl_bad, s2_d2, 0.30, histogram=[30, 40, 20, 10, 0])

        # fl_hetero: 0.60 (z=0), но 60 % пикселей в низких бинах.
        _vi(cls.fl_hetero, s2_d2, 0.60, histogram=[30, 30, 0, 0, 40])

        NdviBaseline.objects.create(
            district=cls.district, day_of_year=D2.timetuple().tm_yday,
            mean_ndvi=0.60, std_ndvi=0.10, crop_type='',
        )

        # Неразрешённый алерт на fl_bad + разрешённый (не считается).
        VegetationAlert.objects.create(
            farmland=cls.fl_bad, alert_type='rapid_drop',
            severity='critical', detected_on=D2, source='raster',
        )
        VegetationAlert.objects.create(
            farmland=cls.fl_bad, alert_type='baseline_deviation',
            severity='warning', detected_on=D1, source='raster',
            status=VegetationAlert.Status.RESOLVED,
        )

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_params_required(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(district='x', year='y').status_code, 400)

    def test_not_found(self):
        self.assertEqual(self._get(district=999999, year=YEAR).status_code, 404)

    def test_ranking_worst_first(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertTrue(resp['ok'])
        ids = [f['farmland_id'] for f in resp['farmlands']]
        # fl_bad: z=-3 (3.0) + low 70% (1.4) + 1 алерт (1.0) = 5.4 — первый.
        # fl_hetero: z=0 + low 60% (1.2) = 1.2 — второй.
        # fl_ok: 0 — последний. fl_nodata отсутствует.
        self.assertEqual(ids, [self.fl_bad.pk, self.fl_hetero.pk, self.fl_ok.pk])

    def test_bad_field_metrics(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        bad = resp['farmlands'][0]
        self.assertEqual(bad['latest_date'], str(D2))
        self.assertAlmostEqual(bad['latest_ndvi'], 0.30, places=3)
        self.assertAlmostEqual(bad['z_score'], -3.0, places=2)
        self.assertAlmostEqual(bad['low_pct'], 70.0, places=1)
        self.assertEqual(bad['active_alerts'], 1)  # resolved не считается
        self.assertEqual(bad['category'], 'anomaly')
        self.assertAlmostEqual(bad['score'], 5.4, places=2)

    def test_hetero_field_flagged_despite_normal_mean(self):
        """Средний NDVI в норме, но 60% пикселей < 0.4 → категория anomaly."""
        resp = self._get(district=self.district.pk, year=YEAR).json()
        hetero = next(
            f for f in resp['farmlands'] if f['farmland_id'] == self.fl_hetero.pk
        )
        self.assertAlmostEqual(hetero['z_score'], 0.0, places=2)
        self.assertEqual(hetero['category'], 'anomaly')

    def test_coverage_counters(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertEqual(resp['farmlands_total'], 4)
        self.assertEqual(resp['farmlands_with_data'], 3)

    def test_sparkline_series(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        bad = resp['farmlands'][0]
        self.assertEqual(len(bad['series']), 2)
        self.assertEqual(bad['series'][0]['date'], str(D1))

    def test_limit(self):
        resp = self._get(district=self.district.pk, year=YEAR, limit=1).json()
        self.assertEqual(len(resp['farmlands']), 1)
        self.assertEqual(resp['farmlands'][0]['farmland_id'], self.fl_bad.pk)
        # Счётчики покрытия не зависят от limit.
        self.assertEqual(resp['farmlands_with_data'], 3)

    def test_outlier_observation_skipped(self):
        """Выброс позднее D2 не должен стать «последним» наблюдением."""
        s2_d3 = _scene('sentinel2', date(YEAR, 7, 12))
        _vi(self.fl_ok, s2_d3, 0.05, is_outlier=True)
        resp = self._get(district=self.district.pk, year=YEAR).json()
        ok = next(
            f for f in resp['farmlands'] if f['farmland_id'] == self.fl_ok.pk
        )
        self.assertEqual(ok['latest_date'], str(D2))

    def test_other_year_empty(self):
        resp = self._get(district=self.district.pk, year=YEAR - 1).json()
        self.assertTrue(resp['ok'])
        self.assertEqual(resp['farmlands'], [])
        self.assertEqual(resp['farmlands_with_data'], 0)


class ScreeningHelpersTests(TestCase):
    """Чистые хелперы скрининга: балл, категория, low_pct."""

    def test_low_pct(self):
        self.assertIsNone(_histogram_low_pct(None))
        self.assertIsNone(_histogram_low_pct([0, 0, 0, 0, 0]))
        self.assertIsNone(_histogram_low_pct([1, 2, 3]))
        self.assertAlmostEqual(_histogram_low_pct([10, 10, 30, 30, 20]), 20.0)

    def test_score_components(self):
        self.assertAlmostEqual(_screening_score(None, None, 0), 0.0)
        self.assertAlmostEqual(_screening_score(-2.0, None, 0), 2.0)
        self.assertAlmostEqual(_screening_score(2.0, None, 0), 0.0)   # выше нормы — не проблема
        self.assertAlmostEqual(_screening_score(None, 50.0, 0), 1.0)
        self.assertAlmostEqual(_screening_score(None, None, 5), 3.0)  # потолок алертов
        self.assertAlmostEqual(_screening_score(-1.0, 25.0, 1), 2.5)

    def test_category_buckets(self):
        self.assertEqual(_screening_category(-1.5, None, 0), 'anomaly')
        self.assertEqual(_screening_category(None, 50, 0), 'anomaly')
        self.assertEqual(_screening_category(None, None, 2), 'anomaly')
        self.assertEqual(_screening_category(-0.5, None, 0), 'below')
        self.assertEqual(_screening_category(None, 30, 0), 'below')
        self.assertEqual(_screening_category(None, None, 1), 'below')
        self.assertEqual(_screening_category(0.0, 10, 0), 'normal')
        self.assertEqual(_screening_category(None, None, 0), 'normal')


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportScreeningPageTests(TestCase):
    def test_page_renders(self):
        resp = self.client.get('/agrocosmos/report/screening/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Проблемные поля')
