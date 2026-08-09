"""
Тесты сводного детального отчёта по району
(``/agrocosmos/api/report/district-detailed/``).

Endpoint агрегирует детальный мониторинг (S2/L8/fused) до уровня района:
покрытие, категории полей по правилам скрининга, area-weighted ряды
(общий и по культурам) и сводку неразрешённых алертов.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, Farmland, NdviBaseline, Region, SatelliteScene,
    VegetationAlert, VegetationIndex,
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
class ReportDistrictDetailedApiTests(TestCase):
    URL = '/agrocosmos/api/report/district-detailed/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50),
        )

        def farmland(x, crop, area):
            return Farmland.objects.create(
                region=cls.region, district=cls.district,
                crop_type=crop, area_ha=area,
                geom=_square(x, 50.1, 0.05),
            )

        # Пашня: здоровое (100 га) + аномальное (50 га).
        cls.fl_ok = farmland(30.1, Farmland.CropType.ARABLE, 100)
        cls.fl_bad = farmland(30.2, Farmland.CropType.ARABLE, 50)
        # Пастбище: одно поле с данными (200 га).
        cls.fl_pasture = farmland(30.3, Farmland.CropType.PASTURE, 200)
        # Без данных (75 га) — категория nodata.
        cls.fl_nodata = farmland(30.4, Farmland.CropType.ARABLE, 75)

        s2_d1 = _scene('sentinel2', D1)
        s2_d2 = _scene('sentinel2', D2)

        _vi(cls.fl_ok, s2_d1, 0.50)
        _vi(cls.fl_ok, s2_d2, 0.60)      # z=0 → normal
        _vi(cls.fl_bad, s2_d2, 0.30)     # z=-3 → anomaly
        _vi(cls.fl_pasture, s2_d2, 0.55)  # z=-0.5 → below

        NdviBaseline.objects.create(
            district=cls.district, day_of_year=D2.timetuple().tm_yday,
            mean_ndvi=0.60, std_ndvi=0.10, crop_type='',
        )

        VegetationAlert.objects.create(
            farmland=cls.fl_bad, alert_type='rapid_drop',
            severity='critical', detected_on=D2, source='raster',
        )
        VegetationAlert.objects.create(
            district=cls.district, crop_type='arable',
            alert_type='baseline_deviation', severity='warning',
            detected_on=D1, source='modis',
        )
        # Разрешённый — не в сводке.
        VegetationAlert.objects.create(
            farmland=cls.fl_ok, alert_type='no_growth',
            severity='warning', detected_on=D1, source='raster',
            status=VegetationAlert.Status.RESOLVED,
        )

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_params_and_404(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(district=999999, year=YEAR).status_code, 404)

    def test_coverage(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertTrue(resp['ok'])
        cov = resp['coverage']
        self.assertEqual(cov['farmlands_total'], 4)
        self.assertEqual(cov['farmlands_with_data'], 3)
        self.assertAlmostEqual(cov['area_with_data_ha'], 350.0, places=1)

    def test_categories(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        cats = resp['categories']
        # fl_bad: z=-3 + алерт → anomaly; fl_pasture: z=-0.5 → below;
        # fl_ok: z=0 → normal; fl_nodata → nodata.
        self.assertEqual(cats['anomaly']['count'], 1)
        self.assertAlmostEqual(cats['anomaly']['area_ha'], 50.0, places=1)
        self.assertEqual(cats['below']['count'], 1)
        self.assertEqual(cats['normal']['count'], 1)
        self.assertEqual(cats['nodata']['count'], 1)
        self.assertAlmostEqual(cats['nodata']['area_ha'], 75.0, places=1)

    def test_overall_series_area_weighted(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        series = resp['overall_series']
        self.assertEqual(len(series), 2)
        self.assertEqual(series[0]['date'], str(D1))
        # D1: только fl_ok → 0.50.
        self.assertAlmostEqual(series[0]['mean_ndvi'], 0.50, places=3)
        # D2: (0.60*100 + 0.30*50 + 0.55*200) / 350 = 0.5286.
        self.assertAlmostEqual(series[1]['mean_ndvi'], 0.5286, places=3)

    def test_crops_breakdown(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        crops = {c['crop_type']: c for c in resp['crops']}
        self.assertEqual(set(crops), {'arable', 'pasture'})

        arable = crops['arable']
        self.assertEqual(arable['farmlands'], 2)
        self.assertAlmostEqual(arable['area_ha'], 150.0, places=1)
        self.assertEqual(arable['problem_count'], 1)  # fl_bad
        # Взвешенный NDVI последних наблюдений: (0.6*100 + 0.3*50)/150 = 0.5.
        self.assertAlmostEqual(arable['latest_ndvi'], 0.50, places=3)
        self.assertEqual(arable['latest_date'], str(D2))
        # Серия пашни: D1 (0.50, только fl_ok) и D2 ((0.6*100+0.3*50)/150).
        self.assertEqual(len(arable['series']), 2)
        self.assertAlmostEqual(arable['series'][1]['mean_ndvi'], 0.50, places=3)

        pasture = crops['pasture']
        self.assertEqual(pasture['farmlands'], 1)
        self.assertEqual(pasture['problem_count'], 1)  # below тоже проблема
        # Сортировка по площади: pasture (200) первым.
        self.assertEqual(resp['crops'][0]['crop_type'], 'pasture')

    def test_alerts_summary(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        sum_ = resp['alerts_summary']
        # per-farmland rapid_drop + district-level baseline_deviation;
        # resolved no_growth не считается.
        self.assertEqual(sum_['active_total'], 2)
        types = {t['alert_type']: t['count'] for t in sum_['by_type']}
        self.assertEqual(types, {'rapid_drop': 1, 'baseline_deviation': 1})

    def test_baseline_series(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertEqual(len(resp['baseline']), 1)
        self.assertAlmostEqual(resp['baseline'][0]['mean_ndvi'], 0.60, places=3)

    def test_other_year_empty(self):
        resp = self._get(district=self.district.pk, year=YEAR - 1).json()
        self.assertTrue(resp['ok'])
        self.assertEqual(resp['coverage']['farmlands_with_data'], 0)
        self.assertEqual(resp['overall_series'], [])
        self.assertEqual(resp['crops'], [])
        self.assertEqual(resp['categories']['nodata']['count'], 4)


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportDistrictDetailedPageTests(TestCase):
    def test_page_renders(self):
        resp = self.client.get('/agrocosmos/report/district-detailed/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Свод по району')
