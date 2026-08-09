"""
Тесты «паспорта поля» (``/agrocosmos/api/report/farmland/``).

Endpoint отдаёт детальный ряд NDVI (fused → raster fallback), референсный
MODIS-ряд, baseline района по культуре, фенологию угодья vs средней по
району, попиксельные гистограммы и алерты сезона.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, Farmland, FarmlandPhenology, NdviBaseline, Region,
    SatelliteScene, VegetationAlert, VegetationIndex,
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
class ReportFarmlandApiTests(TestCase):
    URL = '/agrocosmos/api/report/farmland/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Тестовый регион', code='r-test', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тестовый район', geom=_square(30, 50),
        )
        cls.farmland = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=120.5,
            cadastral_number='77:01:0001',
            geom=_square(30.1, 50.1, 0.05),
        )
        # Второе поле той же культуры — для средней фенологии района.
        cls.farmland2 = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=80,
            geom=_square(30.3, 50.3, 0.05),
        )

        # Детальный ряд: raw S2 (raster) — гистограммы + сглаживание.
        s2_1 = _scene('sentinel2', D1)
        s2_2 = _scene('sentinel2', D2)
        _vi(cls.farmland, s2_1, 0.55, mean_smooth=0.56,
            histogram=[0, 10, 40, 40, 10])
        _vi(cls.farmland, s2_2, 0.62, mean_smooth=0.61,
            histogram=[0, 5, 25, 50, 20])

        # MODIS референс.
        mod = _scene('modis_terra', D2)
        _vi(cls.farmland, mod, 0.58)

        # Baseline района: crop-специфичный для arable.
        NdviBaseline.objects.create(
            district=cls.district, day_of_year=D2.timetuple().tm_yday,
            mean_ndvi=0.60, std_ndvi=0.10, crop_type='arable',
        )
        NdviBaseline.objects.create(
            district=cls.district, day_of_year=D2.timetuple().tm_yday,
            mean_ndvi=0.99, std_ndvi=0.10, crop_type='',
        )

        # Фенология: своя (modis) + вторая ферма для district avg.
        FarmlandPhenology.objects.create(
            farmland=cls.farmland, year=YEAR, source='modis',
            sos_date=date(YEAR, 4, 20), eos_date=date(YEAR, 9, 15),
            pos_date=date(YEAR, 7, 1), max_ndvi=0.75, los_days=148,
            total_ndvi=80.0,
        )
        FarmlandPhenology.objects.create(
            farmland=cls.farmland2, year=YEAR, source='modis',
            sos_date=date(YEAR, 4, 10), max_ndvi=0.70, los_days=150,
        )

        # Алерты: per-farmland + district-level той же культуры.
        VegetationAlert.objects.create(
            farmland=cls.farmland, alert_type='rapid_drop',
            severity='critical', detected_on=D2, source='raster',
            message='Резкое падение NDVI',
        )
        VegetationAlert.objects.create(
            district=cls.district, crop_type='arable',
            alert_type='baseline_deviation', severity='warning',
            detected_on=D1, source='modis',
            message='Ниже нормы по району',
        )
        # Чужой алерт (другая культура района) — не должен попасть.
        VegetationAlert.objects.create(
            district=cls.district, crop_type='pasture',
            alert_type='baseline_deviation', severity='warning',
            detected_on=D1, source='modis',
        )

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_params_required(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(farmland='x', year='y').status_code, 400)
        self.assertEqual(
            self._get(farmland=self.farmland.pk).status_code, 400,
        )

    def test_not_found(self):
        resp = self._get(farmland=999999, year=YEAR)
        self.assertEqual(resp.status_code, 404)

    def test_farmland_info(self):
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        self.assertTrue(resp['ok'])
        f = resp['farmland']
        self.assertEqual(f['id'], self.farmland.pk)
        self.assertEqual(f['crop_type'], 'arable')
        self.assertEqual(f['crop_type_label'], 'Пашня')
        self.assertAlmostEqual(f['area_ha'], 120.5, places=1)
        self.assertEqual(f['cadastral_number'], '77:01:0001')
        self.assertEqual(f['district']['name'], 'Тестовый район')
        self.assertEqual(f['region']['name'], 'Тестовый регион')
        self.assertEqual(f['geometry']['type'], 'MultiPolygon')

    def test_detailed_series_raster_fallback(self):
        """Нет fused-записей → detailed_source='raster', ряд из S2."""
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        self.assertEqual(resp['detailed_source'], 'raster')
        series = resp['detailed_series']
        self.assertEqual(len(series), 2)
        self.assertEqual(series[0]['date'], str(D1))
        self.assertAlmostEqual(series[0]['mean_ndvi'], 0.55, places=3)
        self.assertAlmostEqual(series[0]['mean_smooth'], 0.56, places=3)
        self.assertEqual(series[0]['histogram'], [0, 10, 40, 40, 10])

    def test_fused_preferred_over_raster(self):
        fused = _scene('hls_fused', D2, suffix='_f')
        _vi(self.farmland, fused, 0.60)
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        self.assertEqual(resp['detailed_source'], 'fused')
        self.assertEqual(len(resp['detailed_series']), 1)
        self.assertAlmostEqual(
            resp['detailed_series'][0]['mean_ndvi'], 0.60, places=3,
        )

    def test_modis_series_separate(self):
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        self.assertEqual(len(resp['modis_series']), 1)
        self.assertAlmostEqual(
            resp['modis_series'][0]['mean_ndvi'], 0.58, places=3,
        )

    def test_crop_specific_baseline_and_z_score(self):
        """Baseline arable (0.60), а не общий (0.99): z = (0.62-0.60)/0.1 = 0.2."""
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        self.assertEqual(len(resp['baseline']), 1)
        self.assertAlmostEqual(resp['baseline'][0]['mean_ndvi'], 0.60, places=3)
        latest = resp['latest']
        self.assertEqual(latest['date'], str(D2))
        self.assertAlmostEqual(latest['z_score'], 0.2, places=2)
        self.assertTrue(latest['assessment'])

    def test_latest_skips_outliers(self):
        s2_3 = _scene('sentinel2', date(YEAR, 7, 12))
        _vi(self.farmland, s2_3, 0.10, is_outlier=True)
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        self.assertEqual(resp['latest']['date'], str(D2))

    def test_phenology_own_and_district_avg(self):
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        own = resp['phenology']['modis']
        self.assertEqual(own['sos_date'], f'{YEAR}-04-20')
        self.assertEqual(own['los_days'], 148)
        da = resp['district_phenology']
        self.assertEqual(da['count'], 2)
        # Средний SOS: (110 + 100) / 2 = 105 doy → 2025-04-15
        self.assertEqual(da['avg_sos'], f'{YEAR}-04-15')
        self.assertEqual(da['avg_los'], 149)

    def test_alerts_scoped(self):
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        alerts = resp['alerts']
        self.assertEqual(len(alerts), 2)
        scopes = {a['scope'] for a in alerts}
        self.assertEqual(scopes, {'farmland', 'district'})
        # Сортировка по дате: свежий первым.
        self.assertEqual(alerts[0]['detected_on'], str(D2))
        self.assertEqual(alerts[0]['severity'], 'critical')

    def test_other_year_empty(self):
        resp = self._get(farmland=self.farmland.pk, year=YEAR - 1).json()
        self.assertTrue(resp['ok'])
        self.assertEqual(resp['detailed_series'], [])
        self.assertEqual(resp['modis_series'], [])
        self.assertIsNone(resp['detailed_source'])
        self.assertIsNone(resp['latest']['date'])
        self.assertEqual(resp['alerts'], [])


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportFarmlandPageTests(TestCase):
    def test_page_renders(self):
        resp = self.client.get('/agrocosmos/report/farmland/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Паспорт поля')

    def test_page_prefills_farmland(self):
        resp = self.client.get('/agrocosmos/report/farmland/?farmland=42')
        self.assertContains(resp, 'value="42"')
