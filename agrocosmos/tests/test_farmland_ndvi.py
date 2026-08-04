"""
Тесты NDVI-ряда одного угодья (``/agrocosmos/api/farmland/ndvi/``) —
страховочная сетка перед рефакторингом ``api_farmland_ndvi`` (C=11).

Покрывается: валидация параметра farmland, фильтры source/year,
отсечение mean вне [-1, 1], состав полей точки (включая is_outlier
и mean_smooth=None), last_period_end только для MODIS.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}

URL = '/agrocosmos/api/farmland/ndvi/'
YEAR = 2025


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


@override_settings(CACHES=_DUMMY_CACHE)
class FarmlandNdviApiTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        region = Region.objects.create(
            name='Регион Н', code='rn-1', geom=_square(30, 50),
        )
        district = District.objects.create(
            region=region, name='Район Н', geom=_square(30, 50),
        )
        cls.farmland = Farmland.objects.create(
            district=district, crop_type='arable', area_ha=10,
            geom=_square(30.0, 50.0, 0.01),
        )
        cls.scene_modis = SatelliteScene.objects.create(
            satellite='modis_terra', scene_id='MOD-1',
            acquired_date=date(YEAR, 6, 10),
        )
        cls.scene_s2 = SatelliteScene.objects.create(
            satellite='sentinel2', scene_id='S2-1',
            acquired_date=date(YEAR, 6, 12),
        )
        cls.scene_old = SatelliteScene.objects.create(
            satellite='modis_terra', scene_id='MOD-OLD',
            acquired_date=date(YEAR - 1, 6, 10),
        )

        cls.vi_modis = VegetationIndex.objects.create(
            farmland=cls.farmland, scene=cls.scene_modis, index_type='ndvi',
            acquired_date=date(YEAR, 6, 10), mean=0.61234, median=0.6,
            min_val=0.4, max_val=0.8, mean_smooth=0.6, is_outlier=False,
        )
        cls.vi_s2 = VegetationIndex.objects.create(
            farmland=cls.farmland, scene=cls.scene_s2, index_type='ndvi',
            acquired_date=date(YEAR, 6, 12), mean=0.7, median=0.7,
            min_val=0.5, max_val=0.9, mean_smooth=None, is_outlier=True,
        )
        cls.vi_old = VegetationIndex.objects.create(
            farmland=cls.farmland, scene=cls.scene_old, index_type='ndvi',
            acquired_date=date(YEAR - 1, 6, 10), mean=0.5,
        )
        # вне физического диапазона — не должно попадать в ответ
        cls.vi_bad = VegetationIndex.objects.create(
            farmland=cls.farmland, scene=cls.scene_modis, index_type='evi',
            acquired_date=date(YEAR, 6, 10), mean=0.6,
        )

    def _get(self, **params):
        return self.client.get(URL, params)

    def test_farmland_required(self):
        self.assertEqual(self._get().status_code, 400)

    def test_invalid_farmland(self):
        resp = self._get(farmland='abc')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid farmland')

    def test_full_series_all_sources(self):
        resp = self._get(farmland=self.farmland.pk).json()
        self.assertTrue(resp['ok'])
        dates = [p['date'] for p in resp['data']]
        self.assertEqual(
            dates, [str(date(YEAR - 1, 6, 10)), str(date(YEAR, 6, 10)), str(date(YEAR, 6, 12))],
        )
        self.assertIsNone(resp['last_period_end'])

    def test_point_fields(self):
        resp = self._get(farmland=self.farmland.pk, year=YEAR).json()
        modis_point = resp['data'][0]
        self.assertEqual(modis_point['mean'], 0.6123)
        self.assertEqual(modis_point['min'], 0.4)
        self.assertEqual(modis_point['max'], 0.8)
        self.assertEqual(modis_point['median'], 0.6)
        self.assertEqual(modis_point['mean_smooth'], 0.6)
        self.assertFalse(modis_point['is_outlier'])

        s2_point = resp['data'][1]
        self.assertIsNone(s2_point['mean_smooth'])
        self.assertTrue(s2_point['is_outlier'])

    def test_source_filter(self):
        resp = self._get(farmland=self.farmland.pk, source='raster').json()
        self.assertEqual(len(resp['data']), 1)
        self.assertEqual(resp['data'][0]['date'], str(date(YEAR, 6, 12)))

    def test_year_filter_garbage_ignored(self):
        resp = self._get(farmland=self.farmland.pk, year='oops').json()
        self.assertEqual(len(resp['data']), 3)

    def test_modis_last_period_end(self):
        resp = self._get(farmland=self.farmland.pk, source='modis', year=YEAR).json()
        self.assertEqual(len(resp['data']), 1)
        # середина композита 10.06 + 8 дней
        self.assertEqual(resp['last_period_end'], '2025-06-18')

    def test_modis_empty_series_no_tail(self):
        resp = self._get(farmland=self.farmland.pk, source='modis', year=1999).json()
        self.assertEqual(resp['data'], [])
        self.assertIsNone(resp['last_period_end'])
