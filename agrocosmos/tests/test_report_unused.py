"""
Тесты скрининга неиспользуемых земель (``/agrocosmos/api/report/unused/``).

Endpoint сверяет заявленный факт использования (is_used) со спутниковыми
сигналами сезона: максимум NDVI, амплитуда, детектированный SOS.
"""
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import (
    District, Farmland, FarmlandPhenology, Region, SatelliteScene,
    VegetationIndex,
)
from agrocosmos.views.reports import _unused_signals

YEAR = 2025


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportUnusedApiTests(TestCase):
    URL = '/agrocosmos/api/report/unused/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50),
        )

        def farmland(x, is_used, area=100):
            return Farmland.objects.create(
                region=cls.region, district=cls.district,
                crop_type=Farmland.CropType.ARABLE, area_ha=area,
                is_used=is_used, geom=_square(x, 50.1, 0.05),
            )

        def season(fl, values, month_start=5):
            for i, v in enumerate(values):
                d = date(YEAR, month_start + i, 15)
                scene = SatelliteScene.objects.create(
                    satellite='modis_terra',
                    scene_id=f'mod_{fl.pk}_{d}',
                    acquired_date=d,
                )
                VegetationIndex.objects.create(
                    farmland=fl, scene=scene, index_type='ndvi',
                    acquired_date=d, mean=v,
                )

        # Заявлено используемым, нормальный сезон → чисто.
        cls.fl_used_ok = farmland(30.1, True)
        season(cls.fl_used_ok, [0.3, 0.6, 0.7, 0.4])
        FarmlandPhenology.objects.create(
            farmland=cls.fl_used_ok, year=YEAR, source='modis',
            sos_date=date(YEAR, 5, 1),
        )

        # Заявлено используемым, max NDVI 0.25 → подозрение high (150 га).
        cls.fl_suspect_hi = farmland(30.2, True, area=150)
        season(cls.fl_suspect_hi, [0.20, 0.25, 0.22, 0.18])

        # is_used неизвестен, плоский ряд ~0.45 без SOS → подозрение medium.
        cls.fl_suspect_med = farmland(30.3, None)
        season(cls.fl_suspect_med, [0.44, 0.47, 0.45, 0.43])

        # Заявлено НЕиспользуемым, но полный цикл + SOS → возвращено в оборот.
        cls.fl_react = farmland(30.4, False)
        season(cls.fl_react, [0.3, 0.55, 0.75, 0.4])
        FarmlandPhenology.objects.create(
            farmland=cls.fl_react, year=YEAR, source='modis',
            sos_date=date(YEAR, 5, 10),
        )

        # Заявлено неиспользуемым и правда пустое → согласовано, не в списках.
        cls.fl_unused_ok = farmland(30.5, False)
        season(cls.fl_unused_ok, [0.15, 0.20, 0.18, 0.16])

        # Мало данных (2 наблюдения) → вне анализа.
        cls.fl_thin = farmland(30.6, True)
        season(cls.fl_thin, [0.2, 0.2])

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_params_and_404(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(district=999999, year=YEAR).status_code, 404)

    def test_suspects(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        self.assertTrue(resp['ok'])
        suspects = resp['suspects']
        ids = [s['farmland_id'] for s in suspects]
        # high (fl_suspect_hi) первым, medium (fl_suspect_med) вторым.
        self.assertEqual(
            ids, [self.fl_suspect_hi.pk, self.fl_suspect_med.pk],
        )
        hi = suspects[0]
        self.assertEqual(hi['severity'], 'high')
        self.assertIn('no_vegetation', hi['signals'])
        self.assertAlmostEqual(hi['max_ndvi'], 0.25, places=3)
        med = suspects[1]
        self.assertEqual(med['severity'], 'medium')
        self.assertEqual(med['signals'], ['no_cycle'])
        self.assertFalse(med['has_sos'])

    def test_used_field_with_season_is_clean(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        all_ids = (
            [s['farmland_id'] for s in resp['suspects']]
            + [r['farmland_id'] for r in resp['reactivated']]
        )
        self.assertNotIn(self.fl_used_ok.pk, all_ids)
        self.assertNotIn(self.fl_unused_ok.pk, all_ids)
        self.assertNotIn(self.fl_thin.pk, all_ids)

    def test_reactivated(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        react = resp['reactivated']
        self.assertEqual(len(react), 1)
        self.assertEqual(react[0]['farmland_id'], self.fl_react.pk)
        self.assertTrue(react[0]['has_sos'])
        self.assertAlmostEqual(react[0]['max_ndvi'], 0.75, places=3)

    def test_totals(self):
        resp = self._get(district=self.district.pk, year=YEAR).json()
        t = resp['totals']
        self.assertEqual(t['farmlands_total'], 6)
        self.assertEqual(t['declared_unused'], 2)  # fl_react + fl_unused_ok
        self.assertEqual(t['with_data'], 5)        # все, кроме fl_thin
        self.assertEqual(t['suspects'], 2)
        self.assertEqual(t['reactivated'], 1)
        self.assertAlmostEqual(t['suspect_area'], 250.0, places=1)

    def test_limit(self):
        resp = self._get(district=self.district.pk, year=YEAR, limit=1).json()
        self.assertEqual(len(resp['suspects']), 1)
        # Totals считаются до среза.
        self.assertEqual(resp['totals']['suspects'], 2)

    def test_other_year_empty(self):
        resp = self._get(district=self.district.pk, year=YEAR - 1).json()
        self.assertTrue(resp['ok'])
        self.assertEqual(resp['suspects'], [])
        self.assertEqual(resp['reactivated'], [])
        self.assertEqual(resp['totals']['with_data'], 0)


class UnusedSignalsTests(TestCase):
    """Чистый хелпер сигналов неиспользования."""

    def test_insufficient_data(self):
        self.assertIsNone(_unused_signals(None, False))
        self.assertIsNone(
            _unused_signals({'max_ndvi': 0.2, 'min_ndvi': 0.1, 'n_obs': 2}, False),
        )

    def test_no_vegetation(self):
        s = _unused_signals({'max_ndvi': 0.30, 'min_ndvi': 0.10, 'n_obs': 5}, False)
        self.assertIn('no_vegetation', s)

    def test_no_cycle_requires_no_sos(self):
        stats = {'max_ndvi': 0.45, 'min_ndvi': 0.40, 'n_obs': 5}
        self.assertEqual(_unused_signals(stats, False), ['no_cycle'])
        self.assertEqual(_unused_signals(stats, True), [])

    def test_clean_season(self):
        s = _unused_signals({'max_ndvi': 0.75, 'min_ndvi': 0.25, 'n_obs': 8}, True)
        self.assertEqual(s, [])


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ReportUnusedPageTests(TestCase):
    def test_page_renders(self):
        resp = self.client.get('/agrocosmos/report/unused/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Неиспользуемые земли')
