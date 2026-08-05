"""Тесты команды ``recompute_district_ndvi_status`` — страховочная сетка
перед рефакторингом ``handle`` (C=14). Апсерт гоняем настоящим SQL по
пустым таблицам (0 строк); сервисы кэшей мокаются.
"""
import io
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import District, Farmland, Region

MOD = 'agrocosmos.management.commands.recompute_district_ndvi_status'
GEOJSON = 'agrocosmos.services.districts_status_geojson'
SERIES = 'agrocosmos.services.district_ndvi_series'

SERIES_RES = {
    'source': 'modis', 'date_from': '2025-01-01', 'date_to': '2025-03-01',
    'inserted': 0, 'deleted': 0, 'elapsed_s': 0.1,
}


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class RecomputeDistrictNdviStatusTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        cls.empty_region = Region.objects.create(
            name='Пустой', code='r2', geom=_square(40, 50))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50))
        Farmland.objects.create(
            region=cls.region, district=cls.district,
            geom=_square(30.1, 50.1))

    def _run(self, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{GEOJSON}.refresh_cache',
                        return_value={'features': []}) as geo, \
             mock.patch(f'{GEOJSON}.invalidate_available_dates') as inval, \
             mock.patch(f'{GEOJSON}.list_available_dates',
                        return_value=['2025-05-01', '2025-05-17',
                                      '2025-06-02', '2025-06-18',
                                      '2025-07-04']) as dates, \
             mock.patch(f'{GEOJSON}.prewarm_snapshots',
                        return_value=(4, 0, 1.0)) as prewarm, \
             mock.patch(f'{SERIES}.refresh_recent',
                        return_value=SERIES_RES) as series:
            call_command('recompute_district_ndvi_status',
                         stdout=out, stderr=err, **kwargs)
        mocks = dict(geo=geo, inval=inval, dates=dates,
                     prewarm=prewarm, series=series)
        return out.getvalue(), err.getvalue(), mocks

    def test_only_regions_with_farmlands(self):
        out, _, _ = self._run()
        self.assertIn('for 1 region(s)', out)
        self.assertIn('[Регион] 0 rows', out)
        self.assertNotIn('Пустой', out)

    def test_region_id_filter_ignores_farmland_requirement(self):
        out, _, _ = self._run(region_id=self.empty_region.pk)
        self.assertIn('for 1 region(s)', out)
        self.assertIn('[Пустой]', out)

    def test_summary_and_cache_refreshes(self):
        out, _, m = self._run()
        self.assertIn('0 rows upserted across 1/1 region(s)', out)
        self.assertIn('districts_status GeoJSON cached: 0 features', out)
        self.assertIn('district_ndvi_series (modis 2025-01-01..2025-03-01)',
                      out)
        m['geo'].assert_called_once()
        m['series'].assert_called_once_with(days=70, source='modis')

    def test_prewarm_recent_default_4(self):
        out, _, m = self._run()
        m['inval'].assert_called_once()
        m['prewarm'].assert_called_once_with(
            ['2025-05-17', '2025-06-02', '2025-06-18', '2025-07-04'],
            force=True,
        )
        self.assertIn('timeline prewarm', out)
        self.assertIn('4 built, 0 cached', out)

    def test_prewarm_disabled(self):
        _, _, m = self._run(prewarm_recent=0)
        m['prewarm'].assert_not_called()

    def test_sql_failure_is_per_region(self):
        with mock.patch(f'{MOD}._RECOMPUTE_SQL', 'SELECT broken syntax('):
            out, err, _ = self._run()
        self.assertIn('FAILED after', err)
        self.assertIn('1 region(s) failed: Регион', err)
        self.assertIn('0 rows upserted across 0/1 region(s)', out)

    def test_geojson_failure_non_fatal(self):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{GEOJSON}.refresh_cache',
                        side_effect=RuntimeError('redis down')), \
             mock.patch(f'{SERIES}.refresh_recent',
                        return_value=SERIES_RES):
            call_command('recompute_district_ndvi_status', prewarm_recent=0,
                         stdout=out, stderr=err)
        self.assertIn('GeoJSON cache refresh failed (non-fatal): redis down',
                      err.getvalue())
        self.assertIn('rows upserted', out.getvalue())
