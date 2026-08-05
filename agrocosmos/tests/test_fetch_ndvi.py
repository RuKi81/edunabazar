"""Тесты команды ``fetch_ndvi`` — страховочная сетка перед рефакторингом
``handle`` (C=30). Бэкенд GEE мокается на уровне
``agrocosmos.services.satellite_gee``.
"""
import io
import sys
from datetime import date
from unittest import mock

# Локально нет earthengine-api — подменяем до импорта satellite_gee.
if 'ee' not in sys.modules:
    try:
        import ee  # noqa: F401
    except ImportError:
        sys.modules['ee'] = mock.MagicMock()

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)
from agrocosmos.services.satellite_gee import GEEError

SVC = 'agrocosmos.services.satellite_gee'

STAT = {
    'date': date(2025, 6, 10), 'mean': 0.6, 'median': 0.61,
    'min': 0.2, 'max': 0.9, 'std': 0.1,
    'pixel_count': 100, 'valid_pixel_count': 98,
}


def _square(x, y, size=0.1):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class FetchNdviTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50, 2.0))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50, 2.0))
        cls.farmlands = [
            Farmland.objects.create(
                region=cls.region, district=cls.district,
                geom=_square(30.1 + i * 0.3, 50.1),
            )
            for i in range(2)
        ]

    def _run(self, stats_mock=None, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        stats_mock = stats_mock if stats_mock is not None \
            else mock.MagicMock(return_value=[])
        with mock.patch(f'{SVC}.fetch_ndvi_stats', stats_mock), \
             mock.patch('time.sleep'):
            call_command(
                'fetch_ndvi',
                date_from='2025-06-01', date_to='2025-06-30',
                throttle=0,
                stdout=out, stderr=err,
                **kwargs,
            )
        return out.getvalue(), err.getvalue(), stats_mock

    def test_requires_filter(self):
        out, err, stats_mock = self._run()
        self.assertIn('Specify --region-id, --district-id, or --farmland-id',
                      err)
        stats_mock.assert_not_called()

    def test_no_farmlands(self):
        empty = Region.objects.create(
            name='Пусто', code='r2', geom=_square(40, 50))
        out, err, stats_mock = self._run(region_id=empty.pk)
        self.assertIn('No farmlands found', err)
        stats_mock.assert_not_called()

    def test_saves_records(self):
        stats_mock = mock.MagicMock(return_value=[dict(STAT)])
        out, _, _ = self._run(stats_mock, region_id=self.region.pk)
        # 2 угодья × 1 месяц
        self.assertEqual(stats_mock.call_count, 2)
        kwargs = stats_mock.call_args.kwargs
        self.assertEqual(kwargs['cloud_max'], 30)
        self.assertEqual(kwargs['min_valid_ratio'], 0.95)
        self.assertEqual(kwargs['date_from'], date(2025, 6, 1))
        self.assertEqual(kwargs['date_to'], date(2025, 6, 30))
        self.assertEqual(VegetationIndex.objects.count(), 2)
        scene = SatelliteScene.objects.get()
        self.assertEqual(scene.scene_id,
                         f's2_2025-06-10_{self.district.pk}')
        self.assertEqual(scene.satellite, 'sentinel2')
        self.assertIn('New records: 2', out)

    def test_rerun_updates_without_duplicates(self):
        self._run(mock.MagicMock(return_value=[dict(STAT)]),
                  farmland_id=self.farmlands[0].pk)
        out, _, _ = self._run(
            mock.MagicMock(return_value=[dict(STAT, mean=0.8)]),
            farmland_id=self.farmlands[0].pk)
        self.assertEqual(VegetationIndex.objects.count(), 1)
        self.assertAlmostEqual(VegetationIndex.objects.get().mean, 0.8)
        self.assertIn('Updated records: 1', out)

    def test_resume_skips_covered_month(self):
        self._run(mock.MagicMock(return_value=[dict(STAT)]),
                  farmland_id=self.farmlands[0].pk)
        out, _, stats_mock = self._run(
            farmland_id=self.farmlands[0].pk, resume=True)
        stats_mock.assert_not_called()
        self.assertIn('Skipped (resume): 1', out)

    def test_backend_error_retry_succeeds(self):
        stats_mock = mock.MagicMock(
            side_effect=[GEEError('quota'), [dict(STAT)]])
        out, err, _ = self._run(stats_mock,
                                farmland_id=self.farmlands[0].pk)
        self.assertEqual(stats_mock.call_count, 2)
        self.assertIn('Retrying in 10s', err)
        self.assertEqual(VegetationIndex.objects.count(), 1)
        self.assertIn('Errors: 0', out)

    def test_backend_error_retry_fails(self):
        stats_mock = mock.MagicMock(
            side_effect=[GEEError('quota'), GEEError('quota')])
        out, err, _ = self._run(stats_mock,
                                farmland_id=self.farmlands[0].pk)
        self.assertEqual(VegetationIndex.objects.count(), 0)
        self.assertIn('Errors: 1', out)

    def test_unexpected_error_counted(self):
        stats_mock = mock.MagicMock(side_effect=RuntimeError('boom'))
        out, err, _ = self._run(stats_mock,
                                farmland_id=self.farmlands[0].pk)
        self.assertIn('UNEXPECTED ERROR', err)
        self.assertIn('Errors: 1', out)

    def test_farmland_filter_and_limit(self):
        out, _, stats_mock = self._run(
            region_id=self.region.pk,
            start_from_id=self.farmlands[1].pk, limit=1)
        self.assertEqual(stats_mock.call_count, 1)

    def test_district_filter(self):
        other = District.objects.create(
            region=self.region, name='Другой', geom=_square(32, 50, 0.5))
        Farmland.objects.create(
            region=self.region, district=other, geom=_square(32.1, 50.1))
        out, _, stats_mock = self._run(district_id=other.pk)
        self.assertEqual(stats_mock.call_count, 1)
