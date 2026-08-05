"""Тесты команды ``fetch_ndvi_batch`` — страховочная сетка перед
рефакторингом ``handle`` (C=33). GEE мокается на уровне
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

from agrocosmos.management.commands.fetch_ndvi_batch import (
    _biweekly_chunks, _month_chunks, _simplify_coords,
)
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


class ChunkHelpersTests(TestCase):
    def test_month_chunks(self):
        chunks = _month_chunks(date(2025, 1, 15), date(2025, 3, 10))
        self.assertEqual(chunks, [
            (date(2025, 1, 15), date(2025, 1, 31)),
            (date(2025, 2, 1), date(2025, 2, 28)),
            (date(2025, 3, 1), date(2025, 3, 10)),
        ])

    def test_month_chunks_year_boundary(self):
        chunks = _month_chunks(date(2024, 12, 1), date(2025, 1, 31))
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[1], (date(2025, 1, 1), date(2025, 1, 31)))

    def test_biweekly_chunks(self):
        chunks = _biweekly_chunks(date(2025, 6, 1), date(2025, 6, 20))
        self.assertEqual(chunks, [
            (date(2025, 6, 1), date(2025, 6, 16)),
            (date(2025, 6, 17), date(2025, 6, 20)),
        ])

    def test_simplify_coords(self):
        gj = {'type': 'Polygon',
              'coordinates': [[[30.123456789, 50.987654321]]]}
        out = _simplify_coords(gj, precision=4)
        self.assertEqual(out['coordinates'], [[[30.1235, 50.9877]]])
        # исходник не мутируется
        self.assertEqual(gj['coordinates'][0][0][0], 30.123456789)


class FetchNdviBatchTests(TestCase):
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
            for i in range(3)
        ]

    def _run(self, batch_mock=None, sensor='s2', **kwargs):
        out, err = io.StringIO(), io.StringIO()
        batch_mock = batch_mock if batch_mock is not None \
            else mock.MagicMock(return_value={})
        fn_name = ('fetch_modis_ndvi_batch' if sensor == 'modis'
                   else 'fetch_ndvi_batch')
        with mock.patch(f'{SVC}.{fn_name}', batch_mock), \
             mock.patch('time.sleep'):
            call_command(
                'fetch_ndvi_batch',
                date_from='2025-06-01', date_to='2025-06-30',
                sensor=sensor, throttle=0,
                stdout=out, stderr=err,
                **kwargs,
            )
        return out.getvalue(), err.getvalue(), batch_mock

    def test_requires_region_or_district(self):
        out, err, batch_mock = self._run()
        self.assertIn('Specify --region-id or --district-id', err)
        batch_mock.assert_not_called()

    def test_no_farmlands(self):
        empty = Region.objects.create(
            name='Пусто', code='r2', geom=_square(40, 50))
        out, err, batch_mock = self._run(region_id=empty.pk)
        self.assertIn('No farmlands found', err)
        batch_mock.assert_not_called()

    def test_s2_saves_records(self):
        results = {fl.pk: [dict(STAT)] for fl in self.farmlands}
        out, _, batch_mock = self._run(
            mock.MagicMock(return_value=results), region_id=self.region.pk)
        batch_mock.assert_called_once()
        kwargs = batch_mock.call_args.kwargs
        self.assertEqual(kwargs['cloud_max'], 30)
        self.assertEqual(kwargs['min_valid_ratio'], 0.95)
        self.assertEqual(len(kwargs['farmlands']), 3)
        self.assertEqual(VegetationIndex.objects.count(), 3)
        scene = SatelliteScene.objects.get()
        self.assertEqual(scene.scene_id,
                         f's2_2025-06-10_{self.district.pk}')
        self.assertEqual(scene.satellite, 'sentinel2')
        self.assertIn('+3 new, 0 upd', out)
        self.assertIn('New records: 3', out)

    def test_modis_variant(self):
        results = {self.farmlands[0].pk: [dict(STAT)]}
        out, _, batch_mock = self._run(
            mock.MagicMock(return_value=results),
            sensor='modis', region_id=self.region.pk)
        # 16-дневные чанки: июнь = 2 вызова
        self.assertEqual(batch_mock.call_count, 2)
        kwargs = batch_mock.call_args.kwargs
        self.assertNotIn('cloud_max', kwargs)
        # MODIS: порог валидности снижается с дефолтных 0.95 до 0.5
        self.assertEqual(kwargs['min_valid_ratio'], 0.5)
        scene = SatelliteScene.objects.first()
        self.assertTrue(scene.scene_id.startswith('modis_'))
        self.assertEqual(scene.satellite, 'modis_terra')

    def test_rerun_updates_without_duplicates(self):
        results = {self.farmlands[0].pk: [dict(STAT)]}
        self._run(mock.MagicMock(return_value=results),
                  region_id=self.region.pk)
        updated = {self.farmlands[0].pk: [dict(STAT, mean=0.8)]}
        out, _, _ = self._run(mock.MagicMock(return_value=updated),
                              region_id=self.region.pk)
        self.assertEqual(VegetationIndex.objects.count(), 1)
        self.assertAlmostEqual(VegetationIndex.objects.get().mean, 0.8)
        self.assertIn('+0 new, 1 upd', out)

    def test_gee_error_retry_succeeds(self):
        results = {self.farmlands[0].pk: [dict(STAT)]}
        batch_mock = mock.MagicMock(
            side_effect=[GEEError('quota'), results])
        out, err, _ = self._run(batch_mock, region_id=self.region.pk)
        self.assertEqual(batch_mock.call_count, 2)
        self.assertIn('Retrying in 15s', err)
        self.assertEqual(VegetationIndex.objects.count(), 1)
        self.assertIn('Errors: 0', out)

    def test_gee_error_retry_fails(self):
        batch_mock = mock.MagicMock(
            side_effect=[GEEError('quota'), GEEError('quota')])
        out, err, _ = self._run(batch_mock, region_id=self.region.pk)
        self.assertEqual(VegetationIndex.objects.count(), 0)
        self.assertIn('Errors: 1', out)

    def test_unexpected_error_counted(self):
        batch_mock = mock.MagicMock(side_effect=RuntimeError('boom'))
        out, err, _ = self._run(batch_mock, region_id=self.region.pk)
        self.assertIn('UNEXPECTED: boom', err)
        self.assertIn('Errors: 1', out)

    def test_batch_size_splits_farmlands(self):
        out, _, batch_mock = self._run(
            region_id=self.region.pk, batch_size=2)
        # 3 угодья / 2 = 2 батча × 1 месяц
        self.assertEqual(batch_mock.call_count, 2)
        sizes = sorted(len(c.kwargs['farmlands'])
                       for c in batch_mock.call_args_list)
        self.assertEqual(sizes, [1, 2])

    def test_start_from_id_and_limit(self):
        out, _, batch_mock = self._run(
            region_id=self.region.pk,
            start_from_id=self.farmlands[1].pk, limit=1)
        ids = [d['id'] for d in batch_mock.call_args.kwargs['farmlands']]
        self.assertEqual(ids, [self.farmlands[1].pk])

    def test_district_filter(self):
        other = District.objects.create(
            region=self.region, name='Другой', geom=_square(32, 50, 0.5))
        fl = Farmland.objects.create(
            region=self.region, district=other, geom=_square(32.1, 50.1))
        out, _, batch_mock = self._run(district_id=other.pk)
        ids = [d['id'] for d in batch_mock.call_args.kwargs['farmlands']]
        self.assertEqual(ids, [fl.pk])
