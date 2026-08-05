"""Тесты команды ``modis_ndvi`` — страховочная сетка перед рефакторингом
``handle`` (C=43). Resume-логика уже покрыта в ``test_modis_resume.py``;
здесь: валидация аргументов, шаг скачивания, запись VegetationIndex,
идемпотентный upsert и refresh кэша статусов.
"""
import io
import sys
import tempfile
from datetime import date
from unittest import mock

# В локальном окружении нет GEE/растровых зависимостей — подменяем,
# чтобы импорт agrocosmos.services.satellite_modis_raster не падал.
for _mod in ('ee', 'rasterio', 'rasterio.mask', 'rasterio.features',
             'rasterstats'):
    if _mod not in sys.modules:
        try:
            __import__(_mod)
        except ImportError:
            sys.modules[_mod] = mock.MagicMock()

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)

SVC = 'agrocosmos.services.satellite_modis_raster'

CHUNK = (date(2025, 6, 1), date(2025, 6, 16))
MID_DATE = CHUNK[0] + (CHUNK[1] - CHUNK[0]) / 2

STAT = {
    'mean': 0.6, 'median': 0.61, 'min': 0.2, 'max': 0.9, 'std': 0.1,
    'pixel_count': 100, 'valid_pixel_count': 90,
}


def _square(x, y, size=0.1):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class ModisNdviCommandTests(TestCase):
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

    def _run(self, compute_mock=None, raster_exists=True, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        compute_mock = compute_mock or mock.MagicMock(return_value={})
        with tempfile.NamedTemporaryFile(suffix='.tif') as tif:
            raster = tif.name if raster_exists else tif.name + '.nope'
            with mock.patch(f'{SVC}._biweekly_chunks', return_value=[CHUNK]), \
                 mock.patch(f'{SVC}._raster_path', return_value=raster), \
                 mock.patch(f'{SVC}.compute_zonal_stats', compute_mock):
                kwargs.setdefault('skip_status_refresh', True)
                call_command(
                    'modis_ndvi',
                    date_from=CHUNK[0].isoformat(),
                    date_to=CHUNK[1].isoformat(),
                    stats_only=True,
                    stdout=out, stderr=err,
                    **kwargs,
                )
        return out.getvalue(), err.getvalue(), compute_mock

    # ── валидация аргументов ─────────────────────────────────────────
    def test_requires_region_or_district(self):
        out, err = io.StringIO(), io.StringIO()
        call_command('modis_ndvi', year=2025, stdout=out, stderr=err)
        self.assertIn('Specify --region-id or --district-id', err.getvalue())

    def test_requires_dates(self):
        out, err = io.StringIO(), io.StringIO()
        call_command('modis_ndvi', region_id=self.region.pk,
                     stdout=out, stderr=err)
        self.assertIn('Specify --year or --date-from/--date-to', err.getvalue())

    # ── запись статистики ────────────────────────────────────────────
    def test_saves_vegetation_indices_and_scene(self):
        results = {fl.pk: dict(STAT) for fl in self.farmlands}
        out, _, _ = self._run(mock.MagicMock(return_value=results),
                              region_id=self.region.pk)
        self.assertEqual(VegetationIndex.objects.count(), 2)
        vi = VegetationIndex.objects.get(farmland=self.farmlands[0])
        self.assertEqual(vi.index_type, 'ndvi')
        self.assertEqual(vi.acquired_date, MID_DATE)
        self.assertAlmostEqual(vi.mean, 0.6)
        self.assertEqual(vi.valid_pixel_count, 90)
        scene = SatelliteScene.objects.get()
        self.assertEqual(
            scene.scene_id, f'modis_{MID_DATE.isoformat()}_{self.district.pk}')
        self.assertEqual(scene.satellite, 'modis_terra')
        self.assertIn('2 records saved', out)
        self.assertIn('Records saved: 2', out)

    def test_rerun_upserts_without_duplicates(self):
        results = {fl.pk: dict(STAT) for fl in self.farmlands}
        self._run(mock.MagicMock(return_value=results),
                  region_id=self.region.pk)
        updated = {fl.pk: dict(STAT, mean=0.8) for fl in self.farmlands}
        self._run(mock.MagicMock(return_value=updated),
                  region_id=self.region.pk, recompute_stats=True)
        self.assertEqual(VegetationIndex.objects.count(), 2)
        self.assertAlmostEqual(
            VegetationIndex.objects.first().mean, 0.8)

    def test_district_filter_limits_farmlands(self):
        other_district = District.objects.create(
            region=self.region, name='Другой', geom=_square(32, 50, 0.5))
        Farmland.objects.create(
            region=self.region, district=other_district,
            geom=_square(32.1, 50.1))
        compute_mock = mock.MagicMock(return_value={})
        self._run(compute_mock, district_id=self.district.pk)
        fl_geoms = compute_mock.call_args.args[1]
        self.assertEqual(len(fl_geoms), 2)

    def test_missing_raster_skips_chunk(self):
        out, _, compute_mock = self._run(
            raster_exists=False, region_id=self.region.pk)
        compute_mock.assert_not_called()
        self.assertIn('no raster, skip', out)

    def test_no_farmlands(self):
        empty_region = Region.objects.create(
            name='Пусто', code='r2', geom=_square(40, 50, 1.0))
        out, err, compute_mock = self._run(region_id=empty_region.pk)
        compute_mock.assert_not_called()
        self.assertIn('No farmlands found', err)

    def test_compute_error_counted(self):
        out, err, _ = self._run(
            mock.MagicMock(side_effect=RuntimeError('boom')),
            region_id=self.region.pk)
        self.assertIn('ERROR: boom', err)
        self.assertIn('Errors: 1', out)

    # ── шаг скачивания ───────────────────────────────────────────────
    def test_download_only(self):
        out, err = io.StringIO(), io.StringIO()
        with tempfile.NamedTemporaryFile(suffix='.tif') as tif:
            with mock.patch(f'{SVC}._biweekly_chunks',
                            return_value=[CHUNK, CHUNK, CHUNK]), \
                 mock.patch(f'{SVC}.download_composite',
                            side_effect=[tif.name, None,
                                         RuntimeError('gee down')]) as dl:
                call_command(
                    'modis_ndvi', region_id=self.region.pk, year=2025,
                    download_only=True, skip_status_refresh=True,
                    stdout=out, stderr=err,
                )
        self.assertEqual(dl.call_count, 3)
        self.assertIn('1 files, 1 skipped, 1 errors', out.getvalue())
        self.assertFalse(VegetationIndex.objects.exists())

    # ── refresh кэша статусов ────────────────────────────────────────
    def test_status_refresh_called_unless_skipped(self):
        with mock.patch('django.core.management.call_command') as mock_cc:
            self._run(region_id=self.region.pk, skip_status_refresh=False)
        mock_cc.assert_called_once()
        self.assertEqual(mock_cc.call_args.args[0],
                         'recompute_district_ndvi_status')
        self.assertEqual(mock_cc.call_args.kwargs['region_id'],
                         self.region.pk)

    def test_status_refresh_skipped_flag(self):
        out, _, _ = self._run(region_id=self.region.pk)
        self.assertIn('skipped', out)
