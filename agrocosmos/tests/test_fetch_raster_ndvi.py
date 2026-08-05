"""Тесты команды ``fetch_raster_ndvi`` — страховочная сетка перед
рефакторингом ``handle`` (C=12) и ``_stats_step`` (C=22).

GEE/растровые сервисы мокаются на уровне модулей сервисов.
"""
import io
import sys
import tempfile
from datetime import date
from unittest import mock

# Локально нет earthengine-api/rasterio — подменяем до импорта сервисов.
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

S2 = 'agrocosmos.services.satellite_s2_raster'
L8 = 'agrocosmos.services.satellite_landsat_raster'
MODIS = 'agrocosmos.services.satellite_modis_raster'
ZS = 'agrocosmos.services.zonal_stats'

CHUNK = (date(2025, 6, 1), date(2025, 6, 16))
MID_DATE = CHUNK[0] + (CHUNK[1] - CHUNK[0]) / 2

STAT = {
    'mean': 0.6, 'median': 0.61, 'min': 0.2, 'max': 0.9, 'std': 0.1,
    'pixel_count': 100, 'valid_pixel_count': 90,
}

CHUNK_FNS = {'s2': f'{S2}.s2_chunks', 'l8': f'{L8}.landsat_chunks',
             'modis': f'{MODIS}._biweekly_chunks'}
SVC_MODS = {'s2': S2, 'l8': L8, 'modis': MODIS}


def _square(x, y, size=0.1):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class FetchRasterNdviTests(TestCase):
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

    def _run(self, sensor='s2', compute_mock=None, download_mock=None,
             raster_exists=True, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        compute_mock = compute_mock or mock.MagicMock(return_value={})
        download_mock = download_mock or mock.MagicMock(return_value=None)
        svc = SVC_MODS[sensor]
        with tempfile.NamedTemporaryFile(suffix='.tif') as tif:
            raster = tif.name if raster_exists else tif.name + '.nope'
            with mock.patch(CHUNK_FNS[sensor], return_value=[CHUNK]), \
                 mock.patch(f'{svc}._raster_path', return_value=raster), \
                 mock.patch(f'{svc}.download_composite', download_mock), \
                 mock.patch(f'{ZS}.compute_zonal_stats', compute_mock):
                call_command(
                    'fetch_raster_ndvi', sensor=sensor,
                    date_from=CHUNK[0].isoformat(),
                    date_to=CHUNK[1].isoformat(),
                    stdout=out, stderr=err,
                    **kwargs,
                )
        return out.getvalue(), err.getvalue(), compute_mock, download_mock

    def test_requires_region_or_district(self):
        out, err, compute_mock, _ = self._run()
        self.assertIn('Specify --region-id or --district-id', err)
        compute_mock.assert_not_called()

    def test_requires_dates(self):
        out, err = io.StringIO(), io.StringIO()
        call_command('fetch_raster_ndvi', sensor='s2',
                     region_id=self.region.pk, stdout=out, stderr=err)
        self.assertIn('Specify --year or --date-from/--date-to',
                      err.getvalue())

    def test_download_only(self):
        with tempfile.NamedTemporaryFile(suffix='.tif') as tif:
            dl = mock.MagicMock(return_value=tif.name)
            out, _, compute_mock, _ = self._run(
                download_mock=dl, region_id=self.region.pk,
                download_only=True)
        dl.assert_called_once()
        kw = dl.call_args.kwargs
        self.assertEqual(kw['cloud_max'], 30)     # дефолт s2
        self.assertNotIn('harmonize', kw)          # harmonize только для l8
        compute_mock.assert_not_called()
        self.assertIn('1 OK, 0 empty, 0 errors', out)

    def test_download_error_counted(self):
        dl = mock.MagicMock(side_effect=RuntimeError('gee down'))
        out, err, _, _ = self._run(
            download_mock=dl, region_id=self.region.pk, download_only=True)
        self.assertIn('0 OK, 0 empty, 1 errors', out)

    def test_l8_harmonize_flag(self):
        _, _, _, dl = self._run(
            sensor='l8', region_id=self.region.pk, download_only=True)
        self.assertTrue(dl.call_args.kwargs['harmonize'])
        _, _, _, dl = self._run(
            sensor='l8', region_id=self.region.pk, download_only=True,
            no_harmonize=True)
        self.assertFalse(dl.call_args.kwargs['harmonize'])

    def test_modis_no_cloud_max(self):
        _, _, compute_mock, dl = self._run(
            sensor='modis', region_id=self.region.pk, stats_only=True)
        self.assertNotIn('cloud_max', dl.call_args.kwargs if dl.call_args
                         else {})
        # MODIS: дефолтный min_valid = 0.5
        self.assertEqual(compute_mock.call_args.kwargs['min_valid_ratio'],
                         0.5)

    def test_stats_saves_records(self):
        results = {fl.pk: dict(STAT) for fl in self.farmlands}
        out, _, compute_mock, _ = self._run(
            compute_mock=mock.MagicMock(return_value=results),
            region_id=self.region.pk, stats_only=True)
        self.assertEqual(compute_mock.call_args.kwargs['min_valid_ratio'],
                         0.7)
        self.assertEqual(VegetationIndex.objects.count(), 2)
        scene = SatelliteScene.objects.get()
        self.assertEqual(
            scene.scene_id, f's2_{MID_DATE.isoformat()}_{self.district.pk}')
        self.assertEqual(scene.satellite, 'sentinel2')
        vi = VegetationIndex.objects.first()
        self.assertEqual(vi.acquired_date, MID_DATE)
        self.assertIn('2 records saved', out)

    def test_rerun_upserts_without_duplicates(self):
        results = {fl.pk: dict(STAT) for fl in self.farmlands}
        self._run(compute_mock=mock.MagicMock(return_value=results),
                  region_id=self.region.pk, stats_only=True)
        updated = {fl.pk: dict(STAT, mean=0.8) for fl in self.farmlands}
        self._run(compute_mock=mock.MagicMock(return_value=updated),
                  region_id=self.region.pk, stats_only=True)
        self.assertEqual(VegetationIndex.objects.count(), 2)
        self.assertAlmostEqual(VegetationIndex.objects.first().mean, 0.8)

    def test_l8_scene_prefix(self):
        results = {self.farmlands[0].pk: dict(STAT)}
        self._run(sensor='l8',
                  compute_mock=mock.MagicMock(return_value=results),
                  region_id=self.region.pk, stats_only=True)
        scene = SatelliteScene.objects.get()
        self.assertTrue(scene.scene_id.startswith('landsat_'))
        self.assertEqual(scene.satellite, 'landsat8')

    def test_no_farmlands(self):
        empty = Region.objects.create(
            name='Пусто', code='r2', geom=_square(40, 50, 1.0))
        out, err, compute_mock, _ = self._run(
            region_id=empty.pk, stats_only=True)
        compute_mock.assert_not_called()
        self.assertIn('No farmlands found', err)

    def test_missing_raster_skips(self):
        out, _, compute_mock, _ = self._run(
            raster_exists=False, region_id=self.region.pk, stats_only=True)
        compute_mock.assert_not_called()
        self.assertIn('no raster, skip', out)

    def test_compute_error_counted(self):
        out, err, _, _ = self._run(
            compute_mock=mock.MagicMock(side_effect=RuntimeError('boom')),
            region_id=self.region.pk, stats_only=True)
        self.assertIn('ERROR: boom', err)
        self.assertIn('1 errors', out)

    def test_district_scope(self):
        _, _, compute_mock, _ = self._run(
            district_id=self.district.pk, stats_only=True)
        fl_geoms = compute_mock.call_args.args[1]
        self.assertEqual(len(fl_geoms), 2)
