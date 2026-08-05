"""Тесты команды ``compute_fused_ndvi`` — страховочная сетка перед
рефакторингом ``handle`` (C=12) и ``_persist`` (C=11).

Правило фьюжна (чистый ``_fuse_one``) плюс сквозной прогон по БД.
"""
import io
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import SimpleTestCase, TestCase

from agrocosmos.management.commands.compute_fused_ndvi import Command
from agrocosmos.models import (
    District, Farmland, PipelineRun, Region, SatelliteScene, VegetationIndex,
)


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


class FuseOneTests(SimpleTestCase):

    def test_weighted_mean_pairing(self):
        s2 = [(date(2025, 6, 10), 0.6, 100)]
        landsat = [(date(2025, 6, 12), 0.8, 50)]
        fused = Command._fuse_one(s2, landsat)
        self.assertEqual(len(fused), 1)
        d, mean, n = fused[0]
        self.assertEqual(d, date(2025, 6, 10))
        self.assertAlmostEqual(mean, (0.6 * 100 + 0.8 * 50) / 150, places=4)
        self.assertEqual(n, 150)

    def test_landsat_outside_window_is_orphan(self):
        s2 = [(date(2025, 6, 10), 0.6, 100)]
        landsat = [(date(2025, 6, 25), 0.8, 50)]
        fused = Command._fuse_one(s2, landsat)
        self.assertEqual(len(fused), 2)
        self.assertEqual(fused[0], (date(2025, 6, 10), 0.6, 100))
        self.assertEqual(fused[1], (date(2025, 6, 25), 0.8, 50))

    def test_nearest_landsat_wins(self):
        s2 = [(date(2025, 6, 10), 0.6, 100)]
        landsat = [(date(2025, 6, 17), 0.9, 50), (date(2025, 6, 11), 0.7, 50)]
        fused = Command._fuse_one(s2, landsat)
        paired = fused[0]
        self.assertAlmostEqual(
            paired[1], (0.6 * 100 + 0.7 * 50) / 150, places=4,
        )
        # дальний Landsat остаётся сиротой
        self.assertEqual(len(fused), 2)

    def test_sorted_output(self):
        s2 = [(date(2025, 7, 1), 0.5, 10)]
        landsat = [(date(2025, 5, 1), 0.4, 10)]
        fused = Command._fuse_one(s2, landsat)
        self.assertEqual([f[0] for f in fused],
                         [date(2025, 5, 1), date(2025, 7, 1)])


class FusedCommandTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(name='Р', code='r1', geom=_mpoly())
        cls.district = District.objects.create(
            region=cls.region, name='Район', code='d1', geom=_mpoly(),
        )
        cls.fl = Farmland.objects.create(
            district=cls.district, crop_type='arable', area_ha=10, geom=_mpoly(),
        )
        s2_scene = SatelliteScene.objects.create(
            scene_id='s2_1', satellite='sentinel2',
            acquired_date=date(2025, 6, 10), cloud_cover=0, processed=True,
        )
        l8_scene = SatelliteScene.objects.create(
            scene_id='l8_1', satellite='landsat8',
            acquired_date=date(2025, 6, 12), cloud_cover=0, processed=True,
        )
        for scene, mean, n in ((s2_scene, 0.6, 100), (l8_scene, 0.8, 50)):
            VegetationIndex.objects.create(
                farmland=cls.fl, scene=scene, index_type='ndvi',
                acquired_date=scene.acquired_date, mean=mean, median=mean,
                min_val=0, max_val=1, std_val=0, pixel_count=n,
                valid_pixel_count=n, is_outlier=False,
            )

    def _run(self, *args):
        out, err = io.StringIO(), io.StringIO()
        call_command('compute_fused_ndvi', '--year', '2025', *args,
                     stdout=out, stderr=err)
        return out.getvalue(), err.getvalue()

    def test_requires_scope(self):
        out, err = self._run()
        self.assertIn('required', err)

    def test_fuses_and_persists(self):
        out, _ = self._run('--region-id', str(self.region.pk))
        fused = VegetationIndex.objects.filter(scene__satellite='hls_fused')
        self.assertEqual(fused.count(), 1)
        vi = fused.get()
        self.assertAlmostEqual(
            float(vi.mean), (0.6 * 100 + 0.8 * 50) / 150, places=4,
        )
        self.assertEqual(vi.valid_pixel_count, 150)
        self.assertTrue(vi.scene.scene_id.startswith('hls_'))
        run = PipelineRun.objects.get()
        self.assertEqual(run.status, PipelineRun.Status.COMPLETED)
        self.assertEqual(run.records_count, 1)

    def test_dry_run_writes_nothing(self):
        out, _ = self._run('--region-id', str(self.region.pk), '--dry-run')
        self.assertIn('Dry-run', out)
        self.assertFalse(
            VegetationIndex.objects.filter(scene__satellite='hls_fused').exists()
        )
        self.assertFalse(PipelineRun.objects.exists())

    def test_overwrite_wipes_existing(self):
        self._run('--region-id', str(self.region.pk))
        self._run('--region-id', str(self.region.pk), '--overwrite')
        fused = VegetationIndex.objects.filter(scene__satellite='hls_fused')
        self.assertEqual(fused.count(), 1)

    def test_district_scope(self):
        out, _ = self._run('--district-id', str(self.district.pk))
        self.assertEqual(
            VegetationIndex.objects.filter(scene__satellite='hls_fused').count(), 1,
        )

    def test_unknown_region(self):
        out, err = self._run('--region-id', '999999')
        self.assertIn('not found', err)

    def test_empty_scope_completes(self):
        VegetationIndex.objects.all().delete()
        out, _ = self._run('--region-id', str(self.region.pk))
        self.assertIn('No S2/Landsat observations', out)
        run = PipelineRun.objects.get()
        self.assertEqual(run.status, PipelineRun.Status.COMPLETED)
