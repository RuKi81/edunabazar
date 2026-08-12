"""Тесты команды ``ndvi_postprocess`` — фокус на скоупе (--district-id vs
--region-id) и на том, что сглаживание пишется только в выбранный скоуп.
"""
import io
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


def _series(farmland, dates_means):
    """Create fused VI rows (one scene per date) for a farmland."""
    for d, m in dates_means:
        scene = SatelliteScene.objects.create(
            scene_id=f'hls_{farmland.pk}_{d.isoformat()}',
            satellite='hls_fused', acquired_date=d,
            cloud_cover=0, processed=True,
        )
        VegetationIndex.objects.create(
            farmland=farmland, scene=scene, index_type='ndvi',
            acquired_date=d, mean=m, median=m,
            min_val=0, max_val=1, std_val=0,
            pixel_count=100, valid_pixel_count=100, is_outlier=False,
        )


class PostprocessScopeTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(name='Р', code='r1', geom=_mpoly())
        cls.d1 = District.objects.create(
            region=cls.region, name='Район1', code='d1', geom=_mpoly(),
        )
        cls.d2 = District.objects.create(
            region=cls.region, name='Район2', code='d2', geom=_mpoly(),
        )
        cls.fl1 = Farmland.objects.create(
            district=cls.d1, crop_type='arable', area_ha=10, geom=_mpoly(),
        )
        cls.fl2 = Farmland.objects.create(
            district=cls.d2, crop_type='arable', area_ha=10, geom=_mpoly(),
        )
        pts = [
            (date(2025, 6, 1), 0.5),
            (date(2025, 6, 6), 0.55),
            (date(2025, 6, 11), 0.6),
            (date(2025, 6, 16), 0.58),
        ]
        _series(cls.fl1, pts)
        _series(cls.fl2, pts)

    def _run(self, *args):
        out, err = io.StringIO(), io.StringIO()
        call_command('ndvi_postprocess', '--source', 'fused', *args,
                     stdout=out, stderr=err)
        return out.getvalue(), err.getvalue()

    def test_requires_scope(self):
        _, err = self._run()
        self.assertIn('Specify --region-id or --district-id', err)

    def test_district_scope_only_touches_that_district(self):
        self._run('--district-id', str(self.d1.pk))
        # fl1 (district 1) got smoothed
        self.assertTrue(
            VegetationIndex.objects.filter(
                farmland=self.fl1, mean_smooth__isnull=False,
            ).exists()
        )
        # fl2 (district 2) left untouched
        self.assertFalse(
            VegetationIndex.objects.filter(
                farmland=self.fl2, mean_smooth__isnull=False,
            ).exists()
        )

    def test_region_scope_touches_all_districts(self):
        self._run('--region-id', str(self.region.pk))
        for fl in (self.fl1, self.fl2):
            self.assertTrue(
                VegetationIndex.objects.filter(
                    farmland=fl, mean_smooth__isnull=False,
                ).exists()
            )
