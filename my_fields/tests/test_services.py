"""Юнит-тесты сервисов ``my_fields`` (геометрия, резолв географии)."""
from django.contrib.gis.geos import MultiPolygon, Point, Polygon
from django.test import TestCase

from agrocosmos.models import District, Region
from my_fields.services.geometry import (
    compute_area_ha, ensure_multipolygon, resolve_region_district,
)


def _square(x0, y0, size):
    return Polygon((
        (x0, y0), (x0 + size, y0), (x0 + size, y0 + size), (x0, y0 + size), (x0, y0),
    ))


class EnsureMultipolygonTests(TestCase):
    def test_polygon_is_wrapped(self):
        result = ensure_multipolygon(_square(34, 45, 0.01))
        self.assertIsInstance(result, MultiPolygon)
        self.assertEqual(len(result), 1)

    def test_multipolygon_passthrough(self):
        mp = MultiPolygon(_square(34, 45, 0.01))
        self.assertIs(ensure_multipolygon(mp), mp)

    def test_point_raises(self):
        with self.assertRaises(ValueError):
            ensure_multipolygon(Point(34, 45))


class ComputeAreaTests(TestCase):
    def test_square_at_mid_latitude(self):
        # 0.01°×0.01° на 45° с.ш.: ~1.11 км × ~0.79 км ≈ 87 га.
        # Допуск широкий — важен порядок величины и проекция, не GEOS.
        area = compute_area_ha(_square(34, 45, 0.01))
        self.assertGreater(area, 60)
        self.assertLess(area, 120)

    def test_empty_geometry_is_zero(self):
        self.assertEqual(compute_area_ha(Polygon()), 0.0)

    def test_input_not_mutated(self):
        g = _square(34, 45, 0.01)
        compute_area_ha(g)
        self.assertEqual(g.srid or 4326, 4326)


class ResolveRegionDistrictTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=MultiPolygon(_square(34, 45, 1.0)),
        )
        # Район покрывает только западную половину региона.
        cls.district = District.objects.create(
            region=cls.region, name='Район',
            geom=MultiPolygon(_square(34, 45, 0.5)),
        )

    def test_inside_district(self):
        rid, did = resolve_region_district(_square(34.1, 45.1, 0.01))
        self.assertEqual((rid, did), (self.region.pk, self.district.pk))

    def test_region_fallback_when_no_district(self):
        rid, did = resolve_region_district(_square(34.8, 45.8, 0.01))
        self.assertEqual((rid, did), (self.region.pk, None))

    def test_outside_everything(self):
        self.assertEqual(
            resolve_region_district(_square(50, 60, 0.01)), (None, None),
        )
