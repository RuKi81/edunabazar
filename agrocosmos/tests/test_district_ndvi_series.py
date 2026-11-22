"""Тесты предагрегата ``agro_district_ndvi_series``: пересчёт по одному
региону / одной стране не трогает строки остальных. SQL гоняется настоящий.
"""
import io
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from agrocosmos.models import (
    District, DistrictNdviSeries, Farmland, Region, SatelliteScene,
    VegetationIndex,
)
from agrocosmos.services import district_ndvi_series

D_SPRING = date(2026, 4, 7)
D_AUTUMN = date(2026, 10, 7)
YEAR = (date(2026, 1, 1), date(2026, 12, 31))


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class DistrictNdviSeriesScopeTests(TestCase):
    """РФ и две области Казахстана, у каждой по району с одним угодьем
    (100 га) и NDVI на две даты."""

    @classmethod
    def setUpTestData(cls):
        cls.scenes = {
            d: SatelliteScene.objects.create(
                satellite='modis_terra', scene_id=f'modis_{d}', acquired_date=d,
            ) for d in (D_SPRING, D_AUTUMN)
        }

        def region(name, code, country, x, ndvi):
            r = Region.objects.create(
                name=name, code=code, country=country, geom=_square(x, 50))
            d = District.objects.create(
                region=r, name=f'Район {name}', geom=_square(x, 50))
            f = Farmland.objects.create(
                region=r, district=d, crop_type='arable', area_ha=100,
                geom=_square(x, 50, 0.1))
            for acq_date, scene in cls.scenes.items():
                VegetationIndex.objects.create(
                    farmland=f, scene=scene, index_type='ndvi',
                    acquired_date=acq_date, mean=ndvi)
            return r, d

        cls.ru, cls.d_ru = region('РФ', 'ru-1', 'RU', 30, 0.6)
        cls.kz1, cls.d_kz1 = region('КЗ-1', 'KZ-1', 'KZ', 70, 0.4)
        cls.kz2, cls.d_kz2 = region('КЗ-2', 'KZ-2', 'KZ', 75, 0.2)

    def _rows(self):
        """{(район, дата): среднее NDVI} по всей таблице."""
        return {
            (s.district_id, s.acquired_date):
                round(s.sum_ndvi_area / s.sum_area, 3)
            for s in DistrictNdviSeries.objects.all()
        }

    def _stale(self, district, acq_date=D_SPRING):
        """Заведомо устаревшая строка: пересчёт района её заменит."""
        return DistrictNdviSeries.objects.create(
            district=district, acquired_date=acq_date, crop_type='arable',
            source='modis', sum_ndvi_area=99.0, sum_area=100.0, obs_count=1,
        )

    def test_unscoped_refresh_covers_every_region(self):
        res = district_ndvi_series.refresh_range(*YEAR)
        self.assertEqual(res['inserted'], 6)
        self.assertEqual(self._rows(), {
            (self.d_ru.pk, D_SPRING): 0.6, (self.d_ru.pk, D_AUTUMN): 0.6,
            (self.d_kz1.pk, D_SPRING): 0.4, (self.d_kz1.pk, D_AUTUMN): 0.4,
            (self.d_kz2.pk, D_SPRING): 0.2, (self.d_kz2.pk, D_AUTUMN): 0.2,
        })

    def test_region_scope_leaves_other_regions_untouched(self):
        for d in (self.d_ru, self.d_kz1, self.d_kz2):
            self._stale(d)
        res = district_ndvi_series.refresh_range(
            *YEAR, region_id=self.kz1.pk)
        self.assertEqual((res['deleted'], res['inserted']), (1, 2))
        self.assertEqual(self._rows(), {
            (self.d_kz1.pk, D_SPRING): 0.4, (self.d_kz1.pk, D_AUTUMN): 0.4,
            # Чужие строки не удалены и не пересчитаны.
            (self.d_ru.pk, D_SPRING): 0.99,
            (self.d_kz2.pk, D_SPRING): 0.99,
        })

    def test_country_scope(self):
        for d in (self.d_ru, self.d_kz1, self.d_kz2):
            self._stale(d)
        res = district_ndvi_series.refresh_range(*YEAR, country='KZ')
        self.assertEqual((res['deleted'], res['inserted']), (2, 4))
        rows = self._rows()
        self.assertEqual(rows[(self.d_ru.pk, D_SPRING)], 0.99)
        self.assertNotIn((self.d_ru.pk, D_AUTUMN), rows)
        self.assertEqual(rows[(self.d_kz1.pk, D_SPRING)], 0.4)
        self.assertEqual(rows[(self.d_kz2.pk, D_AUTUMN)], 0.2)

    def test_scope_respects_date_range(self):
        self._stale(self.d_kz1, D_AUTUMN)
        district_ndvi_series.refresh_range(
            date(2026, 1, 1), date(2026, 6, 30), region_id=self.kz1.pk)
        self.assertEqual(self._rows(), {
            (self.d_kz1.pk, D_SPRING): 0.4,
            (self.d_kz1.pk, D_AUTUMN): 0.99,   # вне окна — не тронута
        })

    def test_command_country_and_region(self):
        out = io.StringIO()
        call_command('recompute_district_ndvi_series',
                     range='2026-01-01:2026-12-31', country='kz', stdout=out)
        self.assertIn('inserted=4', out.getvalue())
        self.assertEqual(
            set(DistrictNdviSeries.objects.values_list('district_id', flat=True)),
            {self.d_kz1.pk, self.d_kz2.pk},
        )
        DistrictNdviSeries.objects.all().delete()
        call_command('recompute_district_ndvi_series',
                     range='2026-01-01:2026-12-31', region_id=self.ru.pk,
                     stdout=out)
        self.assertEqual(
            set(DistrictNdviSeries.objects.values_list('district_id', flat=True)),
            {self.d_ru.pk},
        )

    def test_command_rejects_bad_scope(self):
        for kwargs in (
            {'country': 'XX'},
            {'country': 'KZ', 'region_id': self.kz1.pk},
            {'country': 'KZ', 'rebuild': True},
            {'region_id': self.kz1.pk, 'rebuild': True},
        ):
            with self.assertRaises(CommandError, msg=kwargs):
                call_command('recompute_district_ndvi_series',
                             range='2026-01-01:2026-12-31',
                             stdout=io.StringIO(), **kwargs)
        self.assertFalse(DistrictNdviSeries.objects.exists())
