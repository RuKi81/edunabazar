"""
Тесты resumable zonal stats в команде ``modis_ndvi``.

Дорогая часть пайплайна — compute_zonal_stats (60-70% времени региона).
Команда должна пропускать композиты, уже покрытые в VegetationIndex
(>=99% подготовленных угодий), чтобы перезапуск упавшего региона доделывал
хвост, а не пересчитывал всё с нуля. Флаг --recompute-stats обходит skip.
"""
import tempfile
from datetime import date
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)

# Один 16-дневный чанк; mid_date считается так же, как в команде.
CHUNK = (date(2025, 6, 1), date(2025, 6, 16))
MID_DATE = CHUNK[0] + (CHUNK[1] - CHUNK[0]) / 2


def _square(x, y, size=0.1):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class ModisResumableStatsTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Тестовый регион', code='test-region', geom=_square(30, 50, 2.0),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тестовый район', geom=_square(30, 50, 2.0),
        )
        cls.farmlands = [
            Farmland.objects.create(
                region=cls.region, district=cls.district,
                geom=_square(30.1 + i * 0.3, 50.1),
            )
            for i in range(2)
        ]

    def _cover_composite(self, farmlands=None):
        """Создать VegetationIndex-покрытие чанка для заданных угодий."""
        scene = SatelliteScene.objects.create(
            satellite='modis_terra',
            scene_id=f'modis_{MID_DATE.isoformat()}_{self.district.pk}',
            acquired_date=MID_DATE,
            processed=True,
        )
        for fl in (farmlands if farmlands is not None else self.farmlands):
            VegetationIndex.objects.create(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=MID_DATE, mean=0.5,
            )

    def _run_command(self, compute_mock, **kwargs):
        """Запустить modis_ndvi (stats-only) с замоканными GEE/растрами."""
        with tempfile.NamedTemporaryFile(suffix='.tif') as tif:
            with mock.patch(
                'agrocosmos.services.satellite_modis_raster._biweekly_chunks',
                return_value=[CHUNK],
            ), mock.patch(
                'agrocosmos.services.satellite_modis_raster._raster_path',
                return_value=tif.name,
            ), mock.patch(
                'agrocosmos.services.satellite_modis_raster.compute_zonal_stats',
                compute_mock,
            ):
                call_command(
                    'modis_ndvi',
                    region_id=self.region.pk,
                    date_from=CHUNK[0].isoformat(),
                    date_to=CHUNK[1].isoformat(),
                    stats_only=True,
                    skip_status_refresh=True,
                    **kwargs,
                )

    def test_covered_composite_is_skipped(self):
        """Композит, полностью покрытый в БД, не пересчитывается."""
        self._cover_composite()
        compute_mock = mock.MagicMock(return_value={})
        self._run_command(compute_mock)
        compute_mock.assert_not_called()

    def test_uncovered_composite_is_computed(self):
        """Без покрытия в БД зональная статистика считается."""
        compute_mock = mock.MagicMock(return_value={})
        self._run_command(compute_mock)
        compute_mock.assert_called_once()

    def test_partial_coverage_is_recomputed(self):
        """Покрытие 50% (< порога 99%) не считается полным — пересчёт."""
        self._cover_composite(farmlands=self.farmlands[:1])
        compute_mock = mock.MagicMock(return_value={})
        self._run_command(compute_mock)
        compute_mock.assert_called_once()

    def test_recompute_flag_bypasses_skip(self):
        """--recompute-stats форсирует пересчёт даже при полном покрытии."""
        self._cover_composite()
        compute_mock = mock.MagicMock(return_value={})
        self._run_command(compute_mock, recompute_stats=True)
        compute_mock.assert_called_once()
