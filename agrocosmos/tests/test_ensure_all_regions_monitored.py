"""Тесты ``ensure_all_regions_monitored`` — идемпотентность зачисления
регионов в MODIS-мониторинг и изоляция от растровых/районных задач.
"""
import io
from datetime import date

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import District, Farmland, MonitoringTask, Region

YEAR = date.today().year


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class EnsureAllRegionsMonitoredTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50, 0.2))
        Farmland.objects.create(
            region=cls.region, district=cls.district,
            area_ha=10, geom=_square(30, 50, 0.1))

    def _run(self, **kwargs):
        out = io.StringIO()
        call_command('ensure_all_regions_monitored', stdout=out, **kwargs)
        return out.getvalue()

    def _modis_tasks(self):
        return MonitoringTask.objects.filter(
            region=self.region, year=YEAR,
            task_type=MonitoringTask.TaskType.MODIS,
            district__isnull=True,
        )

    def test_creates_region_level_modis_task(self):
        out = self._run()
        self.assertEqual(self._modis_tasks().count(), 1)
        self.assertIn('created=1', out)

    def test_idempotent(self):
        self._run()
        out = self._run()
        self.assertEqual(self._modis_tasks().count(), 1)
        self.assertIn('already_active=1', out)

    def test_reactivates_paused(self):
        task = MonitoringTask.objects.create(
            region=self.region, year=YEAR, status='paused')
        out = self._run()
        task.refresh_from_db()
        self.assertEqual(task.status, 'active')
        self.assertIn('reactivated=1', out)

    def test_raster_task_does_not_mask_enrolment(self):
        """Регрессия: регион с одной лишь растровой задачей считался
        зачисленным и никогда не получал MODIS-задачу."""
        MonitoringTask.objects.create(
            region=self.region, year=YEAR, status='active',
            task_type=MonitoringTask.TaskType.RASTER,
        )
        self._run()
        self.assertEqual(self._modis_tasks().count(), 1)

    def test_district_task_does_not_mask_enrolment(self):
        MonitoringTask.objects.create(
            region=self.region, district=self.district, year=YEAR,
            status='active',
        )
        self._run()
        self.assertEqual(self._modis_tasks().count(), 1)

    def test_region_without_farmlands_not_enrolled(self):
        """Субъекты без вектора угодий не зачисляются и не воскрешаются.

        Команда гоняется на каждом деплое и реактивирует paused-задачи,
        поэтому без этого фильтра пауза Москвы/СПб/Севастополя
        слетала бы при ближайшем деплое.
        """
        empty = Region.objects.create(
            name='Мегаполис', code='r2', geom=_square(40, 55))
        paused = MonitoringTask.objects.create(
            region=empty, year=YEAR, status='paused')
        out = self._run()
        self.assertFalse(
            MonitoringTask.objects.filter(region=empty, status='active')
            .exists()
        )
        paused.refresh_from_db()
        self.assertEqual(paused.status, 'paused')
        self.assertIn('skipped_no_farmlands=1', out)
