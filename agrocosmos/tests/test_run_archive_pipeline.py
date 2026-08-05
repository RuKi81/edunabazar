"""Тесты оркестратора ``run_archive_pipeline`` — страховочная сетка перед
рефакторингом ``handle`` (C=18). Стадии мокаются; проверяем статусы
PipelineRun, маршрутизацию стадий и парсинг ``Records saved:``.
"""
import io
from datetime import date
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import PipelineRun, Region

MOD = 'agrocosmos.management.commands.run_archive_pipeline'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class RunArchivePipelineTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))

    def setUp(self):
        self.run = PipelineRun.objects.create(
            task_type=PipelineRun.TaskType.RASTER_NDVI,
            status=PipelineRun.Status.QUEUED,
        )

    def _run(self, inner=None, region_id=None, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{MOD}.call_command', side_effect=inner) as mock_cc:
            call_command(
                'run_archive_pipeline',
                run_id=self.run.pk,
                region_id=region_id or self.region.pk,
                stdout=out, stderr=err,
                **kwargs,
            )
        return out.getvalue(), err.getvalue(), mock_cc

    def test_invalid_year_range(self):
        _, err, mock_cc = self._run(year_from=2024, year_to=2020)
        self.assertIn('--year-to must be >= --year-from', err)
        mock_cc.assert_not_called()
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.FAILED)

    def test_unknown_region_marks_failed(self):
        self._run(region_id=999999)
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.FAILED)
        self.assertIsNotNone(self.run.finished_at)

    def test_happy_path(self):
        def inner(name, **kw):
            if name == 'modis_ndvi':
                kw['stdout'].write('  Records saved: 500\n')

        _, _, mock_cc = self._run(
            inner=inner, year_from=2020, year_to=2022)
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.COMPLETED)
        self.assertIsNotNone(self.run.pid)
        self.assertIsNotNone(self.run.finished_at)
        self.assertEqual(self.run.records_count, 500)
        self.assertIn('Archive pipeline completed', self.run.log)

        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names, ['modis_ndvi', 'calc_ndvi_baseline'])
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['date_from'], '2020-01-01')
        self.assertEqual(kw['date_to'], '2022-12-31')
        self.assertEqual(kw['min_valid_ratio'], 0.5)

    def test_skip_baseline(self):
        _, _, mock_cc = self._run(
            year_from=2020, year_to=2021, skip_baseline=True)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names, ['modis_ndvi'])

    def test_default_year_to_is_last_year(self):
        _, _, mock_cc = self._run(year_from=2020)
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['date_to'], f'{date.today().year - 1}-12-31')

    def test_stage_failure_marks_failed_and_reraises(self):
        with self.assertRaises(RuntimeError):
            self._run(inner=RuntimeError('boom'),
                      year_from=2020, year_to=2021)
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.FAILED)
        self.assertIsNotNone(self.run.finished_at)
        self.assertIn('boom', self.run.log)
