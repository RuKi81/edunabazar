"""Тесты команды ``run_baseline_ndvi`` — страховочная сетка перед
рефакторингом ``handle`` (C=15). Мониторинг не запускаем
(``--no-monitor``/``--dry-run``); проверяем план (skip/retry/force),
постановку в очередь и валидацию.
"""
import io
import tempfile
from datetime import date
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.management.commands.run_baseline_ndvi import Command
from agrocosmos.models import PipelineRun, Region

PAST = date.today().year - 1


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class FmtDurTests(TestCase):
    def test_fmt_dur(self):
        self.assertEqual(Command._fmt_dur(-5), '0m')
        self.assertEqual(Command._fmt_dur(90), '1m')
        self.assertEqual(Command._fmt_dur(3700), '1h01m')
        self.assertEqual(Command._fmt_dur(90000), '1d01h00m')


class RunBaselineNdviTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.regions = [
            Region.objects.create(
                name=f'Регион {i}', code=f'r{i}', geom=_square(30 + i, 50))
            for i in range(3)
        ]

    def _run(self, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        tmp = tempfile.mkdtemp()
        with self.settings(BASE_DIR=tmp):
            call_command('run_baseline_ndvi', stdout=out, stderr=err,
                         year_from=2020, year_to=PAST, **kwargs)
        return out.getvalue(), err.getvalue()

    def _existing(self, region, status):
        return PipelineRun.objects.create(
            task_type=PipelineRun.TaskType.ARCHIVE_NDVI,
            status=status, region=region, year=PAST,
        )

    def test_bad_region_spec_aborts(self):
        _, err = self._run(regions='12,abc')
        self.assertIn("Bad region id: 'abc'", err)
        self.assertIn('No regions resolved', err)
        self.assertEqual(PipelineRun.objects.count(), 0)

    def test_invalid_year_range(self):
        out, err = io.StringIO(), io.StringIO()
        call_command('run_baseline_ndvi', regions='all',
                     year_from=PAST, year_to=2020,
                     stdout=out, stderr=err)
        self.assertIn('--year-to must be >= --year-from', err.getvalue())

    def test_enqueues_all_regions(self):
        out, _ = self._run(regions='all', no_monitor=True)
        self.assertEqual(
            PipelineRun.objects.filter(
                status=PipelineRun.Status.QUEUED).count(), 3)
        run = PipelineRun.objects.first()
        self.assertEqual(run.task_type, PipelineRun.TaskType.ARCHIVE_NDVI)
        self.assertEqual(run.year, PAST)
        self.assertEqual(run.launch_args['year_from'], 2020)
        self.assertEqual(run.launch_args['min_valid'], 0.5)
        self.assertFalse(run.launch_args['skip_baseline'])
        self.assertIn(f'run_{run.pk}.log', run.log_file)
        self.assertIn('To enqueue      : 3', out)
        self.assertIn('+ queued', out)

    def test_region_id_filter(self):
        self._run(regions=str(self.regions[0].pk), no_monitor=True)
        self.assertEqual(PipelineRun.objects.count(), 1)
        self.assertEqual(PipelineRun.objects.get().region, self.regions[0])

    def test_completed_skipped_failed_requeued_running_kept(self):
        self._existing(self.regions[0], PipelineRun.Status.COMPLETED)
        self._existing(self.regions[1], PipelineRun.Status.FAILED)
        self._existing(self.regions[2], PipelineRun.Status.RUNNING)
        out, _ = self._run(regions='all', no_monitor=True)
        self.assertIn('To enqueue      : 1', out)
        self.assertIn('Skipped         : 1', out)
        self.assertIn('Already in queue: 1', out)
        self.assertIn('already completed', out)
        # Новый run только для региона с failed
        new = PipelineRun.objects.filter(status=PipelineRun.Status.QUEUED)
        self.assertEqual([r.region for r in new], [self.regions[1]])

    def test_force_requeues_completed(self):
        self._existing(self.regions[0], PipelineRun.Status.COMPLETED)
        out, _ = self._run(regions='all', no_monitor=True, force=True)
        self.assertIn('To enqueue      : 3', out)

    def test_dry_run_writes_nothing(self):
        out, _ = self._run(regions='all', dry_run=True)
        self.assertIn('--dry-run: nothing was written.', out)
        self.assertEqual(PipelineRun.objects.count(), 0)

    def test_nothing_to_monitor(self):
        self._existing(self.regions[0], PipelineRun.Status.COMPLETED)
        out, _ = self._run(regions=str(self.regions[0].pk), no_monitor=True)
        self.assertIn('Nothing to monitor', out)

    def test_monitor_called_by_default(self):
        with mock.patch.object(Command, '_monitor') as monitor:
            out, _ = self._run(regions='all')
        monitor.assert_called_once()
        run_ids = monitor.call_args.args[0]
        self.assertEqual(len(run_ids), 3)
