"""Тесты команды ``check_monitoring`` — страховочная сетка перед
рефакторингом ``_process_task`` (C=20). Grid-логика ``_next_aligned_period``
уже покрыта в ``test_monitoring_alignment.py``; здесь — оркестрация:
запуск пайплайна, продвижение last_date_to, кап периодов, батчевый refresh.
"""
import io
from datetime import date, timedelta
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.management.commands.check_monitoring import (
    _next_aligned_period,
)
from agrocosmos.models import MonitoringTask, Region

MOD = 'agrocosmos.management.commands.check_monitoring'

# Полностью прошедший год: все периоды заведомо доступны.
PAST_YEAR = date.today().year - 2


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _saved_inner(n=42):
    """Инерция modis_ndvi: пишет 'Records saved: N' в stdout."""
    def inner(name, **kw):
        kw['stdout'].write(f'  Records saved: {n}\n  Done in 0h00m01s\n')
    return inner


class CheckMonitoringTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))

    def _task(self, **overrides):
        kwargs = dict(region=self.region, year=PAST_YEAR, status='active')
        kwargs.update(overrides)
        return MonitoringTask.objects.create(**kwargs)

    def _run(self, inner=None, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{MOD}.call_command', side_effect=inner) as mock_cc:
            call_command('check_monitoring', stdout=out, stderr=err,
                         **kwargs)
        return out.getvalue(), err.getvalue(), mock_cc

    def test_no_active_tasks(self):
        self._task(status='paused')
        out, _, mock_cc = self._run()
        self.assertIn('No active monitoring tasks', out)
        mock_cc.assert_not_called()

    def test_completed_year_marks_task(self):
        task = self._task(last_date_to=date(PAST_YEAR, 12, 31))
        out, _, mock_cc = self._run()
        mock_cc.assert_not_called()
        task.refresh_from_db()
        self.assertEqual(task.status, 'completed')
        self.assertIn('Year complete', out)

    def test_future_period_stops(self):
        task = self._task(year=date.today().year + 1)
        out, _, mock_cc = self._run()
        mock_cc.assert_not_called()
        self.assertIn('is in the future', out)

    def test_processes_periods_with_cap(self):
        task = self._task()
        out, _, mock_cc = self._run(inner=_saved_inner())
        # Дефолтный кап: 2 периода за запуск + батчевый refresh
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names.count('modis_ndvi'), 2)
        self.assertEqual(names.count('recompute_district_ndvi_status'), 1)
        kw = mock_cc.call_args_list[0].kwargs
        self.assertTrue(kw['skip_status_refresh'])
        self.assertEqual(kw['region_id'], self.region.pk)
        self.assertEqual(kw['date_from'], f'{PAST_YEAR}-01-01')
        self.assertEqual(kw['date_to'], f'{PAST_YEAR}-01-16')
        task.refresh_from_db()
        self.assertEqual(task.last_date_to, date(PAST_YEAR, 2, 1))
        self.assertEqual(task.records_total, 84)
        self.assertIn(f'{PAST_YEAR}-01-01..{PAST_YEAR}-01-16', task.log)
        self.assertIn('reached --max-periods-per-task=2', out)

    def test_max_periods_option(self):
        self._task()
        _, _, mock_cc = self._run(inner=_saved_inner(),
                                  max_periods_per_task=3)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names.count('modis_ndvi'), 3)

    def test_zero_records_stops_without_advance(self):
        task = self._task()
        out, _, mock_cc = self._run(inner=_saved_inner(0))
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names.count('modis_ndvi'), 1)
        # Refresh не запускается: ни один период не продвинулся
        self.assertNotIn('recompute_district_ndvi_status', names)
        task.refresh_from_db()
        self.assertIsNone(task.last_date_to)
        self.assertIn('no data yet, stop', out)
        self.assertIn('skipping status refresh', out)

    def test_pipeline_error_logged_and_stops(self):
        task = self._task()
        out, err, mock_cc = self._run(inner=RuntimeError('gee down'))
        self.assertIn('ERROR: gee down', err)
        task.refresh_from_db()
        self.assertIn('ERROR: gee down', task.log)
        self.assertIsNone(task.last_date_to)
        self.assertIsNotNone(task.last_check)

    def test_dry_run_no_calls_no_save(self):
        task = self._task()
        out, _, mock_cc = self._run(inner=_saved_inner(), dry_run=True)
        mock_cc.assert_not_called()
        self.assertIn('DRY RUN', out)
        task.refresh_from_db()
        self.assertIsNone(task.last_date_to)   # превью не сохраняется

    def test_force_overrides_availability_lag(self):
        # Период, покрывающий сегодня: данные ещё недоступны (lag 7 дней)
        year = date.today().year
        chunk_from, chunk_to = _next_aligned_period(None, year)
        while chunk_to < date.today():
            chunk_from, chunk_to = _next_aligned_period(chunk_to, year)
        prev_end = chunk_from - timedelta(days=1) \
            if chunk_from != date(year, 1, 1) else None
        task = self._task(year=year, last_date_to=prev_end)

        out, _, mock_cc = self._run(inner=_saved_inner())
        self.assertIn('data available after', out)
        mock_cc.assert_not_called()

        out, _, mock_cc = self._run(inner=_saved_inner(), force=True)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertIn('modis_ndvi', names)
