"""Тесты команды ``check_raster_monitoring`` — страховочная сетка перед
рефакторингом ``_process_task`` (C=16). Вызовы подкоманд мокаются;
проверяем вычисление окна, продвижение last_date_to и маршрутизацию стадий.
"""
import io
from datetime import date, timedelta
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import District, MonitoringTask, Region

MOD = 'agrocosmos.management.commands.check_raster_monitoring'

PAST_YEAR = date.today().year - 2


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _fetch_inner(n=10):
    """Имитация fetch_raster_ndvi: печатает сводку 'N records saved'."""
    def inner(name, **kw):
        if name == 'fetch_raster_ndvi':
            kw['stdout'].write(f'\n  Stats: {n} records saved, 0 errors\n')
    return inner


class CheckRasterMonitoringTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50))

    def _task(self, **overrides):
        kwargs = dict(region=self.region, year=PAST_YEAR,
                      status='active', task_type='raster')
        kwargs.update(overrides)
        return MonitoringTask.objects.create(**kwargs)

    def _run(self, inner=None, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{MOD}.call_command', side_effect=inner) as mock_cc:
            call_command('check_raster_monitoring', stdout=out, stderr=err,
                         **kwargs)
        return out.getvalue(), err.getvalue(), mock_cc

    def test_no_active_tasks(self):
        self._task(status='paused')
        out, _, mock_cc = self._run()
        self.assertIn('No active raster monitoring tasks', out)
        mock_cc.assert_not_called()

    def test_task_id_filter(self):
        self._task()
        out, _, mock_cc = self._run(task_id=999999)
        self.assertIn('No active raster monitoring tasks', out)

    def test_completed_year_marks_task(self):
        task = self._task(last_date_to=date(PAST_YEAR, 12, 31))
        out, _, mock_cc = self._run()
        mock_cc.assert_not_called()
        task.refresh_from_db()
        self.assertEqual(task.status, 'completed')
        self.assertIn('year complete', out)

    def test_dry_run_shows_window_without_calls(self):
        task = self._task()
        out, _, mock_cc = self._run(dry_run=True)
        self.assertIn(f'window {PAST_YEAR}-01-01..{PAST_YEAR}-12-31', out)
        self.assertIn('[DRY RUN]', out)
        mock_cc.assert_not_called()

    def test_happy_path_advances_window(self):
        task = self._task()
        out, _, mock_cc = self._run(inner=_fetch_inner())
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names, [
            'fetch_raster_ndvi', 'fetch_raster_ndvi',
            'compute_fused_ndvi', 'ndvi_postprocess',
        ])
        sensors = [c.kwargs['sensor'] for c in mock_cc.call_args_list[:2]]
        self.assertEqual(sensors, ['s2', 'l8'])
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['region_id'], self.region.pk)
        self.assertEqual(kw['date_from'], f'{PAST_YEAR}-01-01')
        self.assertEqual(kw['date_to'], f'{PAST_YEAR}-12-31')
        self.assertEqual(kw['min_valid_ratio'], 0.7)
        self.assertTrue(
            mock_cc.call_args_list[2].kwargs['overwrite'])
        task.refresh_from_db()
        # 10 записей × 2 сенсора; окно закрыло год → completed
        self.assertEqual(task.records_total, 20)
        self.assertEqual(task.last_date_to, date(PAST_YEAR, 12, 31))
        self.assertEqual(task.status, 'completed')
        self.assertIn('S2=10 L8=10', task.log)

    def test_district_scope(self):
        self._task(district=self.district)
        _, _, mock_cc = self._run(inner=_fetch_inner())
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['district_id'], self.district.pk)
        self.assertNotIn('region_id', kw)

    def test_zero_records_does_not_advance(self):
        task = self._task()
        self._run(inner=_fetch_inner(0))
        task.refresh_from_db()
        self.assertIsNone(task.last_date_to)
        self.assertIn('[no new data, will retry]', task.log)
        self.assertIsNotNone(task.last_check)

    def test_error_logged(self):
        task = self._task()
        out, err, _ = self._run(inner=RuntimeError('gee down'))
        self.assertIn('ERROR: gee down', err)
        task.refresh_from_db()
        self.assertIn('ERROR: gee down', task.log)
        self.assertIsNone(task.last_date_to)

    def test_availability_lag_and_force(self):
        # Текущий год: последнее окно закрыто 3 дня назад → lag 7 дней
        # ещё не прошёл, без --force запуска нет.
        year = date.today().year
        task = self._task(year=year,
                          last_date_to=date.today() - timedelta(days=3))
        out, _, mock_cc = self._run(inner=_fetch_inner())
        self.assertIn('no new data yet', out)
        mock_cc.assert_not_called()

        out, _, mock_cc = self._run(inner=_fetch_inner(), force=True)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertIn('fetch_raster_ndvi', names)
        task.refresh_from_db()
        self.assertEqual(task.last_date_to, date.today())
