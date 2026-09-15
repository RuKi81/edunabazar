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
from agrocosmos.models import District, Farmland, MonitoringTask, Region

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


def _covered_inner():
    """Инерция modis_ndvi, когда композит уже посчитан и лежит в БД."""
    def inner(name, **kw):
        kw['stdout'].write(
            '  [1/1] 2024-01-01..2024-01-16 — already in DB '
            '(79142/79150), skip\n  Records saved: 0\n  Done in 0h00m02s\n'
        )
    return inner


class CheckMonitoringTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        # Обход пропускает скоупы без вектора угодий (экономия запросов
        # к GEE), поэтому базовому региону нужно хотя бы одно угодье.
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50, 0.2))
        Farmland.objects.create(
            region=cls.region, district=cls.district,
            area_ha=10, geom=_square(30, 50, 0.1))

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

    def test_raster_tasks_are_ignored(self):
        """Задачи S2+L8 не должны попадать в MODIS-обход.

        Регрессия: без фильтра по ``task_type`` команда гоняла MODIS-пайплайн
        по растровым задачам и переписывала их ``last_date_to`` датами
        16-дневной сетки, из-за чего ``check_raster_monitoring`` получал
        пустое окно и оперативный мониторинг тихо вставал.
        """
        raster = self._task(task_type='raster',
                            last_date_to=date(PAST_YEAR, 6, 1))
        out, _, mock_cc = self._run(inner=_saved_inner())
        self.assertIn('No active monitoring tasks', out)
        mock_cc.assert_not_called()
        raster.refresh_from_db()
        self.assertEqual(raster.last_date_to, date(PAST_YEAR, 6, 1))

    def test_region_without_farmlands_is_paused(self):
        """Регионы без угодий (Москва/СПб/Севастополь) не должны дёргать GEE.

        Регрессия: пайплайн скачивал композит ДО загрузки угодий, писал 0
        записей, ``last_date_to`` не двигался — и один и тот же период
        переспрашивался у GEE каждую ночь бесконечно.
        """
        empty = Region.objects.create(
            name='Мегаполис', code='r2', geom=_square(40, 55))
        task = MonitoringTask.objects.create(
            region=empty, year=PAST_YEAR, status='active')
        out, _, mock_cc = self._run(inner=_saved_inner())
        self.assertIn('No farmlands in scope', out)
        self.assertIn('pausing task', out)
        task.refresh_from_db()
        self.assertEqual(task.status, 'paused')
        self.assertIn('no farmlands in scope', task.log)
        # Пайплайн по пустому региону не запускался
        called_regions = [
            c.kwargs.get('region_id') for c in mock_cc.call_args_list
            if c.args and c.args[0] == 'modis_ndvi'
        ]
        self.assertNotIn(empty.pk, called_regions)

    def test_no_farmlands_dry_run_keeps_status(self):
        empty = Region.objects.create(
            name='Мегаполис', code='r2', geom=_square(40, 55))
        task = MonitoringTask.objects.create(
            region=empty, year=PAST_YEAR, status='active')
        out, _, _ = self._run(inner=_saved_inner(), dry_run=True)
        self.assertIn('would pause', out)
        task.refresh_from_db()
        self.assertEqual(task.status, 'active')

    def test_completed_year_marks_task(self):
        task = self._task(last_date_to=date(PAST_YEAR, 12, 31))
        out, _, mock_cc = self._run()
        mock_cc.assert_not_called()
        task.refresh_from_db()
        self.assertEqual(task.status, 'completed')
        self.assertIn('Year complete', out)

    def test_future_period_stops(self):
        self._task(year=date.today().year + 1)
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

    def test_already_in_db_advances_last_date_to(self):
        """Период, уже посчитанный ранее, считается успешным.

        Регрессия: ``modis_ndvi`` пропускает zonal stats, если строки для
        ≥99% угодий уже есть (так бывает после ручного бекфилла, который
        не трогает MonitoringTask), и пишет 0 записей. Раньше это
        трактовалось как «данных ещё нет», ``last_date_to`` не двигался и
        задача вечно перекачивала тот же композит из GEE.
        """
        task = self._task()
        out, _, mock_cc = self._run(inner=_covered_inner())
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names.count('modis_ndvi'), 2)
        self.assertIn('already in DB in', out)
        task.refresh_from_db()
        self.assertEqual(task.last_date_to, date(PAST_YEAR, 2, 1))
        self.assertEqual(task.records_total, 0)
        # Данные двинулись → батчевый refresh статусов нужен
        self.assertIn('recompute_district_ndvi_status', names)

    def test_zero_records_echoes_pipeline_diagnostics(self):
        """Причина нуля не должна теряться: modis_ndvi глотает ошибки GEE
        внутри себя, а мы забираем его stdout/stderr в StringIO."""
        def inner(name, **kw):
            kw['stdout'].write(
                '  [1/1] ERROR: MODIS download error: RESOURCE_EXHAUSTED\n'
                '  Download done: 0 files, 0 skipped, 1 errors (84s)\n'
                '  Records saved: 0\n'
            )
        task = self._task()
        out, err, _ = self._run(inner=inner)
        self.assertIn('no data yet, stop', out)
        self.assertIn('RESOURCE_EXHAUSTED', err)
        task.refresh_from_db()
        self.assertIsNone(task.last_date_to)
        self.assertIn('RESOURCE_EXHAUSTED', task.log)

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
        self._task(year=year, last_date_to=prev_end)

        out, _, mock_cc = self._run(inner=_saved_inner())
        self.assertIn('data available after', out)
        mock_cc.assert_not_called()

        out, _, mock_cc = self._run(inner=_saved_inner(), force=True)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertIn('modis_ndvi', names)
