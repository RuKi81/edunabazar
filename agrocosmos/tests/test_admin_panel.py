"""Тесты admin-views агропанели (``agrocosmos/admin.py``) — страховочная
сетка перед рефакторингом ``run_status_view`` (C=11) и
``start_raster_monitoring_view`` (C=11).

Покрывается: JSON-статус PipelineRun (хвост лога из файла/поля, liveness
PID), создание/возобновление растрового мониторинга с начальной выкачкой
(окно, постановка в очередь, откат при ошибке).
"""
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase
from django.urls import reverse

from agrocosmos.models import District, MonitoringTask, PipelineRun, Region

User = get_user_model()


def _mpoly(x0=34, y0=45, x1=35, y1=46):
    return MultiPolygon(Polygon((
        (x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0),
    )))


class AdminPanelTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = User.objects.create_superuser('boss', 'b@test.com', 'x')
        cls.region = Region.objects.create(
            name='Тест-регион', code='test-region', geom=_mpoly(),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тест-район', geom=_mpoly(),
        )

    def setUp(self):
        self.client.force_login(self.admin)


class RunStatusViewTests(AdminPanelTestCase):

    def _url(self, pk):
        return reverse('admin:agro_run_status', args=[pk])

    def test_missing_run_is_404(self):
        self.assertEqual(self.client.get(self._url(99999)).status_code, 404)

    def test_status_with_inline_log(self):
        run = PipelineRun.objects.create(
            task_type='modis_ndvi', region=self.region, year=2026,
            status=PipelineRun.Status.COMPLETED, log='строка лога',
            records_count=42,
        )
        data = self.client.get(self._url(run.pk)).json()
        self.assertEqual(data['id'], run.pk)
        self.assertEqual(data['status'], PipelineRun.Status.COMPLETED)
        self.assertEqual(data['tail'], 'строка лога')
        self.assertEqual(data['records_count'], 42)
        self.assertFalse(data['alive'])

    def test_tail_read_from_log_file(self):
        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.log', delete=False, encoding='utf-8',
        )
        self.addCleanup(Path(tmp.name).unlink)
        tmp.write('A' * 10000 + 'КОНЕЦ')
        tmp.close()
        run = PipelineRun.objects.create(
            task_type='modis_ndvi', region=self.region, year=2026,
            log_file=tmp.name,
        )
        tail = self.client.get(self._url(run.pk)).json()['tail']
        self.assertTrue(tail.endswith('КОНЕЦ'))
        self.assertLessEqual(len(tail), 8000)

    def test_missing_log_file_falls_back_to_log(self):
        run = PipelineRun.objects.create(
            task_type='modis_ndvi', region=self.region, year=2026,
            log_file='C:/nope/missing.log', log='запасной лог',
        )
        data = self.client.get(self._url(run.pk)).json()
        self.assertEqual(data['tail'], 'запасной лог')

    def test_alive_checked_for_running_with_pid(self):
        run = PipelineRun.objects.create(
            task_type='modis_ndvi', region=self.region, year=2026,
            status='running', pid=12345,
        )
        with mock.patch(
            'agrocosmos.management.commands.cleanup_stale_runs._pid_alive',
            return_value=True,
        ):
            data = self.client.get(self._url(run.pk)).json()
        self.assertTrue(data['alive'])


class StartRasterMonitoringTests(AdminPanelTestCase):

    URL_NAME = 'admin:agro_start_raster_monitoring'

    def _post(self, **data):
        base = {'region_id': self.region.pk, 'year': 2026, 'min_valid': 0.7}
        base.update(data)
        with mock.patch('agrocosmos.admin._enqueue_ndvi_pipeline') as enq:
            resp = self.client.post(reverse(self.URL_NAME), base)
        return resp, enq

    def test_get_redirects_without_side_effects(self):
        resp = self.client.get(reverse(self.URL_NAME))
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(MonitoringTask.objects.exists())

    def test_missing_region_or_year(self):
        resp, enq = self._post(region_id='')
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(MonitoringTask.objects.exists())
        enq.assert_not_called()

    def test_creates_task_and_enqueues_catchup(self):
        resp, enq = self._post(district_id=self.district.pk)
        self.assertEqual(resp.status_code, 302)

        task = MonitoringTask.objects.get()
        self.assertEqual(task.task_type, 'raster')
        self.assertEqual(task.district_id, self.district.pk)
        self.assertEqual(task.status, 'active')

        run = PipelineRun.objects.get()
        self.assertEqual(run.task_type, 'raster_ndvi')
        self.assertEqual(run.status, PipelineRun.Status.QUEUED)

        enq.assert_called_once()
        kwargs = enq.call_args.kwargs
        self.assertEqual(kwargs['region_id'], self.region.pk)
        self.assertEqual(kwargs['district_id'], self.district.pk)
        self.assertEqual(kwargs['date_from'], '2026-01-01')

    def test_resumes_paused_task(self):
        MonitoringTask.objects.create(
            task_type='raster', region=self.region, district=None,
            year=2026, status='paused',
        )
        self._post()
        task = MonitoringTask.objects.get()
        self.assertEqual(task.status, 'active')

    def test_bad_district_falls_back_to_region_scope(self):
        _, enq = self._post(district_id='мусор')
        task = MonitoringTask.objects.get()
        self.assertIsNone(task.district_id)
        self.assertIsNone(enq.call_args.kwargs['district_id'])

    def test_empty_window_skips_run(self):
        MonitoringTask.objects.create(
            task_type='raster', region=self.region, district=None,
            year=2026, status='active',
            last_date_to=date.today() - timedelta(days=3),
        )
        _, enq = self._post()
        self.assertFalse(PipelineRun.objects.exists())
        enq.assert_not_called()

    def test_enqueue_failure_marks_run_failed(self):
        with mock.patch(
            'agrocosmos.admin._enqueue_ndvi_pipeline',
            side_effect=RuntimeError('нет воркера'),
        ):
            self.client.post(reverse(self.URL_NAME), {
                'region_id': self.region.pk, 'year': 2026,
            })
        run = PipelineRun.objects.get()
        self.assertEqual(run.status, PipelineRun.Status.FAILED)
        self.assertIn('нет воркера', run.log)
