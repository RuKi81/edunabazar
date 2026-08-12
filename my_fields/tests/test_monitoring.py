"""Тесты спутникового мониторинга пользовательских полей.

Фиксируем:
* POST /api/my/fields/<pk>/monitoring/ создаёт queued PipelineRun c
  launch_args.myf_field_id и окном «1 января → сегодня-7дн»;
* повторный POST при активном запуске не создаёт дубликат (200);
* права: 401 без логина, 403 для чужого пользователя;
* GET возвращает статус последнего запуска;
* run_ndvi_pipeline корректно резолвит scope --myf-field-id и передаёт
  его в стадии fetch_raster_ndvi (S2 первым), пропуская fusion.
"""
from datetime import date, timedelta
from unittest import mock

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import District, PipelineRun, Region
from my_fields.models import UserField
from my_fields.services.monitoring import (
    FRESHNESS_LAG_DAYS, enqueue_field_monitoring,
)

User = get_user_model()


def _mpoly(x0, y0, x1, y1):
    return MultiPolygon(Polygon((
        (x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0),
    )))


class MonitoringTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.owner = User.objects.create_user('owner', password='x')
        cls.stranger = User.objects.create_user('stranger', password='x')
        cls.region = Region.objects.create(
            name='Тест-регион', code='test-region', geom=_mpoly(34, 45, 35, 46),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тест-район', geom=_mpoly(34, 45, 35, 46),
        )
        cls.field = UserField.objects.create(
            owner=cls.owner, name='Поле у оврага',
            geom=_mpoly(34.10, 45.10, 34.12, 45.12),
            area_ha=250.0, region=cls.region, district=cls.district,
        )

    @property
    def url(self):
        return f'/api/my/fields/{self.field.pk}/monitoring/'


class EnqueueServiceTests(MonitoringTestCase):
    def test_creates_queued_run_with_field_scope(self):
        run, created = enqueue_field_monitoring(self.field)
        self.assertTrue(created)
        self.assertEqual(run.status, PipelineRun.Status.QUEUED)
        self.assertEqual(run.task_type, PipelineRun.TaskType.RASTER_NDVI)
        self.assertEqual(run.region_id, self.region.pk)
        args = run.launch_args
        self.assertEqual(args['myf_field_id'], self.field.pk)
        today = date.today()
        self.assertEqual(args['year'], today.year)
        self.assertEqual(args['date_from'], date(today.year, 1, 1).isoformat())
        expected_to = max(
            date(today.year, 1, 1), today - timedelta(days=FRESHNESS_LAG_DAYS),
        )
        self.assertEqual(args['date_to'], expected_to.isoformat())
        self.assertFalse(args['fusion'])
        self.assertFalse(args['skip_s2'])
        self.assertFalse(args['skip_l8'])
        # Файл лога прописан — heartbeat/tail заработают сразу.
        self.assertIn(f'run_{run.pk}.log', run.log_file)

    def test_active_run_is_not_duplicated(self):
        run1, created1 = enqueue_field_monitoring(self.field)
        run2, created2 = enqueue_field_monitoring(self.field)
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual(run1.pk, run2.pk)
        self.assertEqual(PipelineRun.objects.count(), 1)

    def test_finished_run_allows_new_enqueue(self):
        run1, _ = enqueue_field_monitoring(self.field)
        PipelineRun.objects.filter(pk=run1.pk).update(
            status=PipelineRun.Status.COMPLETED,
        )
        run2, created = enqueue_field_monitoring(self.field)
        self.assertTrue(created)
        self.assertNotEqual(run1.pk, run2.pk)

    def test_other_fields_run_does_not_block(self):
        other = UserField.objects.create(
            owner=self.owner, name='Другое поле',
            geom=_mpoly(34.20, 45.20, 34.22, 45.22),
            area_ha=100.0, region=self.region,
        )
        enqueue_field_monitoring(other)
        run, created = enqueue_field_monitoring(self.field)
        self.assertTrue(created)
        self.assertEqual(run.launch_args['myf_field_id'], self.field.pk)


class MonitoringApiTests(MonitoringTestCase):
    def test_anonymous_is_401(self):
        self.assertEqual(self.client.get(self.url).status_code, 401)
        self.assertEqual(self.client.post(self.url).status_code, 401)

    def test_stranger_is_403(self):
        self.client.force_login(self.stranger)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(self.client.post(self.url).status_code, 403)
        self.assertEqual(PipelineRun.objects.count(), 0)

    def test_post_enqueues_and_returns_202(self):
        self.client.force_login(self.owner)
        resp = self.client.post(self.url)
        self.assertEqual(resp.status_code, 202)
        data = resp.json()
        self.assertTrue(data['created'])
        self.assertEqual(data['run']['status'], 'queued')
        run = PipelineRun.objects.get()
        self.assertEqual(run.launch_args['myf_field_id'], self.field.pk)

    def test_repeat_post_returns_existing_run_200(self):
        self.client.force_login(self.owner)
        first = self.client.post(self.url).json()
        resp = self.client.post(self.url)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertFalse(data['created'])
        self.assertEqual(data['run']['run_id'], first['run']['run_id'])
        self.assertEqual(PipelineRun.objects.count(), 1)

    def test_get_returns_latest_run_status(self):
        self.client.force_login(self.owner)
        self.assertIsNone(self.client.get(self.url).json()['run'])
        self.client.post(self.url)
        run_json = self.client.get(self.url).json()['run']
        self.assertEqual(run_json['status'], 'queued')
        self.assertIn('run_id', run_json)


class PipelineFieldScopeTests(MonitoringTestCase):
    """run_ndvi_pipeline --myf-field-id: сценарии scope-резолва и стадий."""

    def _make_run(self):
        run, _ = enqueue_field_monitoring(self.field)
        return run

    def test_field_scope_runs_s2_first_then_l8_and_skips_fusion(self):
        run = self._make_run()
        calls = []

        def fake_call_command(name, **kwargs):
            calls.append((name, kwargs))

        with mock.patch(
            'agrocosmos.management.commands.run_ndvi_pipeline.call_command',
            side_effect=fake_call_command,
        ):
            call_command(
                'run_ndvi_pipeline',
                run_id=run.pk, myf_field_id=self.field.pk,
                year=date.today().year, fusion=True,
            )

        names = [c[0] for c in calls]
        self.assertEqual(names, ['fetch_raster_ndvi', 'fetch_raster_ndvi'])
        sensors = [c[1]['sensor'] for c in calls]
        self.assertEqual(sensors, ['s2', 'l8'])  # S2 приоритетнее
        for _, kwargs in calls:
            self.assertEqual(kwargs['myf_field_id'], self.field.pk)
            self.assertNotIn('district_id', kwargs)
            self.assertNotIn('region_id', kwargs)

        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.COMPLETED)

    def test_missing_field_marks_run_failed(self):
        run = self._make_run()
        with mock.patch(
            'agrocosmos.management.commands.run_ndvi_pipeline.call_command',
        ) as cc:
            call_command(
                'run_ndvi_pipeline',
                run_id=run.pk, myf_field_id=999999,
                year=date.today().year,
            )
        cc.assert_not_called()
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.FAILED)


class FetchRasterFieldScopeTests(MonitoringTestCase):
    """fetch_raster_ndvi --myf-field-id: extent с буфером, scope f<id>,
    принудительный download-only.

    Сенсорные модули (satellite_s2_raster и т.п.) тянут ``ee`` на уровне
    импорта, поэтому подменяем ``Command._sensor_imports`` целиком —
    тест не зависит от установленного earthengine-api.
    """

    @staticmethod
    def _fake_sensor(captured):
        def fake_download(region_geom_extent, region_id, date_from, date_to,
                          cloud_max=30, overwrite=False):
            captured.setdefault('calls', []).append(
                (region_geom_extent, region_id),
            )
            return None

        def fake_chunks(date_from, date_to):
            return [(date_from, date_to)]

        def fake_raster_path(region_id, cf, ct):
            return f'/tmp/{region_id}_{cf}_{ct}.tif'

        return staticmethod(
            lambda sensor: (fake_download, fake_chunks, fake_raster_path),
        )

    def test_download_called_with_buffered_field_extent(self):
        captured = {}
        with mock.patch(
            'agrocosmos.management.commands.fetch_raster_ndvi.Command._sensor_imports',
            new=self._fake_sensor(captured),
        ):
            call_command(
                'fetch_raster_ndvi',
                sensor='s2', myf_field_id=self.field.pk,
                date_from='2026-05-01', date_to='2026-05-05',
            )

        self.assertTrue(captured.get('calls'))
        extent, scope_id = captured['calls'][0]
        self.assertEqual(scope_id, f'f{self.field.pk}')
        xmin, ymin, xmax, ymax = extent
        fx0, fy0, fx1, fy1 = self.field.geom.extent
        self.assertLess(xmin, fx0)
        self.assertLess(ymin, fy0)
        self.assertGreater(xmax, fx1)
        self.assertGreater(ymax, fy1)

    def test_unknown_field_id_aborts(self):
        captured = {}
        with mock.patch(
            'agrocosmos.management.commands.fetch_raster_ndvi.Command._sensor_imports',
            new=self._fake_sensor(captured),
        ):
            call_command(
                'fetch_raster_ndvi',
                sensor='s2', myf_field_id=999999,
                date_from='2026-05-01', date_to='2026-05-05',
            )
        self.assertNotIn('calls', captured)
