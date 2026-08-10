"""Тесты ``services.pipeline_requeue.try_requeue`` — возврат прерванного
SIGTERM'ом запуска в очередь воркера вместо пометки failed.
"""
from django.test import TestCase

from agrocosmos.models import PipelineRun
from agrocosmos.services.pipeline_requeue import (
    MAX_REQUEUES, REQUEUE_COUNT_KEY, try_requeue,
)


class TryRequeueTests(TestCase):
    def _run(self, **overrides):
        kwargs = dict(
            task_type=PipelineRun.TaskType.RASTER_NDVI,
            status=PipelineRun.Status.RUNNING,
            launch_args={'year': 2024, 'region_id': 1},
            pid=42,
        )
        kwargs.update(overrides)
        return PipelineRun.objects.create(**kwargs)

    def test_requeues_running_run_with_args(self):
        run = self._run()
        self.assertTrue(try_requeue(run.pk))
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.QUEUED)
        self.assertIsNone(run.pid)
        self.assertEqual(run.launch_args[REQUEUE_COUNT_KEY], 1)
        # Исходные аргументы сохранены — воркер перезапустит с ними же.
        self.assertEqual(run.launch_args['year'], 2024)

    def test_increments_counter(self):
        run = self._run(launch_args={'year': 2024, REQUEUE_COUNT_KEY: 2})
        self.assertTrue(try_requeue(run.pk))
        run.refresh_from_db()
        self.assertEqual(run.launch_args[REQUEUE_COUNT_KEY], 3)

    def test_cap_reached_returns_false(self):
        run = self._run(
            launch_args={'year': 2024, REQUEUE_COUNT_KEY: MAX_REQUEUES})
        self.assertFalse(try_requeue(run.pk))
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.RUNNING)

    def test_empty_launch_args_returns_false(self):
        run = self._run(launch_args={})
        self.assertFalse(try_requeue(run.pk))
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.RUNNING)

    def test_non_running_statuses_not_touched(self):
        for status in (PipelineRun.Status.COMPLETED,
                       PipelineRun.Status.FAILED,
                       PipelineRun.Status.QUEUED):
            run = self._run(status=status)
            self.assertFalse(try_requeue(run.pk))
            run.refresh_from_db()
            self.assertEqual(run.status, status)

    def test_missing_run_returns_false(self):
        self.assertFalse(try_requeue(999999))
        self.assertFalse(try_requeue(None))
