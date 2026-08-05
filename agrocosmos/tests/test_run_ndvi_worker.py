"""Тесты воркера ``run_ndvi_worker`` — страховочная сетка перед
рефакторингом ``_run_one`` (C=12).

Внутренний ``call_command`` мокается; проверяем claim очереди,
маршрутизацию task_type, обработку исключений и лог-файл.
"""
import io
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import PipelineRun

MOD = 'agrocosmos.management.commands.run_ndvi_worker'


class RunNdviWorkerTests(TestCase):
    def _queue(self, **overrides):
        kwargs = dict(
            task_type=PipelineRun.TaskType.RASTER_NDVI,
            status=PipelineRun.Status.QUEUED,
            launch_args={'year': 2024},
        )
        kwargs.update(overrides)
        return PipelineRun.objects.create(**kwargs)

    def _run_worker(self, inner=None):
        out, err = io.StringIO(), io.StringIO()
        with patch(f'{MOD}.call_command', side_effect=inner) as mock_cc, \
             patch(f'{MOD}.close_old_connections'):
            call_command('run_ndvi_worker', '--once',
                         stdout=out, stderr=err)
        return out.getvalue(), err.getvalue(), mock_cc

    def test_claims_and_runs_queued_job(self):
        run = self._queue()
        out, _, mock_cc = self._run_worker()
        mock_cc.assert_called_once()
        cmd_name = mock_cc.call_args.args[0]
        kwargs = mock_cc.call_args.kwargs
        self.assertEqual(cmd_name, 'run_ndvi_pipeline')
        self.assertEqual(kwargs['run_id'], run.pk)
        self.assertEqual(kwargs['year'], 2024)
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.RUNNING)
        self.assertIsNotNone(run.pid)
        self.assertIn('picking up run', out)

    def test_no_queued_jobs_exits_cleanly(self):
        out, _, mock_cc = self._run_worker()
        mock_cc.assert_not_called()
        self.assertIn('exited cleanly', out)

    def test_ignores_unsupported_task_types(self):
        self._queue(task_type=PipelineRun.TaskType.UPLOAD_REGION)
        out, _, mock_cc = self._run_worker()
        mock_cc.assert_not_called()

    def test_exception_marks_failed_with_traceback(self):
        run = self._queue()
        _, _, _ = self._run_worker(inner=RuntimeError('boom'))
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.FAILED)
        self.assertIsNotNone(run.finished_at)
        self.assertIn('boom', run.log)

    def test_completed_status_not_clobbered_by_late_exception(self):
        run = self._queue()

        def inner(*a, **kw):
            PipelineRun.objects.filter(pk=run.pk).update(
                status=PipelineRun.Status.COMPLETED)
            raise RuntimeError('late heartbeat')

        self._run_worker(inner=inner)
        run.refresh_from_db()
        self.assertEqual(run.status, PipelineRun.Status.COMPLETED)

    def test_log_file_receives_streams(self):
        tmp = tempfile.NamedTemporaryFile(suffix='.log', delete=False)
        tmp.close()
        path = Path(tmp.name)
        self.addCleanup(path.unlink)
        self._queue(log_file=str(path))
        _, _, mock_cc = self._run_worker()
        kwargs = mock_cc.call_args.kwargs
        self.assertIn('stdout', kwargs)
        self.assertIn('stderr', kwargs)
