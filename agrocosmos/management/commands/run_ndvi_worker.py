"""Long-lived worker that picks up queued NDVI pipeline runs.

Designed to live in its own Docker container (``worker`` service in
``docker-compose.prod.yml``) so that long-running pipelines survive any
lifecycle change of the web container (deploys, healthcheck restarts,
gunicorn worker recycling).

How it works:
    1. The admin panel creates a ``PipelineRun`` row with
       ``status='queued'`` and the intended CLI arguments in
       ``launch_args`` (a JSON dict).
    2. This worker polls the DB every ``POLL_INTERVAL_SEC`` seconds for
       the oldest ``queued`` run.
    3. When it picks one up, it flips the row to ``status='running'``
       and invokes ``run_ndvi_pipeline`` *in-process* (via
       ``call_command``) — this way the existing staged pipeline logic
       (heartbeats, log tailing, signal handling, PipelineRun updates)
       is reused verbatim.
    4. On SIGTERM/SIGINT the worker stops accepting new jobs; any
       currently running pipeline will be terminated by the existing
       signal handler inside ``run_ndvi_pipeline`` which flips the row
       to ``status='failed``.

Usage:
    python manage.py run_ndvi_worker [--poll-sec 5]
"""
from __future__ import annotations

import logging
import os
import signal
import time
import traceback

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.utils import timezone

from agrocosmos.models import PipelineRun


logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC_DEFAULT = 5
SUPPORTED_TASK_TYPES = {
    PipelineRun.TaskType.RASTER_NDVI,
}


class Command(BaseCommand):
    help = (
        'Long-lived worker that picks up queued PipelineRun rows and '
        'executes them via run_ndvi_pipeline. Meant to run in its own '
        'container so it outlives web deploys.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--poll-sec', type=int, default=POLL_INTERVAL_SEC_DEFAULT,
            help=f'Polling interval in seconds (default {POLL_INTERVAL_SEC_DEFAULT}).',
        )
        parser.add_argument(
            '--once', action='store_true',
            help='Process at most one queued run then exit (for testing).',
        )

    # ------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop = False

    def _on_signal(self, signum, _frame):
        # Mark the worker as stopping. The in-flight pipeline has its own
        # SIGTERM handler (see run_ndvi_pipeline) that will flip its own
        # PipelineRun row to 'failed'.
        self.stdout.write(self.style.WARNING(
            f'[worker] got signal {signum}, will exit after current job'
        ))
        self._stop = True

    # ------------------------------------------------------------------

    def _claim_next_run(self) -> PipelineRun | None:
        """Atomically claim the oldest queued run we can handle.

        Uses ``update()`` with a status predicate to avoid races if two
        workers were ever started in parallel.
        """
        close_old_connections()
        candidate = (
            PipelineRun.objects
            .filter(status=PipelineRun.Status.QUEUED,
                    task_type__in=list(SUPPORTED_TASK_TYPES))
            .order_by('started_at', 'pk')
            .values_list('pk', flat=True)
            .first()
        )
        if candidate is None:
            return None

        updated = (
            PipelineRun.objects
            .filter(pk=candidate, status=PipelineRun.Status.QUEUED)
            .update(
                status=PipelineRun.Status.RUNNING,
                pid=os.getpid(),
                heartbeat_at=timezone.now(),
            )
        )
        if updated != 1:
            return None  # someone else took it
        return PipelineRun.objects.get(pk=candidate)

    def _run_one(self, run: PipelineRun) -> None:
        args = dict(run.launch_args or {})
        args['run_id'] = run.pk

        self.stdout.write(self.style.SUCCESS(
            f'[worker] picking up run #{run.pk} task={run.task_type} '
            f'args={args}'
        ))

        # Mirror stdout of the inner command to the per-run log file, so
        # ``tail -f logs/pipeline/run_<id>.log`` works exactly the same
        # way it did when the admin spawned a detached subprocess.
        log_file_path = run.log_file or ''
        log_f = None
        if log_file_path:
            try:
                os.makedirs(os.path.dirname(log_file_path) or '.', exist_ok=True)
                log_f = open(log_file_path, 'ab', buffering=0)
            except OSError as exc:
                self.stderr.write(f'[worker] cannot open log file {log_file_path}: {exc}')
                log_f = None

        # run_ndvi_pipeline accepts stdout/stderr via call_command kwargs.
        try:
            kwargs = dict(args)
            if log_f is not None:
                # Wrap the binary file object so call_command can .write(str).
                kwargs['stdout'] = _BinaryTextSink(log_f)
                kwargs['stderr'] = kwargs['stdout']
            call_command('run_ndvi_pipeline', **kwargs)
        except SystemExit:
            # run_ndvi_pipeline calls sys.exit(1) on SIGTERM — let it go.
            raise
        except Exception:
            tb = traceback.format_exc()
            logger.exception('worker: pipeline raised')
            try:
                PipelineRun.objects.filter(pk=run.pk).update(
                    status=PipelineRun.Status.FAILED,
                    finished_at=timezone.now(),
                    log=(run.log or '') + '\n[worker] ' + tb[-4000:],
                )
            except Exception:
                logger.exception('worker: failed to mark run as failed')
        finally:
            if log_f is not None:
                try:
                    log_f.close()
                except OSError:
                    pass

    # ------------------------------------------------------------------

    def handle(self, *args, **options):
        poll_sec = max(1, int(options['poll_sec']))
        run_once = bool(options['once'])

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, self._on_signal)
            except (ValueError, OSError):
                pass

        self.stdout.write(self.style.SUCCESS(
            f'[worker] started (pid={os.getpid()}, poll={poll_sec}s)'
        ))

        while not self._stop:
            try:
                run = self._claim_next_run()
            except Exception:
                logger.exception('worker: claim failed')
                run = None

            if run is None:
                if run_once:
                    break
                time.sleep(poll_sec)
                continue

            self._run_one(run)

            if run_once:
                break

        self.stdout.write(self.style.WARNING('[worker] exited cleanly'))


class _BinaryTextSink:
    """Adapt a binary file object so call_command can write text to it."""

    def __init__(self, binary_file):
        self._f = binary_file

    def write(self, s):
        if not isinstance(s, (bytes, bytearray)):
            s = str(s).encode('utf-8', errors='replace')
        self._f.write(s)
        return len(s)

    def flush(self):
        try:
            self._f.flush()
        except OSError:
            pass

    def isatty(self):
        return False

    @property
    def encoding(self):
        return 'utf-8'
