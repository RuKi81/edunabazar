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

import contextlib
import logging
import os
import signal
import time
import traceback

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.db.models import Case, IntegerField, Value, When
from django.utils import timezone

from agrocosmos.models import PipelineRun


logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC_DEFAULT = 5
# task_type -> management command that knows how to run it.
# Both commands accept ``--run-id`` and update the PipelineRun row
# internally (status/heartbeat/log).
TASK_COMMAND = {
    PipelineRun.TaskType.RASTER_NDVI: 'run_ndvi_pipeline',
    PipelineRun.TaskType.ARCHIVE_NDVI: 'run_archive_pipeline',
    PipelineRun.TaskType.GIS_OVERLAY: 'run_gis_overlay',
    PipelineRun.TaskType.RASTER_INGEST: 'run_raster_ingest',
}
SUPPORTED_TASK_TYPES = set(TASK_COMMAND)

# Быстрые интерактивные задачи (секунды–минуты): конвертация растра в COG и
# оверлей ГИС-слоёв. Их забираем из очереди РАНЬШЕ длинных bulk-прогонов
# (raster_ndvi / archive_ndvi / monitoring могут идти часами), иначе такая
# задача голодает за многочасовым мониторингом. Внутри одного приоритета
# порядок остаётся FIFO (по started_at, pk).
QUICK_TASK_TYPES = {
    PipelineRun.TaskType.GIS_OVERLAY,
    PipelineRun.TaskType.RASTER_INGEST,
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
        # Приоритет: быстрые интерактивные задачи (COG-конвертация, оверлей)
        # забираем раньше длинных bulk-прогонов, иначе они голодают за
        # многочасовым мониторингом. Внутри приоритета — FIFO (started_at, pk).
        candidate = (
            PipelineRun.objects
            .filter(status=PipelineRun.Status.QUEUED,
                    task_type__in=list(SUPPORTED_TASK_TYPES))
            .annotate(_prio=Case(
                When(task_type__in=list(QUICK_TASK_TYPES), then=Value(0)),
                default=Value(10),
                output_field=IntegerField(),
            ))
            .order_by('_prio', 'started_at', 'pk')
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
        # Underscore-prefixed keys are service metadata (e.g. ``_requeues``
        # from pipeline_requeue) — never valid CLI arguments.
        args = {k: v for k, v in (run.launch_args or {}).items()
                if not k.startswith('_')}
        args['run_id'] = run.pk

        self.stdout.write(self.style.SUCCESS(
            f'[worker] picking up run #{run.pk} task={run.task_type} '
            f'args={args}'
        ))

        # Mirror stdout of the inner command to the per-run log file, so
        # ``tail -f logs/pipeline/run_<id>.log`` works exactly the same
        # way it did when the admin spawned a detached subprocess.
        #
        # ``run_ndvi_pipeline`` internally uses plain ``print(..., flush=True)``
        # as well as nested ``call_command`` of stage sub-commands whose
        # Django ``OutputWrapper`` grabs ``sys.stdout`` at construction time.
        # To capture *all* of that we must redirect ``sys.stdout``/``stderr``
        # at the interpreter level, not just pass ``stdout=`` to call_command.
        log_f = self._open_log_file(run.log_file or '')
        redirect_ctx = (
            contextlib.ExitStack()
            if log_f is None
            else _redirect_std_to(log_f)
        )

        command_name = TASK_COMMAND.get(run.task_type)
        if not command_name:
            # Shouldn't happen: _claim_next_run filters by SUPPORTED_TASK_TYPES.
            self.stderr.write(
                f'[worker] unsupported task_type={run.task_type!r} on run #{run.pk}'
            )
            PipelineRun.objects.filter(pk=run.pk).update(
                status=PipelineRun.Status.FAILED,
                finished_at=timezone.now(),
                log=f'worker: no handler for task_type={run.task_type!r}',
            )
            return

        try:
            kwargs = dict(args)
            if log_f is not None:
                kwargs['stdout'] = log_f
                kwargs['stderr'] = log_f
            with redirect_ctx:
                call_command(command_name, **kwargs)
        except SystemExit:
            # pipeline orchestrators call sys.exit(1) on SIGTERM — let it go.
            raise
        except Exception:
            logger.exception('worker: pipeline raised')
            self._mark_failed(run, traceback.format_exc())
        finally:
            self._close_log_file(log_f)
            # The pipeline command overrides SIGTERM/SIGINT handlers in this
            # process; restore ours so a signal to an idle worker doesn't
            # invoke a stale handler bound to the finished run.
            self._install_signal_handlers()

    def _open_log_file(self, log_file_path: str):
        if not log_file_path:
            return None
        try:
            os.makedirs(os.path.dirname(log_file_path) or '.', exist_ok=True)
            # line-buffered text mode so ``tail -f`` sees updates live
            return open(log_file_path, 'a', encoding='utf-8',
                        errors='replace', buffering=1)
        except OSError as exc:
            self.stderr.write(f'[worker] cannot open log file {log_file_path}: {exc}')
            return None

    @staticmethod
    def _mark_failed(run: PipelineRun, tb: str) -> None:
        try:
            # Mark FAILED only if the pipeline didn't already mark
            # itself COMPLETED. Otherwise a late background-thread
            # exception (e.g. heartbeat trying to write after the
            # main connection was closed) would clobber a successful
            # run's status.
            updated = PipelineRun.objects.filter(
                pk=run.pk,
            ).exclude(
                status=PipelineRun.Status.COMPLETED,
            ).update(
                status=PipelineRun.Status.FAILED,
                finished_at=timezone.now(),
                log=(run.log or '') + '\n[worker] ' + tb[-4000:],
            )
            if not updated:
                logger.warning(
                    'worker: run #%s already completed, '
                    'ignoring late exception: %s',
                    run.pk, tb.splitlines()[-1] if tb else '',
                )
        except Exception:
            logger.exception('worker: failed to mark run as failed')

    @staticmethod
    def _close_log_file(log_f) -> None:
        if log_f is None:
            return
        try:
            log_f.flush()
        except OSError:
            pass
        try:
            log_f.close()
        except OSError:
            pass

    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, self._on_signal)
            except (ValueError, OSError):
                pass

    def handle(self, *args, **options):
        poll_sec = max(1, int(options['poll_sec']))
        run_once = bool(options['once'])

        self._install_signal_handlers()

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


@contextlib.contextmanager
def _redirect_std_to(file_obj):
    """Temporarily redirect ``sys.stdout`` and ``sys.stderr`` to *file_obj*.

    Unlike ``contextlib.redirect_stdout`` alone, this also swaps
    ``sys.stderr`` so that tracebacks and ``print(..., file=sys.stderr)``
    end up in the same pipeline log file.
    """
    import sys
    old_out, old_err = sys.stdout, sys.stderr
    sys.stdout = file_obj
    sys.stderr = file_obj
    try:
        yield
    finally:
        try:
            file_obj.flush()
        except Exception:
            pass
        sys.stdout = old_out
        sys.stderr = old_err
