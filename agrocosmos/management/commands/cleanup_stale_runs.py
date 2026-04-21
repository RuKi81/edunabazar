from __future__ import annotations

"""Mark stale ``PipelineRun`` rows as failed.

A run is considered stale when ``status='running'`` but either:
  * no heartbeat has been recorded in the last ``--timeout-min`` minutes, OR
  * a PID is present but the OS reports no such process.

Usage:
    python manage.py cleanup_stale_runs [--timeout-min 15] [--dry-run]
"""
import os
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from agrocosmos.models import PipelineRun


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        if os.name == 'posix':
            os.kill(pid, 0)
            return True
        # Windows: fall back to tasklist-free check via OpenProcess
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        h = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION, False, pid,
        )
        if not h:
            return False
        ctypes.windll.kernel32.CloseHandle(h)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False


class Command(BaseCommand):
    help = 'Mark stale running PipelineRun rows as failed.'

    def add_arguments(self, parser):
        parser.add_argument('--timeout-min', type=int, default=15,
                            help='Heartbeat staleness threshold (min)')
        parser.add_argument('--dry-run', action='store_true')

    def handle(self, *args, **options):
        timeout = timedelta(minutes=options['timeout_min'])
        dry = options['dry_run']
        now = timezone.now()

        qs = PipelineRun.objects.filter(status='running')
        stale = []
        for run in qs:
            hb = run.heartbeat_at or run.started_at
            hb_stale = hb is not None and (now - hb) > timeout
            pid_dead = run.pid and not _pid_alive(run.pid)
            if hb_stale or pid_dead:
                reason = []
                if hb_stale:
                    reason.append(f'no heartbeat for {now - hb}')
                if pid_dead:
                    reason.append(f'pid {run.pid} not running')
                stale.append((run, '; '.join(reason)))

        if not stale:
            self.stdout.write(self.style.SUCCESS('No stale runs.'))
            return

        for run, reason in stale:
            self.stdout.write(f'  run #{run.pk} [{run.task_type}] — {reason}')
            if not dry:
                run.status = 'failed'
                run.finished_at = now
                run.log = (run.log or '') + f'\n[cleanup_stale_runs] {reason}'
                run.save(update_fields=['status', 'finished_at', 'log'])

        verb = 'Would mark' if dry else 'Marked'
        self.stdout.write(self.style.WARNING(
            f'{verb} {len(stale)} run(s) as failed.'
        ))
