"""Mass-orchestrator: enqueue archive MODIS NDVI + baseline for many regions.

Workflow:
    1. Pick a set of regions (``--regions all`` or ``--regions 1,2,3``).
    2. For each region not already covered, enqueue a single
       :class:`PipelineRun` with ``task_type=ARCHIVE_NDVI`` and
       ``launch_args`` aimed at :mod:`run_archive_pipeline`.
    3. ``run_ndvi_worker`` containers (run with ``--scale worker=4``)
       each claim one queued run at a time and drive it to completion;
       on success they automatically call ``calc_ndvi_baseline`` for
       that region (because :mod:`run_archive_pipeline` does it as its
       last stage).
    4. This command then *monitors* the queue and prints a live progress
       table every ``--refresh-sec`` seconds: ``done / running / queued
       / failed`` plus an ETA computed from the rolling completion rate.

Resume / idempotency:
    * Regions that already have a ``completed`` PipelineRun for the same
      year window are skipped automatically (unless ``--force``).
    * Regions whose previous run is ``failed`` are re-queued.
    * A ``--dry-run`` flag previews the plan without writing anything.

Usage:
    python manage.py run_baseline_ndvi --regions all \\
        --year-from 2020 --year-to 2025 --concurrency 4
    python manage.py run_baseline_ndvi --regions 12,33,45 --dry-run
    python manage.py run_baseline_ndvi --regions all --no-monitor
"""
from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from agrocosmos.models import PipelineRun, Region


REFRESH_SEC_DEFAULT = 30


class Command(BaseCommand):
    help = (
        'Enqueue archive MODIS NDVI + baseline pipelines for many regions '
        'and (optionally) monitor the queue with a live progress table.'
    )

    # ------------------------------------------------------------------ CLI

    def add_arguments(self, parser):
        parser.add_argument(
            '--regions', type=str, default='all',
            help='"all" or comma-separated Region IDs (e.g. "12,33,45").',
        )
        parser.add_argument(
            '--year-from', type=int, default=2020,
            help='First year of archive window (default 2020).',
        )
        parser.add_argument(
            '--year-to', type=int, default=None,
            help='Last year of archive window (default: current_year - 1).',
        )
        parser.add_argument(
            '--min-valid', type=float, default=0.5,
            help='Min valid pixel ratio passed to modis_ndvi (default 0.5).',
        )
        parser.add_argument(
            '--concurrency', type=int, default=4,
            help='Reminder for the user: how many ``worker`` replicas '
                 'should be running. Does not change anything itself, '
                 'just printed in the summary banner (default 4).',
        )
        parser.add_argument(
            '--force', action='store_true',
            help='Re-enqueue regions even if a completed run for the '
                 'same window already exists.',
        )
        parser.add_argument(
            '--skip-baseline', action='store_true',
            help='Pass --skip-baseline to each pipeline (debug only).',
        )
        parser.add_argument(
            '--no-monitor', action='store_true',
            help='Enqueue and exit immediately, without watching '
                 'progress.',
        )
        parser.add_argument(
            '--refresh-sec', type=int, default=REFRESH_SEC_DEFAULT,
            help=f'Progress refresh interval in seconds '
                 f'(default {REFRESH_SEC_DEFAULT}).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Print the plan without enqueueing anything.',
        )

    # ------------------------------------------------------------------ helpers

    def _resolve_regions(self, spec: str) -> list[Region]:
        if spec.strip().lower() == 'all':
            return list(Region.objects.defer('geom').order_by('name'))
        ids: list[int] = []
        for tok in spec.split(','):
            tok = tok.strip()
            if not tok:
                continue
            try:
                ids.append(int(tok))
            except ValueError:
                self.stderr.write(f'Bad region id: {tok!r}')
                return []
        return list(Region.objects.defer('geom').filter(pk__in=ids).order_by('name'))

    def _existing_run(self, region: Region, year_from: int, year_to: int):
        """Return the most-recent ARCHIVE_NDVI run that matches the window.

        Match is loose: same region + ``year`` (we store ``year_to`` in
        the column, since the model only has a single ``year`` int).
        """
        return (
            PipelineRun.objects
            .filter(task_type=PipelineRun.TaskType.ARCHIVE_NDVI,
                    region=region, year=year_to)
            .order_by('-started_at')
            .first()
        )

    def _pipeline_log_path(self, run_id: int) -> Path:
        base = Path(getattr(settings, 'BASE_DIR', '.'))
        d = base / 'logs' / 'pipeline'
        d.mkdir(parents=True, exist_ok=True)
        return d / f'run_{run_id}.log'

    def _enqueue_one(self, region: Region, year_from: int, year_to: int,
                     min_valid: float, skip_baseline: bool) -> PipelineRun:
        run = PipelineRun.objects.create(
            task_type=PipelineRun.TaskType.ARCHIVE_NDVI,
            status=PipelineRun.Status.QUEUED,
            region=region,
            year=year_to,
            description=(f'{region.name}, {year_from}..{year_to}'
                         + (' (без baseline)' if skip_baseline else '')),
        )
        log_path = self._pipeline_log_path(run.pk)
        try:
            log_path.touch(exist_ok=True)
        except OSError:
            pass
        run.launch_args = {
            'region_id': region.pk,
            'year_from': year_from,
            'year_to': year_to,
            'min_valid': float(f'{min_valid:.3f}'),
            'overwrite': False,
            'skip_baseline': skip_baseline,
        }
        run.log_file = str(log_path)
        run.heartbeat_at = timezone.now()
        run.save(update_fields=['launch_args', 'log_file', 'heartbeat_at'])
        return run

    # ------------------------------------------------------------------ monitor

    def _monitor(self, run_ids: list[int], refresh_sec: int) -> None:
        if not run_ids:
            return
        self.stdout.write('\nLive progress (Ctrl-C to stop monitoring; '
                          'enqueued jobs continue running):\n')
        run_ids_set = set(run_ids)
        total = len(run_ids)
        # Rolling completion rate window for ETA
        history: list[tuple[float, int]] = []  # (timestamp, completed_count)

        while True:
            qs = PipelineRun.objects.filter(pk__in=run_ids_set).values(
                'pk', 'status', 'region__name', 'records_count',
                'started_at', 'finished_at',
            )
            by_status: dict[str, int] = {}
            for row in qs:
                by_status[row['status']] = by_status.get(row['status'], 0) + 1

            done = by_status.get(PipelineRun.Status.COMPLETED, 0)
            failed = by_status.get(PipelineRun.Status.FAILED, 0)
            running = by_status.get(PipelineRun.Status.RUNNING, 0)
            queued = by_status.get(PipelineRun.Status.QUEUED, 0)

            now_ts = time.time()
            history.append((now_ts, done + failed))
            # Keep last 30 minutes of samples
            cutoff = now_ts - 1800
            history = [(t, c) for (t, c) in history if t >= cutoff]

            eta_str = '—'
            if len(history) >= 2 and (done + failed) < total:
                t_old, c_old = history[0]
                rate = (history[-1][1] - c_old) / max(1.0, now_ts - t_old)
                if rate > 0:
                    remaining = total - (done + failed)
                    secs = int(remaining / rate)
                    eta_str = self._fmt_dur(secs)

            self.stdout.write(
                f'\r[{timezone.now().strftime("%H:%M:%S")}] '
                f'done={done}/{total}  running={running}  '
                f'queued={queued}  failed={failed}  ETA={eta_str}',
                ending=''
            )
            self.stdout.flush()

            if (done + failed) >= total:
                self.stdout.write('\n\nAll jobs finished.')
                if failed:
                    self.stdout.write(self.style.WARNING(
                        f'{failed} run(s) failed — see admin or logs/pipeline/.'
                    ))
                return

            try:
                time.sleep(refresh_sec)
            except KeyboardInterrupt:
                self.stdout.write('\n\nMonitoring stopped (jobs keep running).')
                return

    @staticmethod
    def _fmt_dur(secs: int) -> str:
        if secs < 0:
            secs = 0
        d, rem = divmod(secs, 86400)
        h, rem = divmod(rem, 3600)
        m, _ = divmod(rem, 60)
        if d:
            return f'{d}d{h:02d}h{m:02d}m'
        if h:
            return f'{h}h{m:02d}m'
        return f'{m}m'

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        regions = self._resolve_regions(options['regions'])
        if not regions:
            self.stderr.write('No regions resolved — abort.')
            return

        year_from = options['year_from']
        year_to = options['year_to'] or (date.today().year - 1)
        if year_to < year_from:
            self.stderr.write('--year-to must be >= --year-from')
            return

        min_valid = options['min_valid']
        concurrency = options['concurrency']
        force = options['force']
        skip_baseline = options['skip_baseline']
        dry_run = options['dry_run']
        no_monitor = options['no_monitor']
        refresh_sec = max(5, int(options['refresh_sec']))

        # ── Plan ────────────────────────────────────────────────────
        to_enqueue: list[Region] = []
        skipped: list[tuple[Region, str]] = []  # (region, reason)
        already_running: list[tuple[Region, int]] = []  # (region, run_id)

        for r in regions:
            existing = self._existing_run(r, year_from, year_to)
            if existing is None or force:
                to_enqueue.append(r)
                continue
            st = existing.status
            if st == PipelineRun.Status.COMPLETED:
                skipped.append((r, f'already completed (run #{existing.pk})'))
            elif st in (PipelineRun.Status.RUNNING, PipelineRun.Status.QUEUED):
                already_running.append((r, existing.pk))
            elif st == PipelineRun.Status.FAILED:
                # Failed runs are retried automatically.
                to_enqueue.append(r)

        # ── Banner ──────────────────────────────────────────────────
        self.stdout.write('═══════════════════════════════════════════════')
        self.stdout.write('  run_baseline_ndvi — mass orchestrator')
        self.stdout.write(f'  Regions matched : {len(regions)}')
        self.stdout.write(f'  Window          : {year_from}..{year_to}')
        self.stdout.write(f'  Min valid ratio : {min_valid:.0%}')
        self.stdout.write(f'  Skip baseline   : {skip_baseline}')
        self.stdout.write(f'  Force re-run    : {force}')
        self.stdout.write(f'  To enqueue      : {len(to_enqueue)}')
        self.stdout.write(f'  Skipped         : {len(skipped)}')
        self.stdout.write(f'  Already in queue: {len(already_running)}')
        self.stdout.write(f'  Worker conc.    : {concurrency} '
                          f'(set via "docker compose up -d --scale worker={concurrency}")')
        self.stdout.write('═══════════════════════════════════════════════')

        if skipped:
            self.stdout.write('\nSkipped:')
            for r, why in skipped[:20]:
                self.stdout.write(f'  {r.name:40s} — {why}')
            if len(skipped) > 20:
                self.stdout.write(f'  … and {len(skipped) - 20} more')

        if dry_run:
            self.stdout.write('\n--dry-run: nothing was written.')
            return

        # ── Enqueue ────────────────────────────────────────────────
        new_run_ids: list[int] = []
        for r in to_enqueue:
            run = self._enqueue_one(r, year_from, year_to, min_valid,
                                    skip_baseline)
            new_run_ids.append(run.pk)
            self.stdout.write(f'  + queued #{run.pk}: {r.name}')

        all_run_ids = new_run_ids + [rid for _, rid in already_running]

        if not all_run_ids:
            self.stdout.write('\nNothing to monitor — exiting.')
            return

        if no_monitor:
            self.stdout.write(
                f'\nEnqueued {len(new_run_ids)} run(s); --no-monitor set, '
                f'exiting. Workers will pick them up automatically.'
            )
            return

        self._monitor(all_run_ids, refresh_sec=refresh_sec)
