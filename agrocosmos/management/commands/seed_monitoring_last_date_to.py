"""Seed ``MonitoringTask.last_date_to`` for freshly enrolled regions.

When :mod:`ensure_all_regions_monitored` creates a new active task with
``last_date_to=NULL``, the next ``check_monitoring`` cron tries to
catch up from January 1, biweekly period by biweekly period —
synchronously, inside one PipelineRun.  For ~85 freshly enrolled
regions that means up to ~750 sequential ``modis_ndvi`` invocations
in a single cron run, which exceeds the heartbeat window and gets
killed by ``cleanup_stale_runs``.

This one-shot command stops the bleeding by setting ``last_date_to``
to a sensible "we are caught up" value:

* If the region already has MODIS NDVI in the DB for the task year, we
  snap ``MAX(acquired_date)`` to the end of its 16-day biweekly chunk
  (so the next ``check_monitoring`` resumes from the *following*
  chunk, picking up only what is genuinely new).
* Otherwise we set ``last_date_to`` to the most recent biweekly chunk
  whose data is already available (``today >= chunk_end + lag``), so
  the daily ``check_monitoring`` resumes from the upcoming chunk and
  does not try to backfill an empty Jan..today window in one shot.

After seeding, historic backfill (if desired) should be performed via
``run_archive_pipeline`` on a per-region basis, off the operational
cron path.

Usage::

    python manage.py seed_monitoring_last_date_to            # current year
    python manage.py seed_monitoring_last_date_to --year 2026
    python manage.py seed_monitoring_last_date_to --dry-run
"""
from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max

from agrocosmos.models import MonitoringTask, VegetationIndex


CHUNK_DAYS = 16            # MOD13Q1/MYD13Q1 composite cadence
AVAILABILITY_LAG_DAYS = 7  # MODIS L3 composites available ~7 days after period end


def _snap_to_chunk_end(d: date, year: int) -> date:
    """Return the end of the 16-day chunk (anchored to Jan 1 ``year``) that
    contains ``d``. Mirrors ``_next_aligned_period`` in check_monitoring."""
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    days_since = (d - year_start).days
    chunk_idx = days_since // CHUNK_DAYS
    chunk_end = year_start + timedelta(days=(chunk_idx + 1) * CHUNK_DAYS - 1)
    return min(chunk_end, year_end)


def _last_available_chunk_end(today: date, year: int) -> date | None:
    """Most recent biweekly ``chunk_end`` such that
    ``today >= chunk_end + AVAILABILITY_LAG_DAYS``. Returns ``None`` if no
    such chunk has elapsed yet (i.e. very early in the year)."""
    year_start = date(year, 1, 1)
    if today < year_start:
        return None
    n_chunks = (today - year_start).days // CHUNK_DAYS + 2  # safety margin
    for i in range(n_chunks, -1, -1):
        chunk_end = year_start + timedelta(days=(i + 1) * CHUNK_DAYS - 1)
        if today >= chunk_end + timedelta(days=AVAILABILITY_LAG_DAYS):
            return chunk_end
    return None


class Command(BaseCommand):
    help = (
        'Seed last_date_to for active MonitoringTask rows where it is NULL, '
        'so check_monitoring does not attempt a Jan-1 catch-up on every '
        'freshly enrolled region (which exceeds the heartbeat window).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--year', type=int, default=date.today().year,
            help='Monitoring year to seed (default: current year).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Show planned changes without writing.',
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        year = opts['year']
        dry_run = opts['dry_run']
        today = date.today()

        tasks = list(
            MonitoringTask.objects
            .filter(year=year, status='active', last_date_to__isnull=True)
            .select_related('region')
        )
        if not tasks:
            self.stdout.write(self.style.SUCCESS(
                f'No NULL last_date_to active tasks for year={year}.'
            ))
            return

        fallback_end = _last_available_chunk_end(today, year)
        self.stdout.write(
            f'Found {len(tasks)} task(s) to seed. '
            f'Fallback chunk_end={fallback_end}. dry_run={dry_run}'
        )

        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)

        from_data = 0
        from_fallback = 0
        skipped = 0

        for task in tasks:
            latest = (
                VegetationIndex.objects
                .filter(
                    farmland__district__region_id=task.region_id,
                    index_type='ndvi',
                    is_outlier=False,
                    acquired_date__gte=year_start,
                    acquired_date__lte=year_end,
                    scene__satellite__in=('modis_terra', 'modis_aqua'),
                )
                .aggregate(latest=Max('acquired_date'))
                .get('latest')
            )

            if latest:
                chosen = _snap_to_chunk_end(latest, year)
                from_data += 1
                source = f'data(max={latest})'
            elif fallback_end:
                chosen = fallback_end
                from_fallback += 1
                source = 'fallback'
            else:
                skipped += 1
                self.stdout.write(self.style.WARNING(
                    f'  - {task.region.name}: no data and no fallback, leaving NULL'
                ))
                continue

            self.stdout.write(
                f'  - {task.region.name:<45s} -> {chosen}  ({source})'
            )
            if not dry_run:
                task.last_date_to = chosen
                task.save(update_fields=['last_date_to'])

        self.stdout.write(self.style.SUCCESS(
            f'seed_monitoring_last_date_to year={year} dry_run={dry_run}: '
            f'from_data={from_data}  from_fallback={from_fallback}  '
            f'skipped={skipped}'
        ))
