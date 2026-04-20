"""
Check active monitoring tasks and run NDVI pipeline for new 16-day periods.

Designed to be called daily via cron/systemd timer:
    docker compose exec -T web python manage.py check_monitoring

Logic:
- For each MonitoringTask with status='active':
  1. Determine the next 16-day period to process
  2. If today >= next period end + lag (data available), run pipeline
  3. Update task.last_date_to and task.last_check

Pipeline: ``modis_ndvi`` (MOD13Q1+MYD13Q1 16-day composites, ~7 day lag
after the compositing window ends).
"""
import logging
import time
from datetime import date, timedelta
from io import StringIO

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone

from agrocosmos.models import MonitoringTask

logger = logging.getLogger('agrocosmos')

NDVI_COMMAND = 'modis_ndvi'
AVAILABILITY_LAG_DAYS = 7  # MOD13Q1 composites available ~7 days after period ends

CHUNK_DAYS = 16  # MOD13Q1 composite cadence


def _next_aligned_period(last_date_to, year):
    """
    Compute the next 16-day processing window aligned to Jan 1 of ``year``.

    Must match the grid used by ``_biweekly_chunks`` in
    ``satellite_modis_raster.py`` (both anchored to Jan 1).

    Args:
        last_date_to: last processed period end (``date`` or ``None``)
        year: int, the monitoring year

    Returns:
        tuple (next_from, next_to) of ``date`` objects.
    """
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)

    if last_date_to:
        # Find which 16-day chunk last_date_to falls in, then advance to the next.
        days_since = (last_date_to - year_start).days
        current_chunk_idx = days_since // CHUNK_DAYS
        next_from = year_start + timedelta(days=(current_chunk_idx + 1) * CHUNK_DAYS)
    else:
        next_from = year_start

    next_to = min(next_from + timedelta(days=CHUNK_DAYS - 1), year_end)
    return next_from, next_to


class Command(BaseCommand):
    help = 'Check active monitoring tasks and fetch new NDVI data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Show what would be done without actually running',
        )
        parser.add_argument(
            '--force', action='store_true',
            help='Force run even if period is not yet due',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        force = options['force']
        today = date.today()

        tasks = MonitoringTask.objects.filter(
            status='active',
        ).select_related('region')

        if not tasks.exists():
            self.stdout.write('No active monitoring tasks.')
            return

        self.stdout.write(f'Found {tasks.count()} active task(s), today={today}')

        for task in tasks:
            self._process_task(task, today, dry_run, force)

    def _process_task(self, task, today, dry_run, force):
        region = task.region
        year = task.year

        year_end = date(year, 12, 31)

        # If we've already completed the year, mark as completed
        if task.last_date_to and task.last_date_to >= year_end:
            task.status = 'completed'
            task.save()
            self.stdout.write(
                f'  [{region.name} {year}] Year complete, marking as completed.'
            )
            return

        # Process ALL available periods in one run (catch-up from year start)
        periods_done = 0
        while True:
            next_from, next_to = _next_aligned_period(task.last_date_to, year)

            # Don't process future dates (even with --force)
            if next_from > today:
                self.stdout.write(
                    f'  [{region.name} {year}] Next period {next_from} is in the future. Stop.'
                )
                break

            # Check if data should be available (--force skips this check)
            data_available_date = next_to + timedelta(days=AVAILABILITY_LAG_DAYS)
            if today < data_available_date and not force:
                self.stdout.write(
                    f'  [{region.name} {year}] Period {next_from}..{next_to}: '
                    f'data available after {data_available_date}. Stop.'
                )
                break

            self.stdout.write(
                f'  [{region.name} {year}] Processing {next_from}..{next_to}'
            )

            if dry_run:
                self.stdout.write('    → DRY RUN, skipping.')
                # Simulate advance for dry-run preview
                task.last_date_to = next_to
                periods_done += 1
                if next_to >= year_end:
                    break
                continue

            # Run NDVI pipeline for this specific period
            t0 = time.time()
            out = StringIO()
            try:
                call_command(
                    NDVI_COMMAND,
                    region_id=region.pk,
                    date_from=next_from.isoformat(),
                    date_to=next_to.isoformat(),
                    stdout=out,
                    stderr=out,
                )
                elapsed = time.time() - t0
                log_text = out.getvalue()

                # Parse records count
                records_saved = 0
                for line in log_text.splitlines():
                    if 'Records saved:' in line:
                        try:
                            records_saved = int(line.split('Records saved:')[1].strip())
                        except (ValueError, IndexError):
                            pass

                # Update task
                task.last_check = timezone.now()
                task.records_total += records_saved

                # Append to log (keep last 10K chars)
                entry = f'\n[{timezone.now():%Y-%m-%d %H:%M}] {next_from}..{next_to}: '
                for line in log_text.splitlines():
                    if 'records saved' in line.lower() or 'Done in' in line:
                        entry += line.strip() + ' '
                task.log = (task.log + entry)[-10000:]

                # Only advance last_date_to if data was actually saved;
                # if 0 records — data likely not available yet, retry next time
                if records_saved > 0:
                    task.last_date_to = next_to
                    periods_done += 1
                    self.stdout.write(
                        f'    → {records_saved} records in {elapsed:.0f}s (period {periods_done})'
                    )
                else:
                    self.stdout.write(
                        f'    → 0 records in {elapsed:.0f}s — no data yet, stop.'
                    )
                    task.save()
                    break

                # Check if year is now complete
                if next_to >= year_end:
                    task.status = 'completed'

                task.save()

                if next_to >= year_end:
                    break

            except Exception as e:
                logger.error('check_monitoring error for %s %s: %s', region.name, year, e)
                self.stderr.write(f'    → ERROR: {e}')

                task.last_check = timezone.now()
                task.log = (task.log + f'\n[{timezone.now():%Y-%m-%d %H:%M}] ERROR: {e}')[-10000:]
                task.save()
                break  # Stop on error, will retry next cron run

        self.stdout.write(
            f'  [{region.name} {year}] Finished: {periods_done} period(s) processed, '
            f'total records: {task.records_total}'
        )
