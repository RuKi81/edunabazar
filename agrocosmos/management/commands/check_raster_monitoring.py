from __future__ import annotations

"""Оперативный мониторинг S2+L8: добирает новые сцены для активных задач.

Предназначен для cron (раз в сутки). Для каждой активной
``MonitoringTask(task_type='raster')`` вычисляет окно
``[last_date_to+1 .. today - AVAILABILITY_LAG_DAYS]`` и при непустом окне
последовательно запускает:

    1. ``fetch_raster_ndvi --sensor s2 --date-from X --date-to Y``
    2. ``fetch_raster_ndvi --sensor l8 --date-from X --date-to Y``
    3. ``compute_fused_ndvi --year <task.year> --overwrite``
    4. ``ndvi_postprocess --year <task.year> --source fused``

Если данных скачано >0 — ``task.last_date_to`` сдвигается; иначе пауза
до следующего запуска (возможно, сцены ещё не опубликованы Copernicus/USGS).

Пример:
    python manage.py check_raster_monitoring
    python manage.py check_raster_monitoring --force --dry-run
    python manage.py check_raster_monitoring --task-id 7
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

# Sentinel-2 L2A обычно в Copernicus CDSE через 1-3 сут, Landsat Collection 2
# Level-2 через 3-7 сут. Берём 7 — консервативно.
AVAILABILITY_LAG_DAYS = 7
LOG_MAX_CHARS = 10_000


class Command(BaseCommand):
    help = 'Проверка оперативных задач мониторинга NDVI (Sentinel-2 + Landsat).'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true',
                            help='Только показать, что будет сделано')
        parser.add_argument('--force', action='store_true',
                            help='Не учитывать lag публикации сцен')
        parser.add_argument('--task-id', type=int, default=None,
                            help='Обработать ровно одну задачу по id')
        parser.add_argument('--min-valid', type=float, default=0.7)

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        dry = options['dry_run']
        force = options['force']
        task_id = options['task_id']
        min_valid = options['min_valid']
        today = date.today()

        qs = MonitoringTask.objects.filter(
            task_type='raster', status='active',
        ).select_related('region', 'district')
        if task_id:
            qs = qs.filter(pk=task_id)

        tasks = list(qs)
        if not tasks:
            self.stdout.write('No active raster monitoring tasks.')
            return
        self.stdout.write(f'Found {len(tasks)} raster task(s), today={today}')

        for task in tasks:
            self._process_task(task, today, dry, force, min_valid)

    # ------------------------------------------------------------------ per-task

    def _process_task(self, task: MonitoringTask, today: date,
                      dry: bool, force: bool, min_valid: float) -> None:
        region = task.region
        district = task.district
        year = task.year
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        scope = f'{region.name}' + (f' / {district.name}' if district else '')

        if task.last_date_to and task.last_date_to >= year_end:
            task.status = 'completed'
            task.save(update_fields=['status'])
            self.stdout.write(f'  [{scope} {year}] year complete — marking completed.')
            return

        window_from = (task.last_date_to + timedelta(days=1)) if task.last_date_to else year_start
        if window_from > year_end:
            self.stdout.write(f'  [{scope} {year}] next window past year end — done.')
            task.status = 'completed'
            task.save(update_fields=['status'])
            return

        hard_cap = today if force else today - timedelta(days=AVAILABILITY_LAG_DAYS)
        window_to = min(year_end, hard_cap)

        if window_to < window_from:
            self.stdout.write(
                f'  [{scope} {year}] no new data yet '
                f'(window_from={window_from}, cap={window_to}).'
            )
            task.last_check = timezone.now()
            task.save(update_fields=['last_check'])
            return

        self.stdout.write(
            f'  [{scope} {year}] window {window_from}..{window_to}'
            + (' [DRY RUN]' if dry else '')
        )
        if dry:
            return

        total_records = 0
        log_entry = f'\n[{timezone.now():%Y-%m-%d %H:%M}] window {window_from}..{window_to}: '
        t0 = time.time()

        try:
            for sensor in ('s2', 'l8'):
                kwargs = {
                    'sensor': sensor,
                    'date_from': window_from.isoformat(),
                    'date_to': window_to.isoformat(),
                    'min_valid_ratio': min_valid,
                }
                if district:
                    kwargs['district_id'] = district.pk
                else:
                    kwargs['region_id'] = region.pk

                out = StringIO()
                call_command('fetch_raster_ndvi', stdout=out, stderr=out, **kwargs)
                text = out.getvalue()
                records = 0
                for line in text.splitlines():
                    if 'records saved' in line.lower():
                        try:
                            token = line.split('records saved')[0].split()[-1]
                            records += int(''.join(c for c in token if c.isdigit()) or 0)
                        except (ValueError, IndexError):
                            pass
                total_records += records
                log_entry += f'{sensor.upper()}={records} '

            # Full-year fusion & postprocess (идемпотентно, ~1-2 мин)
            fusion_kwargs = {'year': year, 'overwrite': True}
            if district:
                fusion_kwargs['district_id'] = district.pk
            else:
                fusion_kwargs['region_id'] = region.pk
            call_command('compute_fused_ndvi', **fusion_kwargs)

            call_command(
                'ndvi_postprocess',
                region_id=region.pk, year=year, source='fused',
            )

            elapsed = time.time() - t0
            log_entry += f'fusion+pp in {elapsed:.0f}s'
            task.records_total = (task.records_total or 0) + total_records
            task.last_check = timezone.now()
            if total_records > 0:
                task.last_date_to = window_to
            else:
                log_entry += ' [no new data, will retry]'
            if window_to >= year_end:
                task.status = 'completed'
            task.log = (task.log + log_entry)[-LOG_MAX_CHARS:]
            task.save()
            self.stdout.write(
                f'    → {total_records} records in {elapsed:.0f}s '
                f'(last_date_to={task.last_date_to})'
            )

        except Exception as exc:
            logger.exception('check_raster_monitoring error for %s %s', scope, year)
            log_entry += f'ERROR: {exc}'
            task.log = (task.log + log_entry)[-LOG_MAX_CHARS:]
            task.last_check = timezone.now()
            task.save(update_fields=['log', 'last_check'])
            self.stderr.write(f'    → ERROR: {exc}')
