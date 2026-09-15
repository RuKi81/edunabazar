"""Idempotently put every Region on operational MODIS monitoring.

The daily ``check_monitoring`` cron only walks ``MonitoringTask`` rows
with ``status='active'``. Historically those were created one-by-one
through the admin panel — which means a freshly added region (or a
fresh year for an existing one) silently skipped operational coverage
until somebody clicked through the UI.

This command makes the desired state explicit: for the target year,
every ``Region`` that has a farmland vector gets an ``active``
region-level MODIS ``MonitoringTask``. Farmland-less subjects (Москва,
Санкт-Петербург, Севастополь) are deliberately skipped — the pipeline
would only burn GEE requests and write nothing.
It is safe to re-run — existing rows are left in place unless they
were ``paused`` (then we reactivate, which mirrors the admin's "resume"
button).

Designed to run:

* on every deploy (CI/CD post-deploy hook), so a new region picked up
  from a fresh OSM import is auto-enrolled;
* once on Jan 1st each year (cron), so the year-rollover doesn't drop
  any subject until somebody notices.

Manual usage::

    python manage.py ensure_all_regions_monitored
    python manage.py ensure_all_regions_monitored --year 2027
    python manage.py ensure_all_regions_monitored --no-reactivate
"""
from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Exists, OuterRef

from agrocosmos.models import Farmland, MonitoringTask, Region


class Command(BaseCommand):
    help = (
        'Ensure every Region has an active MonitoringTask for the given year. '
        'Idempotent; safe to run on every deploy.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--year', type=int, default=date.today().year,
            help='Monitoring year to enrol regions into (default: current year).',
        )
        parser.add_argument(
            '--no-reactivate', action='store_true',
            help='Do NOT flip paused tasks back to active. By default we do.',
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        year = opts['year']
        reactivate = not opts['no_reactivate']

        # Only regions that actually have a farmland vector are enrolled.
        # ``modis_ndvi`` downloads the GEE composite BEFORE it loads
        # farmlands, so a farmland-less subject (Москва, Санкт-Петербург,
        # Севастополь — no agricultural land in the ЗСН layer) spent a
        # GEE request every night and then wrote 0 rows forever. The
        # predicate mirrors ``modis_ndvi._load_farmlands``.
        eligible = Region.objects.filter(
            Exists(Farmland.objects.filter(district__region=OuterRef('pk')))
        )
        regions = list(eligible.only('id', 'name'))
        total = len(regions)
        skipped_empty = Region.objects.count() - total
        if not total:
            self.stdout.write(self.style.WARNING(
                'No regions with farmlands in DB — nothing to do.'))
            return

        # Bulk-fetch existing tasks for the target year to avoid N queries.
        # Scoped to region-level MODIS rows — exactly what we enrol below.
        # Without the scope a region that only had a ``raster`` task (or a
        # district-scoped one) looked "already covered" and silently never
        # got its MODIS enrolment.
        existing = {
            t.region_id: t for t in
            MonitoringTask.objects
            .filter(
                year=year,
                task_type=MonitoringTask.TaskType.MODIS,
                district__isnull=True,
            )
            .only('id', 'region_id', 'status')
        }

        created = 0
        reactivated = 0
        already_active = 0
        already_completed = 0
        still_paused = 0

        for region in regions:
            task = existing.get(region.pk)
            if task is None:
                MonitoringTask.objects.create(
                    region=region, year=year, status='active',
                )
                created += 1
                continue

            if task.status == 'active':
                already_active += 1
            elif task.status == 'completed':
                already_completed += 1
            elif task.status == 'paused':
                if reactivate:
                    task.status = 'active'
                    task.save(update_fields=['status'])
                    reactivated += 1
                else:
                    still_paused += 1
            else:
                # Unknown status — leave alone, just count
                self.stdout.write(self.style.WARNING(
                    f'  {region.name}: unknown status={task.status!r}, skipping'
                ))

        self.stdout.write(self.style.SUCCESS(
            f'ensure_all_regions_monitored year={year}: '
            f'regions={total}  created={created}  reactivated={reactivated}  '
            f'already_active={already_active}  '
            f'already_completed={already_completed}  paused_kept={still_paused}  '
            f'skipped_no_farmlands={skipped_empty}'
        ))
