"""Rebuild or incrementally refresh ``agro_district_ndvi_series``.

This pre-aggregated table (see :class:`agrocosmos.models.DistrictNdviSeries`)
powers the region-level NDVI dashboard chart and reports — without it,
``/agrocosmos/api/ndvi-stats/?region=45`` (Moscow Oblast) scans ~14 M
raw VI rows and times out.

Usage::

    # Daily cron: incremental 60-day window (what
    # ``recompute_district_ndvi_status`` calls internally).
    python manage.py recompute_district_ndvi_series --days 60

    # Deploy-time full rebuild (slow, ~minutes):
    python manage.py recompute_district_ndvi_series --rebuild
"""
from __future__ import annotations

from datetime import date as _date

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = (
        'Populate agro_district_ndvi_series from agro_vegetation_index '
        '(area-weighted per district × date × crop_type).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--days', type=int, default=60,
            help='Incremental window size (days, default 60). Ignored '
                 'when --rebuild or --range is used.',
        )
        parser.add_argument(
            '--rebuild', action='store_true',
            help='Full rebuild across all MODIS history (slow).',
        )
        parser.add_argument(
            '--range', type=str, default='',
            help='Explicit date range "YYYY-MM-DD:YYYY-MM-DD" (inclusive).',
        )
        parser.add_argument(
            '--source', type=str, default='modis',
            choices=('modis', 'raster', 'fused'),
        )

    def handle(self, *args, **opts):
        from agrocosmos.services import district_ndvi_series

        source = opts['source']
        if opts['rebuild']:
            res = district_ndvi_series.rebuild(source=source)
        elif opts['range']:
            raw = opts['range']
            try:
                a, b = raw.split(':', 1)
                d_from = _date.fromisoformat(a.strip())
                d_to = _date.fromisoformat(b.strip())
            except ValueError as e:
                raise CommandError(f'invalid --range {raw!r}: {e}') from e
            res = district_ndvi_series.refresh_range(d_from, d_to, source=source)
        else:
            days = max(1, int(opts['days']))
            res = district_ndvi_series.refresh_recent(days=days, source=source)

        self.stdout.write(self.style.SUCCESS(
            f'{source} {res["date_from"]}..{res["date_to"]}: '
            f'inserted={res["inserted"]} deleted={res["deleted"]} '
            f'in {res["elapsed_s"]}s'
        ))
