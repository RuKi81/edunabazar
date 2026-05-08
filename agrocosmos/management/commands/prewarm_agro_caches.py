"""Pre-warm Redis-backed Agrocosmos caches after a deploy.

The all-Russia districts choropleth (``/agrocosmos/api/districts/status/``)
needs ~20 s on a cold cache to build the GeoJSON. If we let the first
unlucky user trigger this rebuild post-deploy, they sit on a blocked
gunicorn thread for 20 s — and during traffic bursts that's enough to
queue everyone else behind them.

This command rebuilds the cache itself, then exits, so by the time
nginx routes real traffic to the freshly deployed container the hot
path is sub-millisecond.

With ``--with-timeline`` it additionally builds the per-date snapshots
consumed by the timeline slider on the dashboard. Each snapshot is
~10-20 s of SQL on cold cache; with ~23 MODIS biweekly composites per
year this can take ~30 minutes for a 5-year window. Run it manually
on deploy days when you want zero-wait timeline UX, or schedule it
nightly.

It does NOT recompute ``agro_district_ndvi_status`` (that's a 35-min
SQL job done daily by ``recompute_district_ndvi_status``). It only
re-serialises the existing rows into GeoJSON / per-date snapshots.
"""
from __future__ import annotations

import time
from datetime import date

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        'Refresh Redis-backed Agrocosmos caches: the all-Russia districts '
        'status GeoJSON, and (with --with-timeline) per-date timeline '
        'snapshots. Safe to run repeatedly; idempotent.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--with-timeline', action='store_true',
            help='Also pre-build per-date snapshots used by the timeline '
                 'slider. Adds ~10-20 s × N dates of work.',
        )
        parser.add_argument(
            '--years', type=str, default='',
            help='Comma-separated years to prewarm timeline snapshots for. '
                 'Defaults to the current year only (use "all" to span every '
                 'year that has MODIS NDVI data).',
        )
        parser.add_argument(
            '--limit-recent', type=int, default=0,
            help='When prewarming a single year, only build the N most '
                 'recent dates (0 = all). Useful for nightly jobs.',
        )
        parser.add_argument(
            '--force', action='store_true',
            help='Rebuild snapshots even if Redis already has them.',
        )

    def handle(self, *args, **opts):
        from agrocosmos.services import districts_status_geojson

        # 1. Always-on choropleth GeoJSON.
        t = time.time()
        try:
            payload = districts_status_geojson.refresh_cache()
        except Exception as exc:
            # Never fail a deploy on a cache prewarm — the view falls
            # back to inline rebuild on miss.
            self.stderr.write(self.style.ERROR(
                f'districts_status_geojson prewarm failed: {exc}'
            ))
            return

        self.stdout.write(self.style.SUCCESS(
            f'districts_status_geojson: {len(payload["features"])} features '
            f'cached in {time.time() - t:.1f}s'
        ))

        # 2. Optional per-date timeline snapshots.
        if not opts['with_timeline']:
            return

        years = self._resolve_years(opts['years'], districts_status_geojson)
        limit_recent = max(0, int(opts['limit_recent'] or 0))
        force = bool(opts['force'])

        for year in years:
            # The list-of-dates cache may be stale (1 h TTL) — bust it so
            # any composite ingested in the last hour is included.
            districts_status_geojson.invalidate_available_dates(year)
            dates = districts_status_geojson.list_available_dates(year)
            if limit_recent and len(dates) > limit_recent:
                dates = dates[-limit_recent:]
            if not dates:
                self.stdout.write(f'timeline {year}: no MODIS dates, skipped')
                continue

            built, skipped, elapsed = districts_status_geojson.prewarm_snapshots(
                dates, force=force,
            )
            self.stdout.write(self.style.SUCCESS(
                f'timeline {year}: {built} built, {skipped} cached '
                f'(of {len(dates)} dates) in {elapsed:.1f}s'
            ))

    def _resolve_years(self, raw: str, svc) -> list[int]:
        """Parse the ``--years`` argument into a concrete list of years."""
        raw = (raw or '').strip().lower()
        if not raw:
            return [date.today().year]
        if raw == 'all':
            # Years that actually have MODIS data, derived the cheap way
            # via SatelliteScene MIN/MAX (same source as the dashboard year
            # picker). We import lazily to avoid pulling Django ORM into
            # module import time.
            from django.db.models import Min, Max
            from agrocosmos.models import SatelliteScene
            agg = (SatelliteScene.objects
                   .filter(satellite__in=('modis_terra', 'modis_aqua'))
                   .aggregate(first=Min('acquired_date'),
                              last=Max('acquired_date')))
            if not agg['first'] or not agg['last']:
                return [date.today().year]
            return list(range(agg['first'].year, agg['last'].year + 1))
        out = []
        for chunk in raw.split(','):
            chunk = chunk.strip()
            if chunk.isdigit():
                out.append(int(chunk))
        return out or [date.today().year]
