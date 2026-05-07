"""Pre-warm Redis-backed Agrocosmos caches after a deploy.

The all-Russia districts choropleth (``/agrocosmos/api/districts/status/``)
needs ~20 s on a cold cache to build the GeoJSON. If we let the first
unlucky user trigger this rebuild post-deploy, they sit on a blocked
gunicorn thread for 20 s — and during traffic bursts that's enough to
queue everyone else behind them.

This command rebuilds the cache itself, then exits, so by the time
nginx routes real traffic to the freshly deployed container the hot
path is sub-millisecond.

It does NOT recompute ``agro_district_ndvi_status`` (that's a 35-min
SQL job done daily by ``recompute_district_ndvi_status``). It only
re-serialises the existing rows into GeoJSON.
"""
from __future__ import annotations

import time

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        'Refresh Redis-backed Agrocosmos caches (currently the all-Russia '
        'districts status GeoJSON). Safe to run repeatedly; cheap (~20 s).'
    )

    def handle(self, *args, **opts):
        from agrocosmos.services import districts_status_geojson

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
