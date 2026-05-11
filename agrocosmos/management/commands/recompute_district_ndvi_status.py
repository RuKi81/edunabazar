"""Recompute the cached per-district NDVI status used by the all-Russia
choropleth (`/agrocosmos/api/districts/status/`).

This is an upsert into ``agro_district_ndvi_status`` driven by a single
PostgreSQL CTE that:

1. Picks the latest MODIS-NDVI composite date that has data for every
   district within the last 60 days.
2. Computes an area-weighted mean NDVI on that date over all farmlands
   of the district.
3. Joins the matching ``agro_ndvi_baseline`` row by DOY (with ±8/±16 day
   fallback) and stores ``pct_of_baseline``.

The query is heavy on cold cache (~5-10 minutes against ~25M rows), but
runs once a day at the tail of the MODIS pipeline, so the user-facing
endpoint just SELECTs ~2200 small rows.
"""
from __future__ import annotations

import time

from django.core.management.base import BaseCommand
from django.db import connection, transaction


_RECOMPUTE_SQL = """
WITH latest_per_district AS (
    SELECT f.district_id, MAX(vi.acquired_date) AS latest_date
    FROM agro_vegetation_index vi
    JOIN agro_farmland f         ON f.id = vi.farmland_id
    JOIN agro_satellite_scene s  ON s.id = vi.scene_id
    WHERE vi.index_type = 'ndvi'
      AND vi.is_outlier = false
      AND vi.mean BETWEEN -0.2 AND 1
      AND s.satellite IN ('modis_terra', 'modis_aqua')
      AND vi.acquired_date >= CURRENT_DATE - INTERVAL '60 days'
      AND f.district_id IS NOT NULL
    GROUP BY f.district_id
),
current_ndvi AS (
    SELECT  l.district_id,
            l.latest_date,
            SUM(vi.mean * f.area_ha) / NULLIF(SUM(f.area_ha), 0) AS w_ndvi
    FROM   latest_per_district l
    JOIN   agro_farmland f ON f.district_id = l.district_id
    JOIN   agro_vegetation_index vi
                ON vi.farmland_id   = f.id
               AND vi.acquired_date = l.latest_date
    JOIN   agro_satellite_scene s ON s.id = vi.scene_id
    WHERE  vi.index_type = 'ndvi'
      AND  vi.is_outlier = false
      AND  vi.mean BETWEEN -0.2 AND 1
      AND  s.satellite IN ('modis_terra', 'modis_aqua')
    GROUP BY l.district_id, l.latest_date
),
matched_baseline AS (
    -- Try the exact DOY first; if missing, fall back to ±8 / ±16 to
    -- tolerate MODIS biweekly composite drift across years. We pick
    -- the closest available DOY via DISTINCT ON.
    SELECT DISTINCT ON (cn.district_id)
           cn.district_id,
           bl.mean_ndvi AS baseline_ndvi
    FROM   current_ndvi cn
    LEFT JOIN agro_ndvi_baseline bl
           ON bl.district_id = cn.district_id
          AND bl.crop_type   = ''
          AND bl.day_of_year BETWEEN
                  EXTRACT(DOY FROM cn.latest_date)::int - 16
              AND EXTRACT(DOY FROM cn.latest_date)::int + 16
    ORDER BY cn.district_id,
             ABS(bl.day_of_year - EXTRACT(DOY FROM cn.latest_date)::int) NULLS LAST
)
INSERT INTO agro_district_ndvi_status
        (district_id, latest_date, current_ndvi,
         baseline_ndvi, pct_of_baseline, computed_at)
SELECT  cn.district_id,
        cn.latest_date,
        cn.w_ndvi,
        mb.baseline_ndvi,
        CASE
            WHEN mb.baseline_ndvi IS NOT NULL AND mb.baseline_ndvi > 0.05
            THEN ROUND((cn.w_ndvi / mb.baseline_ndvi * 100.0)::numeric, 1)
            ELSE NULL
        END,
        NOW()
FROM    current_ndvi cn
LEFT JOIN matched_baseline mb USING (district_id)
WHERE   cn.w_ndvi IS NOT NULL
ON CONFLICT (district_id) DO UPDATE
SET     latest_date     = EXCLUDED.latest_date,
        current_ndvi    = EXCLUDED.current_ndvi,
        baseline_ndvi   = EXCLUDED.baseline_ndvi,
        pct_of_baseline = EXCLUDED.pct_of_baseline,
        computed_at     = EXCLUDED.computed_at
"""


class Command(BaseCommand):
    help = (
        'Recompute the cached per-district NDVI status table '
        '(agro_district_ndvi_status) used by the all-Russia choropleth.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--statement-timeout', type=int, default=3_600_000,
            help='PostgreSQL statement_timeout in milliseconds '
                 '(default 3_600_000 = 60 min). Effective only when wrapped '
                 'in a transaction; under autocommit the session-level '
                 'default applies.',
        )
        parser.add_argument(
            '--prewarm-recent', type=int, default=4,
            help='After the upsert, pre-build per-date timeline snapshots '
                 'for the N most recent MODIS composites of the current '
                 'year so users never wait on a cold cache for fresh dates. '
                 '0 disables. Default: 4 (~2 months of MODIS biweekly data).',
        )

    def handle(self, *args, **opts):
        timeout_ms = opts['statement_timeout']
        t = time.time()
        # Wrap in an explicit transaction so SET LOCAL actually scopes the
        # timeout to this query (under autocommit it would be a no-op).
        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute(f'SET LOCAL statement_timeout = {int(timeout_ms)}')
                cur.execute(_RECOMPUTE_SQL)
                rowcount = cur.rowcount

        elapsed = time.time() - t
        self.stdout.write(self.style.SUCCESS(
            f'agro_district_ndvi_status: {rowcount} rows upserted in {elapsed:.1f}s'
        ))

        # Refresh the cached GeoJSON FeatureCollection that backs the
        # all-Russia choropleth endpoint. Doing it here means the next
        # API hit is a sub-millisecond `cache.get()` instead of a 20s
        # rebuild — important on deploy days when traffic ramps up.
        # Failure must NOT mask the successful upsert above; the view
        # transparently falls back to inline rebuild.
        try:
            from agrocosmos.services import districts_status_geojson
            t2 = time.time()
            payload = districts_status_geojson.refresh_cache()
            self.stdout.write(self.style.SUCCESS(
                f'districts_status GeoJSON cached: {len(payload["features"])} '
                f'features in {time.time() - t2:.1f}s'
            ))
        except Exception as exc:
            self.stderr.write(self.style.WARNING(
                f'  GeoJSON cache refresh failed (non-fatal): {exc}'
            ))

        # Refresh the pre-aggregated district × date × crop NDVI series.
        # Keeps the region-level dashboard chart (``/api/ndvi-stats/``)
        # fast even for huge subjects (Moscow Oblast: ~56 districts ×
        # ~23 composites × 5 crop types instead of millions of raw VI
        # rows). Covers a 70-day window to absorb any late-arriving MODIS
        # composite from the last 60-day look-back used above.
        try:
            from agrocosmos.services import district_ndvi_series
            res = district_ndvi_series.refresh_recent(days=70, source='modis')
            self.stdout.write(self.style.SUCCESS(
                f'district_ndvi_series ({res["source"]} {res["date_from"]}'
                f'..{res["date_to"]}): inserted={res["inserted"]} '
                f'deleted={res["deleted"]} in {res["elapsed_s"]}s'
            ))
        except Exception as exc:
            self.stderr.write(self.style.WARNING(
                f'  district_ndvi_series refresh failed (non-fatal): {exc}'
            ))

        # Pre-build the most recent timeline snapshots so the dashboard
        # slider is instant for the dates users actually scrub through
        # right after a fresh MODIS ingest. Older dates remain lazy —
        # rebuilding them all every day would be wasteful.
        prewarm_recent = int(opts.get('prewarm_recent') or 0)
        if prewarm_recent > 0:
            try:
                from datetime import date as _date
                from agrocosmos.services import districts_status_geojson as svc
                year = _date.today().year
                # Bust the 1 h list-of-dates cache so the composite that
                # was just ingested actually shows up.
                svc.invalidate_available_dates(year)
                dates = svc.list_available_dates(year)
                if dates:
                    recent = dates[-prewarm_recent:]
                    built, skipped, elapsed = svc.prewarm_snapshots(recent)
                    self.stdout.write(self.style.SUCCESS(
                        f'timeline prewarm ({year}, last {len(recent)}): '
                        f'{built} built, {skipped} cached in {elapsed:.1f}s'
                    ))
                else:
                    self.stdout.write(
                        f'timeline prewarm: no dates for {year}, skipped'
                    )
            except Exception as exc:
                self.stderr.write(self.style.WARNING(
                    f'  timeline prewarm failed (non-fatal): {exc}'
                ))
