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
from django.db import connection


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
            '--statement-timeout', type=int, default=900_000,
            help='PostgreSQL statement_timeout in milliseconds '
                 '(default 900_000 = 15 min).',
        )

    def handle(self, *args, **opts):
        timeout_ms = opts['statement_timeout']
        t = time.time()
        with connection.cursor() as cur:
            # Bound the worst-case so a runaway query can't lock a connection
            # forever — refresh is idempotent and can be retried later.
            cur.execute(f'SET LOCAL statement_timeout = {int(timeout_ms)}')
            cur.execute(_RECOMPUTE_SQL)
            rowcount = cur.rowcount

        elapsed = time.time() - t
        self.stdout.write(self.style.SUCCESS(
            f'agro_district_ndvi_status: {rowcount} rows upserted in {elapsed:.1f}s'
        ))
