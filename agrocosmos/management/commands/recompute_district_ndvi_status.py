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

Per-region execution
--------------------
Historically the SQL ran in one shot across all ~85 regions / ~25M VI
rows of the last 60 days. On a growing dataset that single statement
eventually exceeded the 60-minute statement_timeout and the entire
batch-tail refresh failed — taking the timeline, the choropleth and
the snapshot prewarm down with it. We now drive the SAME CTE one
region at a time (filtered by ``agro_farmland.region_id``), each in
its own short transaction. A single slow region can no longer poison
the whole refresh, and a per-region failure leaves the other 84 ok.
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
      AND f.region_id = %(region_id)s
    GROUP BY f.district_id
),
current_ndvi AS (
    SELECT  l.district_id,
            l.latest_date,
            SUM(vi.mean * f.area_ha) / NULLIF(SUM(f.area_ha), 0) AS w_ndvi,
            -- Number of distinct farmlands in this district that
            -- actually contributed a valid VI row to the weighted
            -- mean above. Surfaced in the admin overlay so the
            -- operator can spot districts coloured by a handful of
            -- fields after a cloudy composite.
            COUNT(DISTINCT vi.farmland_id) AS fl_with_data
    FROM   latest_per_district l
    JOIN   agro_farmland f ON f.district_id = l.district_id
                          AND f.region_id = %(region_id)s
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
-- Total farmlands per district (denominator of the coverage ratio).
-- Counted independently of any composite / date so the figure is
-- stable even when ingestion is patchy. Restricted to districts that
-- ended up with current NDVI data so we don't compute counts the
-- INSERT below would discard anyway.
farmlands_total AS (
    SELECT f.district_id, COUNT(*) AS total
    FROM   agro_farmland f
    WHERE  f.region_id = %(region_id)s
      AND  f.district_id IN (SELECT district_id FROM current_ndvi)
    GROUP BY f.district_id
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
         baseline_ndvi, pct_of_baseline,
         farmlands_with_data, farmlands_total,
         computed_at)
SELECT  cn.district_id,
        cn.latest_date,
        cn.w_ndvi,
        mb.baseline_ndvi,
        CASE
            WHEN mb.baseline_ndvi IS NOT NULL AND mb.baseline_ndvi > 0.05
            THEN ROUND((cn.w_ndvi / mb.baseline_ndvi * 100.0)::numeric, 1)
            ELSE NULL
        END,
        COALESCE(cn.fl_with_data, 0),
        COALESCE(ft.total, 0),
        NOW()
FROM    current_ndvi cn
LEFT JOIN matched_baseline mb  USING (district_id)
LEFT JOIN farmlands_total  ft  USING (district_id)
WHERE   cn.w_ndvi IS NOT NULL
ON CONFLICT (district_id) DO UPDATE
SET     latest_date          = EXCLUDED.latest_date,
        current_ndvi         = EXCLUDED.current_ndvi,
        baseline_ndvi        = EXCLUDED.baseline_ndvi,
        pct_of_baseline      = EXCLUDED.pct_of_baseline,
        farmlands_with_data  = EXCLUDED.farmlands_with_data,
        farmlands_total      = EXCLUDED.farmlands_total,
        computed_at          = EXCLUDED.computed_at
"""


class Command(BaseCommand):
    help = (
        'Recompute the cached per-district NDVI status table '
        '(agro_district_ndvi_status) used by the all-Russia choropleth.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--statement-timeout', type=int, default=900_000,
            help='Per-region PostgreSQL statement_timeout in milliseconds '
                 '(default 900_000 = 15 min). Each region is processed in '
                 'its own short transaction so a single slow region cannot '
                 'time out the whole refresh.',
        )
        parser.add_argument(
            '--region-id', type=int, default=None,
            help='Recompute only this region (used by the single-region '
                 'modis_ndvi pipeline). Default: iterate over all regions.',
        )
        parser.add_argument(
            '--prewarm-recent', type=int, default=4,
            help='After the upsert, pre-build per-date timeline snapshots '
                 'for the N most recent MODIS composites of the current '
                 'year so users never wait on a cold cache for fresh dates. '
                 '0 disables. Default: 4 (~2 months of MODIS biweekly data).',
        )

    def handle(self, *args, **opts):
        from agrocosmos.models import Region

        timeout_ms = int(opts['statement_timeout'])
        only_region = opts.get('region_id')

        # Build the list of regions to process. We restrict to regions
        # that actually have farmlands attached — empty regions would
        # just do a no-op join and waste a round-trip.
        regions_qs = Region.objects.all()
        if only_region:
            regions_qs = regions_qs.filter(pk=only_region)
        else:
            regions_qs = regions_qs.filter(farmlands__isnull=False).distinct()
        regions = list(regions_qs.order_by('name').values_list('id', 'name'))

        self.stdout.write(
            f'Recomputing district NDVI status for {len(regions)} region(s) '
            f'(per-region statement_timeout = {timeout_ms} ms)'
        )

        t_all = time.time()
        total_rows = 0
        ok = 0
        failed: list[tuple[int, str, str]] = []  # (region_id, name, error)

        for region_id, region_name in regions:
            t = time.time()
            try:
                # Each region in its own transaction. SET LOCAL is
                # transaction-scoped, so wrapping is mandatory here.
                # ``max_parallel_workers_per_gather = 0`` defuses the
                # known shared-memory pressure on VM2 — see
                # ARCHITECTURE.md / Postgres /dev/shm note. Per-region
                # the SQL is small enough that parallelism gives no
                # measurable speedup anyway.
                with transaction.atomic():
                    with connection.cursor() as cur:
                        cur.execute(f'SET LOCAL statement_timeout = {timeout_ms}')
                        cur.execute(
                            'SET LOCAL max_parallel_workers_per_gather = 0'
                        )
                        cur.execute(
                            _RECOMPUTE_SQL, {'region_id': region_id}
                        )
                        rowcount = cur.rowcount
                elapsed = time.time() - t
                total_rows += rowcount
                ok += 1
                self.stdout.write(
                    f'  [{region_name}] {rowcount} rows in {elapsed:.1f}s'
                )
            except Exception as exc:  # noqa: BLE001 — log + continue
                elapsed = time.time() - t
                failed.append((region_id, region_name, str(exc)))
                self.stderr.write(self.style.WARNING(
                    f'  [{region_name}] FAILED after {elapsed:.1f}s: {exc}'
                ))

        elapsed_all = time.time() - t_all
        self.stdout.write(self.style.SUCCESS(
            f'agro_district_ndvi_status: {total_rows} rows upserted across '
            f'{ok}/{len(regions)} region(s) in {elapsed_all:.1f}s'
        ))
        if failed:
            self.stderr.write(self.style.WARNING(
                f'  {len(failed)} region(s) failed: '
                + ', '.join(name for _, name, _ in failed)
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
                    # ``force=True``: snapshots for the freshest composites
                    # may have been cached *during* the ingest pipeline,
                    # while ``agro_vegetation_index`` still held only a
                    # subset of regions. The 60-day carry-forward in
                    # ``build_snapshot`` would then have backfilled those
                    # missing regions with stale values from older
                    # composites and stored the partial snapshot
                    # eternally (``timeout=None``). Past-cycle dates are
                    # truly immutable, but the last few are not — we
                    # always rebuild them at the end of the pipeline so
                    # the slider stays consistent with the always-on
                    # choropleth.
                    built, skipped, elapsed = svc.prewarm_snapshots(
                        recent, force=True,
                    )
                    self.stdout.write(self.style.SUCCESS(
                        f'timeline prewarm ({year}, last {len(recent)}, '
                        f'force=True): {built} built, {skipped} cached '
                        f'in {elapsed:.1f}s'
                    ))
                else:
                    self.stdout.write(
                        f'timeline prewarm: no dates for {year}, skipped'
                    )
            except Exception as exc:
                self.stderr.write(self.style.WARNING(
                    f'  timeline prewarm failed (non-fatal): {exc}'
                ))
