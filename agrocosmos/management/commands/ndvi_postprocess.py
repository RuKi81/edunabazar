"""
Post-processing of NDVI time series: spike detection + Savitzky-Golay smoothing.

Runs per-farmland across a region/year. For each farmland:
1. Load NDVI time series sorted by date
2. Detect spikes (anomalies) using a rolling-median absolute deviation test
3. Apply Savitzky-Golay filter to the clean series
4. Save is_outlier and mean_smooth back to DB

Usage:
    python manage.py ndvi_postprocess --region-id 37 --year 2024
    python manage.py ndvi_postprocess --region-id 37 --year 2024 --source modis
    python manage.py ndvi_postprocess --region-id 37 --year 2024 --source raster
    python manage.py ndvi_postprocess --region-id 37 --year 2024 --source fused
    python manage.py ndvi_postprocess --region-id 37  # all years
"""
import time
from itertools import groupby
from operator import itemgetter

import numpy as np
from django.core.management.base import BaseCommand
from django.db import connection

from agrocosmos.models import Farmland, VegetationIndex

# Spike detection: if |value - rolling_median| > threshold, mark as anomaly
SPIKE_THRESHOLD = 0.15  # NDVI units
ROLLING_WINDOW = 3      # half-window for rolling median (±3 points)

# Savitzky-Golay parameters
SG_WINDOW = 5           # must be odd, ≥ 5 for cubic
SG_POLYORDER = 3

MODIS_SATELLITES = ('modis_terra', 'modis_aqua')
RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')
FUSED_SATELLITES = ('hls_fused',)

DB_BATCH = 5000  # raw SQL update batch


def _process_series(pks, vals, threshold):
    """Spike detection + Savitzky-Golay for one farmland. Returns list of (pk, is_outlier, mean_smooth)."""
    from scipy.signal import savgol_filter

    n = len(vals)
    # --- Spike detection via rolling median ---
    is_spike = np.zeros(n, dtype=bool)
    for j in range(n):
        lo = max(0, j - ROLLING_WINDOW)
        hi = min(n, j + ROLLING_WINDOW + 1)
        local_median = np.nanmedian(vals[lo:hi])
        if abs(vals[j] - local_median) > threshold:
            is_spike[j] = True

    # --- Savitzky-Golay smoothing on clean values ---
    clean_vals = vals.copy()
    clean_vals[is_spike] = np.nan

    nans = np.isnan(clean_vals)
    if nans.all():
        smoothed = np.full(n, np.nan)
    else:
        if nans.any():
            good = ~nans
            xp = np.where(good)[0]
            fp = clean_vals[good]
            clean_vals = np.interp(np.arange(n), xp, fp)

        win = min(SG_WINDOW, n)
        if win % 2 == 0:
            win -= 1
        if win < 3:
            smoothed = clean_vals
        else:
            poly = min(SG_POLYORDER, win - 1)
            smoothed = savgol_filter(clean_vals, win, poly)
            smoothed = np.clip(smoothed, -0.2, 1.0)

    results = []
    for j, pk in enumerate(pks):
        s = None if np.isnan(smoothed[j]) else round(float(smoothed[j]), 4)
        results.append((pk, bool(is_spike[j]), s))
    return results, int(is_spike.sum())


class Command(BaseCommand):
    help = 'NDVI post-processing: spike detection + Savitzky-Golay smoothing'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, required=True)
        parser.add_argument('--year', type=int, help='Year (optional, processes all years if omitted)')
        parser.add_argument('--source', type=str,
                            choices=['modis', 'raster', 'fused'],
                            help='Filter by satellite source')
        parser.add_argument('--threshold', type=float, default=SPIKE_THRESHOLD,
                            help=f'Spike threshold (default: {SPIKE_THRESHOLD})')

    def handle(self, *args, **options):
        region_id = options['region_id']
        year = options.get('year')
        source = options.get('source')
        threshold = options['threshold']

        # Build raw SQL to load all records in ONE query, sorted for groupby
        where_parts = [
            "vi.index_type = 'ndvi'",
            "f.district_id IN (SELECT id FROM agro_district WHERE region_id = %s)",
        ]
        params = [region_id]

        if source == 'modis':
            where_parts.append("sc.satellite IN ('modis_terra', 'modis_aqua')")
        elif source == 'raster':
            where_parts.append("sc.satellite IN ('sentinel2', 'landsat8', 'landsat9')")
        elif source == 'fused':
            where_parts.append("sc.satellite IN ('hls_fused')")

        if year:
            where_parts.append("EXTRACT(year FROM vi.acquired_date) = %s")
            params.append(year)

        where_clause = " AND ".join(where_parts)

        sql = f"""
            SELECT vi.id, vi.farmland_id, vi.mean
            FROM agro_vegetation_index vi
            JOIN agro_farmland f ON f.id = vi.farmland_id
            JOIN agro_satellite_scene sc ON sc.id = vi.scene_id
            WHERE {where_clause}
            ORDER BY vi.farmland_id, vi.acquired_date
        """

        self.stdout.write(f'Loading records from DB...')
        t0 = time.time()

        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()  # (vi_id, farmland_id, mean)

        elapsed = time.time() - t0
        self.stdout.write(f'Loaded {len(rows)} records in {elapsed:.1f}s')

        if not rows:
            return

        # Group by farmland_id and process
        anomalies_total = 0
        smoothed_total = 0
        update_batch = []  # [(pk, is_outlier_bool, mean_smooth_float_or_None), ...]
        fl_count = 0

        for fl_id, group in groupby(rows, key=itemgetter(1)):
            records = list(group)
            if len(records) < 3:
                continue

            pks = [r[0] for r in records]
            vals = np.array([r[2] for r in records], dtype=np.float64)

            results, n_anom = _process_series(pks, vals, threshold)
            anomalies_total += n_anom
            smoothed_total += len(results)
            update_batch.extend(results)

            fl_count += 1

            # Flush batch via raw SQL for speed
            if len(update_batch) >= DB_BATCH:
                _flush_updates(update_batch)
                update_batch = []
                self.stdout.write(f'  {smoothed_total} records processed, {fl_count} farmlands...')

        # Final flush
        if update_batch:
            _flush_updates(update_batch)

        self.stdout.write(
            f'\nDone: {smoothed_total} records smoothed, '
            f'{anomalies_total} anomalies detected, '
            f'{fl_count} farmlands'
        )


def _flush_updates(batch):
    """Bulk update is_outlier + mean_smooth via raw SQL with unnest for speed."""
    if not batch:
        return

    pks = [r[0] for r in batch]
    anomalies = [r[1] for r in batch]
    smooths = [r[2] for r in batch]

    sql = """
        UPDATE agro_vegetation_index AS vi SET
            is_outlier  = data.is_outlier,
            mean_smooth = data.mean_smooth
        FROM (
            SELECT
                unnest(%s::bigint[])  AS id,
                unnest(%s::boolean[]) AS is_outlier,
                unnest(%s::float8[])  AS mean_smooth
        ) AS data
        WHERE vi.id = data.id
    """
    with connection.cursor() as cur:
        cur.execute(sql, [pks, anomalies, smooths])
