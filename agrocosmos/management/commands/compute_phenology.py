"""
Compute phenological metrics (SOS, EOS, POS, LOS, MaxNDVI, MeanNDVI, TI) from smoothed NDVI.

Requires ndvi_postprocess to be run first (populates mean_smooth).

Algorithm (threshold-based, Jönsson & Eklundh, 2002 / TIMESAT):
- SOS = first date when smoothed NDVI crosses 20% of (max - base) above base
- EOS = last date when smoothed NDVI drops below 20% of (max - base)
- POS = date of maximum smoothed NDVI
- LOS = EOS - SOS (days)
- MaxNDVI = peak smoothed value
- MeanNDVI = average smoothed NDVI during SOS..EOS
- TI = trapezoidal integral of NDVI from SOS to EOS

Usage:
    python manage.py compute_phenology --region-id 37 --year 2025
    python manage.py compute_phenology --region-id 37 --year 2025 --source modis
"""
import time
from itertools import groupby
from operator import itemgetter

import numpy as np
from django.core.management.base import BaseCommand
from django.db import connection

SOS_EOS_RATIO = 0.30   # 30% of amplitude above baseline (stricter)
BASE_NDVI = 0.20       # winter dormant NDVI for Crimea (wheat, evergreens)
MIN_DOY_SOS = 32       # earliest SOS = ~February 1 (winter wheat green-up)
MAX_DOY_SOS = 152      # latest  SOS = ~June 1
MIN_DOY_EOS = 152      # earliest EOS = ~June 1
MAX_DOY_EOS = 305      # latest  EOS = ~November 1
MIN_DOY_POS = 60       # earliest POS = ~March 1
MAX_DOY_POS = 244      # latest  POS = ~September 1 (spring peak, NOT autumn)
MAX_LOS_DAYS = 210     # max growing season length (~7 months)

DB_BATCH = 2000


class Command(BaseCommand):
    help = 'Compute phenological metrics from smoothed NDVI time series'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, required=True)
        parser.add_argument('--year', type=int, required=True)
        parser.add_argument('--source', type=str, choices=['modis', 'raster'],
                            default='modis')

    def handle(self, *args, **options):
        region_id = options['region_id']
        year = options['year']
        source = options['source']

        # Build single SQL to load all smoothed series at once
        where_parts = [
            "vi.index_type = 'ndvi'",
            "vi.is_outlier = false",
            "vi.mean_smooth IS NOT NULL",
            "f.district_id IN (SELECT id FROM agro_district WHERE region_id = %s)",
            "EXTRACT(year FROM vi.acquired_date) = %s",
        ]
        params = [region_id, year]

        if source == 'modis':
            where_parts.append("sc.satellite IN ('modis_terra', 'modis_aqua')")
        else:
            where_parts.append("sc.satellite IN ('sentinel2', 'landsat8', 'landsat9')")

        where_clause = " AND ".join(where_parts)

        sql = f"""
            SELECT vi.farmland_id, vi.acquired_date, vi.mean_smooth
            FROM agro_vegetation_index vi
            JOIN agro_farmland f ON f.id = vi.farmland_id
            JOIN agro_satellite_scene sc ON sc.id = vi.scene_id
            WHERE {where_clause}
            ORDER BY vi.farmland_id, vi.acquired_date
        """

        # Delete stale records first — ensures only valid phenology remains
        with connection.cursor() as cur:
            cur.execute("""
                DELETE FROM agro_farmland_phenology p
                USING agro_farmland f, agro_district d
                WHERE p.farmland_id = f.id AND f.district_id = d.id
                  AND d.region_id = %s AND p.year = %s AND p.source = %s
            """, [region_id, year, source])
            deleted = cur.rowcount
        if deleted:
            self.stdout.write(f'Deleted {deleted} old phenology records')

        self.stdout.write(f'Loading smoothed NDVI for region {region_id}, year {year}, source {source}...')
        t0 = time.time()

        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()  # (farmland_id, acquired_date, mean_smooth)

        elapsed = time.time() - t0
        self.stdout.write(f'Loaded {len(rows)} records in {elapsed:.1f}s')

        if not rows:
            return

        # Group by farmland_id and compute phenology
        created = 0
        skipped = 0
        batch = []  # (fl_id, year, source, sos, eos, pos, max, mean, los, ti)

        for fl_id, group in groupby(rows, key=itemgetter(0)):
            records = list(group)
            if len(records) < 5:
                skipped += 1
                continue

            dates = [r[1] for r in records]
            vals = np.array([r[2] for r in records], dtype=np.float64)

            pheno = _compute_phenology(dates, vals)
            if pheno is None:
                skipped += 1
                continue

            batch.append((
                fl_id, year, source,
                pheno['sos'], pheno['eos'], pheno['pos'],
                pheno['max_ndvi'], pheno['mean_ndvi'],
                pheno['los'], pheno['ti'],
            ))
            created += 1

            if len(batch) >= DB_BATCH:
                _flush_phenology(batch)
                batch = []
                self.stdout.write(f'  {created} phenology records saved...')

        if batch:
            _flush_phenology(batch)

        self.stdout.write(
            f'\nDone: {created} phenology records, {skipped} skipped'
        )


def _flush_phenology(batch):
    """Upsert phenology records via raw SQL for speed."""
    if not batch:
        return

    sql = """
        INSERT INTO agro_farmland_phenology
            (farmland_id, year, source, sos_date, eos_date, pos_date,
             max_ndvi, mean_ndvi, los_days, total_ndvi, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (farmland_id, year, source)
        DO UPDATE SET
            sos_date   = EXCLUDED.sos_date,
            eos_date   = EXCLUDED.eos_date,
            pos_date   = EXCLUDED.pos_date,
            max_ndvi   = EXCLUDED.max_ndvi,
            mean_ndvi  = EXCLUDED.mean_ndvi,
            los_days   = EXCLUDED.los_days,
            total_ndvi = EXCLUDED.total_ndvi
    """
    with connection.cursor() as cur:
        cur.executemany(sql, batch)


def _compute_phenology(dates, vals):
    """
    Threshold-based phenology extraction with calendar window constraints
    tuned for Crimea / southern temperate agroclimatic zone.

    Returns dict with sos, eos, pos, max_ndvi, mean_ndvi, los, ti or None.
    """
    doys = [d.timetuple().tm_yday for d in dates]

    # Find POS only within the spring/summer window (Mar–Sep)
    # This prevents selecting the autumn re-greening peak (bimodal winter-wheat pattern)
    best_idx = None
    best_val = -1
    for j in range(len(vals)):
        if MIN_DOY_POS <= doys[j] <= MAX_DOY_POS and vals[j] > best_val:
            best_val = vals[j]
            best_idx = j

    if best_idx is None:
        return None  # no observations in the growing window

    max_ndvi = float(best_val)
    pos_date = dates[best_idx]

    amplitude = max_ndvi - BASE_NDVI
    if amplitude < 0.10:
        return None  # no real vegetation signal

    threshold = BASE_NDVI + SOS_EOS_RATIO * amplitude

    # SOS: first crossing above threshold, constrained to [MIN_DOY_SOS, POS]
    sos_date = None
    for j in range(best_idx + 1):
        if doys[j] >= MIN_DOY_SOS and vals[j] >= threshold:
            sos_date = dates[j]
            break

    # EOS: last crossing above threshold, constrained to [POS, MAX_DOY_EOS]
    eos_date = None
    for j in range(len(vals) - 1, best_idx - 1, -1):
        if doys[j] <= MAX_DOY_EOS and vals[j] >= threshold:
            eos_date = dates[j]
            break

    if sos_date is None or eos_date is None:
        return None

    los = (eos_date - sos_date).days
    if los < 30 or los > MAX_LOS_DAYS:
        return None  # unrealistic season length

    # Collect values within the growing season (SOS..EOS)
    season_vals = []
    ti = 0.0
    for j in range(len(dates)):
        if sos_date <= dates[j] <= eos_date:
            season_vals.append(vals[j])
        if j < len(dates) - 1 and dates[j] >= sos_date and dates[j + 1] <= eos_date:
            dt = (dates[j + 1] - dates[j]).days
            ti += 0.5 * (vals[j] + vals[j + 1]) * dt

    mean_ndvi = float(np.mean(season_vals)) if season_vals else None

    return {
        'sos': sos_date,
        'eos': eos_date,
        'pos': pos_date,
        'max_ndvi': round(max_ndvi, 4),
        'mean_ndvi': round(mean_ndvi, 4) if mean_ndvi is not None else None,
        'los': los,
        'ti': round(ti, 2),
    }
