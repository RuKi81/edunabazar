"""
Recalculate historical NDVI baseline averages per district + day-of-year.

For every district, aggregate mean NDVI across all years **except the current
year**, grouped by EXTRACT(doy FROM acquired_date).  The result is stored in
``NdviBaseline`` and used on the dashboard chart as a grey dashed line.

Because MODIS is 8-day composite (≈46 DOYs/year) the raw aggregation is
sparse and jagged.  After aggregation we therefore:

1. Linearly interpolate mean/std to every DOY 1..366 (wrapping year-end via
   periodic boundary so Jan 1 joins Dec 31 smoothly);
2. Smooth with Savitzky-Golay (window 21, poly 3) — wide enough to absorb
   MODIS 8-day cadence but still preserves seasonal curve shape;
3. Persist the full 366-point smoothed curve; ``years_count`` is preserved
   for observed DOYs and set to 0 for interpolated ones so callers can tell
   real samples from synthetic ones.

Schedule: run once a year on **7 January** (cron / celery-beat / systemd timer).

Usage:
    python manage.py calc_ndvi_baseline                  # all regions
    python manage.py calc_ndvi_baseline --region-id 1    # single region
    python manage.py calc_ndvi_baseline --dry-run        # preview SQL, no write
    python manage.py calc_ndvi_baseline --no-smooth      # store raw sparse avgs
"""

from collections import defaultdict
from datetime import date

import numpy as np
from django.core.management.base import BaseCommand
from django.db import connection


SMOOTH_WINDOW = 21      # Savitzky-Golay window (days); must be odd
SMOOTH_POLY = 3


class Command(BaseCommand):
    help = 'Recalculate historical NDVI baseline (all years except current).'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, default=None,
                            help='Limit to a single region ID.')
        parser.add_argument('--dry-run', action='store_true',
                            help='Print SQL and row counts without writing.')
        parser.add_argument('--no-smooth', action='store_true',
                            help='Store raw sparse averages without '
                                 'interpolation/smoothing.')

    def handle(self, *args, **options):
        region_id = options['region_id']
        dry_run = options['dry_run']
        smooth = not options['no_smooth']
        current_year = date.today().year

        self.stdout.write(f'Current year excluded: {current_year}')

        # ── Build per-district, per-doy, per-crop_type aggregation ──
        where = "vi.acquired_date < %s AND vi.mean BETWEEN -0.2 AND 1 AND vi.is_outlier = false"
        params = [date(current_year, 1, 1)]

        if region_id:
            where += " AND d.region_id = %s"
            params.append(region_id)

        agg_sql = f"""
            SELECT
                f.district_id,
                EXTRACT(doy FROM vi.acquired_date)::int AS doy,
                ''::varchar                             AS crop_type,
                AVG(vi.mean)                            AS mean_ndvi,
                COALESCE(STDDEV_POP(vi.mean), 0)         AS std_ndvi,
                COUNT(DISTINCT EXTRACT(year FROM vi.acquired_date))::int AS years_count
            FROM agro_vegetation_index vi
            JOIN agro_farmland f ON f.id = vi.farmland_id
            JOIN agro_district d ON d.id = f.district_id
            WHERE {where} AND vi.index_type = 'ndvi'
            GROUP BY f.district_id, doy

            UNION ALL

            SELECT
                f.district_id,
                EXTRACT(doy FROM vi.acquired_date)::int AS doy,
                f.crop_type,
                AVG(vi.mean)                            AS mean_ndvi,
                COALESCE(STDDEV_POP(vi.mean), 0)         AS std_ndvi,
                COUNT(DISTINCT EXTRACT(year FROM vi.acquired_date))::int AS years_count
            FROM agro_vegetation_index vi
            JOIN agro_farmland f ON f.id = vi.farmland_id
            JOIN agro_district d ON d.id = f.district_id
            WHERE {where} AND vi.index_type = 'ndvi'
            GROUP BY f.district_id, doy, f.crop_type
        """

        with connection.cursor() as cur:
            cur.execute(agg_sql, params + params)  # params used twice (UNION)
            rows = cur.fetchall()

        self.stdout.write(f'Aggregated rows: {len(rows)}')

        if dry_run:
            for r in rows[:20]:
                self.stdout.write(f'  district={r[0]} doy={r[1]} crop={r[2]!r} '
                                  f'ndvi={r[3]:.4f} std={r[4]:.4f} years={r[5]}')
            if len(rows) > 20:
                self.stdout.write(f'  ... and {len(rows) - 20} more')
            return

        if not rows:
            self.stdout.write(self.style.WARNING('No data to write.'))
            return

        if smooth:
            rows = _smooth_baseline(rows, logger=self.stdout)
            self.stdout.write(f'Smoothed rows: {len(rows)}')

        # ── Upsert into agro_ndvi_baseline ──
        upsert_sql = """
            INSERT INTO agro_ndvi_baseline
                (district_id, day_of_year, crop_type, mean_ndvi, std_ndvi, years_count, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (district_id, day_of_year, crop_type)
            DO UPDATE SET
                mean_ndvi   = EXCLUDED.mean_ndvi,
                std_ndvi    = EXCLUDED.std_ndvi,
                years_count = EXCLUDED.years_count,
                updated_at  = NOW()
        """

        batch = [(r[0], r[1], r[2], round(r[3], 6), round(r[4], 6), r[5]) for r in rows]

        with connection.cursor() as cur:
            cur.executemany(upsert_sql, batch)

        self.stdout.write(self.style.SUCCESS(
            f'Upserted {len(batch)} baseline records.'
        ))


def _smooth_baseline(rows, logger=None):
    """Interpolate raw (district, crop_type) -> doy series to 1..366 and
    apply Savitzky-Golay smoothing with periodic boundary.

    Input rows format: ``(district_id, doy, crop_type, mean, std, years_count)``.
    Output has the same shape but ``doy`` spans 1..366 densely; interpolated
    DOYs carry ``years_count=0``.
    """
    from scipy.signal import savgol_filter

    groups = defaultdict(list)
    for r in rows:
        groups[(r[0], r[2])].append((int(r[1]), float(r[3]), float(r[4]), int(r[5])))

    out = []
    skipped = 0
    for (district_id, crop_type), pts in groups.items():
        pts.sort(key=lambda p: p[0])
        if len(pts) < 3:
            # Not enough points to smooth meaningfully — keep raw.
            for d, m, s, yc in pts:
                out.append((district_id, d, crop_type, m, s, yc))
            skipped += 1
            continue

        doys = np.array([p[0] for p in pts], dtype=np.float64)
        means = np.array([p[1] for p in pts], dtype=np.float64)
        stds = np.array([p[2] for p in pts], dtype=np.float64)
        yc_lookup = {p[0]: p[3] for p in pts}

        # Periodic extension for smooth year-end wrap: prepend last→first-366
        # and append first→last+366 so np.interp behaves as circular.
        ext_doys = np.concatenate([doys - 366, doys, doys + 366])
        ext_means = np.concatenate([means, means, means])
        ext_stds = np.concatenate([stds, stds, stds])

        full_doys = np.arange(1, 367)
        full_means = np.interp(full_doys, ext_doys, ext_means)
        full_stds = np.interp(full_doys, ext_doys, ext_stds)

        win = min(SMOOTH_WINDOW, len(full_means))
        if win % 2 == 0:
            win -= 1
        if win >= 5:
            poly = min(SMOOTH_POLY, win - 1)
            full_means = savgol_filter(full_means, win, poly, mode='wrap')
            full_stds = savgol_filter(full_stds, win, poly, mode='wrap')
            full_means = np.clip(full_means, -0.2, 1.0)
            full_stds = np.clip(full_stds, 0.0, 1.0)

        for d in range(1, 367):
            out.append((
                district_id, d, crop_type,
                float(full_means[d - 1]),
                float(full_stds[d - 1]),
                yc_lookup.get(d, 0),
            ))

    if logger and skipped:
        logger.write(f'  {skipped} groups kept raw (<3 observed DOYs).')
    return out
