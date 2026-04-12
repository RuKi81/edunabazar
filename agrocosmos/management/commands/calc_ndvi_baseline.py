"""
Recalculate historical NDVI baseline averages per district + day-of-year.

For every district, aggregate mean NDVI across all years **except the current
year**, grouped by EXTRACT(doy FROM acquired_date).  The result is stored in
``NdviBaseline`` and used on the dashboard chart as a grey dashed line.

Schedule: run once a year on **7 January** (cron / celery-beat / systemd timer).

Usage:
    python manage.py calc_ndvi_baseline                  # all regions
    python manage.py calc_ndvi_baseline --region-id 1    # single region
    python manage.py calc_ndvi_baseline --dry-run        # preview SQL, no write
"""

from datetime import date

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = 'Recalculate historical NDVI baseline (all years except current).'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, default=None,
                            help='Limit to a single region ID.')
        parser.add_argument('--dry-run', action='store_true',
                            help='Print SQL and row counts without writing.')

    def handle(self, *args, **options):
        region_id = options['region_id']
        dry_run = options['dry_run']
        current_year = date.today().year

        self.stdout.write(f'Current year excluded: {current_year}')

        # ── Build per-district, per-doy, per-crop_type aggregation ──
        where = "vi.acquired_date < %s AND vi.mean BETWEEN -0.2 AND 1 AND vi.is_anomaly = false"
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
