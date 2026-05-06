"""
Per-region NDVI coverage report.

For each region, prints how many farmlands exist, how many NDVI rows
are stored, the earliest/latest acquired_date, distinct years, and which
years (since 2000) have **no** records — the "off-grid" years.

Usage:
    # single region (recommended — fast, ~1-5s on partial index)
    python manage.py report_ndvi_coverage --region-id 37

    # all regions (heavy — uses sequential parts of the table; avoid
    # while pg_dump or other heavy IO is running)
    python manage.py report_ndvi_coverage --all

    # CSV output for further processing
    python manage.py report_ndvi_coverage --all --csv > coverage.csv

    # tweak the "expected years" window (default: 2018..current_year)
    python manage.py report_ndvi_coverage --all --year-from 2015

The query restricts to ``index_type='ndvi'`` and uses the partial index
``(farmland, index_type, acquired_date)``. With 410 GB of vi data,
running ``--all`` adds noticeable IO load; prefer per-region during
production hours.
"""
from __future__ import annotations

import csv
import sys
from datetime import date

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = 'Per-region NDVI coverage and gap report.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--region-id', type=int, default=None,
            help='Limit to a single region (recommended).',
        )
        parser.add_argument(
            '--all', action='store_true',
            help='Run for every region — heavy on production DB.',
        )
        parser.add_argument(
            '--year-from', type=int, default=2018,
            help='First year to expect coverage for gap calculation '
                 '(default: 2018).',
        )
        parser.add_argument(
            '--csv', action='store_true',
            help='Print machine-readable CSV instead of table.',
        )
        parser.add_argument(
            '--statement-timeout-sec', type=int, default=600,
            help='Per-query Postgres statement_timeout in seconds '
                 '(default: 600).',
        )

    def handle(self, *args, **options):
        region_id = options['region_id']
        all_regions = options['all']
        year_from = options['year_from']
        as_csv = options['csv']
        timeout_sec = options['statement_timeout_sec']

        if not region_id and not all_regions:
            self.stderr.write(
                'Specify --region-id N or --all (heavy).'
            )
            return

        # Resolve list of regions to process
        with connection.cursor() as cur:
            cur.execute(f'SET LOCAL statement_timeout = {timeout_sec * 1000}')
            if region_id:
                cur.execute(
                    'SELECT id, name FROM agro_region WHERE id = %s',
                    [region_id],
                )
            else:
                cur.execute(
                    'SELECT id, name FROM agro_region ORDER BY name'
                )
            regions = cur.fetchall()

        if not regions:
            self.stderr.write('No regions found.')
            return

        year_to = date.today().year
        expected_years = list(range(year_from, year_to + 1))

        rows: list[dict] = []
        for rid, rname in regions:
            stats = self._region_stats(rid, timeout_sec)
            stats['region_id'] = rid
            stats['region_name'] = rname

            covered = set(stats['years_set'])
            stats['missing_years'] = [
                y for y in expected_years if y not in covered
            ]
            rows.append(stats)

            if not as_csv:
                # Live progress for --all, since each region takes a while.
                self.stdout.write(
                    f'  …{rname}: {stats["n_farms"]} farms, '
                    f'{stats["n_ndvi"]} NDVI, '
                    f'{stats["earliest"]}..{stats["latest"]}'
                )

        # ── Output ────────────────────────────────────────────────────
        if as_csv:
            writer = csv.writer(sys.stdout)
            writer.writerow([
                'region_id', 'region_name', 'farmlands', 'ndvi_records',
                'earliest', 'latest', 'years_covered', 'missing_years',
                'last_30d',
            ])
            for r in rows:
                writer.writerow([
                    r['region_id'], r['region_name'], r['n_farms'],
                    r['n_ndvi'],
                    r['earliest'] or '', r['latest'] or '',
                    ','.join(str(y) for y in sorted(r['years_set'])),
                    ','.join(str(y) for y in r['missing_years']),
                    r['last_30d'],
                ])
            return

        # Table
        self.stdout.write('')
        self.stdout.write(
            f'{"#":>3} {"Регион":<32} {"Угодья":>8} '
            f'{"NDVI rows":>10} {"Старейш.":<10} {"Свежий":<10} '
            f'{"Лет":>4} {"Пробелы":<25} {"30д":>5}'
        )
        self.stdout.write('─' * 120)
        for r in rows:
            missing = ','.join(str(y) for y in r['missing_years'][:7])
            if len(r['missing_years']) > 7:
                missing += f' +{len(r["missing_years"]) - 7}'
            self.stdout.write(
                f'{r["region_id"]:>3} '
                f'{r["region_name"][:32]:<32} '
                f'{r["n_farms"]:>8} '
                f'{r["n_ndvi"]:>10} '
                f'{(r["earliest"].isoformat() if r["earliest"] else "—"):<10} '
                f'{(r["latest"].isoformat() if r["latest"] else "—"):<10} '
                f'{len(r["years_set"]):>4} '
                f'{missing:<25} '
                f'{r["last_30d"]:>5}'
            )

        # Aggregate footer
        total_farms = sum(r['n_farms'] for r in rows)
        total_ndvi = sum(r['n_ndvi'] for r in rows)
        regions_no_data = sum(1 for r in rows if r['n_ndvi'] == 0)
        self.stdout.write('─' * 120)
        self.stdout.write(
            f'  TOTAL: {len(rows)} регионов, '
            f'{total_farms} угодий, '
            f'{total_ndvi} NDVI-записей. '
            f'Без данных: {regions_no_data}.'
        )

    # ------------------------------------------------------------------

    def _region_stats(self, region_id: int, timeout_sec: int) -> dict:
        """Two queries per region (farms + vi); returns a stats dict."""
        with connection.cursor() as cur:
            cur.execute(f'SET LOCAL statement_timeout = {timeout_sec * 1000}')

            cur.execute(
                'SELECT COUNT(*) FROM agro_farmland WHERE region_id = %s',
                [region_id],
            )
            n_farms = cur.fetchone()[0] or 0

            # Use district join — Farmland.region FK is nullable
            # (SET_NULL on delete) but every farmland is bound to a
            # district, and district→region is mandatory. Going via
            # district guarantees no orphan rows are skipped if a
            # region was renumbered. The (farmland, index_type,
            # acquired_date) index keeps this efficient even on big
            # tables.
            cur.execute(
                """
                SELECT
                    COUNT(*) AS n,
                    MIN(vi.acquired_date) AS earliest,
                    MAX(vi.acquired_date) AS latest,
                    ARRAY(
                        SELECT DISTINCT EXTRACT(YEAR FROM vi2.acquired_date)::int
                        FROM agro_vegetation_index vi2
                        JOIN agro_farmland f2 ON f2.id = vi2.farmland_id
                        JOIN agro_district  d2 ON d2.id = f2.district_id
                        WHERE d2.region_id = %s
                          AND vi2.index_type = 'ndvi'
                        ORDER BY 1
                    ) AS years,
                    SUM(CASE
                        WHEN vi.acquired_date >= CURRENT_DATE - INTERVAL '30 days'
                        THEN 1 ELSE 0 END) AS last_30d
                FROM agro_vegetation_index vi
                JOIN agro_farmland f ON f.id = vi.farmland_id
                JOIN agro_district  d ON d.id = f.district_id
                WHERE d.region_id = %s
                  AND vi.index_type = 'ndvi'
                """,
                [region_id, region_id],
            )
            n_ndvi, earliest, latest, years, last_30d = cur.fetchone()

        return {
            'n_farms': n_farms,
            'n_ndvi': n_ndvi or 0,
            'earliest': earliest,
            'latest': latest,
            'years_set': set(years or []),
            'last_30d': last_30d or 0,
        }
