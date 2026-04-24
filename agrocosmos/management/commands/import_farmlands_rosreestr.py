"""Bulk-import Rosreestr ЗСН farmlands for all Russian regions.

Walks a directory of per-region shapefile folders, detects each file's
.dbf schema, runs ``ogr2ogr`` into a per-region staging table, and then
``INSERT … SELECT``-s into ``agro_farmland`` with strict crop_type
mapping + ``is_used`` + ``cadastral_number`` + JSONB ``properties``.

Usage::

    # Dry-run: detect schemas, print the plan, don't touch the DB.
    python manage.py import_farmlands_rosreestr \\
        --base /data/import/rosreestr_zsn \\
        --dry-run

    # One region only (folder name must match what's on disk).
    python manage.py import_farmlands_rosreestr \\
        --base /data/import/rosreestr_zsn \\
        --region-dir "Республика Крым"

    # TRUNCATE agro_farmland first, then import everything.
    python manage.py import_farmlands_rosreestr \\
        --base /data/import/rosreestr_zsn --truncate

    # Resume: skip regions that already have any Farmland rows.
    python manage.py import_farmlands_rosreestr \\
        --base /data/import/rosreestr_zsn --skip-existing

District assignment is intentionally **deferred** — all rows are written
with ``district_id = NULL`` and a follow-up ``assign_farmland_district``
command performs the spatial-join (ST_Contains(centroid)) in bulk. This
keeps import-time simple and fast.
"""
from __future__ import annotations

import logging
import re
import shutil
import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from agrocosmos.models import Farmland, Region
from agrocosmos.services.farmland_importer import (
    build_count_staging_sql,
    build_drop_staging_sql,
    build_insert_sql,
    run_ogr2ogr,
)
from agrocosmos.services.farmland_schemas import FarmlandSchema, detect_schema


logger = logging.getLogger(__name__)


# Names of region folders the scanner produced that should be skipped
# entirely (e.g. archive-only). Populated empty for now; we unpacked
# Saha earlier so everything on disk is walkable.
_SKIP_DIRS: frozenset[str] = frozenset()


class Command(BaseCommand):
    help = 'Bulk-import Rosreestr ЗСН farmland polygons into agro_farmland.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--base', required=True,
            help='Path to the root folder containing per-region subfolders.',
        )
        parser.add_argument(
            '--region-dir', default=None,
            help='Process only this single region subfolder (name as on disk).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Detect schemas and print the plan; do not touch the DB.',
        )
        parser.add_argument(
            '--truncate', action='store_true',
            help='TRUNCATE agro_farmland before starting (dangerous).',
        )
        parser.add_argument(
            '--skip-existing', action='store_true',
            help='Skip regions that already have farmlands in the DB.',
        )
        parser.add_argument(
            '--ogr2ogr', default='ogr2ogr',
            help='Path to ogr2ogr binary (default: rely on PATH).',
        )
        parser.add_argument(
            '--analyze', action='store_true',
            help='Run ANALYZE agro_farmland at the end.',
        )

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def handle(self, *args, **options):
        base = Path(options['base']).expanduser().resolve()
        if not base.is_dir():
            raise CommandError(f'Base folder not found: {base}')

        binary = options['ogr2ogr']
        if not options['dry_run'] and not shutil.which(binary):
            raise CommandError(
                f'{binary!r} not found in PATH. Install gdal-bin or pass --ogr2ogr.'
            )

        if options['truncate'] and not options['dry_run']:
            self._truncate_farmland()

        region_dirs = self._list_region_dirs(base, options['region_dir'])
        self.stdout.write(self.style.NOTICE(
            f'Will process {len(region_dirs)} region folder(s) from {base}'
        ))

        totals = {'ok': 0, 'failed': 0, 'skipped': 0,
                  'rows_inserted': 0, 'rows_staged': 0}
        started = time.time()

        for rd in region_dirs:
            t0 = time.time()
            try:
                result = self._process_region(rd, options)
            except Exception as exc:
                logger.exception('[%s] FAILED', rd.name)
                self.stderr.write(self.style.ERROR(
                    f'[{rd.name}] failed: {exc}'
                ))
                totals['failed'] += 1
                continue
            dt = time.time() - t0
            if result['status'] == 'skipped':
                totals['skipped'] += 1
                self.stdout.write(self.style.WARNING(
                    f'[{rd.name}] skipped: {result["reason"]}'
                ))
            else:
                totals['ok'] += 1
                totals['rows_staged'] += result.get('staged', 0)
                totals['rows_inserted'] += result.get('inserted', 0)
                self.stdout.write(self.style.SUCCESS(
                    f'[{rd.name}] staged={result.get("staged", 0):,} '
                    f'inserted={result.get("inserted", 0):,}  '
                    f'{dt:.1f}s'
                ))

        if options['analyze'] and not options['dry_run']:
            with connection.cursor() as cur:
                cur.execute('ANALYZE agro_farmland;')
            self.stdout.write('ANALYZE agro_farmland done.')

        elapsed = time.time() - started
        self.stdout.write(self.style.NOTICE(
            f'\n=== Totals: ok={totals["ok"]}, failed={totals["failed"]}, '
            f'skipped={totals["skipped"]},  '
            f'staged={totals["rows_staged"]:,}, '
            f'inserted={totals["rows_inserted"]:,}  '
            f'in {elapsed:.1f}s'
        ))

    # ------------------------------------------------------------------
    # Per-region pipeline
    # ------------------------------------------------------------------

    def _process_region(self, region_dir: Path, options: dict) -> dict:
        # 1. Find the .shp
        shp = self._find_shp(region_dir)
        if shp is None:
            return {'status': 'skipped', 'reason': 'no .shp file'}

        # 2. Detect schema
        schema = detect_schema(shp)
        if not schema.is_usable:
            return {'status': 'skipped',
                    'reason': f'unrecognised schema {schema.all_fields}'}

        # 3. Resolve Region
        region = self._resolve_region(region_dir.name)
        if region is None:
            return {'status': 'skipped',
                    'reason': f'no matching Region for {region_dir.name!r}'}

        if options['skip_existing'] and not options['dry_run']:
            if Farmland.objects.filter(region=region).exists():
                return {'status': 'skipped',
                        'reason': 'already has farmlands (use --no-skip to reimport)'}

        # ``source`` is the schema fingerprint (schema_id) — truncated to
        # the column's max_length. Lets us re-run one schema class later
        # with a simple WHERE source = '…' filter.
        source_id = schema.schema_id[:40]
        staging = f'staging_farmland_{self._slug(region.code)}'

        self.stdout.write(
            f'[{region_dir.name}] region={region.name} code={region.code} '
            f'schema={schema.schema_id} usage={schema.usage_field!r} '
            f'fact_isp={schema.fact_isp_field!r} cad={schema.cadastral_field!r}'
        )

        if options['dry_run']:
            self.stdout.write(f'  DRY: would ogr2ogr {shp.name} → {staging}')
            return {'status': 'ok', 'staged': 0, 'inserted': 0}

        # 4. ogr2ogr shp → staging
        self.stdout.write(f'  running ogr2ogr → {staging}…')
        ogr_result = run_ogr2ogr(
            shp, staging, schema,
            binary=options['ogr2ogr'],
            log_stream=self.stdout,
        )
        if ogr_result.returncode != 0:
            # still try the rest: ogr2ogr exits non-zero on skipfailures
            self.stderr.write(self.style.WARNING(
                f'  ogr2ogr rc={ogr_result.returncode} '
                f'(continuing — some features may have been skipped)'
            ))

        # 5. Count staged, then INSERT into agro_farmland
        with connection.cursor() as cur:
            try:
                cur.execute(build_count_staging_sql(staging))
                staged_rows = cur.fetchone()[0]
            except Exception as exc:
                self._drop_staging_silent(staging)
                return {'status': 'skipped',
                        'reason': f'staging table missing after ogr2ogr: {exc}'}

            if staged_rows == 0:
                self.stdout.write(self.style.WARNING(
                    '  staging is empty (no agricultural rows matched)'
                ))
                cur.execute(build_drop_staging_sql(staging))
                return {'status': 'ok', 'staged': 0, 'inserted': 0}

            self.stdout.write(f'  staged {staged_rows:,} rows, promoting…')
            insert_sql = build_insert_sql(
                schema, staging, region_id=region.pk, source_id=source_id,
            )
            with transaction.atomic():
                cur.execute(insert_sql)
                inserted = cur.rowcount
            cur.execute(build_drop_staging_sql(staging))

        return {'status': 'ok', 'staged': staged_rows, 'inserted': inserted}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _list_region_dirs(self, base: Path, only: str | None) -> list[Path]:
        if only:
            target = base / only
            if not target.is_dir():
                raise CommandError(f'Region subfolder not found: {target}')
            return [target]
        return sorted(
            p for p in base.iterdir()
            if p.is_dir() and p.name not in _SKIP_DIRS
        )

    @staticmethod
    def _find_shp(region_dir: Path) -> Path | None:
        """Pick the largest .shp in the region folder (handles the rare
        case of Omsk-style split halves — we prefer the one with the
        most features, which is the largest file). Recurses into
        sub-folders."""
        shps = sorted(region_dir.rglob('*.shp'),
                      key=lambda p: p.stat().st_size, reverse=True)
        return shps[0] if shps else None

    @staticmethod
    def _slug(text: str) -> str:
        """Safe snake_case slug for staging-table names."""
        s = re.sub(r'[^A-Za-z0-9]+', '_', text).strip('_').lower()
        return s or 'unknown'

    def _resolve_region(self, dir_name: str) -> Region | None:
        """Map a folder name like 'Алтайский_край' or 'Республика Крым'
        to an existing Region row. Matches by ``name`` (iexact, then
        trimmed substring) and falls back to transliterated ``code``."""
        normalised = dir_name.strip().rstrip('_').replace('_', ' ')
        # Drop trailing "__" / double spaces artefacts from some folders.
        normalised = re.sub(r'\s+', ' ', normalised).strip()

        # 1. Exact iexact match (most common)
        r = Region.objects.filter(name__iexact=normalised).first()
        if r is not None:
            return r

        # 2. Try without "Республика ", "обл." etc. stripped
        bare = re.sub(
            r'^(?:Республика|г\.ф\.з\.|Город)\s+|(?:\s+(?:область|край|автономный\s+округ|АО))$',
            '', normalised, flags=re.IGNORECASE,
        ).strip()
        if bare and bare != normalised:
            r = (Region.objects.filter(name__icontains=bare).first()
                 or Region.objects.filter(name__icontains=normalised).first())
            if r is not None:
                return r

        # 3. Substring match both directions
        return (Region.objects.filter(name__icontains=normalised).first()
                or Region.objects.filter(name__icontains=bare or normalised).first())

    def _truncate_farmland(self) -> None:
        with connection.cursor() as cur:
            cur.execute('TRUNCATE agro_farmland RESTART IDENTITY;')
        self.stdout.write(self.style.WARNING(
            'agro_farmland TRUNCATEd (RESTART IDENTITY).'
        ))

    def _drop_staging_silent(self, staging: str) -> None:
        try:
            with connection.cursor() as cur:
                cur.execute(build_drop_staging_sql(staging))
        except Exception:
            pass
