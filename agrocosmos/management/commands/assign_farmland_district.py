"""Spatially assign ``district_id`` to ``agro_farmland`` rows.

The bulk import (``import_farmlands_rosreestr``) intentionally writes
every row with ``district_id = NULL`` to keep import-time simple and
fast. This command performs the deferred spatial join in bulk:

    UPDATE agro_farmland f
       SET district_id = d.id
      FROM agro_district d
     WHERE f.district_id IS NULL
       AND f.region_id   = d.region_id
       AND ST_Contains(d.geom, ST_PointOnSurface(f.geom));

It iterates region by region (instead of one giant transaction) so that
progress is visible, locks stay short and a crash leaves a clean
"resume from where we stopped" state — already-assigned regions are
skipped because their rows no longer match ``district_id IS NULL``.

Usage::

    # All regions:
    python manage.py assign_farmland_district

    # One region by id or by code/name:
    python manage.py assign_farmland_district --region 37
    python manage.py assign_farmland_district --region "Краснодарский край"

    # Dry-run: count NULL rows per region without updating.
    python manage.py assign_farmland_district --dry-run
"""
from __future__ import annotations

import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from agrocosmos.models import Region


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Assign agro_farmland.district_id by spatial join with agro_district."

    def add_arguments(self, parser):
        parser.add_argument(
            '--region',
            help='Limit to a single region (id, code, or exact name).',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Only count NULL rows per region; do not UPDATE.',
        )
        parser.add_argument(
            '--reassign',
            action='store_true',
            help='Re-assign every row (drop the district_id IS NULL filter).',
        )

    def handle(self, *args, **opts):
        region_arg: str | None = opts.get('region')
        dry_run: bool = bool(opts.get('dry_run'))
        reassign: bool = bool(opts.get('reassign'))

        regions = self._resolve_regions(region_arg)
        if not regions:
            raise CommandError('No matching regions found.')

        self.stdout.write(self.style.NOTICE(
            f'[assign] regions to process: {len(regions)} '
            f'(dry_run={dry_run}, reassign={reassign})'
        ))

        total_updated = 0
        total_skipped = 0
        t_global = time.monotonic()

        for region in regions:
            t_region = time.monotonic()
            null_filter = '' if reassign else 'AND f.district_id IS NULL'

            with connection.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM agro_farmland f
                     WHERE f.region_id = %s {null_filter};
                    """,
                    [region.id],
                )
                pending = int(cur.fetchone()[0] or 0)

            if pending == 0:
                self.stdout.write(
                    f'[assign] {region.name}: nothing to do.'
                )
                total_skipped += 1
                continue

            self.stdout.write(self.style.NOTICE(
                f'[assign] {region.name} (id={region.id}): '
                f'{pending:,} farmlands to assign'
                + (' [dry-run]' if dry_run else '')
            ))

            if dry_run:
                continue

            with transaction.atomic():
                with connection.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE agro_farmland f
                           SET district_id = d.id
                          FROM agro_district d
                         WHERE f.region_id = %s
                           {null_filter}
                           AND d.region_id = f.region_id
                           AND ST_Contains(d.geom, ST_PointOnSurface(f.geom));
                        """,
                        [region.id],
                    )
                    updated = cur.rowcount or 0

            elapsed = time.monotonic() - t_region
            total_updated += updated
            self.stdout.write(self.style.SUCCESS(
                f'[assign] {region.name}: updated {updated:,} of {pending:,} '
                f'in {elapsed:.1f}s'
                + (f' (unmatched: {pending - updated:,})' if updated < pending else '')
            ))

        elapsed_global = time.monotonic() - t_global
        self.stdout.write(self.style.SUCCESS(
            f'[assign] DONE. updated={total_updated:,} '
            f'regions_skipped={total_skipped} time={elapsed_global:.1f}s'
        ))

    # ------------------------------------------------------------------
    def _resolve_regions(self, region_arg: str | None) -> list[Region]:
        qs = Region.objects.order_by('name')
        if not region_arg:
            return list(qs)

        # try numeric id
        if region_arg.isdigit():
            r = qs.filter(id=int(region_arg)).first()
            if r is not None:
                return [r]

        # exact code, then exact name, then iexact name
        r = (
            qs.filter(code=region_arg).first()
            or qs.filter(name=region_arg).first()
            or qs.filter(name__iexact=region_arg).first()
        )
        return [r] if r else []
