"""
Bulk-import all Russian municipal districts / urban okrugs
(admin_level=6) from OSM.

Strategy: iterate over stored :class:`Region` rows that have an
``osm_id`` set, and for each one run a per-region Overpass query
(``area(region.osm_id + 3_600_000_000)`` scope). One country-wide
``admin_level=6`` query times out at Overpass's 15-min QL limit; the
per-region decomposition finishes one subject in a few seconds.

Each district is upserted by its OSM relation id (``District.osm_id``),
which makes the import idempotent even when the same district name
repeats across subjects.

Usage::

    # whole country (all Region rows with osm_id populated):
    python manage.py import_russia_districts --sleep 2

    # one subject at a time (uses region.osm_id automatically):
    python manage.py import_russia_districts --region-code krim_resp

    # manual Overpass scope override (rare — e.g. pre-osm_id backfill):
    python manage.py import_russia_districts --region-code krim_resp \\
        --parent-osm-id 3795586

Prerequisite: populate ``Region.osm_id`` first via
``import_russia_regions --refresh-osm-ids`` (fast, one Overpass call).
"""
from __future__ import annotations

import time

from django.core.management.base import BaseCommand
from django.db import transaction

from agrocosmos.management.commands.import_russia_regions import (
    _coerce_multipolygon,
)
from agrocosmos.models import District, Region
from agrocosmos.services.osm_overpass import (
    fetch_admin_relations_in,
    fetch_polygon_geojson,
)


class Command(BaseCommand):
    help = (
        'Bulk-import all Russian municipal districts (admin_level=6) from OSM '
        'and attach each to its parent Region via spatial-join.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit', type=int, default=None,
            help='Stop after N relations (for testing).',
        )
        parser.add_argument(
            '--sleep', type=float, default=2.5,
            help='Delay between polygons.osm.fr calls, seconds.',
        )
        parser.add_argument(
            '--skip-existing', action='store_true',
            help='Do not touch District rows whose (region, code) already exists.',
        )
        parser.add_argument(
            '--region-code', default=None,
            help='Restrict to a single federal subject (e.g. RU-KDA).',
        )
        parser.add_argument(
            '--parent-osm-id', type=int, default=None,
            help=(
                'Scope the Overpass query to a single parent relation '
                '(e.g. 3795586 for Republic of Crimea). Strongly recommended '
                'for production: querying all of Russia for admin_level=6 in '
                'one shot times out at Overpass\'s 15-min QL limit.'
            ),
        )
        parser.add_argument(
            '--admin-level', type=int, default=6,
            help='OSM admin_level to fetch (default 6 = mun. districts).',
        )

    def handle(self, *args, **opts):
        # Resolve which Region rows to process.
        if opts['region_code']:
            try:
                regions = [Region.objects.get(code=opts['region_code'])]
            except Region.DoesNotExist:
                self.stderr.write(f'Region code {opts["region_code"]!r} not found.')
                return
        else:
            regions = list(Region.objects.exclude(osm_id__isnull=True).order_by('name'))
            if not regions:
                self.stderr.write(
                    'No Region rows with osm_id set. Run '
                    '`python manage.py import_russia_regions --refresh-osm-ids` '
                    'first, or pass --region-code with --parent-osm-id.'
                )
                return

        manual_parent = opts.get('parent_osm_id')
        if manual_parent and len(regions) > 1:
            self.stderr.write(
                '--parent-osm-id can only be used with --region-code '
                '(one region at a time).'
            )
            return

        admin_level = opts['admin_level']
        limit = opts['limit']
        sleep_s = opts['sleep']
        skip_existing = opts['skip_existing']

        total_created = total_updated = total_skipped = 0
        total_failed = total_regions_skipped = 0

        for ri, region in enumerate(regions, 1):
            scope_osm_id = manual_parent or region.osm_id
            if not scope_osm_id:
                self.stderr.write(
                    f'[region {ri}/{len(regions)}] {region.name}: '
                    'no osm_id — skipping (run import_russia_regions '
                    '--refresh-osm-ids).'
                )
                total_regions_skipped += 1
                continue

            self.stdout.write(self.style.HTTP_INFO(
                f'[region {ri}/{len(regions)}] {region.name} '
                f'(code={region.code}, scope=osm:{scope_osm_id}) — '
                f'fetching admin_level={admin_level}…'
            ))
            try:
                relations = fetch_admin_relations_in(scope_osm_id, admin_level)
            except Exception as exc:
                self.stderr.write(f'  ! Overpass error: {exc}')
                total_regions_skipped += 1
                continue

            if limit:
                relations = relations[:limit]
            total = len(relations)
            self.stdout.write(f'  {total} districts in {region.name}')

            c, u, s, f = self._process_region(
                region, relations, skip_existing, sleep_s,
            )
            total_created += c
            total_updated += u
            total_skipped += s
            total_failed += f

        self.stdout.write(self.style.SUCCESS(
            f'Done across {len(regions)} region(s): '
            f'{total_created} created, {total_updated} updated, '
            f'{total_skipped} skipped, {total_failed} failed'
            + (f', {total_regions_skipped} region(s) skipped'
               if total_regions_skipped else '')
        ))

    def _process_region(self, region, relations, skip_existing, sleep_s):
        created = updated = skipped = failed = 0
        total = len(relations)
        for i, rel in enumerate(relations, 1):
            name = rel['name']
            tags = rel['tags']
            code = (
                tags.get('ref:OKTMO')
                or tags.get('ref')
                or f'osm_{rel["osm_id"]}'
            ).strip()

            if not name:
                self.stderr.write(
                    f'    [{i}/{total}] skip: no name on relation {rel["osm_id"]}'
                )
                failed += 1
                continue

            if skip_existing and District.objects.filter(osm_id=rel['osm_id']).exists():
                skipped += 1
                continue

            self.stdout.write(f'    [{i}/{total}] {name} ({code}) …')
            raw = fetch_polygon_geojson(rel['osm_id'])
            if not raw:
                self.stderr.write('      ! polygons.osm.fr returned no geometry')
                failed += 1
                time.sleep(sleep_s)
                continue

            try:
                geom = _coerce_multipolygon(raw)
            except Exception as exc:
                self.stderr.write(f'      ! geometry decode failed: {exc}')
                failed += 1
                time.sleep(sleep_s)
                continue

            with transaction.atomic():
                _, is_new = District.objects.update_or_create(
                    osm_id=rel['osm_id'],
                    defaults={
                        'region': region,
                        'name': name,
                        'code': code,
                        'geom': geom,
                    },
                )
            if is_new:
                created += 1
            else:
                updated += 1
            time.sleep(sleep_s)
        return created, updated, skipped, failed
