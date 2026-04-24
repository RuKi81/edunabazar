"""
Bulk-import all Russian municipal districts / urban okrugs
(admin_level=6) from OSM.

Mirrors :mod:`import_russia_regions` but at a finer level, and adds a
spatial-join step because OSM relations don't carry an explicit link
from district → federal subject: we match each district's centroid
against the stored Region geometries.

Usage::

    python manage.py import_russia_districts
    python manage.py import_russia_districts --limit 50 --sleep 1
    python manage.py import_russia_districts --skip-existing
    python manage.py import_russia_districts --region-code RU-KDA  # only Krasnodar

Districts whose centroid doesn't fall inside any imported Region are
skipped with a warning — typically they're either offshore (disputed)
or the regions dataset is incomplete.
"""
from __future__ import annotations

import time

from django.contrib.gis.geos import MultiPolygon
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q

from agrocosmos.management.commands.import_russia_regions import (
    _coerce_multipolygon,
)
from agrocosmos.models import District, Region
from agrocosmos.services.osm_overpass import (
    fetch_polygon_geojson,
    fetch_russia_admin_relations,
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
            '--admin-level', type=int, default=6,
            help='OSM admin_level to fetch (default 6 = mun. districts).',
        )

    def handle(self, *args, **opts):
        regions = list(Region.objects.all())
        if not regions:
            self.stderr.write(
                'No Region rows found. Import regions first: '
                '`python manage.py import_russia_regions`.'
            )
            return

        region_filter = None
        if opts['region_code']:
            try:
                region_filter = Region.objects.get(code=opts['region_code'])
            except Region.DoesNotExist:
                self.stderr.write(f'Region code {opts["region_code"]!r} not found.')
                return

        self.stdout.write(
            f'Fetching admin_level={opts["admin_level"]} relations from Overpass…'
        )
        try:
            relations = fetch_russia_admin_relations(opts['admin_level'])
        except Exception as exc:
            self.stderr.write(f'Overpass error: {exc}')
            return

        if opts['limit']:
            relations = relations[: opts['limit']]
        total = len(relations)
        self.stdout.write(f'Processing {total} relations…')

        created = updated = skipped = failed = unmatched = 0

        for i, rel in enumerate(relations, 1):
            name = rel['name']
            tags = rel['tags']
            code = (
                tags.get('ref:OKTMO')
                or tags.get('ref')
                or f'osm_{rel["osm_id"]}'
            ).strip()

            if not name:
                self.stderr.write(f'[{i}/{total}] skip: no name on relation {rel["osm_id"]}')
                failed += 1
                continue

            # Cheap pre-check: if --skip-existing and any matching district
            # already exists in any region, skip the expensive geometry fetch.
            if opts['skip_existing'] and District.objects.filter(
                Q(code=code) & ~Q(code='')
            ).exists():
                skipped += 1
                continue

            self.stdout.write(f'[{i}/{total}] {name} ({code}) …')
            raw = fetch_polygon_geojson(rel['osm_id'])
            if not raw:
                self.stderr.write('  ! polygons.osm.fr returned no geometry')
                failed += 1
                time.sleep(opts['sleep'])
                continue

            try:
                geom = _coerce_multipolygon(raw)
            except Exception as exc:
                self.stderr.write(f'  ! geometry decode failed: {exc}')
                failed += 1
                time.sleep(opts['sleep'])
                continue

            region = _match_region(geom, region_filter)
            if region is None:
                self.stderr.write('  ! no matching Region (skipped)')
                unmatched += 1
                time.sleep(opts['sleep'])
                continue

            with transaction.atomic():
                _, is_new = District.objects.update_or_create(
                    region=region,
                    name=name,
                    defaults={'code': code, 'geom': geom},
                )
            tag = 'created' if is_new else 'updated'
            self.stdout.write(f'  → {tag} in {region.name}')
            if is_new:
                created += 1
            else:
                updated += 1

            time.sleep(opts['sleep'])

        self.stdout.write(self.style.SUCCESS(
            f'Done: {created} created, {updated} updated, '
            f'{skipped} skipped, {unmatched} unmatched, {failed} failed'
        ))


def _match_region(district_geom: MultiPolygon,
                  region_filter: Region | None) -> Region | None:
    """
    Pick the Region whose geometry contains the district's centroid.

    Using the centroid (instead of Intersects on the full polygon) makes
    the match deterministic for districts that straddle subject borders
    because of coarse OSM edits. If ``region_filter`` is given, only
    that region is considered — useful for partial re-imports.
    """
    centroid = district_geom.centroid
    qs = (
        Region.objects.filter(pk=region_filter.pk)
        if region_filter is not None
        else Region.objects.all()
    )
    return qs.filter(geom__contains=centroid).first()
