"""
Bulk-import all Russian federal subjects (admin_level=4) from OSM.

Fetches the full list of admin_level=4 relations inside Russia via
Overpass, then grabs each multipolygon from polygons.openstreetmap.fr
and upserts a ``Region`` row keyed by ISO-3166-2 code (e.g. ``RU-KDA``).

Usage::

    python manage.py import_russia_regions
    python manage.py import_russia_regions --limit 5 --sleep 1
    python manage.py import_russia_regions --skip-existing

    # Point-update one region bypassing the Overpass list (useful for
    # disputed territories that don't appear under area[ISO3166-1=RU],
    # e.g. Crimea — OSM places it under Ukraine):
    python manage.py import_russia_regions --osm-id 72639 \\
        --code RU-CR --name "Республика Крым"
"""
from __future__ import annotations

import json
import time

from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
from django.core.management.base import BaseCommand
from django.db import transaction

from agrocosmos.models import Region
from agrocosmos.services.osm_overpass import (
    fetch_polygon_geojson,
    fetch_russia_admin_relations,
)


class Command(BaseCommand):
    help = 'Bulk-import all Russian federal subjects (admin_level=4) from OSM.'

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
            help='Do not touch Region rows whose code already exists.',
        )
        parser.add_argument(
            '--osm-id', type=int, default=None,
            help=(
                'Point-update mode: fetch geometry for this single OSM '
                'relation id directly, bypassing the Overpass list. Use '
                '--code and/or --name to set the upsert key and label.'
            ),
        )
        parser.add_argument(
            '--code', default=None,
            help='Explicit region code to upsert by (used with --osm-id).',
        )
        parser.add_argument(
            '--name', default=None,
            help='Explicit region name to write (used with --osm-id).',
        )

    def handle(self, *args, **opts):
        if opts['osm_id']:
            self._handle_single(opts)
            return

        self.stdout.write('Fetching admin_level=4 relations from Overpass…')
        try:
            relations = fetch_russia_admin_relations(4)
        except Exception as exc:
            self.stderr.write(f'Overpass error: {exc}')
            return

        if opts['limit']:
            relations = relations[: opts['limit']]
        total = len(relations)
        self.stdout.write(f'Processing {total} relations…')

        created = updated = skipped = failed = 0

        for i, rel in enumerate(relations, 1):
            name = rel['name']
            tags = rel['tags']
            code = (
                tags.get('ISO3166-2')
                or tags.get('ref')
                or f'osm_{rel["osm_id"]}'
            ).strip()

            if not name:
                self.stderr.write(f'[{i}/{total}] skip: no name on relation {rel["osm_id"]}')
                failed += 1
                continue

            if opts['skip_existing'] and Region.objects.filter(code=code).exists():
                self.stdout.write(f'[{i}/{total}] skip existing: {name} ({code})')
                skipped += 1
                continue

            self.stdout.write(f'[{i}/{total}] {name} ({code}) …')
            raw = fetch_polygon_geojson(rel['osm_id'])
            if not raw:
                self.stderr.write(f'  ! polygons.osm.fr returned no geometry')
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

            with transaction.atomic():
                _, is_new = Region.objects.update_or_create(
                    code=code,
                    defaults={'name': name, 'geom': geom},
                )
            if is_new:
                created += 1
            else:
                updated += 1

            time.sleep(opts['sleep'])

        self.stdout.write(self.style.SUCCESS(
            f'Done: {created} created, {updated} updated, '
            f'{skipped} skipped, {failed} failed'
        ))

    def _handle_single(self, opts: dict) -> None:
        """Point-update one Region by OSM relation id."""
        osm_id = int(opts['osm_id'])
        code = (opts['code'] or '').strip() or f'osm_{osm_id}'
        self.stdout.write(f'Fetching polygon for OSM relation {osm_id}…')
        raw = fetch_polygon_geojson(osm_id)
        if not raw:
            self.stderr.write('polygons.osm.fr returned no geometry')
            return
        try:
            geom = _coerce_multipolygon(raw)
        except Exception as exc:
            self.stderr.write(f'geometry decode failed: {exc}')
            return

        existing = Region.objects.filter(code=code).first()
        name = (
            (opts['name'] or '').strip()
            or (existing.name if existing else '')
            or f'OSM relation {osm_id}'
        )
        with transaction.atomic():
            obj, is_new = Region.objects.update_or_create(
                code=code,
                defaults={'name': name, 'geom': geom},
            )
        tag = 'created' if is_new else 'updated'
        self.stdout.write(self.style.SUCCESS(
            f'{tag}: {obj.name} (code={obj.code}, pk={obj.pk})'
        ))


def _coerce_multipolygon(raw: dict) -> MultiPolygon:
    """
    Convert the GeoJSON payload returned by polygons.openstreetmap.fr
    (which may be a FeatureCollection, Feature, GeometryCollection,
    Polygon or MultiPolygon) into a MultiPolygon at SRID 4326.
    """
    if raw.get('type') == 'FeatureCollection':
        geometries = [f['geometry'] for f in raw.get('features', []) if f.get('geometry')]
        if not geometries:
            raise ValueError('empty FeatureCollection')
        raw = geometries[0] if len(geometries) == 1 else {
            'type': 'GeometryCollection', 'geometries': geometries,
        }
    elif raw.get('type') == 'Feature':
        raw = raw['geometry']

    if raw.get('type') == 'GeometryCollection':
        # Take the first polygonal member
        for g in raw.get('geometries', []):
            if g.get('type') in ('Polygon', 'MultiPolygon'):
                raw = g
                break
        else:
            raise ValueError('GeometryCollection has no polygonal geometry')

    geom = GEOSGeometry(json.dumps(raw), srid=4326)
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon(geom, srid=4326)
    elif geom.geom_type != 'MultiPolygon':
        raise ValueError(f'unexpected geometry type: {geom.geom_type}')
    return geom
