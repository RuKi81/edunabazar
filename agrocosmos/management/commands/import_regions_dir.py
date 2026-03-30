"""
Batch-import all regions from a directory of individual GeoJSON files.

Each file = one region (FeatureCollection with a single Feature).
Expected properties: NAME (region name), optionally ADM3_NAME (federal district).

Usage:
    python manage.py import_regions_dir "C:/path/to/geojson_folder"
    python manage.py import_regions_dir "C:/path/to/geojson_folder" --clear
"""
import json
import os

from django.core.management.base import BaseCommand
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon

from agrocosmos.models import Region


class Command(BaseCommand):
    help = 'Batch-import regions from a directory of GeoJSON files (one file per region)'

    def add_arguments(self, parser):
        parser.add_argument('directory', help='Path to directory with .geojson files')
        parser.add_argument('--name-field', default='NAME', help='Property field for region name')
        parser.add_argument('--encoding', default='utf-8', help='File encoding')
        parser.add_argument('--clear', action='store_true', help='Delete all existing regions first')

    def handle(self, *args, **options):
        directory = options['directory']
        name_field = options['name_field']
        encoding = options['encoding']

        if not os.path.isdir(directory):
            self.stderr.write(f'Directory not found: {directory}')
            return

        if options['clear']:
            deleted, _ = Region.objects.all().delete()
            self.stdout.write(f'Deleted {deleted} existing region(s)')

        files = sorted([
            f for f in os.listdir(directory)
            if f.lower().endswith('.geojson') or f.lower().endswith('.json')
        ])

        if not files:
            self.stderr.write('No .geojson files found in directory')
            return

        self.stdout.write(f'Found {len(files)} file(s) in {directory}')

        created = 0
        updated = 0
        errors = 0

        for fname in files:
            filepath = os.path.join(directory, fname)
            code = os.path.splitext(fname)[0]  # filename without extension as code

            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    data = json.load(f)
            except Exception as e:
                self.stderr.write(f'  ERROR reading {fname}: {e}')
                errors += 1
                continue

            features = data.get('features', [])
            if not features:
                self.stderr.write(f'  SKIP {fname}: no features')
                errors += 1
                continue

            # Take first feature (each file = one region)
            feat = features[0]
            props = feat.get('properties', {})
            name = str(props.get(name_field, '')).strip()

            if not name:
                self.stderr.write(f'  SKIP {fname}: no NAME property')
                errors += 1
                continue

            try:
                geom_json = json.dumps(feat['geometry'])
                geom = GEOSGeometry(geom_json, srid=4326)
                if geom.geom_type == 'Polygon':
                    geom = MultiPolygon(geom, srid=4326)
            except Exception as e:
                self.stderr.write(f'  ERROR {fname} geometry: {e}')
                errors += 1
                continue

            obj, is_new = Region.objects.update_or_create(
                code=code,
                defaults={'name': name, 'geom': geom},
            )

            if is_new:
                created += 1
                self.stdout.write(f'  + {name} ({code})')
            else:
                updated += 1
                self.stdout.write(f'  ~ {name} ({code})')

        self.stdout.write(self.style.SUCCESS(
            f'\nDone: {created} created, {updated} updated, {errors} errors'
        ))
