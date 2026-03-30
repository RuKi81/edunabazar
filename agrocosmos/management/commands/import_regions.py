"""
Import regions (субъекты РФ) from a GeoJSON or Shapefile.

Usage:
    python manage.py import_regions /path/to/regions.geojson --name-field NAME --code-field CODE
    python manage.py import_regions /path/to/regions.shp --name-field NAME --code-field OKATO
"""
import json

from django.core.management.base import BaseCommand
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon

from agrocosmos.models import Region


class Command(BaseCommand):
    help = 'Import regions from GeoJSON or Shapefile'

    def add_arguments(self, parser):
        parser.add_argument('source', help='Path to GeoJSON or Shapefile')
        parser.add_argument('--name-field', default='NAME', help='Property field for region name')
        parser.add_argument('--code-field', default='CODE', help='Property field for region code')
        parser.add_argument('--encoding', default='utf-8', help='File encoding')
        parser.add_argument('--clear', action='store_true', help='Delete all existing regions first')

    def handle(self, *args, **options):
        source = options['source']
        name_field = options['name_field']
        code_field = options['code_field']

        if options['clear']:
            deleted, _ = Region.objects.all().delete()
            self.stdout.write(f'Deleted {deleted} existing region(s)')

        if source.lower().endswith('.geojson') or source.lower().endswith('.json'):
            self._import_geojson(source, name_field, code_field, options['encoding'])
        else:
            self._import_shapefile(source, name_field, code_field, options['encoding'])

    def _to_multi(self, geom):
        if geom.geom_type == 'Polygon':
            return MultiPolygon(geom, srid=geom.srid)
        return geom

    def _import_geojson(self, path, name_field, code_field, encoding):
        with open(path, 'r', encoding=encoding) as f:
            data = json.load(f)

        features = data.get('features', [])
        created = 0
        updated = 0

        for feat in features:
            props = feat.get('properties', {})
            name = str(props.get(name_field, '')).strip()
            code = str(props.get(code_field, '')).strip()

            if not name:
                self.stderr.write(f'Skipping feature without name: {props}')
                continue

            geom_json = json.dumps(feat['geometry'])
            geom = GEOSGeometry(geom_json, srid=4326)
            geom = self._to_multi(geom)

            obj, is_new = Region.objects.update_or_create(
                code=code or f'auto_{name[:20]}',
                defaults={'name': name, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(f'Done: {created} created, {updated} updated'))

    def _import_shapefile(self, path, name_field, code_field, encoding):
        try:
            from django.contrib.gis.gdal import DataSource
        except ImportError:
            self.stderr.write('GDAL DataSource not available')
            return

        ds = DataSource(path, encoding=encoding)
        layer = ds[0]
        created = 0
        updated = 0

        for feat in layer:
            name = str(feat.get(name_field)).strip()
            code = str(feat.get(code_field)).strip()

            if not name:
                continue

            geom = GEOSGeometry(feat.geom.wkt, srid=feat.geom.srid or 4326)
            if geom.srid != 4326:
                geom.transform(4326)
            geom = self._to_multi(geom)

            obj, is_new = Region.objects.update_or_create(
                code=code or f'auto_{name[:20]}',
                defaults={'name': name, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(f'Done: {created} created, {updated} updated'))
