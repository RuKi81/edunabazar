"""
Import districts (муниципальные районы) from a GeoJSON or Shapefile.

Usage:
    python manage.py import_districts /path/to/districts.geojson --region-code 91 --name-field NAME --code-field CODE
"""
import json

from django.core.management.base import BaseCommand
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon

from agrocosmos.models import Region, District


class Command(BaseCommand):
    help = 'Import districts from GeoJSON or Shapefile'

    def add_arguments(self, parser):
        parser.add_argument('source', help='Path to GeoJSON or Shapefile')
        parser.add_argument('--region-code', required=True, help='Region code to attach districts to')
        parser.add_argument('--name-field', default='NAME', help='Property field for district name')
        parser.add_argument('--code-field', default='CODE', help='Property field for district code')
        parser.add_argument('--encoding', default='utf-8', help='File encoding')
        parser.add_argument('--clear', action='store_true', help='Delete existing districts for this region')

    def handle(self, *args, **options):
        source = options['source']
        region_code = options['region_code']

        try:
            region = Region.objects.get(code=region_code)
        except Region.DoesNotExist:
            self.stderr.write(f'Region with code={region_code} not found')
            return

        if options['clear']:
            deleted, _ = District.objects.filter(region=region).delete()
            self.stdout.write(f'Deleted {deleted} existing district(s) for {region.name}')

        if source.lower().endswith(('.geojson', '.json')):
            self._import_geojson(source, region, options)
        else:
            self._import_shapefile(source, region, options)

    def _to_multi(self, geom):
        if geom.geom_type == 'Polygon':
            return MultiPolygon(geom, srid=geom.srid)
        return geom

    def _import_geojson(self, path, region, options):
        name_field = options['name_field']
        code_field = options['code_field']

        with open(path, 'r', encoding=options['encoding']) as f:
            data = json.load(f)

        features = data.get('features', [])
        created = updated = 0

        for feat in features:
            props = feat.get('properties', {})
            name = str(props.get(name_field, '')).strip()
            code = str(props.get(code_field, '')).strip()

            if not name:
                self.stderr.write(f'Skipping feature without name: {props}')
                continue

            geom = GEOSGeometry(json.dumps(feat['geometry']), srid=4326)
            geom = self._to_multi(geom)

            obj, is_new = District.objects.update_or_create(
                region=region,
                name=name,
                defaults={'code': code, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(f'{region.name}: {created} created, {updated} updated'))

    def _import_shapefile(self, path, region, options):
        try:
            from django.contrib.gis.gdal import DataSource
        except ImportError:
            self.stderr.write('GDAL DataSource not available')
            return

        name_field = options['name_field']
        code_field = options['code_field']

        ds = DataSource(path, encoding=options['encoding'])
        layer = ds[0]
        created = updated = 0

        for feat in layer:
            name = str(feat.get(name_field)).strip()
            code = str(feat.get(code_field)).strip()
            if not name:
                continue

            geom = GEOSGeometry(feat.geom.wkt, srid=feat.geom.srid or 4326)
            if geom.srid != 4326:
                geom.transform(4326)
            geom = self._to_multi(geom)

            obj, is_new = District.objects.update_or_create(
                region=region,
                name=name,
                defaults={'code': code, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(f'{region.name}: {created} created, {updated} updated'))
