"""
Import farmland polygons from a GeoJSON or Shapefile.

Usage:
    python manage.py import_farmlands /path/to/farmlands.geojson \
        --region-code 91 \
        --crop-type-field LAND_TYPE \
        --area-field AREA_HA \
        --cadastral-field CAD_NUM \
        --district-field DISTRICT
"""
import json

from django.core.management.base import BaseCommand

from agrocosmos.models import Region, District, Farmland
from agrocosmos.services.import_vector import (
    _FarmlandBatch,
    _geojson_farmland,
    _shp_farmland,
)


class Command(BaseCommand):
    help = 'Import farmland polygons from GeoJSON or Shapefile'

    def add_arguments(self, parser):
        parser.add_argument('source', help='Path to GeoJSON or Shapefile')
        parser.add_argument('--region-code', required=True, help='Region code')
        parser.add_argument('--crop-type-field', default='LAND_TYPE', help='Property field for crop type')
        parser.add_argument('--area-field', default='AREA_HA', help='Property field for area in hectares')
        parser.add_argument('--cadastral-field', default='CAD_NUM', help='Property field for cadastral number')
        parser.add_argument('--district-field', default='DISTRICT', help='Property field for district name')
        parser.add_argument('--default-crop-type', default='arable', help='Default crop type if not in data')
        parser.add_argument('--encoding', default='utf-8', help='File encoding')
        parser.add_argument('--clear', action='store_true', help='Delete existing farmlands for this region')
        parser.add_argument('--batch-size', type=int, default=500, help='Batch size for bulk_create')
        parser.add_argument('--auto-create-districts', action='store_true',
                            help='Auto-create District objects from unique values in district field')

    def handle(self, *args, **options):
        source = options['source']
        region_code = options['region_code']

        try:
            region = Region.objects.get(code=region_code)
        except Region.DoesNotExist:
            self.stderr.write(f'Region with code={region_code} not found')
            return

        districts = {d.name.lower(): d for d in District.objects.filter(region=region)}

        if options['auto_create_districts']:
            self.stdout.write('Auto-create districts enabled — will create from data')
        elif not districts:
            self.stderr.write(
                f'No districts found for {region.name}. '
                f'Use --auto-create-districts or import districts first.'
            )
            return

        if options['clear']:
            deleted, _ = Farmland.objects.filter(district__region=region).delete()
            self.stdout.write(f'Deleted {deleted} existing farmland(s)')

        self.stdout.write(f'Loading {source} ...')

        if source.lower().endswith(('.geojson', '.json')):
            self._import_geojson(source, region, districts, options)
        else:
            self._import_shapefile(source, region, districts, options)

    def _import_geojson(self, path, region, districts, options):
        with open(path, 'r', encoding=options['encoding']) as f:
            data = json.load(f)

        features = data.get('features', [])
        self.stdout.write(f'Found {len(features)} features')

        fallback = list(districts.values())[0] if len(districts) == 1 else None

        def parse(feat):
            return _geojson_farmland(
                feat, region,
                options['crop_type_field'], options['area_field'],
                options['cadastral_field'], options['district_field'],
                districts, fallback,
                options.get('auto_create_districts', False),
                default_crop=options['default_crop_type'],
            )

        self._bulk_import(features, parse, options['batch_size'])

    def _import_shapefile(self, path, region, districts, options):
        try:
            from django.contrib.gis.gdal import DataSource
        except ImportError:
            self.stderr.write('GDAL DataSource not available')
            return

        ds = DataSource(path, encoding=options['encoding'])
        layer = ds[0]
        self.stdout.write(f'Found {len(layer)} features, fields: {layer.fields}')

        fallback = list(districts.values())[0] if len(districts) == 1 else None

        def parse(feat):
            return _shp_farmland(
                feat, layer.fields, region,
                options['crop_type_field'], options['area_field'],
                options['cadastral_field'], options['district_field'],
                districts, fallback,
                options.get('auto_create_districts', False),
                default_crop=options['default_crop_type'],
            )

        self._bulk_import(layer, parse, options['batch_size'])

    def _bulk_import(self, features, parse, batch_size):
        """Общий цикл: parse → пакетный bulk_create с прогрессом."""
        batch = _FarmlandBatch(batch_size)
        skipped = 0
        reported = 0

        for feat in features:
            farmland, error = parse(feat)
            if error:
                self.stderr.write(f'Bad geometry: {error}')
            if farmland is None:
                skipped += 1
                continue
            batch.add(farmland)
            if batch.created > reported:
                reported = batch.created
                self.stdout.write(f'  ... {batch.created} created')

        batch.flush()
        self.stdout.write(self.style.SUCCESS(
            f'Done: {batch.created} created, {skipped} skipped'
        ))
