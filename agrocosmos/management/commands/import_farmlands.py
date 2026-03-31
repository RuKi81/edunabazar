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
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
from django.db import transaction

from agrocosmos.models import Region, District, Farmland


# Map common Russian/English names to our CropType enum
_CROP_TYPE_MAP = {
    'пашня': 'arable',
    'пашни': 'arable',
    'arable': 'arable',
    'сенокос': 'hayfield',
    'сенокосы': 'hayfield',
    'hayfield': 'hayfield',
    'пастбище': 'pasture',
    'пастбища': 'pasture',
    'pasture': 'pasture',
    'многолетнее насаждение': 'perennial',
    'многолетние насаждения': 'perennial',
    'perennial': 'perennial',
}


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

    def _to_multi(self, geom):
        if geom.geom_type == 'Polygon':
            return MultiPolygon(geom, srid=geom.srid)
        return geom

    def _resolve_crop_type(self, raw, default):
        if not raw:
            return default
        key = raw.strip().lower()
        return _CROP_TYPE_MAP.get(key, default)

    def _resolve_district(self, raw, districts, region=None, auto_create=False, fallback=None):
        if not raw:
            return fallback
        key = raw.strip().lower()
        if key in districts:
            return districts[key]
        # Fuzzy: try startswith
        for dname, d in districts.items():
            if dname.startswith(key) or key.startswith(dname):
                return d
        # Auto-create if enabled
        if auto_create and region and raw.strip():
            code = raw.strip().lower().replace(' ', '_')[:100]
            d, created = District.objects.get_or_create(
                region=region, name=raw.strip(),
                defaults={'code': code, 'geom': region.geom},
            )
            districts[key] = d
            if created:
                self.stdout.write(f'  + District: {raw.strip()}')
            return d
        return fallback

    def _import_geojson(self, path, region, districts, options):
        crop_field = options['crop_type_field']
        area_field = options['area_field']
        cad_field = options['cadastral_field']
        dist_field = options['district_field']
        default_crop = options['default_crop_type']
        batch_size = options['batch_size']

        with open(path, 'r', encoding=options['encoding']) as f:
            data = json.load(f)

        features = data.get('features', [])
        self.stdout.write(f'Found {len(features)} features')

        # Use first district as fallback if only one exists
        fallback_district = list(districts.values())[0] if len(districts) == 1 else None

        batch = []
        created = 0
        skipped = 0

        for feat in features:
            props = feat.get('properties', {})
            district = self._resolve_district(
                str(props.get(dist_field, '')), districts,
                region=region, auto_create=options.get('auto_create_districts', False),
                fallback=fallback_district,
            )
            if not district:
                skipped += 1
                continue

            crop_type = self._resolve_crop_type(str(props.get(crop_field, '')), default_crop)

            try:
                area = float(props.get(area_field, 0) or 0)
            except (TypeError, ValueError):
                area = 0

            cadastral = str(props.get(cad_field, '') or '').strip()

            try:
                geom = GEOSGeometry(json.dumps(feat['geometry']), srid=4326)
                geom = self._to_multi(geom)
            except Exception as e:
                self.stderr.write(f'Bad geometry: {e}')
                skipped += 1
                continue

            # If area not in properties, compute from geometry
            if area <= 0:
                try:
                    geom_proj = geom.clone()
                    geom_proj.transform(32637)  # UTM zone 37N (Crimea)
                    area = round(geom_proj.area / 10000, 4)
                except Exception:
                    area = 0

            batch.append(Farmland(
                district=district,
                crop_type=crop_type,
                cadastral_number=cadastral,
                area_ha=area,
                geom=geom,
                properties={k: str(v) for k, v in props.items()} if props else None,
            ))

            if len(batch) >= batch_size:
                Farmland.objects.bulk_create(batch)
                created += len(batch)
                self.stdout.write(f'  ... {created} created')
                batch = []

        if batch:
            Farmland.objects.bulk_create(batch)
            created += len(batch)

        self.stdout.write(self.style.SUCCESS(f'Done: {created} created, {skipped} skipped'))

    def _import_shapefile(self, path, region, districts, options):
        try:
            from django.contrib.gis.gdal import DataSource
        except ImportError:
            self.stderr.write('GDAL DataSource not available')
            return

        crop_field = options['crop_type_field']
        area_field = options['area_field']
        cad_field = options['cadastral_field']
        dist_field = options['district_field']
        default_crop = options['default_crop_type']
        batch_size = options['batch_size']

        ds = DataSource(path, encoding=options['encoding'])
        layer = ds[0]
        self.stdout.write(f'Found {len(layer)} features, fields: {layer.fields}')

        fallback_district = list(districts.values())[0] if len(districts) == 1 else None

        batch = []
        created = 0
        skipped = 0

        for feat in layer:
            dist_name = ''
            try:
                dist_name = str(feat.get(dist_field))
            except Exception:
                pass

            district = self._resolve_district(
                dist_name, districts,
                region=region, auto_create=options.get('auto_create_districts', False),
                fallback=fallback_district,
            )
            if not district:
                skipped += 1
                continue

            crop_raw = ''
            try:
                crop_raw = str(feat.get(crop_field))
            except Exception:
                pass
            crop_type = self._resolve_crop_type(crop_raw, default_crop)

            try:
                area = float(feat.get(area_field) or 0)
            except Exception:
                area = 0

            try:
                cadastral = str(feat.get(cad_field) or '').strip()
            except Exception:
                cadastral = ''

            try:
                geom = GEOSGeometry(feat.geom.wkt, srid=feat.geom.srid or 4326)
                if geom.srid != 4326:
                    geom.transform(4326)
                geom = self._to_multi(geom)
            except Exception as e:
                self.stderr.write(f'Bad geometry: {e}')
                skipped += 1
                continue

            if area <= 0:
                try:
                    geom_proj = geom.clone()
                    geom_proj.transform(32637)
                    area = round(geom_proj.area / 10000, 4)
                except Exception:
                    area = 0

            # Collect extra properties
            extra_props = {}
            for field_name in layer.fields:
                if field_name not in (dist_field, crop_field, area_field, cad_field):
                    try:
                        val = feat.get(field_name)
                        if val is not None:
                            extra_props[field_name] = str(val)
                    except Exception:
                        pass

            batch.append(Farmland(
                district=district,
                crop_type=crop_type,
                cadastral_number=cadastral,
                area_ha=area,
                geom=geom,
                properties=extra_props if extra_props else None,
            ))

            if len(batch) >= batch_size:
                Farmland.objects.bulk_create(batch)
                created += len(batch)
                self.stdout.write(f'  ... {created} created')
                batch = []

        if batch:
            Farmland.objects.bulk_create(batch)
            created += len(batch)

        self.stdout.write(self.style.SUCCESS(f'Done: {created} created, {skipped} skipped'))
