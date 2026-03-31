"""
Fetch NDVI zonal statistics for farmlands via CDSE Sentinel Hub Statistical API.

Usage:
    # All farmlands in a region, last 30 days
    python manage.py fetch_ndvi --region-id 1

    # Specific district
    python manage.py fetch_ndvi --district-id 5

    # Single farmland, custom date range
    python manage.py fetch_ndvi --farmland-id 42 --date-from 2025-05-01 --date-to 2025-06-30

    # Limit cloud cover
    python manage.py fetch_ndvi --region-id 1 --cloud-max 20
"""
import json
from datetime import date, timedelta

from django.core.management.base import BaseCommand

from agrocosmos.models import Farmland, SatelliteScene, VegetationIndex
from agrocosmos.services.satellite import fetch_ndvi_stats, CDSEError


class Command(BaseCommand):
    help = 'Fetch NDVI statistics for farmlands from Sentinel-2 via CDSE'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, help='Process farmlands in this region')
        parser.add_argument('--district-id', type=int, help='Process farmlands in this district')
        parser.add_argument('--farmland-id', type=int, help='Process a single farmland')
        parser.add_argument('--date-from', type=str, help='Start date YYYY-MM-DD (default: 30 days ago)')
        parser.add_argument('--date-to', type=str, help='End date YYYY-MM-DD (default: today)')
        parser.add_argument('--cloud-max', type=int, default=30, help='Max cloud cover %% (default: 30)')
        parser.add_argument('--limit', type=int, default=0, help='Limit number of farmlands to process')

    def handle(self, *args, **options):
        # Build queryset
        qs = Farmland.objects.all()
        if options['farmland_id']:
            qs = qs.filter(pk=options['farmland_id'])
        elif options['district_id']:
            qs = qs.filter(district_id=options['district_id'])
        elif options['region_id']:
            qs = qs.filter(district__region_id=options['region_id'])
        else:
            self.stderr.write('Specify --region-id, --district-id, or --farmland-id')
            return

        if options['limit']:
            qs = qs[:options['limit']]

        farmlands = list(qs)
        if not farmlands:
            self.stderr.write('No farmlands found matching criteria')
            return

        # Date range
        date_to = date.today()
        date_from = date_to - timedelta(days=30)
        if options['date_from']:
            date_from = date.fromisoformat(options['date_from'])
        if options['date_to']:
            date_to = date.fromisoformat(options['date_to'])

        cloud_max = options['cloud_max']

        self.stdout.write(
            f'Processing {len(farmlands)} farmland(s), '
            f'{date_from} → {date_to}, cloud ≤ {cloud_max}%'
        )

        created_total = 0
        errors = 0

        for i, fl in enumerate(farmlands, 1):
            self.stdout.write(f'  [{i}/{len(farmlands)}] Farmland #{fl.pk} ({fl.area_ha:.1f} ha)...')

            # Convert MultiPolygon → Polygon GeoJSON for API
            geom = fl.geom
            if geom.geom_type == 'MultiPolygon' and len(geom) == 1:
                geom_json = json.loads(geom[0].geojson)
            else:
                geom_json = json.loads(geom.geojson)

            try:
                stats = fetch_ndvi_stats(
                    geometry_geojson=geom_json,
                    date_from=date_from,
                    date_to=date_to,
                    cloud_max=cloud_max,
                )
            except CDSEError as e:
                self.stderr.write(f'    ERROR: {e}')
                errors += 1
                continue
            except Exception as e:
                self.stderr.write(f'    UNEXPECTED ERROR: {e}')
                errors += 1
                continue

            if not stats:
                self.stdout.write(f'    No valid data for this period')
                continue

            created = 0
            for s in stats:
                # Get or create a SatelliteScene placeholder
                scene_id = f's2_{s["date"]}_{fl.district_id or 0}'
                scene, _ = SatelliteScene.objects.get_or_create(
                    scene_id=scene_id,
                    defaults={
                        'satellite': 'sentinel2',
                        'acquired_date': s['date'],
                        'cloud_cover': 0,
                        'processed': True,
                    },
                )

                # Upsert VegetationIndex
                _, is_new = VegetationIndex.objects.update_or_create(
                    farmland=fl,
                    scene=scene,
                    index_type='ndvi',
                    defaults={
                        'acquired_date': s['date'],
                        'mean': s['mean'],
                        'median': s['median'],
                        'min_val': s['min'],
                        'max_val': s['max'],
                        'std_val': s['std'],
                        'pixel_count': s['pixel_count'],
                        'valid_pixel_count': s['valid_pixel_count'],
                    },
                )
                if is_new:
                    created += 1

            created_total += created
            self.stdout.write(f'    → {len(stats)} dates, {created} new records')

        self.stdout.write(self.style.SUCCESS(
            f'\nDone: {created_total} new NDVI records, {errors} error(s)'
        ))
