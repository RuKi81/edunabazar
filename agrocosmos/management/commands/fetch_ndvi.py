"""
Fetch NDVI zonal statistics for farmlands via CDSE Sentinel Hub Statistical API.

Usage:
    # All farmlands in Crimea for 2025 (splits into monthly chunks automatically)
    python manage.py fetch_ndvi --region-id 1 --date-from 2025-01-01 --date-to 2025-12-31

    # Specific district
    python manage.py fetch_ndvi --district-id 5 --date-from 2025-03-01 --date-to 2025-10-31

    # Single farmland, custom date range
    python manage.py fetch_ndvi --farmland-id 42 --date-from 2025-05-01 --date-to 2025-06-30

    # Adjust valid pixel threshold (skip if >10% cloud/nodata)
    python manage.py fetch_ndvi --region-id 1 --min-valid-ratio 0.90

    # Resume interrupted run (skips farmland+month pairs already in DB)
    python manage.py fetch_ndvi --region-id 1 --date-from 2025-01-01 --date-to 2025-12-31 --resume

    # Limit to first N farmlands (for testing)
    python manage.py fetch_ndvi --region-id 1 --limit 5

    # Start from a specific farmland ID (skip earlier ones)
    python manage.py fetch_ndvi --region-id 1 --start-from-id 50000

Features:
- Automatically splits date range into monthly chunks for API reliability
- Resume mode: skips farmland+month pairs that already have data in DB
- Throttle between API calls to avoid rate limits (default 1.5s)
- Graceful Ctrl+C: prints progress summary before exit
- Filters out dates where >5% of polygon pixels are cloud/nodata
"""
import json
import signal
import time
from calendar import monthrange
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db.models import Min, Max

from agrocosmos.models import Farmland, SatelliteScene, VegetationIndex
from agrocosmos.services.satellite import fetch_ndvi_stats, CDSEError


def _month_chunks(date_from, date_to):
    """Split date range into (first_day, last_day) tuples per month."""
    chunks = []
    cursor = date_from.replace(day=1)
    while cursor <= date_to:
        y, m = cursor.year, cursor.month
        first = max(cursor, date_from)
        last = min(date(y, m, monthrange(y, m)[1]), date_to)
        chunks.append((first, last))
        # Move to first day of next month
        if m == 12:
            cursor = date(y + 1, 1, 1)
        else:
            cursor = date(y, m + 1, 1)
    return chunks


class Command(BaseCommand):
    help = 'Fetch NDVI statistics for farmlands from Sentinel-2 via CDSE'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_requested = False

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, help='Process farmlands in this region')
        parser.add_argument('--district-id', type=int, help='Process farmlands in this district')
        parser.add_argument('--farmland-id', type=int, help='Process a single farmland')
        parser.add_argument('--date-from', type=str, help='Start date YYYY-MM-DD (default: 30 days ago)')
        parser.add_argument('--date-to', type=str, help='End date YYYY-MM-DD (default: today)')
        parser.add_argument('--cloud-max', type=int, default=30,
                            help='Max cloud cover %% for scene pre-filter (default: 30)')
        parser.add_argument('--min-valid-ratio', type=float, default=0.95,
                            help='Min ratio of valid pixels per date (default: 0.95 = skip if >5%% cloud/nodata)')
        parser.add_argument('--limit', type=int, default=0,
                            help='Limit number of farmlands to process')
        parser.add_argument('--start-from-id', type=int, default=0,
                            help='Start processing from farmland ID >= this value')
        parser.add_argument('--resume', action='store_true',
                            help='Skip farmland+month pairs that already have data in DB')
        parser.add_argument('--throttle', type=float, default=1.5,
                            help='Seconds to wait between API calls (default: 1.5)')

    def handle(self, *args, **options):
        # Graceful stop on Ctrl+C
        def _signal_handler(sig, frame):
            self._stop_requested = True
            self.stderr.write(self.style.WARNING('\n⚠ Ctrl+C — finishing current farmland…'))
        signal.signal(signal.SIGINT, _signal_handler)

        # Build queryset
        qs = Farmland.objects.select_related('district').all()
        if options['farmland_id']:
            qs = qs.filter(pk=options['farmland_id'])
        elif options['district_id']:
            qs = qs.filter(district_id=options['district_id'])
        elif options['region_id']:
            qs = qs.filter(district__region_id=options['region_id'])
        else:
            self.stderr.write('Specify --region-id, --district-id, or --farmland-id')
            return

        if options['start_from_id']:
            qs = qs.filter(pk__gte=options['start_from_id'])

        qs = qs.order_by('pk')

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
        min_valid = options['min_valid_ratio']
        resume = options['resume']
        throttle = options['throttle']

        # Split into monthly chunks
        chunks = _month_chunks(date_from, date_to)

        self.stdout.write(
            f'═══════════════════════════════════════════════\n'
            f'  NDVI Fetch: {len(farmlands)} farmland(s)\n'
            f'  Period: {date_from} → {date_to} ({len(chunks)} month chunks)\n'
            f'  Cloud pre-filter: ≤{cloud_max}%  |  Valid pixel threshold: ≥{min_valid*100:.0f}%\n'
            f'  Resume: {resume}  |  Throttle: {throttle}s\n'
            f'═══════════════════════════════════════════════'
        )

        created_total = 0
        updated_total = 0
        skipped_total = 0
        errors = 0
        api_calls = 0
        t0 = time.time()

        for i, fl in enumerate(farmlands, 1):
            if self._stop_requested:
                break

            self.stdout.write(
                f'\n  [{i}/{len(farmlands)}] Farmland #{fl.pk}'
                f' ({fl.area_ha:.1f} ha, {fl.district.name})'
            )

            # Convert MultiPolygon → Polygon GeoJSON for API
            geom = fl.geom
            if geom.geom_type == 'MultiPolygon' and len(geom) == 1:
                geom_json = json.loads(geom[0].geojson)
            else:
                geom_json = json.loads(geom.geojson)

            fl_created = 0
            fl_updated = 0

            for chunk_from, chunk_to in chunks:
                if self._stop_requested:
                    break

                # Resume: check if data already exists for this farmland+month
                if resume:
                    existing = VegetationIndex.objects.filter(
                        farmland=fl,
                        index_type='ndvi',
                        acquired_date__gte=chunk_from,
                        acquired_date__lte=chunk_to,
                    ).exists()
                    if existing:
                        skipped_total += 1
                        continue

                # Throttle between API calls
                if api_calls > 0:
                    time.sleep(throttle)

                try:
                    stats = fetch_ndvi_stats(
                        geometry_geojson=geom_json,
                        date_from=chunk_from,
                        date_to=chunk_to,
                        cloud_max=cloud_max,
                        min_valid_ratio=min_valid,
                    )
                    api_calls += 1
                except CDSEError as e:
                    self.stderr.write(f'    ERROR ({chunk_from}..{chunk_to}): {e}')
                    errors += 1
                    # If token error, wait and retry once
                    if '401' in str(e) or '403' in str(e):
                        self.stderr.write('    Retrying in 10s…')
                        time.sleep(10)
                        try:
                            stats = fetch_ndvi_stats(
                                geometry_geojson=geom_json,
                                date_from=chunk_from,
                                date_to=chunk_to,
                                cloud_max=cloud_max,
                                min_valid_ratio=min_valid,
                            )
                            api_calls += 1
                            errors -= 1  # retry succeeded
                        except Exception:
                            continue
                    else:
                        continue
                except Exception as e:
                    self.stderr.write(f'    UNEXPECTED ERROR ({chunk_from}..{chunk_to}): {e}')
                    errors += 1
                    continue

                if not stats:
                    continue

                for s in stats:
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
                        fl_created += 1
                    else:
                        fl_updated += 1

            created_total += fl_created
            updated_total += fl_updated
            if fl_created or fl_updated:
                self.stdout.write(
                    f'    → +{fl_created} new, {fl_updated} updated'
                )

            # Progress estimate
            elapsed = time.time() - t0
            if i > 0 and elapsed > 0:
                rate = i / elapsed
                eta = (len(farmlands) - i) / rate if rate > 0 else 0
                eta_min = int(eta // 60)
                eta_sec = int(eta % 60)
                self.stdout.write(
                    f'    [{i}/{len(farmlands)}] '
                    f'{api_calls} API calls, '
                    f'{created_total} new, {errors} err | '
                    f'ETA: {eta_min}m{eta_sec:02d}s'
                )

        # Final summary
        elapsed = time.time() - t0
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        self.stdout.write(
            f'\n═══════════════════════════════════════════════\n'
            f'  Done in {minutes}m{seconds:02d}s\n'
            f'  API calls: {api_calls}\n'
            f'  New records: {created_total}\n'
            f'  Updated records: {updated_total}\n'
            f'  Skipped (resume): {skipped_total}\n'
            f'  Errors: {errors}\n'
            f'═══════════════════════════════════════════════'
        )
        if self._stop_requested:
            self.stderr.write(self.style.WARNING(
                f'Interrupted at farmland #{farmlands[min(i, len(farmlands))-1].pk}. '
                f'Use --start-from-id {farmlands[min(i, len(farmlands))-1].pk} to continue.'
            ))
