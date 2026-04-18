"""
Batch fetch NDVI statistics for farmlands using GEE reduceRegions().

Processes multiple polygons per API call (~500x faster than per-polygon mode).
Supports Sentinel-2 (10m, monthly composites) and MODIS (250m, 16-day composites).

Usage:
    # Sentinel-2: All farmlands in Crimea for 2025
    python manage.py fetch_ndvi_batch --region-id 37 \
        --date-from 2025-01-01 --date-to 2025-12-31

    # MODIS: faster, 16-day frequency, good for region-level assessment
    python manage.py fetch_ndvi_batch --region-id 37 --sensor modis \
        --date-from 2025-01-01 --date-to 2025-12-31

    # Resume from a specific farmland ID
    python manage.py fetch_ndvi_batch --region-id 37 \
        --date-from 2025-01-01 --date-to 2025-12-31 --start-from-id 50000

    # Specific district
    python manage.py fetch_ndvi_batch --district-id 5 \
        --date-from 2025-03-01 --date-to 2025-10-31

Performance (133K farmlands, 12 months, batch-size 500):
    Sentinel-2: ~3,200 calls × ~2.5min ≈ 5-6 days
    MODIS:      ~3,200 calls × ~3sec  ≈ 2-3 hours
"""
import json
import signal
import time
from calendar import monthrange
from datetime import date, timedelta

from django.core.management.base import BaseCommand

from agrocosmos.models import Farmland, SatelliteScene, VegetationIndex


def _simplify_coords(geojson, precision=4):
    """Truncate coordinate precision to reduce GeoJSON payload size.

    precision=4 → ~11m accuracy (fine for MODIS 250m).
    precision=6 → ~0.11m accuracy (fine for Sentinel-2 10m).
    """
    def _round(coords):
        if isinstance(coords[0], (int, float)):
            return [round(c, precision) for c in coords]
        return [_round(c) for c in coords]

    result = dict(geojson)
    result['coordinates'] = _round(geojson['coordinates'])
    return result


def _month_chunks(date_from, date_to):
    """Split date range into (first_day, last_day) tuples per month."""
    chunks = []
    cursor = date_from.replace(day=1)
    while cursor <= date_to:
        y, m = cursor.year, cursor.month
        first = max(cursor, date_from)
        last = min(date(y, m, monthrange(y, m)[1]), date_to)
        chunks.append((first, last))
        if m == 12:
            cursor = date(y + 1, 1, 1)
        else:
            cursor = date(y, m + 1, 1)
    return chunks


def _biweekly_chunks(date_from, date_to):
    """Split date range into 5-day periods (Terra+Aqua offset gives ~5-day cadence)."""
    chunks = []
    cursor = date_from
    while cursor <= date_to:
        end = min(cursor + timedelta(days=4), date_to)
        chunks.append((cursor, end))
        cursor = end + timedelta(days=1)
    return chunks


class Command(BaseCommand):
    help = 'Batch fetch NDVI for farmlands using GEE reduceRegions (fast)'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_requested = False

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, help='Process farmlands in this region')
        parser.add_argument('--district-id', type=int, help='Process farmlands in this district')
        parser.add_argument('--date-from', type=str, required=True, help='Start date YYYY-MM-DD')
        parser.add_argument('--date-to', type=str, required=True, help='End date YYYY-MM-DD')
        parser.add_argument('--batch-size', type=int, default=500,
                            help='Polygons per GEE reduceRegions call (default: 500)')
        parser.add_argument('--cloud-max', type=int, default=30,
                            help='Max cloud cover %% for scene pre-filter (default: 30)')
        parser.add_argument('--min-valid-ratio', type=float, default=0.95,
                            help='Min valid pixel ratio (default: 0.95)')
        parser.add_argument('--start-from-id', type=int, default=0,
                            help='Start from farmland PK >= this value (for resume)')
        parser.add_argument('--throttle', type=float, default=2.0,
                            help='Seconds between GEE batch calls (default: 2.0)')
        parser.add_argument('--limit', type=int, default=0,
                            help='Limit total farmlands (for testing)')
        parser.add_argument('--sensor', type=str, default='s2',
                            choices=['s2', 'modis'],
                            help='Sensor: s2 (Sentinel-2, 10m) or modis (MODIS Terra, 250m)')

    def handle(self, *args, **options):
        from agrocosmos.services.satellite_gee import GEEError

        sensor = options['sensor']
        if sensor == 'modis':
            from agrocosmos.services.satellite_gee import fetch_modis_ndvi_batch as batch_fn
            satellite_type = 'modis_terra'
            scene_prefix = 'modis'
        else:
            from agrocosmos.services.satellite_gee import fetch_ndvi_batch as batch_fn
            satellite_type = 'sentinel2'
            scene_prefix = 's2'

        # Graceful stop
        def _signal_handler(sig, frame):
            self._stop_requested = True
            self.stderr.write(self.style.WARNING(
                '\n⚠ Ctrl+C — finishing current batch…'
            ))
        signal.signal(signal.SIGINT, _signal_handler)

        # Build queryset
        qs = Farmland.objects.select_related('district').all()
        if options['district_id']:
            qs = qs.filter(district_id=options['district_id'])
        elif options['region_id']:
            qs = qs.filter(district__region_id=options['region_id'])
        else:
            self.stderr.write('Specify --region-id or --district-id')
            return

        if options['start_from_id']:
            qs = qs.filter(pk__gte=options['start_from_id'])

        qs = qs.order_by('district_id', 'pk')

        if options['limit']:
            qs = qs[:options['limit']]

        farmlands = list(qs)
        if not farmlands:
            self.stderr.write('No farmlands found')
            return

        date_from = date.fromisoformat(options['date_from'])
        date_to = date.fromisoformat(options['date_to'])
        cloud_max = options['cloud_max']
        min_valid = options['min_valid_ratio']
        # MODIS default: lower threshold (250m pixels, few per small farmland)
        if sensor == 'modis' and '--min-valid-ratio' not in ' '.join(options.get('_args', [])):
            if min_valid == 0.95:
                min_valid = 0.5
        batch_size = options['batch_size']
        throttle = options['throttle']

        if sensor == 'modis':
            chunks = _biweekly_chunks(date_from, date_to)
            chunk_label = '16-day periods'
        else:
            chunks = _month_chunks(date_from, date_to)
            chunk_label = 'months'

        # Split farmlands into batches
        batches = []
        for start in range(0, len(farmlands), batch_size):
            batches.append(farmlands[start:start + batch_size])

        total_work = len(batches) * len(chunks)

        sensor_label = 'MODIS Terra 250m' if sensor == 'modis' else 'Sentinel-2 10m'
        self.stdout.write(
            f'═══════════════════════════════════════════════\n'
            f'  NDVI Batch Fetch — {sensor_label}\n'
            f'  Farmlands: {len(farmlands)} → {len(batches)} batches × {batch_size}\n'
            f'  Period: {date_from} → {date_to} ({len(chunks)} {chunk_label})\n'
            f'  Total work units: {total_work} (batch × month)\n'
            f'  Cloud ≤{cloud_max}%  |  Valid ≥{min_valid*100:.0f}%  |  Throttle: {throttle}s\n'
            f'═══════════════════════════════════════════════'
        )

        created_total = 0
        updated_total = 0
        errors = 0
        gee_calls = 0
        work_done = 0
        t0 = time.time()

        for bi, batch in enumerate(batches):
            if self._stop_requested:
                break

            # Prepare batch geometry data
            batch_data = []
            fl_map = {}  # pk → Farmland object
            # MODIS 250m: simplify shapes (tolerance ~0.002° ≈ 200m) + round coords
            # S2 10m: only round coords to 6dp
            coord_precision = 4 if sensor == 'modis' else 6
            simplify_tolerance = 0.002 if sensor == 'modis' else 0
            for fl in batch:
                geom = fl.geom
                if simplify_tolerance:
                    geom = geom.simplify(simplify_tolerance, preserve_topology=True)
                    if geom.empty:
                        continue
                if geom.geom_type == 'MultiPolygon' and len(geom) == 1:
                    geom_json = json.loads(geom[0].geojson)
                else:
                    geom_json = json.loads(geom.geojson)
                geom_json = _simplify_coords(geom_json, coord_precision)
                batch_data.append({'id': fl.pk, 'geometry': geom_json})
                fl_map[fl.pk] = fl

            first_pk = batch[0].pk
            last_pk = batch[-1].pk

            for ci, (chunk_from, chunk_to) in enumerate(chunks):
                if self._stop_requested:
                    break

                work_done += 1

                if gee_calls > 0:
                    time.sleep(throttle)

                self.stdout.write(
                    f'  Batch {bi+1}/{len(batches)} '
                    f'(#{first_pk}..#{last_pk}) '
                    f'month {ci+1}/{len(chunks)} '
                    f'({chunk_from}..{chunk_to})'
                )

                # Build kwargs (MODIS doesn't use cloud_max)
                call_kwargs = dict(
                    farmlands=batch_data,
                    date_from=chunk_from,
                    date_to=chunk_to,
                    min_valid_ratio=min_valid,
                )
                if sensor == 's2':
                    call_kwargs['cloud_max'] = cloud_max

                try:
                    results = batch_fn(**call_kwargs)
                    gee_calls += 1
                except GEEError as e:
                    self.stderr.write(f'    ERROR: {e}')
                    errors += 1
                    # Retry once
                    self.stderr.write('    Retrying in 15s…')
                    time.sleep(15)
                    try:
                        results = batch_fn(**call_kwargs)
                        gee_calls += 1
                        errors -= 1
                    except Exception:
                        continue
                except Exception as e:
                    self.stderr.write(f'    UNEXPECTED: {e}')
                    errors += 1
                    continue

                if not results:
                    self.stdout.write('    → 0 farmlands with valid data')
                    continue

                # Save to DB
                batch_created = 0
                batch_updated = 0

                for fl_id, stats_list in results.items():
                    fl_obj = fl_map.get(fl_id)
                    if not fl_obj:
                        continue

                    for s in stats_list:
                        scene_id = f'{scene_prefix}_{s["date"]}_{fl_obj.district_id or 0}'
                        scene, _ = SatelliteScene.objects.get_or_create(
                            scene_id=scene_id,
                            defaults={
                                'satellite': satellite_type,
                                'acquired_date': s['date'],
                                'cloud_cover': 0,
                                'processed': True,
                            },
                        )

                        _, is_new = VegetationIndex.objects.update_or_create(
                            farmland=fl_obj,
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
                            batch_created += 1
                        else:
                            batch_updated += 1

                created_total += batch_created
                updated_total += batch_updated

                self.stdout.write(
                    f'    → {len(results)} farmlands, '
                    f'+{batch_created} new, {batch_updated} upd'
                )

                # ETA
                elapsed = time.time() - t0
                rate = work_done / elapsed if elapsed > 0 else 0
                remaining = total_work - work_done
                eta = remaining / rate if rate > 0 else 0
                eta_h = int(eta // 3600)
                eta_m = int((eta % 3600) // 60)
                self.stdout.write(
                    f'    [{work_done}/{total_work}] '
                    f'{gee_calls} calls, {created_total} new, {errors} err | '
                    f'ETA: {eta_h}h{eta_m:02d}m'
                )

        # Summary
        elapsed = time.time() - t0
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        self.stdout.write(
            f'\n═══════════════════════════════════════════════\n'
            f'  Done in {hours}h{minutes:02d}m{seconds:02d}s\n'
            f'  GEE calls: {gee_calls}\n'
            f'  New records: {created_total}\n'
            f'  Updated records: {updated_total}\n'
            f'  Errors: {errors}\n'
            f'═══════════════════════════════════════════════'
        )

        if self._stop_requested:
            last_pk = batch[-1].pk if batch else 0
            self.stderr.write(self.style.WARNING(
                f'Interrupted. Resume with --start-from-id {first_pk}'
            ))
