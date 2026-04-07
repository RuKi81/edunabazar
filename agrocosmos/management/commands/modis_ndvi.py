"""
Download MODIS NDVI composites and compute zonal stats locally.

Downloads cloud-free 16-day composites from GEE as GeoTIFF,
then computes zonal statistics for all farmlands using rasterio/rasterstats.

Usage:
    # Full cycle: download + compute for Crimea 2025
    python manage.py modis_ndvi --region-id 37 --year 2025

    # Only download rasters (for later reprocessing)
    python manage.py modis_ndvi --region-id 37 --year 2025 --download-only

    # Only compute stats (rasters already downloaded)
    python manage.py modis_ndvi --region-id 37 --year 2025 --stats-only

    # Custom date range
    python manage.py modis_ndvi --region-id 37 \
        --date-from 2025-03-01 --date-to 2025-10-31

    # Specific district
    python manage.py modis_ndvi --district-id 5 --year 2025

Performance (Crimea, 133K farmlands, 1 year):
    Download: 23 composites × ~30s = ~12 min
    Stats:    23 composites × ~1min = ~23 min
    Total:    ~35 min (vs ~12 hours via GEE reduceRegions)
"""
import json
import signal
import time
from datetime import date

from django.core.management.base import BaseCommand

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)


class Command(BaseCommand):
    help = 'Download MODIS NDVI composites from GEE and compute zonal stats locally'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_requested = False

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, help='Region ID')
        parser.add_argument('--district-id', type=int, help='District ID')
        parser.add_argument('--year', type=int, help='Year (shortcut for full year)')
        parser.add_argument('--date-from', type=str, help='Start date YYYY-MM-DD')
        parser.add_argument('--date-to', type=str, help='End date YYYY-MM-DD')
        parser.add_argument('--download-only', action='store_true',
                            help='Only download rasters, skip stats')
        parser.add_argument('--stats-only', action='store_true',
                            help='Only compute stats (rasters must exist)')
        parser.add_argument('--min-valid-ratio', type=float, default=0.5,
                            help='Min valid pixel ratio (default: 0.5)')
        parser.add_argument('--overwrite', action='store_true',
                            help='Re-download existing rasters')

    def handle(self, *args, **options):
        from agrocosmos.services.satellite_modis_raster import (
            compute_zonal_stats, download_year, _biweekly_chunks, _raster_path,
        )
        from agrocosmos.services.satellite_gee import GEEError

        # Graceful stop
        def _signal_handler(sig, frame):
            self._stop_requested = True
            self.stderr.write(self.style.WARNING(
                '\n⚠ Ctrl+C — finishing current step…'
            ))
        signal.signal(signal.SIGINT, _signal_handler)

        # Resolve region
        region = None
        district = None
        if options['district_id']:
            district = District.objects.select_related('region').get(
                pk=options['district_id']
            )
            region = district.region
        elif options['region_id']:
            region = Region.objects.get(pk=options['region_id'])
        else:
            self.stderr.write('Specify --region-id or --district-id')
            return

        # Date range
        if options['year']:
            date_from = date(options['year'], 1, 1)
            date_to = date(options['year'], 12, 31)
        elif options['date_from'] and options['date_to']:
            date_from = date.fromisoformat(options['date_from'])
            date_to = date.fromisoformat(options['date_to'])
        else:
            self.stderr.write('Specify --year or --date-from/--date-to')
            return

        download_only = options['download_only']
        stats_only = options['stats_only']
        min_valid = options['min_valid_ratio']
        overwrite = options['overwrite']

        # Use region bbox for download, district or region for farmlands
        region_extent = region.geom.extent  # (xmin, ymin, xmax, ymax)
        region_id = region.pk

        chunks = _biweekly_chunks(date_from, date_to)

        self.stdout.write(
            f'═══════════════════════════════════════════════\n'
            f'  MODIS NDVI — Raster Pipeline\n'
            f'  Region: {region.name} (id={region_id})\n'
            f'  Period: {date_from} → {date_to} ({len(chunks)} composites)\n'
            f'  Mode: {"download" if download_only else "stats" if stats_only else "download + stats"}\n'
            f'═══════════════════════════════════════════════'
        )

        t0 = time.time()

        # --- STEP 1: Download composites ---
        if not stats_only:
            self.stdout.write('\n📡 Step 1: Downloading MODIS composites from GEE…')
            downloaded = 0
            skipped = 0
            errors = 0

            for i, (cf, ct) in enumerate(chunks):
                if self._stop_requested:
                    break

                self.stdout.write(
                    f'  [{i+1}/{len(chunks)}] {cf}..{ct}',
                    ending='',
                )

                try:
                    from agrocosmos.services.satellite_modis_raster import download_composite
                    path = download_composite(
                        region_extent, region_id, cf, ct,
                        overwrite=overwrite,
                    )
                    if path:
                        import os
                        size_mb = os.path.getsize(path) / 1e6
                        self.stdout.write(f'  → {size_mb:.1f} MB')
                        downloaded += 1
                    else:
                        self.stdout.write('  → no data')
                        skipped += 1
                except GEEError as e:
                    self.stderr.write(f'  → ERROR: {e}')
                    errors += 1
                except Exception as e:
                    self.stderr.write(f'  → ERROR: {e}')
                    errors += 1

            dl_time = time.time() - t0
            self.stdout.write(
                f'\n  Download done: {downloaded} files, '
                f'{skipped} skipped, {errors} errors '
                f'({dl_time:.0f}s)'
            )

        if download_only or self._stop_requested:
            return

        # --- STEP 2: Compute zonal stats ---
        self.stdout.write('\n📊 Step 2: Computing zonal statistics…')

        # Load farmlands
        qs = Farmland.objects.select_related('district')
        if district:
            qs = qs.filter(district=district)
        else:
            qs = qs.filter(district__region=region)
        qs = qs.order_by('district_id', 'pk')

        farmlands = list(qs)
        if not farmlands:
            self.stderr.write('No farmlands found')
            return

        self.stdout.write(f'  Loaded {len(farmlands)} farmlands')

        # Prepare geometry data (simplified for MODIS 250m)
        fl_geoms = []
        fl_map = {}
        for fl in farmlands:
            geom = fl.geom
            geom = geom.simplify(0.002, preserve_topology=True)
            if geom.empty:
                continue
            if geom.geom_type == 'MultiPolygon' and len(geom) == 1:
                geom_json = json.loads(geom[0].geojson)
            else:
                geom_json = json.loads(geom.geojson)
            fl_geoms.append({'id': fl.pk, 'geometry': geom_json})
            fl_map[fl.pk] = fl

        self.stdout.write(f'  Prepared {len(fl_geoms)} geometries')

        created_total = 0
        updated_total = 0
        stats_errors = 0
        t_stats = time.time()

        for i, (cf, ct) in enumerate(chunks):
            if self._stop_requested:
                break

            tif_path = _raster_path(region_id, cf, ct)
            import os
            if not os.path.exists(tif_path):
                self.stdout.write(f'  [{i+1}/{len(chunks)}] {cf}..{ct} — no raster, skip')
                continue

            self.stdout.write(
                f'  [{i+1}/{len(chunks)}] {cf}..{ct}',
                ending='',
            )

            try:
                results = compute_zonal_stats(
                    tif_path, fl_geoms, min_valid_ratio=min_valid,
                )
            except Exception as e:
                self.stderr.write(f'  → ERROR: {e}')
                stats_errors += 1
                continue

            if not results:
                self.stdout.write(f'  → 0 farmlands')
                continue

            # Midpoint date for the composite record
            mid_date = (cf + (ct - cf) / 2).isoformat()

            # Save to DB
            batch_created = 0
            batch_updated = 0

            for fl_id, st in results.items():
                fl_obj = fl_map.get(fl_id)
                if not fl_obj:
                    continue

                scene_id = f'modis_{mid_date}_{fl_obj.district_id or 0}'
                scene, _ = SatelliteScene.objects.get_or_create(
                    scene_id=scene_id,
                    defaults={
                        'satellite': 'modis_terra',
                        'acquired_date': mid_date,
                        'cloud_cover': 0,
                        'processed': True,
                    },
                )

                _, is_new = VegetationIndex.objects.update_or_create(
                    farmland=fl_obj,
                    scene=scene,
                    index_type='ndvi',
                    defaults={
                        'acquired_date': mid_date,
                        'mean': st['mean'],
                        'median': st['median'],
                        'min_val': st['min'],
                        'max_val': st['max'],
                        'std_val': st['std'],
                        'pixel_count': st['pixel_count'],
                        'valid_pixel_count': st['valid_pixel_count'],
                    },
                )
                if is_new:
                    batch_created += 1
                else:
                    batch_updated += 1

            created_total += batch_created
            updated_total += batch_updated
            self.stdout.write(
                f'  → {len(results)} farmlands, '
                f'+{batch_created} new, {batch_updated} upd'
            )

        # Summary
        elapsed = time.time() - t0
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)

        self.stdout.write(
            f'\n═══════════════════════════════════════════════\n'
            f'  Done in {hours}h{minutes:02d}m{seconds:02d}s\n'
            f'  New records: {created_total}\n'
            f'  Updated records: {updated_total}\n'
            f'  Errors: {stats_errors}\n'
            f'═══════════════════════════════════════════════'
        )

        if self._stop_requested:
            self.stderr.write(self.style.WARNING(
                'Interrupted. Re-run with --stats-only to resume stats calculation.'
            ))
