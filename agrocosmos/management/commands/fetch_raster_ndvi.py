"""
Unified raster NDVI pipeline: download composites from GEE + local zonal stats.

Supports Sentinel-2, Landsat 8/9, and MODIS via a single --sensor flag.

Usage:
    # Sentinel-2, 10m, 5-day composites
    python manage.py fetch_raster_ndvi --sensor s2 --region-id 37 --year 2025

    # Landsat 8/9, 30m (harmonized to S2), 16-day composites
    python manage.py fetch_raster_ndvi --sensor l8 --region-id 37 --year 2024

    # MODIS Terra+Aqua, 250m, 16-day composites
    python manage.py fetch_raster_ndvi --sensor modis --region-id 37 --year 2020

    # Download only (no zonal stats)
    python manage.py fetch_raster_ndvi --sensor s2 --region-id 37 --year 2025 --download-only

    # Stats only (rasters already downloaded)
    python manage.py fetch_raster_ndvi --sensor s2 --region-id 37 --year 2025 --stats-only

    # Custom date range + specific district
    python manage.py fetch_raster_ndvi --sensor l8 --district-id 5 \
        --date-from 2024-04-01 --date-to 2024-09-30

    # Skip harmonization for Landsat
    python manage.py fetch_raster_ndvi --sensor l8 --region-id 37 --year 2024 --no-harmonize

Sensors:
    s2    — Sentinel-2 L2A (10m, 2015+), cloud mask: SCL band
    l8    — Landsat 8+9 Collection 2 L2 (30m, 2013+), cloud mask: QA_PIXEL
    modis — MODIS Terra+Aqua (250m, 2000+), quality filter: SummaryQA

Performance (Crimea, 133K farmlands, 1 year):
    S2:      ~73 composites × ~30s download + ~2min stats ≈ ~3h
    Landsat: ~23 composites × ~15s download + ~30s stats  ≈ ~20min
    MODIS:   ~23 composites × ~10s download + ~1min stats ≈ ~25min
"""
import json  # noqa: used in _stats_step
import os
import signal
import sys
import threading
import time
from datetime import date

from django.core.management.base import BaseCommand

from agrocosmos.models import (
    District, Farmland, Region, SatelliteScene, VegetationIndex,
)

# Sensor configuration
SENSOR_CONFIG = {
    's2': {
        'label': 'Sentinel-2 L2A (10m)',
        'satellite_type': 'sentinel2',
        'scene_prefix': 's2',
        'simplify_tolerance': 0,   # 10m — no simplification
        'coord_precision': 6,
        'default_min_valid': 0.70,
        'default_cloud_max': 30,
    },
    'l8': {
        'label': 'Landsat 8/9 C2L2 (30m, harmonized)',
        'satellite_type': 'landsat8',
        'scene_prefix': 'landsat',
        'simplify_tolerance': 0.0003,  # ~30m
        'coord_precision': 5,
        'default_min_valid': 0.70,
        'default_cloud_max': 30,
    },
    'modis': {
        'label': 'MODIS Terra+Aqua (250m)',
        'satellite_type': 'modis_terra',
        'scene_prefix': 'modis',
        'simplify_tolerance': 0.002,   # ~200m
        'coord_precision': 4,
        'default_min_valid': 0.50,
        'default_cloud_max': None,  # MODIS uses quality flag, not cloud %
    },
}

# Буфер вокруг bbox пользовательского поля (в градусах, ~300 м по широте).
# Нужен, чтобы края поля не попадали на границу растра при ресемплинге и
# чтобы зоны/превью в карточке поля имели небольшой контекст вокруг контура.
MYF_FIELD_BUFFER_DEG = 0.003


class Command(BaseCommand):
    help = 'Download NDVI composites from GEE and compute local zonal stats (S2/L8/MODIS)'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_requested = False

    def add_arguments(self, parser):
        parser.add_argument('--sensor', type=str, required=True,
                            choices=['s2', 'l8', 'modis'],
                            help='Sensor: s2 (Sentinel-2), l8 (Landsat 8/9), modis')
        parser.add_argument('--region-id', type=int, help='Region ID')
        parser.add_argument('--district-id', type=int, help='District ID')
        parser.add_argument('--myf-field-id', type=int,
                            help='UserField ID (my_fields): download composites '
                                 'for the field bbox only (scope f<id>, no stats)')
        parser.add_argument('--year', type=int, help='Year (full year)')
        parser.add_argument('--date-from', type=str, help='Start date YYYY-MM-DD')
        parser.add_argument('--date-to', type=str, help='End date YYYY-MM-DD')
        parser.add_argument('--download-only', action='store_true',
                            help='Only download rasters, skip stats')
        parser.add_argument('--stats-only', action='store_true',
                            help='Only compute stats (rasters must exist)')
        parser.add_argument('--cloud-max', type=int, default=None,
                            help='Max cloud cover %% (default: sensor-specific)')
        parser.add_argument('--min-valid-ratio', type=float, default=None,
                            help='Min valid pixel ratio (default: sensor-specific)')
        parser.add_argument('--overwrite', action='store_true',
                            help='Re-download existing rasters')
        parser.add_argument('--no-harmonize', action='store_true',
                            help='Skip Landsat→S2 harmonization (l8 only)')

    def handle(self, *args, **options):
        sensor = options['sensor']
        cfg = SENSOR_CONFIG[sensor]

        download_composite, get_chunks, _raster_path = self._sensor_imports(sensor)
        from agrocosmos.services.zonal_stats import compute_zonal_stats

        self._install_signal_handler()

        # Resolve scope: user field (my_fields) or region/district
        myf_field = self._resolve_myf_field(options)
        if options.get('myf_field_id') and myf_field is None:
            return
        if myf_field is not None:
            region, district = myf_field.region, myf_field.district
        else:
            region, district = self._resolve_region(options)
            if not region:
                return

        # Date range
        date_from, date_to = self._resolve_dates(options)
        if not date_from:
            return

        download_only = options['download_only']
        stats_only = options['stats_only']
        overwrite = options['overwrite']
        harmonize = not options['no_harmonize']

        cloud_max = options['cloud_max'] or cfg['default_cloud_max']
        min_valid = options['min_valid_ratio']
        if min_valid is None:
            min_valid = cfg['default_min_valid']

        # Use the smallest available extent (field ≪ district ≪ region)
        if myf_field is not None:
            xmin, ymin, xmax, ymax = myf_field.geom.extent
            b = MYF_FIELD_BUFFER_DEG
            download_extent = (xmin - b, ymin - b, xmax + b, ymax + b)
            scope_id = f'f{myf_field.pk}'
            # Зональная статистика считается по Farmland-контурам и для
            # пользовательского поля не имеет смысла — только скачивание.
            download_only = True
        elif district:
            download_extent = district.geom.extent
            scope_id = f'd{district.pk}'
        else:
            download_extent = region.geom.extent
            scope_id = str(region.pk)

        chunks = get_chunks(date_from, date_to)

        self.stdout.write(
            f'═══════════════════════════════════════════════\n'
            f'  Raster NDVI — {cfg["label"]}\n'
            f'  Region: {region.name + " (id=" + str(region.pk) + ")" if region else "—"}\n'
            f'  {"Field: #" + str(myf_field.pk) + " " + myf_field.name + " | " if myf_field else ""}'
            f'  {"District: " + district.name + " | " if district else ""}'
            f'Period: {date_from} → {date_to} ({len(chunks)} composites)\n'
            f'  Cloud ≤{cloud_max or "N/A"}%  |  Valid ≥{min_valid*100:.0f}%'
            f'{" | harmonize=off" if sensor == "l8" and not harmonize else ""}\n'
            f'  Mode: {"download" if download_only else "stats" if stats_only else "download + stats"}\n'
            f'═══════════════════════════════════════════════'
        )

        t0 = time.time()

        # ── STEP 1: Download composites ──
        if not stats_only:
            self._download_step(
                chunks, download_composite, download_extent, scope_id,
                cloud_max, harmonize if sensor == 'l8' else None,
                overwrite, sensor,
            )

        if download_only or self._stop_requested:
            return

        # ── STEP 2: Zonal stats ──
        self._stats_step(
            chunks, _raster_path, scope_id, region, district,
            cfg, min_valid, compute_zonal_stats,
        )

        # ── Summary ──
        elapsed = time.time() - t0
        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)
        s = int(elapsed % 60)
        self.stdout.write(
            f'\n═══════════════════════════════════════════════\n'
            f'  Done in {h}h{m:02d}m{s:02d}s\n'
            f'═══════════════════════════════════════════════'
        )
        if self._stop_requested:
            self.stderr.write(self.style.WARNING(
                'Interrupted. Re-run with --stats-only to resume.'
            ))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sensor_imports(sensor):
        """Import sensor-specific modules → (download_composite, get_chunks,
        _raster_path)."""
        if sensor == 's2':
            from agrocosmos.services.satellite_s2_raster import (
                download_composite, s2_chunks as get_chunks, _raster_path,
            )
        elif sensor == 'l8':
            from agrocosmos.services.satellite_landsat_raster import (
                download_composite, landsat_chunks as get_chunks, _raster_path,
            )
        else:  # modis
            from agrocosmos.services.satellite_modis_raster import (
                download_composite, _biweekly_chunks as get_chunks, _raster_path,
            )
        return download_composite, get_chunks, _raster_path

    def _install_signal_handler(self):
        """Graceful Ctrl+C (only works in main thread)."""
        if threading.current_thread() is not threading.main_thread():
            return

        def _signal_handler(sig, frame):
            self._stop_requested = True
            self.stderr.write(self.style.WARNING(
                '\n⚠ Ctrl+C — finishing current step…'
            ))
        signal.signal(signal.SIGINT, _signal_handler)

    def _resolve_myf_field(self, options):
        """Загрузить UserField для scope ``--myf-field-id`` (или None).

        Ленивый импорт ``my_fields`` — чтобы agrocosmos не зависел от него
        на уровне модуля (my_fields сам импортирует agrocosmos.models).
        """
        field_id = options.get('myf_field_id')
        if not field_id:
            return None
        from my_fields.models import UserField
        field = (
            UserField.objects
            .select_related('region', 'district')
            .filter(pk=field_id)
            .first()
        )
        if field is None:
            self.stderr.write(f'UserField id={field_id} not found')
        return field

    def _resolve_region(self, options):
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
        return region, district

    def _resolve_dates(self, options):
        if options['year']:
            return date(options['year'], 1, 1), date(options['year'], 12, 31)
        elif options['date_from'] and options['date_to']:
            return (date.fromisoformat(options['date_from']),
                    date.fromisoformat(options['date_to']))
        else:
            self.stderr.write('Specify --year or --date-from/--date-to')
            return None, None

    def _download_step(self, chunks, download_fn, region_extent, region_id,
                       cloud_max, harmonize, overwrite, sensor):
        self.stdout.write('\n📡 Downloading composites from GEE…')
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
                kwargs = dict(
                    region_geom_extent=region_extent,
                    region_id=region_id,
                    date_from=cf,
                    date_to=ct,
                    overwrite=overwrite,
                )
                if cloud_max is not None:
                    kwargs['cloud_max'] = cloud_max
                if harmonize is not None:
                    kwargs['harmonize'] = harmonize

                path = download_fn(**kwargs)
                if path:
                    size_mb = os.path.getsize(path) / 1e6
                    self.stdout.write(f'  → {size_mb:.1f} MB')
                    downloaded += 1
                else:
                    self.stdout.write('  → no data')
                    skipped += 1
            except Exception as e:
                self.stderr.write(f'  → ERROR: {e}')
                errors += 1

        self.stdout.write(
            f'\n  Download: {downloaded} OK, {skipped} empty, {errors} errors'
        )

    def _load_farmland_geoms(self, region, district, cfg):
        """Load farmlands + prepare GeoJSON → (fl_geoms, fl_map)."""
        self.stdout.write('  Loading farmlands…', ending='')
        sys.stdout.flush()

        qs = Farmland.objects.select_related('district')
        if district:
            qs = qs.filter(district=district)
        else:
            qs = qs.filter(district__region=region)
        qs = qs.order_by('district_id', 'pk')

        farmlands = list(qs)
        if not farmlands:
            return [], {}

        self.stdout.write(f' {len(farmlands)}')
        sys.stdout.flush()

        # Prepare geometries
        self.stdout.write('  Preparing geometries…', ending='')
        sys.stdout.flush()

        simplify_tol = cfg['simplify_tolerance']
        fl_geoms = []
        fl_map = {}

        for idx, fl in enumerate(farmlands):
            geom = fl.geom
            if simplify_tol:
                geom = geom.simplify(simplify_tol, preserve_topology=True)
                if geom.empty:
                    continue
            if geom.geom_type == 'MultiPolygon' and len(geom) == 1:
                geom_json = json.loads(geom[0].geojson)
            else:
                geom_json = json.loads(geom.geojson)
            fl_geoms.append({'id': fl.pk, 'geometry': geom_json})
            fl_map[fl.pk] = fl
            if (idx + 1) % 20000 == 0:
                self.stdout.write(f' {idx+1}', ending='')
                sys.stdout.flush()

        self.stdout.write(f' → {len(fl_geoms)} ready')
        sys.stdout.flush()
        return fl_geoms, fl_map

    def _compute_chunk(self, compute_fn, tif_path, fl_geoms, min_valid):
        """Zonal stats for one composite; None → error (counted by caller)."""
        def _progress(done, total):
            self.stdout.write(f' [{done}/{total}]', ending='')
            sys.stdout.flush()

        try:
            return compute_fn(
                tif_path, fl_geoms,
                min_valid_ratio=min_valid,
                progress_callback=_progress,
            )
        except Exception as e:
            self.stderr.write(f'  → ERROR: {e}')
            return None

    def _save_chunk_results(self, results, fl_map, mid_date, cfg):
        """Upsert VegetationIndex rows for one composite; returns count."""
        satellite_type = cfg['satellite_type']
        scene_prefix = cfg['scene_prefix']

        # Pre-create scenes per district
        district_scenes = {}
        district_ids_needed = set()
        for fl_id in results:
            fl_obj = fl_map.get(fl_id)
            if fl_obj:
                district_ids_needed.add(fl_obj.district_id or 0)

        for did in district_ids_needed:
            scene_id = f'{scene_prefix}_{mid_date.isoformat()}_{did}'
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=scene_id,
                defaults={
                    'satellite': satellite_type,
                    'acquired_date': mid_date,
                    'cloud_cover': 0,
                    'processed': True,
                },
            )
            district_scenes[did] = scene

        # Bulk upsert
        self.stdout.write(' DB…', ending='')
        sys.stdout.flush()

        objs = []
        for fl_id, st in results.items():
            fl_obj = fl_map.get(fl_id)
            if not fl_obj:
                continue
            scene = district_scenes.get(fl_obj.district_id or 0)
            if not scene:
                continue
            objs.append(VegetationIndex(
                farmland=fl_obj,
                scene=scene,
                index_type='ndvi',
                acquired_date=mid_date,
                mean=st['mean'],
                median=st['median'],
                min_val=st['min'],
                max_val=st['max'],
                std_val=st['std'],
                pixel_count=st['pixel_count'],
                valid_pixel_count=st['valid_pixel_count'],
                histogram=st.get('histogram'),
            ))

        if objs:
            VegetationIndex.objects.bulk_create(
                objs,
                batch_size=5000,
                update_conflicts=True,
                unique_fields=['farmland', 'scene', 'index_type'],
                update_fields=[
                    'acquired_date', 'mean', 'median', 'min_val',
                    'max_val', 'std_val', 'pixel_count',
                    'valid_pixel_count', 'histogram',
                ],
            )
        return len(objs)

    def _stats_step(self, chunks, raster_path_fn, region_id, region,
                    district, cfg, min_valid, compute_fn):
        self.stdout.write('\n📊 Computing zonal statistics…')
        sys.stdout.flush()

        fl_geoms, fl_map = self._load_farmland_geoms(region, district, cfg)
        if not fl_geoms:
            self.stderr.write('No farmlands found')
            return

        created_total = 0
        errors = 0

        for i, (cf, ct) in enumerate(chunks):
            if self._stop_requested:
                break

            tif_path = raster_path_fn(region_id, cf, ct)
            if not os.path.exists(tif_path):
                self.stdout.write(f'  [{i+1}/{len(chunks)}] {cf}..{ct} — no raster, skip')
                continue

            self.stdout.write(
                f'  [{i+1}/{len(chunks)}] {cf}..{ct}',
                ending='',
            )

            results = self._compute_chunk(
                compute_fn, tif_path, fl_geoms, min_valid)
            if results is None:
                errors += 1
                continue
            if not results:
                self.stdout.write('  → 0 farmlands')
                continue

            # Midpoint date for the composite record
            mid_date = cf + (ct - cf) / 2

            saved = self._save_chunk_results(results, fl_map, mid_date, cfg)
            created_total += saved
            self.stdout.write(f'  → {saved} records')

        self.stdout.write(
            f'\n  Stats: {created_total} records saved, {errors} errors'
        )
