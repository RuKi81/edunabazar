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
import os
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
        # Zonal stats are resumable: composites already covered in
        # VegetationIndex are skipped, so a crashed/re-queued region
        # finishes the tail instead of redoing days of CPU work (Altai:
        # ~137 composites x 500K farmlands). Pass this flag to force a
        # full recompute, e.g. after changing --min-valid-ratio or the
        # simplification tolerance.
        parser.add_argument('--recompute-stats', action='store_true',
                            help='Recompute zonal stats even for composites '
                                 'already fully present in VegetationIndex')
        # Batch callers (check_monitoring) should pass this flag so the
        # 10-minute global ``recompute_district_ndvi_status`` runs ONCE
        # at the end of the batch instead of after every region. With 85
        # regions the per-region refresh used to cost ~14 hours of pure
        # SQL per cron run on top of the actual data processing.
        parser.add_argument('--skip-status-refresh', action='store_true',
                            help='Do not refresh district NDVI status '
                                 'cache after this run (caller will do '
                                 'it once at the end of a batch).')

    def handle(self, *args, **options):
        from agrocosmos.services.satellite_modis_raster import _biweekly_chunks

        self._install_signal_handler()

        target = self._resolve_target(options)
        if target is None:
            return
        region, district = target

        dates = self._resolve_dates(options)
        if dates is None:
            return
        date_from, date_to = dates

        download_only = options['download_only']
        stats_only = options['stats_only']
        min_valid = options['min_valid_ratio']
        overwrite = options['overwrite']
        skip_status_refresh = options['skip_status_refresh']

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
            self._download_step(chunks, region_extent, region_id,
                                overwrite, t0)

        if download_only or self._stop_requested:
            return

        # --- STEP 2: Compute zonal stats ---
        self.stdout.write('\n📊 Step 2: Computing zonal statistics…')

        fl_geoms, fl_district = self._load_farmlands(region, district)
        if not fl_geoms:
            self.stderr.write('No farmlands found')
            return

        created_total, stats_errors = self._stats_step(
            chunks, region, district, region_id,
            fl_geoms, fl_district, min_valid,
            options.get('recompute_stats', False),
        )

        # Summary
        elapsed = time.time() - t0
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)

        self.stdout.write(
            f'\n═══════════════════════════════════════════════\n'
            f'  Done in {hours}h{minutes:02d}m{seconds:02d}s\n'
            f'  Records saved: {created_total}\n'
            f'  Errors: {stats_errors}\n'
            f'═══════════════════════════════════════════════'
        )

        if self._stop_requested:
            self.stderr.write(self.style.WARNING(
                'Interrupted. Re-run with --stats-only to resume stats calculation.'
            ))
            return

        self._refresh_status_cache(region_id, skip_status_refresh)

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _install_signal_handler(self):
        """Graceful stop on Ctrl+C (only works in main thread)."""
        import threading
        if threading.current_thread() is not threading.main_thread():
            return

        def _signal_handler(sig, frame):
            self._stop_requested = True
            self.stderr.write(self.style.WARNING(
                '\n⚠ Ctrl+C — finishing current step…'
            ))
        signal.signal(signal.SIGINT, _signal_handler)

    def _resolve_target(self, options):
        """Resolve (region, district) from options; None → abort."""
        if options['district_id']:
            district = District.objects.select_related('region').get(
                pk=options['district_id']
            )
            return district.region, district
        if options['region_id']:
            return Region.objects.get(pk=options['region_id']), None
        self.stderr.write('Specify --region-id or --district-id')
        return None

    def _resolve_dates(self, options):
        """Resolve (date_from, date_to) from options; None → abort."""
        if options['year']:
            return date(options['year'], 1, 1), date(options['year'], 12, 31)
        if options['date_from'] and options['date_to']:
            return (date.fromisoformat(options['date_from']),
                    date.fromisoformat(options['date_to']))
        self.stderr.write('Specify --year or --date-from/--date-to')
        return None

    # ------------------------------------------------------------------
    # STEP 1: Download composites
    # ------------------------------------------------------------------

    def _download_step(self, chunks, region_extent, region_id, overwrite, t0):
        from agrocosmos.services.satellite_modis_raster import download_composite
        from agrocosmos.services.satellite_gee import GEEError

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
                path = download_composite(
                    region_extent, region_id, cf, ct,
                    overwrite=overwrite,
                )
                if path:
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

    # ------------------------------------------------------------------
    # STEP 2: Zonal statistics
    # ------------------------------------------------------------------

    def _load_farmlands(self, region, district):
        """Stream farmlands → (fl_geoms, fl_district).

        Memory-safe streaming: regions like Алтайский край have 500K+
        farmlands with MultiPolygon geometries, so the previous
        ``list(qs)`` + ``select_related('district')`` combo pulled
        ~dozens of GB into RAM and got OOM-killed (exit 137) by the
        container. We now:
          * ``only('id','district_id','geom')`` — skip cadastral_number
            (50-byte strings x 500K), properties (JSONField, can be
            huge), full district row (FK), etc.
          * ``.iterator(chunk_size=5000)`` — stream rows from Postgres
            instead of caching the whole queryset in the Python side.
          * drop the Farmland object as soon as we've extracted the
            simplified GeoJSON + district_id; store a compact
            ``fl_district: {fl_id: district_id}`` dict for scene
            lookup later, not the full model instance.

        Simplified geometries are prepared in the same streaming pass —
        loading and simplification are fused to keep peak memory low.
        """
        import sys

        self.stdout.write('  Loading farmlands (streamed)…', ending='')
        sys.stdout.flush()
        qs = Farmland.objects.only('id', 'district_id', 'geom')
        if district:
            qs = qs.filter(district=district)
        else:
            qs = qs.filter(district__region=region)
        qs = qs.order_by('district_id', 'pk')

        fl_geoms: list[dict] = []
        fl_district: dict[int, int] = {}
        loaded = 0
        for fl in qs.iterator(chunk_size=5000):
            loaded += 1
            geom = fl.geom
            if geom is None:
                continue
            geom = geom.simplify(0.002, preserve_topology=True)
            if geom.empty:
                continue
            if geom.geom_type == 'MultiPolygon' and len(geom) == 1:
                geom_json = json.loads(geom[0].geojson)
            else:
                geom_json = json.loads(geom.geojson)
            fl_geoms.append({'id': fl.pk, 'geometry': geom_json})
            fl_district[fl.pk] = fl.district_id or 0
            if loaded % 20000 == 0:
                self.stdout.write(f' {loaded}', ending='')
                sys.stdout.flush()

        if fl_geoms:
            self.stdout.write(f' → {loaded} loaded, {len(fl_geoms)} ready')
            sys.stdout.flush()
        return fl_geoms, fl_district

    def _is_covered(self, mid_date, region, district, n_geoms):
        """Resume support: is this composite already covered in the DB?

        "Covered" = rows for >=99% of the prepared farmlands — the
        missing <=1% are polygons that produced no valid pixels
        (clouds/nodata) and legitimately have no row. Without this a
        re-queued region redoes 60-70% of its pipeline time on zonal
        stats whose results are already stored (writes are UPSERTs,
        so only CPU was wasted — days of it for large regions).
        """
        cov_qs = VegetationIndex.objects.filter(
            index_type='ndvi', acquired_date=mid_date,
        )
        if district:
            cov_qs = cov_qs.filter(farmland__district=district)
        else:
            cov_qs = cov_qs.filter(farmland__district__region=region)
        have = cov_qs.count()
        return have >= n_geoms * 0.99, have

    def _stats_step(self, chunks, region, district, region_id,
                    fl_geoms, fl_district, min_valid, recompute_stats):
        from agrocosmos.services.satellite_modis_raster import _raster_path

        created_total = 0
        stats_errors = 0

        for i, (cf, ct) in enumerate(chunks):
            if self._stop_requested:
                break

            tif_path = _raster_path(region_id, cf, ct)
            if not os.path.exists(tif_path):
                self.stdout.write(f'  [{i+1}/{len(chunks)}] {cf}..{ct} — no raster, skip')
                continue

            # Midpoint date identifies the composite record (see the write
            # phase in _save_results — acquired_date is derived the same
            # way, so (index_type, acquired_date) is a stable composite key).
            mid_date = (cf + (ct - cf) / 2)

            if not recompute_stats:
                covered, have = self._is_covered(
                    mid_date, region, district, len(fl_geoms))
                if covered:
                    self.stdout.write(
                        f'  [{i+1}/{len(chunks)}] {cf}..{ct} — already in DB '
                        f'({have}/{len(fl_geoms)}), skip'
                    )
                    continue

            self.stdout.write(
                f'  [{i+1}/{len(chunks)}] {cf}..{ct}',
                ending='',
            )

            results = self._compute_chunk(tif_path, fl_geoms, min_valid)
            if results is None:
                stats_errors += 1
                continue
            if not results:
                self.stdout.write('  → 0 farmlands')
                continue

            saved = self._save_results(results, fl_district, mid_date)
            created_total += saved
            self.stdout.write(
                f'  → {saved} records saved'
            )

        return created_total, stats_errors

    def _compute_chunk(self, tif_path, fl_geoms, min_valid):
        """Zonal stats for one composite; None → error (counted by caller)."""
        import sys
        from agrocosmos.services.satellite_modis_raster import compute_zonal_stats

        def _progress(done, total):
            self.stdout.write(
                f' [{done}/{total}]', ending='',
            )
            sys.stdout.flush()

        try:
            return compute_zonal_stats(
                tif_path, fl_geoms, min_valid_ratio=min_valid,
                progress_callback=_progress,
            )
        except Exception as e:
            self.stderr.write(f'  → ERROR: {e}')
            return None

    def _save_results(self, results, fl_district, mid_date):
        """Upsert VegetationIndex rows for one composite; returns count."""
        import sys

        # Group farmlands by district for scene_id.
        # Pre-create scenes (few districts, fast).
        district_ids_needed = set()
        for fl_id in results:
            if fl_id in fl_district:
                district_ids_needed.add(fl_district[fl_id])

        district_scenes = {}  # district_id → scene
        for did in district_ids_needed:
            scene_id = f'modis_{mid_date.isoformat()}_{did}'
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=scene_id,
                defaults={
                    'satellite': 'modis_terra',
                    'acquired_date': mid_date,
                    'cloud_cover': 0,
                    'processed': True,
                },
            )
            district_scenes[did] = scene

        # Upsert via INSERT ... ON CONFLICT UPDATE (no SELECT needed).
        # Use farmland_id / scene_id to avoid materialising the full
        # Farmland instance (we only have fl_district dict now).
        self.stdout.write(' DB…', ending='')
        sys.stdout.flush()

        objs = []
        for fl_id, st in results.items():
            did = fl_district.get(fl_id)
            if did is None:
                continue
            scene = district_scenes.get(did)
            if not scene:
                continue
            objs.append(VegetationIndex(
                farmland_id=fl_id,
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

    # ------------------------------------------------------------------
    # Wrap-up
    # ------------------------------------------------------------------

    def _refresh_status_cache(self, region_id, skip_status_refresh):
        """Refresh the cached per-district NDVI status used by the
        all-Russia choropleth (`/agrocosmos/api/districts/status/`).

        This must run AFTER new VI rows are saved, otherwise the
        cached status would lag one MODIS composite behind. Failure
        here must NOT fail the pipeline — the status cache is only
        used by a non-critical map view.

        Batch callers (``check_monitoring`` walking ~85 regions in
        one cron run) MUST pass ``--skip-status-refresh`` and call
        ``recompute_district_ndvi_status`` themselves exactly once
        at the end of the batch. The refresh is GLOBAL (all 2200
        districts of all 85 regions, ~10 min on cold DB cache) so
        invoking it once per region multiplied a 15-minute pipeline
        by 85 and consumed ~14 hours of pure SQL per cron run.
        """
        if skip_status_refresh:
            self.stdout.write(
                '\n📌 District NDVI status cache: skipped '
                '(--skip-status-refresh; caller will refresh once at end)'
            )
            return
        try:
            self.stdout.write('\n📌 Refreshing district NDVI status cache…')
            from django.core.management import call_command
            # Scope the upsert to the region we just ingested — no
            # point rescanning the other 84 regions' VI rows when we
            # only changed this one. The post-steps (GeoJSON refresh,
            # series, prewarm) still run once and cover everything.
            call_command(
                'recompute_district_ndvi_status',
                region_id=region_id,
                stdout=self.stdout,
            )
        except Exception as exc:
            self.stderr.write(self.style.WARNING(
                f'  district status refresh failed (non-fatal): {exc}'
            ))
