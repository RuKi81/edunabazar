"""HLS-style fusion of Sentinel-2 + Landsat NDVI into a single timeline.

Design (see docs/AGROCOSMOS_API.md §HLS and ARCHITECTURE.md §14.3):

Grid       : S2-native 5-day cadence (Landsat records are paired to the
             nearest S2 observation within ±8 days).
Fusion     : weighted mean by each sensor's *clean-coverage fraction*
             ``valid_pixel_count / pixel_count`` — NOT the absolute pixel
             count. S2 (10 m) yields ~9× more pixels per field than
             Landsat (30 m), so weighting by the raw count silently drowned
             the Landsat contribution (fused ≈ 0.9·S2 + 0.1·L regardless of
             observation quality). The coverage fraction is a genuine,
             resolution-independent confidence measure.
Orphan L   : Landsat records that have no S2 neighbour within ±8 days
             are written as standalone fused points (gap-fill during
             heavy-cloud periods).
Validation : inherited from source — both S2 and L have already passed
             ``min_valid_ratio`` during zonal stats; no extra filter here.

Usage:
    python manage.py compute_fused_ndvi --region-id 37 --year 2025
    python manage.py compute_fused_ndvi --region-id 37 --year 2025 --overwrite
    python manage.py compute_fused_ndvi --district-id 5 --year 2025

Idempotency:
    Without ``--overwrite`` existing fused records for the
    (region/district, year) scope are kept. With ``--overwrite`` they
    are deleted first and the pipeline rebuilds them.
"""
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from agrocosmos.models import (
    District, Farmland, PipelineRun, Region, SatelliteScene, VegetationIndex,
)


SOURCE_SATS = ('sentinel2', 'landsat8')
FUSED_SAT = 'hls_fused'
L_PAIR_WINDOW_DAYS = 8  # ±8 days around S2 midpoint


def _coverage_weight(n_valid, n_total):
    """Clean-coverage fraction ``n_valid / n_total`` used to weight a
    sensor in the fusion.

    This replaces the old absolute-``valid_pixel_count`` weighting, which
    was implicitly resolution-biased: S2 (10 m) has ~9× more pixels per
    field than Landsat (30 m), so the raw count made Landsat almost
    irrelevant when both sensors observed the same period.

    Falls back to ``1.0`` (equal weight) when the total pixel count is
    unknown/zero — e.g. older rows without ``pixel_count`` — so the
    observation still contributes.
    """
    if n_total and n_total > 0:
        frac = n_valid / n_total
        if frac > 0:
            return min(frac, 1.0)
    return 1.0


class Command(BaseCommand):
    help = 'Fuse Sentinel-2 + Landsat NDVI into a single HLS-style timeline.'

    # ------------------------------------------------------------------ CLI

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, help='Region ID')
        parser.add_argument('--district-id', type=int, help='District ID')
        parser.add_argument('--year', type=int, required=True, help='Year')
        parser.add_argument(
            '--overwrite', action='store_true',
            help='Delete existing fused records for the scope before rebuilding',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Compute but do not write to DB',
        )

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        region_id = options.get('region_id')
        district_id = options.get('district_id')
        year = options['year']
        overwrite = options['overwrite']
        dry_run = options['dry_run']

        if not region_id and not district_id:
            self.stderr.write(self.style.ERROR(
                'Either --region-id or --district-id is required'
            ))
            return

        # Resolve scope
        region, district = self._resolve_scope(region_id, district_id)
        if not region:
            return

        self.stdout.write(
            f'═══════════════════════════════════════════════\n'
            f'  HLS Fusion (S2 + Landsat)\n'
            f'  Region: {region.name} (id={region.pk})\n'
            f'  {"District: " + district.name + " | " if district else ""}'
            f'Year: {year}\n'
            f'  Overwrite: {overwrite}   Dry-run: {dry_run}\n'
            f'═══════════════════════════════════════════════'
        )

        run = None
        if not dry_run:
            run = PipelineRun.objects.create(
                task_type=PipelineRun.TaskType.RASTER_NDVI,
                status=PipelineRun.Status.RUNNING,
                region=region,
                year=year,
                description=(
                    f'HLS fusion (S2+L) — '
                    f'{"district " + str(district.pk) if district else "region " + str(region.pk)} '
                    f'year={year}'
                ),
            )

        try:
            self._execute(region, district, year, overwrite, dry_run, run)
        except Exception as exc:
            self._fail_run(run, exc)
            raise

    def _execute(self, region, district, year, overwrite, dry_run, run):
        """Run the fusion pipeline: wipe → load → fuse → persist."""
        # 1. Optional wipe
        if overwrite and not dry_run:
            deleted = self._wipe_existing(region, district, year)
            self.stdout.write(f'  [wipe] Removed {deleted} existing fused records')

        # 2. Stream source observations and bucket per farmland
        per_fl = self._load_observations(region, district, year)
        if not per_fl:
            self.stdout.write(self.style.WARNING(
                '  No S2/Landsat observations found for the given scope.'
            ))
            self._complete_run(run)
            return

        self.stdout.write(f'  [load] {len(per_fl)} farmlands with source data')

        # 3. Fuse in memory
        fused_by_farmland = self._fuse_all(per_fl)
        total_points = sum(len(v) for v in fused_by_farmland.values())
        self.stdout.write(f'  [fuse] {total_points} fused observations produced')

        if dry_run:
            self.stdout.write(self.style.SUCCESS('  Dry-run — nothing written.'))
            return

        # 4. Persist: scenes + vegetation indices
        created_scenes, created_vi = self._persist(
            fused_by_farmland, region, district,
        )
        self.stdout.write(self.style.SUCCESS(
            f'  [write] scenes={created_scenes}, vegetation_index={created_vi}'
        ))
        self._complete_run(run, records_count=created_vi)

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _complete_run(run, records_count=None):
        if not run:
            return
        run.status = PipelineRun.Status.COMPLETED
        run.finished_at = timezone.now()
        fields = ['status', 'finished_at']
        if records_count is not None:
            run.records_count = records_count
            fields.append('records_count')
        run.save(update_fields=fields)

    @staticmethod
    def _fail_run(run, exc):
        if not run:
            return
        run.status = PipelineRun.Status.FAILED
        run.log = str(exc)[:4000]
        run.finished_at = timezone.now()
        run.save(update_fields=['status', 'log', 'finished_at'])

    def _resolve_scope(self, region_id, district_id):
        if district_id:
            try:
                district = District.objects.select_related('region').get(pk=district_id)
            except District.DoesNotExist:
                self.stderr.write(self.style.ERROR(f'District {district_id} not found'))
                return None, None
            return district.region, district
        try:
            region = Region.objects.get(pk=region_id)
        except Region.DoesNotExist:
            self.stderr.write(self.style.ERROR(f'Region {region_id} not found'))
            return None, None
        return region, None

    def _wipe_existing(self, region, district, year):
        qs = VegetationIndex.objects.filter(
            scene__satellite=FUSED_SAT,
            acquired_date__year=year,
        )
        if district:
            qs = qs.filter(farmland__district=district)
        else:
            qs = qs.filter(farmland__district__region=region)
        deleted, _ = qs.delete()

        # Best-effort scene cleanup: orphan scenes with no remaining VI rows
        SatelliteScene.objects.filter(
            satellite=FUSED_SAT,
            acquired_date__year=year,
            indices__isnull=True,
        ).delete()
        return deleted

    def _load_observations(self, region, district, year):
        """Return ``{farmland_id: {'s2': [(date, mean, n_valid, n_total), ...], 'l': [...]}}``."""
        qs = VegetationIndex.objects.filter(
            scene__satellite__in=SOURCE_SATS,
            index_type='ndvi',
            acquired_date__year=year,
            is_outlier=False,
            mean__gte=-0.2, mean__lte=1,
        )
        if district:
            qs = qs.filter(farmland__district=district)
        else:
            qs = qs.filter(farmland__district__region=region)

        rows = qs.values_list(
            'farmland_id', 'acquired_date', 'mean',
            'valid_pixel_count', 'pixel_count', 'scene__satellite',
        ).iterator(chunk_size=50_000)

        per_fl = defaultdict(lambda: {'s2': [], 'l': []})
        for fl_id, acq_date, mean_v, n_valid, n_total, sat in rows:
            if mean_v is None or acq_date is None:
                continue
            nv = int(n_valid or 0)
            nt = int(n_total or 0)
            # If valid_pixel_count is zero/missing, fallback to 1 so the
            # observation still contributes (weight handled in _fuse_one).
            if nv <= 0:
                nv = 1
            key = 's2' if sat == 'sentinel2' else 'l'
            per_fl[fl_id][key].append((acq_date, float(mean_v), nv, nt))
        return per_fl

    def _fuse_all(self, per_fl):
        """Apply the fusion rule for every farmland.

        Returns ``{farmland_id: [(date, mean, n_valid), ...]}``.
        """
        fused_by_farmland = {}
        for fl_id, obs in per_fl.items():
            fused_by_farmland[fl_id] = self._fuse_one(obs['s2'], obs['l'])
        return fused_by_farmland

    @staticmethod
    def _fuse_one(s2_obs, l_obs):
        """Fuse observations for a single farmland.

        Observations are ``(date, mean, n_valid, n_total)`` tuples, where
        ``n_total`` is the field's pixel count for that sensor.

        Rules:
        - Each S2 observation is kept. If there's a Landsat record within
          ±8 days, both are merged by a weighted mean whose weights are
          the *clean-coverage fraction* ``n_valid / n_total`` of each
          sensor — NOT the absolute pixel count (which is ~9× larger for
          S2 at 10 m than Landsat at 30 m and would otherwise drown the
          Landsat contribution).
        - Landsat records with no S2 neighbour within ±8 days are added
          as standalone fused points.

        Returns ``(date, fused_mean, n_valid)`` tuples — the stored pixel
        count is the sum of actually-observed valid pixels (for the UI's
        confidence display), independent of the fusion weights.
        """
        fused = []
        used_l_dates = set()

        # Sort L for deterministic "nearest" pick
        l_obs_sorted = sorted(l_obs, key=lambda x: x[0])

        for s2_date, s2_mean, s2_nv, s2_nt in s2_obs:
            best = None  # (delta, ld, lm, l_nv, l_nt)
            for ld, lm, l_nv, l_nt in l_obs_sorted:
                delta = abs((s2_date - ld).days)
                if delta > L_PAIR_WINDOW_DAYS:
                    continue
                if best is None or delta < best[0]:
                    best = (delta, ld, lm, l_nv, l_nt)
            if best is not None:
                _, ld, lm, l_nv, l_nt = best
                w_s2 = _coverage_weight(s2_nv, s2_nt)
                w_l = _coverage_weight(l_nv, l_nt)
                fused_mean = (s2_mean * w_s2 + lm * w_l) / (w_s2 + w_l)
                fused.append((s2_date, round(fused_mean, 4), s2_nv + l_nv))
                used_l_dates.add(ld)
            else:
                fused.append((s2_date, round(s2_mean, 4), s2_nv))

        # Orphan Landsat points (not paired with any S2)
        for ld, lm, l_nv, l_nt in l_obs_sorted:
            if ld not in used_l_dates:
                fused.append((ld, round(lm, 4), l_nv))

        fused.sort(key=lambda x: x[0])
        return fused

    # ------------------------------------------------------------------ persist

    def _persist(self, fused_by_farmland, region, district):
        """Bulk-create scenes + vegetation_index rows."""
        # Preload district_id for every farmland
        fl_ids = list(fused_by_farmland.keys())
        fl_to_district = dict(
            Farmland.objects.filter(pk__in=fl_ids)
            .values_list('pk', 'district_id')
        )

        scene_keys = self._collect_scene_keys(fused_by_farmland, fl_to_district)
        scene_lookup, created_scenes = self._upsert_scenes(scene_keys)
        objs = self._build_vi_rows(fused_by_farmland, fl_to_district, scene_lookup)
        created_vi = self._bulk_upsert_vi(objs)
        return created_scenes, created_vi

    @staticmethod
    def _collect_scene_keys(fused_by_farmland, fl_to_district):
        """Unique ``(district_id, acquired_date)`` scene keys."""
        scene_keys = set()
        for fl_id, pts in fused_by_farmland.items():
            did = fl_to_district.get(fl_id)
            if did is None:
                continue
            for acq_date, _, _ in pts:
                scene_keys.add((did, acq_date))
        return scene_keys

    @staticmethod
    def _upsert_scenes(scene_keys):
        """One fused scene per (district, date); returns lookup + created count."""
        scene_lookup = {}
        created_scenes = 0
        with transaction.atomic():
            for did, acq_date in scene_keys:
                scene_id = f'hls_{did}_{acq_date.isoformat()}'
                scene, created = SatelliteScene.objects.get_or_create(
                    scene_id=scene_id,
                    defaults={
                        'satellite': FUSED_SAT,
                        'acquired_date': acq_date,
                        'cloud_cover': 0,
                        'processed': True,
                    },
                )
                scene_lookup[(did, acq_date)] = scene
                if created:
                    created_scenes += 1
        return scene_lookup, created_scenes

    @staticmethod
    def _build_vi_rows(fused_by_farmland, fl_to_district, scene_lookup):
        objs = []
        for fl_id, pts in fused_by_farmland.items():
            did = fl_to_district.get(fl_id)
            if did is None:
                continue
            for acq_date, fused_mean, fused_n in pts:
                scene = scene_lookup.get((did, acq_date))
                if not scene:
                    continue
                objs.append(VegetationIndex(
                    farmland_id=fl_id,
                    scene=scene,
                    index_type='ndvi',
                    acquired_date=acq_date,
                    mean=fused_mean,
                    median=fused_mean,
                    min_val=0,
                    max_val=0,
                    std_val=0,
                    pixel_count=0,
                    valid_pixel_count=fused_n,
                    is_outlier=False,
                ))
        return objs

    @staticmethod
    def _bulk_upsert_vi(objs, batch_size=5_000):
        """bulk_create with update_conflicts — same pattern as fetch_raster_ndvi."""
        created_vi = 0
        for offset in range(0, len(objs), batch_size):
            batch = objs[offset:offset + batch_size]
            VegetationIndex.objects.bulk_create(
                batch,
                update_conflicts=True,
                unique_fields=['farmland', 'scene', 'index_type'],
                update_fields=[
                    'acquired_date', 'mean', 'median', 'min_val',
                    'max_val', 'std_val', 'pixel_count', 'valid_pixel_count',
                ],
            )
            created_vi += len(batch)
        return created_vi
