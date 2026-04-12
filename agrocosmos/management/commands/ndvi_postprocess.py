"""
Post-processing of NDVI time series: spike detection + Savitzky-Golay smoothing.

Runs per-farmland across a region/year. For each farmland:
1. Load NDVI time series sorted by date
2. Detect spikes (anomalies) using a rolling-median absolute deviation test
3. Apply Savitzky-Golay filter to the clean series
4. Save is_anomaly and mean_smooth back to DB

Usage:
    python manage.py ndvi_postprocess --region-id 37 --year 2024
    python manage.py ndvi_postprocess --region-id 37 --year 2024 --source modis
    python manage.py ndvi_postprocess --region-id 37 --year 2024 --source raster
    python manage.py ndvi_postprocess --region-id 37  # all years
"""
import numpy as np
from django.core.management.base import BaseCommand
from django.db.models import Q

from agrocosmos.models import Farmland, VegetationIndex

# Spike detection: if |value - rolling_median| > threshold, mark as anomaly
SPIKE_THRESHOLD = 0.15  # NDVI units
ROLLING_WINDOW = 3      # half-window for rolling median (±3 points)

# Savitzky-Golay parameters
SG_WINDOW = 5           # must be odd, ≥ 5 for cubic
SG_POLYORDER = 3

MODIS_SATELLITES = ('modis_terra', 'modis_aqua')
RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')

BATCH_SIZE = 500        # bulk_update batch


class Command(BaseCommand):
    help = 'NDVI post-processing: spike detection + Savitzky-Golay smoothing'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, required=True)
        parser.add_argument('--year', type=int, help='Year (optional, processes all years if omitted)')
        parser.add_argument('--source', type=str, choices=['modis', 'raster'],
                            help='Filter by satellite source')
        parser.add_argument('--threshold', type=float, default=SPIKE_THRESHOLD,
                            help=f'Spike threshold (default: {SPIKE_THRESHOLD})')

    def handle(self, *args, **options):
        from scipy.signal import savgol_filter

        region_id = options['region_id']
        year = options.get('year')
        source = options.get('source')
        threshold = options['threshold']

        # Satellite filter
        sat_kw = {}
        if source == 'modis':
            sat_kw = {'scene__satellite__in': MODIS_SATELLITES}
        elif source == 'raster':
            sat_kw = {'scene__satellite__in': RASTER_SATELLITES}

        # Get farmland IDs in region
        fl_ids = list(
            Farmland.objects
            .filter(district__region_id=region_id)
            .values_list('id', flat=True)
        )
        self.stdout.write(f'Region {region_id}: {len(fl_ids)} farmlands')

        # Base queryset
        vi_base = VegetationIndex.objects.filter(
            farmland_id__in=fl_ids,
            index_type='ndvi',
            **sat_kw,
        )
        if year:
            vi_base = vi_base.filter(acquired_date__year=year)

        total_records = vi_base.count()
        self.stdout.write(f'Total VI records: {total_records}')

        if total_records == 0:
            return

        # Process per farmland
        anomalies_total = 0
        smoothed_total = 0
        to_update = []

        for i, fl_id in enumerate(fl_ids):
            records = list(
                vi_base
                .filter(farmland_id=fl_id)
                .order_by('acquired_date')
                .values_list('pk', 'mean', named=False)
            )
            if len(records) < 3:
                continue

            pks = [r[0] for r in records]
            vals = np.array([r[1] for r in records], dtype=np.float64)
            n = len(vals)

            # --- Spike detection via rolling median ---
            is_spike = np.zeros(n, dtype=bool)
            for j in range(n):
                lo = max(0, j - ROLLING_WINDOW)
                hi = min(n, j + ROLLING_WINDOW + 1)
                local_median = np.nanmedian(vals[lo:hi])
                if abs(vals[j] - local_median) > threshold:
                    is_spike[j] = True

            anomalies_total += int(is_spike.sum())

            # --- Savitzky-Golay smoothing on clean values ---
            clean_vals = vals.copy()
            clean_vals[is_spike] = np.nan

            # Interpolate NaN gaps for smoothing
            nans = np.isnan(clean_vals)
            if nans.all():
                # All anomalies — skip smoothing
                smoothed = np.full(n, np.nan)
            else:
                if nans.any():
                    # Linear interpolation of gaps
                    good = ~nans
                    xp = np.where(good)[0]
                    fp = clean_vals[good]
                    clean_vals = np.interp(np.arange(n), xp, fp)

                win = min(SG_WINDOW, n)
                if win % 2 == 0:
                    win -= 1
                if win < 3:
                    smoothed = clean_vals
                else:
                    poly = min(SG_POLYORDER, win - 1)
                    smoothed = savgol_filter(clean_vals, win, poly)
                    smoothed = np.clip(smoothed, -0.2, 1.0)

            # --- Prepare bulk update ---
            for j, pk in enumerate(pks):
                obj = VegetationIndex(pk=pk)
                obj.is_anomaly = bool(is_spike[j])
                obj.mean_smooth = round(float(smoothed[j]), 4) if not np.isnan(smoothed[j]) else None
                to_update.append(obj)
                smoothed_total += 1

            # Flush batch
            if len(to_update) >= BATCH_SIZE:
                VegetationIndex.objects.bulk_update(
                    to_update, ['is_anomaly', 'mean_smooth'], batch_size=BATCH_SIZE
                )
                to_update = []

            if (i + 1) % 10000 == 0:
                self.stdout.write(f'  [{i+1}/{len(fl_ids)}] farmlands processed')

        # Final flush
        if to_update:
            VegetationIndex.objects.bulk_update(
                to_update, ['is_anomaly', 'mean_smooth'], batch_size=BATCH_SIZE
            )

        self.stdout.write(
            f'\nDone: {smoothed_total} records smoothed, '
            f'{anomalies_total} anomalies detected'
        )
