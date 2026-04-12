"""
Compute phenological metrics (SOS, EOS, POS, LOS, MaxNDVI, TI) from smoothed NDVI.

Requires ndvi_postprocess to be run first (populates mean_smooth).

Algorithm (threshold-based, Jönsson & Eklundh, 2002 / TIMESAT):
- SOS = first date when smoothed NDVI crosses 20% of (max - base) above base
- EOS = last date when smoothed NDVI drops below 20% of (max - base)
- POS = date of maximum smoothed NDVI
- LOS = EOS - SOS (days)
- MaxNDVI = peak smoothed value
- TI = trapezoidal integral of NDVI from SOS to EOS

Usage:
    python manage.py compute_phenology --region-id 37 --year 2024
    python manage.py compute_phenology --region-id 37 --year 2024 --source modis
"""
import numpy as np
from datetime import date, timedelta

from django.core.management.base import BaseCommand

from agrocosmos.models import Farmland, FarmlandPhenology, VegetationIndex

SOS_EOS_RATIO = 0.20   # 20% of amplitude above baseline
BASE_NDVI = 0.10       # assumed dormant/bare NDVI

MODIS_SATELLITES = ('modis_terra', 'modis_aqua')
RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')

BATCH_SIZE = 2000


class Command(BaseCommand):
    help = 'Compute phenological metrics from smoothed NDVI time series'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, required=True)
        parser.add_argument('--year', type=int, required=True)
        parser.add_argument('--source', type=str, choices=['modis', 'raster'],
                            default='modis')

    def handle(self, *args, **options):
        region_id = options['region_id']
        year = options['year']
        source = options['source']

        sat_kw = {}
        if source == 'modis':
            sat_kw = {'scene__satellite__in': MODIS_SATELLITES}
        else:
            sat_kw = {'scene__satellite__in': RASTER_SATELLITES}

        fl_ids = list(
            Farmland.objects
            .filter(district__region_id=region_id)
            .values_list('id', flat=True)
        )
        self.stdout.write(f'Region {region_id}, year {year}, source {source}: {len(fl_ids)} farmlands')

        created = 0
        skipped = 0
        to_save = []

        for i, fl_id in enumerate(fl_ids):
            records = list(
                VegetationIndex.objects
                .filter(
                    farmland_id=fl_id,
                    index_type='ndvi',
                    acquired_date__year=year,
                    is_anomaly=False,
                    mean_smooth__isnull=False,
                    **sat_kw,
                )
                .order_by('acquired_date')
                .values_list('acquired_date', 'mean_smooth', named=False)
            )

            if len(records) < 5:
                skipped += 1
                continue

            dates = [r[0] for r in records]
            vals = np.array([r[1] for r in records], dtype=np.float64)

            pheno = _compute_phenology(dates, vals)
            if pheno is None:
                skipped += 1
                continue

            to_save.append(FarmlandPhenology(
                farmland_id=fl_id,
                year=year,
                source=source,
                sos_date=pheno['sos'],
                eos_date=pheno['eos'],
                pos_date=pheno['pos'],
                max_ndvi=pheno['max_ndvi'],
                los_days=pheno['los'],
                total_ndvi=pheno['ti'],
            ))
            created += 1

            if len(to_save) >= BATCH_SIZE:
                FarmlandPhenology.objects.bulk_create(
                    to_save, batch_size=BATCH_SIZE,
                    update_conflicts=True,
                    unique_fields=['farmland', 'year', 'source'],
                    update_fields=['sos_date', 'eos_date', 'pos_date',
                                   'max_ndvi', 'los_days', 'total_ndvi'],
                )
                to_save = []

            if (i + 1) % 10000 == 0:
                self.stdout.write(f'  [{i+1}/{len(fl_ids)}]')

        if to_save:
            FarmlandPhenology.objects.bulk_create(
                to_save, batch_size=BATCH_SIZE,
                update_conflicts=True,
                unique_fields=['farmland', 'year', 'source'],
                update_fields=['sos_date', 'eos_date', 'pos_date',
                               'max_ndvi', 'los_days', 'total_ndvi'],
            )

        self.stdout.write(f'\nDone: {created} phenology records, {skipped} skipped')


def _compute_phenology(dates, vals):
    """
    Threshold-based phenology extraction.

    Returns dict with sos, eos, pos, max_ndvi, los, ti or None.
    """
    max_idx = int(np.argmax(vals))
    max_ndvi = float(vals[max_idx])
    pos_date = dates[max_idx]

    amplitude = max_ndvi - BASE_NDVI
    if amplitude < 0.1:
        return None  # no real vegetation signal

    threshold = BASE_NDVI + SOS_EOS_RATIO * amplitude

    # SOS: first crossing above threshold (search from left to peak)
    sos_date = None
    for j in range(max_idx + 1):
        if vals[j] >= threshold:
            sos_date = dates[j]
            break

    # EOS: last crossing above threshold (search from right to peak)
    eos_date = None
    for j in range(len(vals) - 1, max_idx - 1, -1):
        if vals[j] >= threshold:
            eos_date = dates[j]
            break

    if sos_date is None or eos_date is None:
        return None

    los = (eos_date - sos_date).days
    if los < 30:
        return None  # too short

    # Trapezoidal integral (NDVI × days) from SOS to EOS
    ti = 0.0
    for j in range(len(dates) - 1):
        if dates[j] >= sos_date and dates[j + 1] <= eos_date:
            dt = (dates[j + 1] - dates[j]).days
            ti += 0.5 * (vals[j] + vals[j + 1]) * dt

    return {
        'sos': sos_date,
        'eos': eos_date,
        'pos': pos_date,
        'max_ndvi': round(max_ndvi, 4),
        'los': los,
        'ti': round(ti, 2),
    }
