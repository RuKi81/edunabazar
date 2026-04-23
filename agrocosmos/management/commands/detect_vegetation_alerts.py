"""
Detect vegetation alerts for each farmland.

Walks recent NDVI observations for every farmland and raises/resolves
alerts based on two patterns:

1. **baseline_deviation** — ``k`` consecutive cloud-free observations
   whose smoothed NDVI is below the district+crop historical mean by
   more than ``Z_WARN`` (≈1.5σ) or ``Z_CRIT`` (≈2.0σ).  Requires a
   populated ``NdviBaseline``.

2. **rapid_drop** — the most recent smoothed NDVI is at least
   ``DROP_WARN`` (≈0.15) below the value ~16 days earlier.
   ``DROP_CRIT`` (≈0.20) escalates to critical severity.

Alerts are **deduplicated per (farmland, alert_type)**: if an unresolved
alert of the same type already exists, we update its severity/context
but don't spawn a new row.  Once the metric recovers past recovery
thresholds, the active alert auto-resolves.

Typical run window: last 30 days.  Safe to run nightly.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from agrocosmos.models import (
    Farmland, NdviBaseline, VegetationAlert, VegetationIndex,
)

logger = logging.getLogger(__name__)


# Thresholds — tuned conservatively to avoid alert fatigue.
Z_WARN = -1.5
Z_CRIT = -2.0
Z_RECOVER = -1.0           # z ≥ this for ≥1 obs → resolve baseline_deviation alert
CONSECUTIVE_BELOW = 2      # how many obs in a row must satisfy z ≤ Z_WARN
LOOKBACK_DAYS = 30         # window of VegetationIndex history to consider

DROP_WARN = 0.15
DROP_CRIT = 0.20
DROP_REF_DAYS = 16         # compare latest to obs ≥ this many days earlier
DROP_RECOVER = 0.08        # latest NDVI recovered this much → resolve rapid_drop


class Command(BaseCommand):
    help = 'Detect baseline-deviation and rapid-drop NDVI alerts.'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int,
                            help='Restrict to farmlands of one region.')
        parser.add_argument('--district-id', type=int,
                            help='Restrict to farmlands of one district.')
        parser.add_argument('--dry-run', action='store_true',
                            help='Report detections without writing alerts.')
        parser.add_argument('--verbose-farmlands', action='store_true',
                            help='Log every farmland scan (noisy).')

    def handle(self, *args, **options):
        dry = options['dry_run']

        qs = Farmland.objects.select_related('district').all()
        if options.get('district_id'):
            qs = qs.filter(district_id=options['district_id'])
        elif options.get('region_id'):
            qs = qs.filter(district__region_id=options['region_id'])

        total = qs.count()
        self.stdout.write(
            f'Scanning {total} farmlands'
            + ('  [DRY RUN]' if dry else '')
        )

        today = date.today()
        lookback = today - timedelta(days=LOOKBACK_DAYS)

        created = updated = resolved = 0
        skipped_no_data = 0

        for farmland in qs.iterator(chunk_size=500):
            series = self._load_series(farmland.pk, lookback)
            if not series:
                skipped_no_data += 1
                continue

            if options.get('verbose_farmlands'):
                self.stdout.write(f'  farmland={farmland.pk} n={len(series)}')

            # Baseline lookup for this farmland's district+crop.  Fall
            # back to the "all crops" baseline (crop_type='') if a
            # crop-specific one isn't available.
            baseline_map = self._load_baseline_map(
                district_id=farmland.district_id,
                crop_type=farmland.crop_type,
            )

            det_b = self._check_baseline_deviation(series, baseline_map)
            det_d = self._check_rapid_drop(series)

            for alert_type, detection in [
                (VegetationAlert.AlertType.BASELINE_DEVIATION, det_b),
                (VegetationAlert.AlertType.RAPID_DROP, det_d),
            ]:
                c, u, r = self._reconcile(
                    farmland=farmland,
                    alert_type=alert_type,
                    detection=detection,
                    dry=dry,
                )
                created += c
                updated += u
                resolved += r

        self.stdout.write(self.style.SUCCESS(
            f'Done: created={created}, updated={updated}, resolved={resolved}, '
            f'skipped(no data)={skipped_no_data}'
            + (' [dry run, no DB writes]' if dry else '')
        ))

    # ------------------------------------------------------------------
    # Data loaders
    # ------------------------------------------------------------------

    def _load_series(self, farmland_id: int, since: date):
        """Return list of (date, value) for cloud-free NDVI observations.

        Uses mean_smooth when available (gap-filled, SG-smoothed), else
        falls back to the raw per-date mean.  Sorted ascending.
        """
        rows = (
            VegetationIndex.objects
            .filter(
                farmland_id=farmland_id,
                index_type='ndvi',
                is_outlier=False,
                acquired_date__gte=since,
            )
            .order_by('acquired_date')
            .values('acquired_date', 'mean', 'mean_smooth')
        )
        series = []
        for r in rows:
            v = r['mean_smooth'] if r['mean_smooth'] is not None else r['mean']
            if v is None:
                continue
            series.append((r['acquired_date'], float(v)))
        return series

    def _load_baseline_map(self, district_id: int, crop_type: str):
        """Return {doy: (mean, std)}.  Crop-specific baseline takes precedence."""
        def _fetch(ct):
            return {
                b['day_of_year']: (b['mean_ndvi'], b['std_ndvi'])
                for b in NdviBaseline.objects.filter(
                    district_id=district_id, crop_type=ct,
                ).values('day_of_year', 'mean_ndvi', 'std_ndvi')
            }

        crop_map = _fetch(crop_type) if crop_type else {}
        if crop_map:
            return crop_map
        return _fetch('')  # fall-back: all-crops baseline

    # ------------------------------------------------------------------
    # Detectors
    # ------------------------------------------------------------------

    def _check_baseline_deviation(self, series, baseline_map):
        """Return dict describing detection or ``None``."""
        if not baseline_map or len(series) < CONSECUTIVE_BELOW:
            return None

        # Walk from the most recent obs backwards, need N-in-a-row below Z_WARN.
        recent = series[-CONSECUTIVE_BELOW:]
        zs = []
        latest_detected_on = None
        worst_z = 0.0
        for d, v in recent:
            doy = d.timetuple().tm_yday
            entry = baseline_map.get(doy)
            # If exact DOY missing, try nearest within ±8 days (biweekly grid).
            if entry is None:
                for delta in range(1, 9):
                    entry = baseline_map.get(doy - delta) or baseline_map.get(doy + delta)
                    if entry:
                        break
            if not entry:
                return None
            mean, std = entry
            if std is None or std < 0.01:
                return None
            z = (v - mean) / std
            zs.append((d, v, mean, std, z))
            latest_detected_on = d
            if z < worst_z:
                worst_z = z

        # All recent observations must be below Z_WARN.
        if any(z >= Z_WARN for _, _, _, _, z in zs):
            return None

        severity = (VegetationAlert.Severity.CRITICAL
                    if worst_z <= Z_CRIT
                    else VegetationAlert.Severity.WARNING)

        last = zs[-1]
        d, v, mean, std, z = last
        return {
            'detected_on': latest_detected_on,
            'severity': severity,
            'context': {
                'ndvi': round(v, 3),
                'baseline_mean': round(mean, 3),
                'baseline_std': round(std, 3),
                'z_score': round(z, 2),
                'worst_z': round(worst_z, 2),
                'obs_below_in_row': len(zs),
                'recent': [
                    {'date': d_.isoformat(), 'ndvi': round(v_, 3),
                     'z': round(z_, 2)}
                    for d_, v_, _, _, z_ in zs
                ],
            },
            'message': (
                f'NDVI {v:.2f} ниже нормы {mean:.2f}±{std:.2f} '
                f'(z={z:.2f}) {CONSECUTIVE_BELOW} наблюдения подряд'
            ),
            'recovery': {
                # used by auto-resolve logic
                'min_z_required': Z_RECOVER,
            },
        }

    def _check_rapid_drop(self, series):
        if len(series) < 2:
            return None
        today_d, today_v = series[-1]
        cutoff = today_d - timedelta(days=DROP_REF_DAYS)

        # Reference = the latest observation that is at least
        # DROP_REF_DAYS earlier than the current one.
        ref = None
        for d, v in reversed(series[:-1]):
            if d <= cutoff:
                ref = (d, v)
                break
        if ref is None:
            return None
        ref_d, ref_v = ref
        diff = today_v - ref_v
        if diff > -DROP_WARN:
            return None

        severity = (VegetationAlert.Severity.CRITICAL
                    if diff <= -DROP_CRIT
                    else VegetationAlert.Severity.WARNING)

        return {
            'detected_on': today_d,
            'severity': severity,
            'context': {
                'ndvi': round(today_v, 3),
                'ndvi_ref': round(ref_v, 3),
                'drop': round(diff, 3),
                'ref_date': ref_d.isoformat(),
                'days_between': (today_d - ref_d).days,
            },
            'message': (
                f'NDVI упал {diff:+.2f} за {(today_d - ref_d).days} дней '
                f'({ref_v:.2f} → {today_v:.2f})'
            ),
            'recovery': {
                'min_recovery': DROP_RECOVER,
                'ref_ndvi': ref_v,
            },
        }

    # ------------------------------------------------------------------
    # Reconcile — create / update / resolve
    # ------------------------------------------------------------------

    def _reconcile(self, farmland, alert_type, detection, dry):
        """Sync one (farmland, alert_type) cell against new detection."""
        existing = (
            VegetationAlert.objects
            .filter(farmland=farmland, alert_type=alert_type)
            .exclude(status=VegetationAlert.Status.RESOLVED)
            .order_by('-triggered_at')
            .first()
        )

        if detection is None:
            # Nothing wrong right now — resolve any active alert.
            if existing is not None:
                if not dry:
                    with transaction.atomic():
                        existing.status = VegetationAlert.Status.RESOLVED
                        existing.resolved_at = timezone.now()
                        existing.save(update_fields=['status', 'resolved_at'])
                return (0, 0, 1)
            return (0, 0, 0)

        # We have a detection.
        if existing is None:
            if not dry:
                VegetationAlert.objects.create(
                    farmland=farmland,
                    alert_type=alert_type,
                    severity=detection['severity'],
                    status=VegetationAlert.Status.ACTIVE,
                    detected_on=detection['detected_on'],
                    context=detection['context'],
                    message=detection['message'],
                )
            return (1, 0, 0)

        # Update existing (same or escalated severity, newer observation).
        changed = False
        if existing.severity != detection['severity']:
            existing.severity = detection['severity']
            changed = True
        if existing.detected_on != detection['detected_on']:
            existing.detected_on = detection['detected_on']
            changed = True
        if existing.context != detection['context']:
            existing.context = detection['context']
            changed = True
        if existing.message != detection['message']:
            existing.message = detection['message']
            changed = True

        if changed and not dry:
            existing.save(update_fields=[
                'severity', 'detected_on', 'context', 'message',
            ])

        return (0, 1 if changed else 0, 0)
