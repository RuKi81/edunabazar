"""
District-level NDVI alert detection (MODIS).

Runs the same two detectors as the legacy ``detect_vegetation_alerts``
command (baseline deviation, rapid drop) but at the **district × crop_type**
granularity, reading from the ``DistrictNdviSeries`` pre-aggregate
instead of walking every ``VegetationIndex`` row.

Scope is **restricted to subscribed districts** by default — i.e. only
districts that appear in at least one ``AgroSubscription`` with
``notify_anomalies=True`` (region-level subscriptions are expanded to
their member districts). For Moscow Oblast this typically reduces the
working set from ~hundreds of thousands of farmland scans to ~tens of
district aggregates per night.

Rationale: with MODIS 250 m, per-farmland NDVI is statistically noisy
and almost every farmland aggregates the same handful of pixels as its
neighbours. A district-level z-score against ``NdviBaseline`` is more
robust and gives subscribers a usable signal ("в Истринском районе
пашня просела на 1.8σ от нормы") without flooding their inbox with
per-field rows.

Flags:
    --all                Ignore subscriptions, scan every district that
                         has DistrictNdviSeries data. Use for a weekly
                         deep sweep.
    --district-id N      Single-district debug run.
    --crop-type CODE     Limit to one crop_type code.
    --dry-run            No DB writes / no emails.

Per-farmland alerts (S2/L8 use-case) are intentionally NOT handled
here — the legacy ``detect_vegetation_alerts`` command stays in place
for that and is currently muted in cron (see ``.github/workflows/ci.yml``).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from agrocosmos.models import (
    AgroSubscription, District, DistrictNdviSeries, NdviBaseline,
    VegetationAlert,
)

logger = logging.getLogger(__name__)


# Detector thresholds — mirror the per-farmland constants so both
# pipelines speak the same z-score language to subscribers.
Z_WARN = -1.5
Z_CRIT = -2.0
Z_RECOVER = -1.0
CONSECUTIVE_BELOW = 2
LOOKBACK_DAYS = 45         # ~3 MODIS composites; gives room for gap-fill

DROP_WARN = 0.15
DROP_CRIT = 0.20
DROP_REF_DAYS = 16
DROP_RECOVER = 0.08


class Command(BaseCommand):
    help = 'Detect district-level NDVI alerts (MODIS) for subscribed scopes.'

    def add_arguments(self, parser):
        parser.add_argument('--all', action='store_true',
                            help='Scan every district with data, not just subscribed scopes.')
        parser.add_argument('--district-id', type=int,
                            help='Restrict to one district (debug).')
        parser.add_argument('--crop-type',
                            help='Restrict to one crop_type code (e.g. "arable").')
        parser.add_argument('--dry-run', action='store_true',
                            help='Detect without DB writes / emails.')

    def handle(self, *args, **options):
        dry = options['dry_run']

        district_ids = self._resolve_scope(options)
        if not district_ids:
            self.stdout.write(self.style.WARNING(
                'No subscribed districts found — nothing to do. '
                'Run with --all for a full sweep.'
            ))
            return

        self.stdout.write(
            f'Scanning {len(district_ids)} districts'
            + (' (full sweep)' if options['all'] else ' (subscribed scopes)')
            + ('  [DRY RUN]' if dry else '')
        )

        today = date.today()
        since = today - timedelta(days=LOOKBACK_DAYS)
        crop_filter = options.get('crop_type') or None

        created = updated = resolved = 0
        scanned_cells = 0

        # Pull all relevant series rows in one query, group by
        # (district_id, crop_type) → list[(date, mean_ndvi)].
        series_qs = (
            DistrictNdviSeries.objects
            .filter(
                district_id__in=district_ids,
                source=DistrictNdviSeries.Source.MODIS,
                acquired_date__gte=since,
                sum_area__gt=0,
            )
            .values('district_id', 'crop_type', 'acquired_date',
                    'sum_ndvi_area', 'sum_area')
        )
        if crop_filter:
            series_qs = series_qs.filter(crop_type=crop_filter)

        grouped: dict[tuple[int, str], list[tuple[date, float]]] = defaultdict(list)
        for r in series_qs:
            mean = r['sum_ndvi_area'] / r['sum_area']
            grouped[(r['district_id'], r['crop_type'])].append(
                (r['acquired_date'], float(mean))
            )

        # Sort each cell's series ascending by date.
        for cell in grouped.values():
            cell.sort(key=lambda x: x[0])

        # Resolve district → region for notifications + report links.
        districts = {
            d.pk: d for d in District.objects
            .select_related('region')
            .filter(pk__in=district_ids)
        }

        for (district_id, crop_type), series in grouped.items():
            scanned_cells += 1
            if len(series) < CONSECUTIVE_BELOW:
                continue
            district = districts.get(district_id)
            if district is None:
                continue

            baseline_map = self._load_baseline_map(district_id, crop_type)

            det_b = self._check_baseline_deviation(series, baseline_map)
            det_d = self._check_rapid_drop(series)

            for alert_type, detection in [
                (VegetationAlert.AlertType.BASELINE_DEVIATION, det_b),
                (VegetationAlert.AlertType.RAPID_DROP, det_d),
            ]:
                c, u, r = self._reconcile(
                    district=district,
                    crop_type=crop_type,
                    alert_type=alert_type,
                    detection=detection,
                    dry=dry,
                )
                created += c
                updated += u
                resolved += r

        self.stdout.write(self.style.SUCCESS(
            f'Done: districts={len(district_ids)}, cells_scanned={scanned_cells}, '
            f'created={created}, updated={updated}, resolved={resolved}'
            + (' [dry run, no DB writes]' if dry else '')
        ))

    # ------------------------------------------------------------------
    # Scope resolution
    # ------------------------------------------------------------------

    def _resolve_scope(self, options) -> set[int]:
        if options.get('district_id'):
            return {options['district_id']}

        if options['all']:
            # Any district that has at least one MODIS series row.
            return set(
                DistrictNdviSeries.objects
                .filter(source=DistrictNdviSeries.Source.MODIS)
                .values_list('district_id', flat=True)
                .distinct()
            )

        # Expand subscriptions: per-district + per-region.
        district_ids: set[int] = set()
        subs = AgroSubscription.objects.filter(notify_anomalies=True)
        region_ids: set[int] = set()
        for s in subs.values('district_id', 'region_id'):
            if s['district_id']:
                district_ids.add(s['district_id'])
            elif s['region_id']:
                region_ids.add(s['region_id'])
        if region_ids:
            district_ids.update(
                District.objects
                .filter(region_id__in=region_ids)
                .values_list('pk', flat=True)
            )
        return district_ids

    # ------------------------------------------------------------------
    # Baseline lookup (district + crop_type + DOY → mean, std)
    # ------------------------------------------------------------------

    def _load_baseline_map(self, district_id: int, crop_type: str) -> dict[int, tuple[float, float]]:
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
        return _fetch('')  # fallback: all-crops baseline

    # ------------------------------------------------------------------
    # Detectors — same shape as the per-farmland version.
    # ------------------------------------------------------------------

    def _check_baseline_deviation(self, series, baseline_map):
        if not baseline_map or len(series) < CONSECUTIVE_BELOW:
            return None

        recent = series[-CONSECUTIVE_BELOW:]
        zs = []
        latest_detected_on = None
        worst_z = 0.0
        for d, v in recent:
            doy = d.timetuple().tm_yday
            entry = baseline_map.get(doy)
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
                f'(z={z:.2f}), {CONSECUTIVE_BELOW} композиты подряд'
            ),
        }

    def _check_rapid_drop(self, series):
        if len(series) < 2:
            return None
        today_d, today_v = series[-1]
        cutoff = today_d - timedelta(days=DROP_REF_DAYS)

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
        }

    # ------------------------------------------------------------------
    # Reconcile — create / update / resolve, gated on (district, crop_type,
    # alert_type, source=MODIS, farmland IS NULL).
    # ------------------------------------------------------------------

    def _reconcile(self, district, crop_type, alert_type, detection, dry):
        from agrocosmos.services.notifications import send_anomaly_email

        existing = (
            VegetationAlert.objects
            .filter(
                farmland__isnull=True,
                district=district,
                crop_type=crop_type,
                alert_type=alert_type,
                source=VegetationAlert.Source.MODIS,
            )
            .exclude(status=VegetationAlert.Status.RESOLVED)
            .order_by('-triggered_at')
            .first()
        )

        if detection is None:
            if existing is not None and not dry:
                with transaction.atomic():
                    existing.status = VegetationAlert.Status.RESOLVED
                    existing.resolved_at = timezone.now()
                    existing.save(update_fields=['status', 'resolved_at'])
                return (0, 0, 1)
            return (0, 0, 0)

        if existing is None:
            if dry:
                return (1, 0, 0)
            alert = VegetationAlert.objects.create(
                farmland=None,
                district=district,
                crop_type=crop_type,
                source=VegetationAlert.Source.MODIS,
                alert_type=alert_type,
                severity=detection['severity'],
                status=VegetationAlert.Status.ACTIVE,
                detected_on=detection['detected_on'],
                context=detection['context'],
                message=detection['message'],
            )
            try:
                send_anomaly_email(alert)
            except Exception:
                logger.exception('Failed to notify subscribers for alert #%s', alert.pk)
            return (1, 0, 0)

        prev_severity = existing.severity
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
            escalated = (
                prev_severity == VegetationAlert.Severity.WARNING
                and existing.severity == VegetationAlert.Severity.CRITICAL
            )
            if escalated:
                try:
                    send_anomaly_email(existing)
                except Exception:
                    logger.exception('Failed to notify on escalation alert=%s', existing.pk)

        return (0, 1 if changed else 0, 0)
