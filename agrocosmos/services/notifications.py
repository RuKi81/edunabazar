"""
Email notifications for Agrocosmos subscribers.

Two flows:

1. **Anomaly notifications** — triggered from ``detect_vegetation_alerts``
   whenever a new ``VegetationAlert`` is created or escalates severity.
   Recipients: any ``AgroSubscription`` with ``notify_anomalies=True``
   whose scope covers the alert's farmland (by district, or by region).

2. **Update notifications** — daily digest driven by
   ``send_agrocosmos_updates``.  Recipients: any ``AgroSubscription``
   with ``notify_updates=True`` whose scope saw fresh NDVI data since
   the subscription's ``last_update_notified_at``.

Uses Django's ``EmailMultiAlternatives`` with the configured SMTP
(settings.EMAIL_BACKEND / EMAIL_HOST / DEFAULT_FROM_EMAIL) — the same
transport that powers ``/legacy-admin/campaigns/``.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Iterable

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.urls import reverse

from legacy.models import LegacyUser

from ..models import AgroSubscription, Farmland, VegetationAlert

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _site_url() -> str:
    """Base absolute URL for the site (prod or dev)."""
    base = getattr(settings, 'SITE_URL', '').rstrip('/')
    if base:
        return base
    # Sensible fallback — matches the prod deployment.
    return 'https://edunabazar.ru'


def _district_report_url(region_id: int, district_id: int, year: int | None = None) -> str:
    year = year or date.today().year
    path = reverse('report_district')
    return f'{_site_url()}{path}?region={region_id}&district={district_id}&year={year}'


def _region_report_url(region_id: int, year: int | None = None) -> str:
    year = year or date.today().year
    path = reverse('report_region')
    return f'{_site_url()}{path}?region={region_id}&year={year}'


# ---------------------------------------------------------------------------
# Recipient resolution
# ---------------------------------------------------------------------------

def _emails_for_subscriptions(subscriptions: Iterable[AgroSubscription]) -> list[tuple[int, str]]:
    """Resolve ``(user_id, email)`` pairs for a list of subscriptions."""
    ids = {s.legacy_user_id for s in subscriptions}
    if not ids:
        return []
    rows = LegacyUser.objects.filter(pk__in=ids).exclude(
        email='').exclude(email__isnull=True).values_list('pk', 'email')
    return list(rows)


def _subscriptions_for_farmland(farmland: Farmland, flag: str):
    """Return subscriptions whose scope covers this farmland for the given flag.

    A subscription covers a farmland if:
        - ``district`` matches the farmland's district, OR
        - ``region`` matches the farmland's region AND ``district`` is NULL.
    """
    district_id = farmland.district_id
    region_id = farmland.district.region_id if farmland.district_id else None
    q = AgroSubscription.objects.filter(**{flag: True})
    from django.db.models import Q
    return q.filter(
        Q(district_id=district_id)
        | Q(district__isnull=True, region_id=region_id)
    )


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

def _send(to_email: str, subject: str, text: str, html: str | None = None) -> bool:
    """Thin wrapper that logs + swallows exceptions."""
    try:
        msg = EmailMultiAlternatives(
            subject=subject,
            body=text,
            from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', None),
            to=[to_email],
        )
        if html:
            msg.attach_alternative(html, 'text/html')
        msg.send(fail_silently=False)
        return True
    except Exception:
        logger.exception('Agrocosmos notification send failed to=%s subject=%s',
                         to_email, subject[:80])
        return False


# ---------------------------------------------------------------------------
# Public: anomaly alert
# ---------------------------------------------------------------------------

def send_anomaly_email(alert: VegetationAlert) -> int:
    """Email all subscribers about a newly-raised vegetation alert.

    Returns count of successfully delivered emails.  Idempotent only
    per invocation — call sites should gate on "new alert" themselves.
    """
    farmland = alert.farmland
    district = farmland.district
    region = district.region

    subs = _subscriptions_for_farmland(farmland, 'notify_anomalies')
    pairs = _emails_for_subscriptions(list(subs))
    if not pairs:
        return 0

    report_url = _district_report_url(region.pk, district.pk)
    sev_label = alert.get_severity_display().upper()
    type_label = alert.get_alert_type_display()

    subject = f'[{sev_label}] {type_label} — {district.name}, {region.name}'
    text = (
        f'На угодье #{farmland.pk} ({farmland.get_crop_type_display()}, '
        f'{farmland.area_ha:.1f} га) в районе «{district.name}» ({region.name}) '
        f'зафиксирован алерт вегетации.\n\n'
        f'Тип: {type_label}\n'
        f'Критичность: {alert.get_severity_display()}\n'
        f'Дата наблюдения: {alert.detected_on}\n'
        f'Описание: {alert.message}\n\n'
        f'Посмотреть отчёт с графиком NDVI по району:\n{report_url}\n\n'
        f'Управление подписками: {_site_url()}/me/agrocosmos/\n'
    )
    html = f'''
    <p>На угодье <strong>#{farmland.pk}</strong>
    ({farmland.get_crop_type_display()}, {farmland.area_ha:.1f} га)
    в районе <strong>«{district.name}»</strong> ({region.name})
    зафиксирован алерт вегетации.</p>
    <ul>
      <li><strong>Тип:</strong> {type_label}</li>
      <li><strong>Критичность:</strong> {alert.get_severity_display()}</li>
      <li><strong>Дата наблюдения:</strong> {alert.detected_on}</li>
    </ul>
    <p>{alert.message}</p>
    <p><a href="{report_url}" style="padding:8px 14px; background:#417690; color:#fff;
    border-radius:3px; text-decoration:none;">Открыть отчёт с графиком NDVI</a></p>
    <p style="color:#888; font-size:12px;">
      Управление подписками: <a href="{_site_url()}/me/agrocosmos/">{_site_url()}/me/agrocosmos/</a>
    </p>
    '''

    delivered = 0
    for _uid, email in pairs:
        if _send(email, subject, text, html):
            delivered += 1
    logger.info('Anomaly email: alert=%s delivered=%d/%d', alert.pk, delivered, len(pairs))
    return delivered


# ---------------------------------------------------------------------------
# Public: update digest
# ---------------------------------------------------------------------------

def send_update_email(subscription: AgroSubscription, new_obs_count: int) -> bool:
    """Email a single subscriber about fresh NDVI data in their scope.

    Returns True if delivered.  Caller is expected to bump
    ``last_update_notified_at`` on success to avoid re-sending.
    """
    user = LegacyUser.objects.filter(pk=subscription.legacy_user_id).first()
    if not user or not user.email:
        return False

    if subscription.district_id:
        district = subscription.district
        region = district.region
        scope_label = f'районе «{district.name}» ({region.name})'
        report_url = _district_report_url(region.pk, district.pk)
    elif subscription.region_id:
        region = subscription.region
        scope_label = f'субъекте «{region.name}»'
        report_url = _region_report_url(region.pk)
    else:
        return False

    subject = f'Новые данные NDVI — {scope_label}'
    text = (
        f'Здравствуйте!\n\n'
        f'В {scope_label} появились новые данные NDVI '
        f'({new_obs_count} свежих наблюдений).\n\n'
        f'Посмотреть обновлённый график:\n{report_url}\n\n'
        f'Управление подписками: {_site_url()}/me/agrocosmos/\n'
    )
    html = f'''
    <p>Здравствуйте!</p>
    <p>В {scope_label} появились новые данные NDVI
    (<strong>{new_obs_count}</strong> свежих наблюдений).</p>
    <p><a href="{report_url}" style="padding:8px 14px; background:#417690; color:#fff;
    border-radius:3px; text-decoration:none;">Открыть обновлённый график</a></p>
    <p style="color:#888; font-size:12px;">
      Управление подписками: <a href="{_site_url()}/me/agrocosmos/">{_site_url()}/me/agrocosmos/</a>
    </p>
    '''
    return _send(user.email, subject, text, html)
