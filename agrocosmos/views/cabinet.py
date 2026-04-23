"""
User cabinet page for Agrocosmos notification subscriptions.

Mounted at ``/me/agrocosmos/`` (registered in ``legacy/urls.py``).
Lets a logged-in LegacyUser manage per-region / per-district
subscriptions with two flags:

    - ``notify_anomalies`` — email on new / escalated VegetationAlert.
    - ``notify_updates`` — email when fresh NDVI data arrives (daily
      digest from ``send_agrocosmos_updates``).
"""
from __future__ import annotations

from django.contrib import messages
from django.db import IntegrityError
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from legacy.models import Message
from legacy.views.helpers import _get_current_legacy_user

from ..models import AgroSubscription, District, Region


def me_agrocosmos(request: HttpRequest) -> HttpResponse:
    """GET: show existing subscriptions + add/edit form.

    POST actions (via ``action`` field):
        - ``add``    — create a new subscription.
        - ``update`` — toggle flags on an existing one.
        - ``delete`` — remove a subscription row.
    """
    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/me/agrocosmos/')

    if request.method == 'POST':
        action = (request.POST.get('action') or '').strip()

        if action == 'delete':
            sub_id = _to_int(request.POST.get('subscription_id'))
            if sub_id:
                AgroSubscription.objects.filter(
                    pk=sub_id, legacy_user_id=user.pk
                ).delete()
                messages.success(request, 'Подписка удалена.')
            return redirect('/me/agrocosmos/')

        if action == 'update':
            sub_id = _to_int(request.POST.get('subscription_id'))
            if sub_id:
                try:
                    sub = AgroSubscription.objects.get(
                        pk=sub_id, legacy_user_id=user.pk)
                except AgroSubscription.DoesNotExist:
                    messages.error(request, 'Подписка не найдена.')
                    return redirect('/me/agrocosmos/')
                sub.notify_anomalies = _checkbox(request.POST.get('notify_anomalies'))
                sub.notify_updates = _checkbox(request.POST.get('notify_updates'))
                sub.save(update_fields=['notify_anomalies', 'notify_updates', 'updated_at'])
                messages.success(request, 'Настройки сохранены.')
            return redirect('/me/agrocosmos/')

        if action == 'add':
            region_id = _to_int(request.POST.get('region'))
            district_id = _to_int(request.POST.get('district'))
            notify_anomalies = _checkbox(request.POST.get('notify_anomalies'))
            notify_updates = _checkbox(request.POST.get('notify_updates'))

            if not region_id and not district_id:
                messages.error(request, 'Укажите субъект или район.')
                return redirect('/me/agrocosmos/')

            # Normalise: if district given, derive region from it.
            if district_id:
                try:
                    district = District.objects.select_related('region').get(pk=district_id)
                except District.DoesNotExist:
                    messages.error(request, 'Район не найден.')
                    return redirect('/me/agrocosmos/')
                region_id = district.region_id

            if not notify_anomalies and not notify_updates:
                messages.error(request, 'Включите хотя бы один тип уведомлений.')
                return redirect('/me/agrocosmos/')

            try:
                AgroSubscription.objects.create(
                    legacy_user_id=user.pk,
                    region_id=region_id,
                    district_id=district_id or None,
                    notify_anomalies=notify_anomalies,
                    notify_updates=notify_updates,
                )
                messages.success(request, 'Подписка добавлена.')
            except IntegrityError:
                messages.warning(
                    request,
                    'Подписка на этот scope уже существует. Измените существующую.',
                )
            return redirect('/me/agrocosmos/')

        messages.error(request, 'Неизвестное действие.')
        return redirect('/me/agrocosmos/')

    # GET — render page
    subscriptions = (
        AgroSubscription.objects
        .filter(legacy_user_id=user.pk)
        .select_related('region', 'district', 'district__region')
        .order_by('region__name', 'district__name')
    )
    regions = Region.objects.order_by('name')

    # For client-side district filtering by region.  District is
    # rendered with a ``data-region`` attribute so the JS on the page
    # can filter without an extra API call.
    districts = (
        District.objects
        .select_related('region')
        .order_by('region__name', 'name')
        .values('id', 'name', 'region_id', 'region__name')
    )

    return render(request, 'agrocosmos/cabinet.html', {
        'legacy_user': user,
        'subscriptions': subscriptions,
        'regions': regions,
        'districts': list(districts),
        'active_section': 'agrocosmos',
        'messages_unread_count': Message.objects.filter(
            recipient_id=user.id, is_read=False,
        ).count(),
    })


def _to_int(raw) -> int | None:
    try:
        n = int(raw)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _checkbox(raw) -> bool:
    return (raw or '').strip().lower() in {'1', 'true', 'on', 'yes'}
