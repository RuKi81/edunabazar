import io
import re
import time

from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.core.mail import EmailMultiAlternatives, get_connection
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Q
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme

from ..models import (
    Advert, AdvertPhoto, LegacyUser, Catalog, Review, Seller, Message,
    EmailCampaign, EmailLog,
)
from ..constants import (
    USER_STATUS_ACTIVE,
    REVIEW_STATUS_DELETED, REVIEW_STATUS_HIDDEN, REVIEW_STATUS_MODERATION,
    REVIEW_STATUS_PUBLISHED,
)
from .helpers import (
    _require_admin, _no_store, logger,
)


# ---------------------------------------------------------------------------
#  User management
# ---------------------------------------------------------------------------

def admin_users(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    q = (request.GET.get('q') or '').strip()
    qs = LegacyUser.objects.all()
    if q:
        qs = qs.filter(Q(username__icontains=q) | Q(email__icontains=q) | Q(name__icontains=q))
    paginator = Paginator(qs.order_by('-created_at', '-id'), 25)
    page = paginator.get_page(request.GET.get('page') or 1)
    page_range = paginator.get_elided_page_range(page.number)
    resp = render(
        request,
        'legacy/admin_users.html',
        {
            'legacy_user': admin_user,
            'users': page,
            'page_size': 25,
            'page_range': page_range,
            'q': q,
        },
    )
    return _no_store(resp)


def _safe_next_url(request: HttpRequest, default: str) -> str:
    """Return POST['next'] if it is a local URL, else ``default``."""
    next_raw = (request.POST.get('next') or '').strip()
    if next_raw and url_has_allowed_host_and_scheme(
        url=next_raw,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return next_raw
    return default


def _parse_user_ids(raw_ids) -> list[int]:
    """Parse ids from POST: garbage is skipped, duplicates and non-positive dropped."""
    ids = []
    for rid in raw_ids:
        try:
            ids.append(int(str(rid).strip()))
        except Exception:
            continue
    return sorted({x for x in ids if x > 0})


def _delete_users_cascade(delete_ids: list[int]) -> None:
    """Delete users and all their related rows in a single transaction."""
    try:
        with transaction.atomic():
            advert_qs = Advert.objects.filter(author_id__in=delete_ids)
            advert_ids = list(advert_qs.values_list('id', flat=True))
            if advert_ids:
                AdvertPhoto.objects.filter(advert_id__in=advert_ids).delete()
            advert_qs.delete()
            Review.objects.filter(author_id__in=delete_ids).delete()
            Seller.objects.filter(user_id__in=delete_ids).delete()
            Message.objects.filter(Q(sender_id__in=delete_ids) | Q(recipient_id__in=delete_ids)).delete()
            LegacyUser.objects.filter(id__in=delete_ids).delete()
    except Exception:
        logger.exception('admin_users_bulk_delete failed for ids=%s', delete_ids)
        raise


def admin_users_bulk_delete(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    if request.method != 'POST':
        return redirect('/legacy-admin/')
    safe_next = _safe_next_url(request, '/legacy-admin/')

    ids = _parse_user_ids(request.POST.getlist('user_id'))
    if not ids:
        return redirect(safe_next)

    protected_ids = set(LegacyUser.objects.filter(username__iexact='admin').values_list('id', flat=True))
    delete_ids = [uid for uid in ids if uid not in protected_ids]
    if not delete_ids:
        return redirect(safe_next)

    _delete_users_cascade(delete_ids)
    return redirect(safe_next)


def admin_catalogs(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    ok_message = ''
    if request.method == 'POST':
        catalogs = Catalog.objects.all().order_by('sort', 'title', 'id')
        for c in catalogs:
            raw = (request.POST.get(f"sort_{int(c.id)}") or '').strip()
            try:
                sort_val = int(raw) if raw else 0
            except Exception:
                sort_val = 0
            active_val = 1 if (request.POST.get(f"active_{int(c.id)}") or '').strip().lower() in {'1', 'true', 'on', 'yes'} else 0
            Catalog.objects.filter(pk=int(c.id)).update(sort=sort_val, active=active_val)
        ok_message = 'Сохранено'
    catalogs = Catalog.objects.all().order_by('sort', 'title', 'id')
    resp = render(
        request,
        'legacy/admin_catalogs.html',
        {'legacy_user': admin_user, 'catalogs': catalogs, 'ok_message': ok_message},
    )
    return _no_store(resp)


def admin_user_detail(request: HttpRequest, user_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    u = get_object_or_404(LegacyUser, pk=user_id)
    errors: dict[str, str] = {}
    ok_message = ''
    if request.method == 'POST':
        new_password = (request.POST.get('new_password') or '').strip()
        if not new_password:
            errors['new_password'] = 'Введите новый пароль'
        elif len(new_password) < 4:
            errors['new_password'] = 'Пароль слишком короткий'
        if not errors:
            LegacyUser.objects.filter(pk=int(u.id)).update(password_hash=make_password(new_password), updated_at=timezone.now())
            ok_message = 'Пароль изменён'
    adverts = Advert.objects.filter(author_id=u.id).select_related('category').order_by('-updated_at', '-id')
    resp = render(
        request,
        'legacy/admin_user_detail.html',
        {'legacy_user': admin_user, 'u': u, 'errors': errors, 'ok_message': ok_message, 'adverts': adverts},
    )
    return _no_store(resp)


# ---------------------------------------------------------------------------
#  Moderation: Reviews & Messages
# ---------------------------------------------------------------------------

def admin_reviews(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    status_filter = (request.GET.get('status') or 'moderation').strip()
    status_map = {
        'moderation': REVIEW_STATUS_MODERATION,
        'published': REVIEW_STATUS_PUBLISHED,
        'hidden': REVIEW_STATUS_HIDDEN,
    }
    qs = Review.objects.select_related('author').order_by('-created_at', '-id')
    if status_filter in status_map:
        qs = qs.filter(status=status_map[status_filter])
    else:
        qs = qs.exclude(status=REVIEW_STATUS_DELETED)

    paginator = Paginator(qs, 50)
    page = paginator.get_page(request.GET.get('page', 1))

    return _no_store(render(request, 'legacy/admin_reviews.html', {
        'legacy_user': admin_user,
        'reviews': page,
        'status_filter': status_filter,
        'count_moderation': Review.objects.filter(status=REVIEW_STATUS_MODERATION).count(),
        'count_published': Review.objects.filter(status=REVIEW_STATUS_PUBLISHED).count(),
        'count_hidden': Review.objects.filter(status=REVIEW_STATUS_HIDDEN).count(),
    }))


def admin_review_action(request: HttpRequest, review_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    if request.method != 'POST':
        return redirect('/legacy-admin/reviews/')

    action = (request.POST.get('action') or '').strip()
    now = timezone.now()

    if action == 'publish':
        Review.objects.filter(pk=review_id).update(status=REVIEW_STATUS_PUBLISHED, updated_at=now)
    elif action == 'hide':
        Review.objects.filter(pk=review_id).update(status=REVIEW_STATUS_HIDDEN, updated_at=now)
    elif action == 'delete':
        Review.objects.filter(pk=review_id).update(status=REVIEW_STATUS_DELETED, updated_at=now)

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/legacy-admin/reviews/')


def admin_messages(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    q = (request.GET.get('q') or '').strip()
    qs = Message.objects.select_related('sender', 'recipient', 'advert').order_by('-created_at')
    if q:
        qs = qs.filter(
            Q(text__icontains=q)
            | Q(sender__username__icontains=q)
            | Q(sender__name__icontains=q)
            | Q(recipient__username__icontains=q)
            | Q(recipient__name__icontains=q)
        )

    paginator = Paginator(qs, 50)
    page = paginator.get_page(request.GET.get('page', 1))

    return _no_store(render(request, 'legacy/admin_messages.html', {
        'legacy_user': admin_user,
        'messages_page': page,
        'q': q,
        'total_count': Message.objects.count(),
    }))


def admin_message_delete(request: HttpRequest, message_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    if request.method == 'POST':
        Message.objects.filter(pk=message_id).delete()

    next_url = (request.POST.get('next') or '').strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url, allowed_hosts={request.get_host()}, require_https=request.is_secure(),
    ):
        return redirect(next_url)
    return redirect('/legacy-admin/messages/')


# ---------------------------------------------------------------------------
#  Email Campaigns
# ---------------------------------------------------------------------------

def admin_campaigns(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    campaigns = EmailCampaign.objects.all()
    return _no_store(render(request, 'legacy/admin_campaigns.html', {
        'legacy_user': admin_user,
        'campaigns': campaigns,
    }))


def admin_campaign_create(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    errors: dict[str, str] = {}

    if request.method == 'POST':
        name = (request.POST.get('name') or '').strip()
        subject = (request.POST.get('subject') or '').strip()
        body_html = (request.POST.get('body_html') or '').strip()
        body_text = (request.POST.get('body_text') or '').strip()
        from_email = (request.POST.get('from_email') or '').strip()
        audience = (request.POST.get('audience') or 'all').strip()

        if not name:
            errors['name'] = 'Введите название кампании'
        if not subject:
            errors['subject'] = 'Введите тему письма'
        if not body_html:
            errors['body_html'] = 'Введите HTML-тело письма'

        if not errors:
            campaign = EmailCampaign.objects.create(
                name=name,
                subject=subject,
                body_html=body_html,
                body_text=body_text,
                from_email=from_email or settings.DEFAULT_FROM_EMAIL,
                audience=audience,
            )
            return redirect(f'/legacy-admin/campaigns/{campaign.pk}/')

    return _no_store(render(request, 'legacy/admin_campaign_form.html', {
        'legacy_user': admin_user,
        'errors': errors,
        'form_data': request.POST if request.method == 'POST' else {},
        'audience_choices': EmailCampaign.AUDIENCE_CHOICES,
        'default_from': settings.DEFAULT_FROM_EMAIL,
    }))


def admin_campaign_detail(request: HttpRequest, campaign_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    campaign = get_object_or_404(EmailCampaign, pk=campaign_id)

    upload_message = request.session.pop('campaign_upload_message', '')
    upload_error = request.session.pop('campaign_upload_error', '')

    # Stats
    sent = EmailLog.objects.filter(campaign=campaign, status=EmailLog.STATUS_SENT).count()
    failed = EmailLog.objects.filter(campaign=campaign, status=EmailLog.STATUS_FAILED).count()
    pending = EmailLog.objects.filter(campaign=campaign, status=EmailLog.STATUS_PENDING).count()
    recent_errors = EmailLog.objects.filter(
        campaign=campaign, status=EmailLog.STATUS_FAILED,
    ).order_by('-created_at')[:20]

    # Estimated audience size (for draft campaigns)
    audience_count = 0
    if campaign.status == EmailCampaign.STATUS_DRAFT:
        qs = LegacyUser.objects.exclude(email='').exclude(email__isnull=True)
        if campaign.audience == EmailCampaign.AUDIENCE_REGISTERED:
            qs = qs.filter(status=USER_STATUS_ACTIVE)
        elif campaign.audience == EmailCampaign.AUDIENCE_IMPORTED:
            qs = qs.exclude(status=USER_STATUS_ACTIVE)
        audience_count = qs.count()

    # Build batches of 180
    BATCH_SIZE = 180
    all_logs = list(
        EmailLog.objects.filter(campaign=campaign)
        .order_by('id')
        .values_list('id', 'status', named=True)
    )
    batches = []
    for i in range(0, len(all_logs), BATCH_SIZE):
        chunk = all_logs[i:i + BATCH_SIZE]
        b_sent = sum(1 for r in chunk if r.status == EmailLog.STATUS_SENT)
        b_failed = sum(1 for r in chunk if r.status == EmailLog.STATUS_FAILED)
        b_pending = sum(1 for r in chunk if r.status == EmailLog.STATUS_PENDING)
        batch_num = (i // BATCH_SIZE) + 1
        first_id = chunk[0].id
        last_id = chunk[-1].id
        batches.append({
            'num': batch_num,
            'total': len(chunk),
            'sent': b_sent,
            'failed': b_failed,
            'pending': b_pending,
            'first_id': first_id,
            'last_id': last_id,
            'done': b_pending == 0,
        })

    return _no_store(render(request, 'legacy/admin_campaign_detail.html', {
        'legacy_user': admin_user,
        'campaign': campaign,
        'sent': sent,
        'failed': failed,
        'pending': pending,
        'recent_errors': recent_errors,
        'audience_count': audience_count,
        'batches': batches,
        'upload_message': upload_message,
        'upload_error': upload_error,
    }))


def admin_campaign_delete(request: HttpRequest, campaign_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    campaign = get_object_or_404(EmailCampaign, pk=campaign_id)
    if request.method == 'POST':
        campaign.delete()
    return redirect('/legacy-admin/campaigns/')


def admin_campaign_send_test(request: HttpRequest, campaign_id: int) -> JsonResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return JsonResponse({'ok': False, 'error': 'Forbidden'}, status=403)

    campaign = get_object_or_404(EmailCampaign, pk=campaign_id)
    test_email = (request.POST.get('test_email') or '').strip()
    if not test_email:
        return JsonResponse({'ok': False, 'error': 'Укажите email'})

    from_email = campaign.from_email or settings.DEFAULT_FROM_EMAIL
    try:
        msg = EmailMultiAlternatives(
            subject=f'[ТЕСТ] {campaign.subject}',
            body=campaign.body_text or campaign.subject,
            from_email=from_email,
            to=[test_email],
        )
        if campaign.body_html:
            msg.attach_alternative(campaign.body_html, 'text/html')
        msg.send(fail_silently=False)
        return JsonResponse({'ok': True})
    except Exception as e:
        return JsonResponse({'ok': False, 'error': str(e)[:500]})


def _validate_excel_upload(excel_file) -> str | None:
    """Return an error string if the uploaded file is missing or not Excel."""
    if not excel_file:
        return 'Файл не выбран'
    if not excel_file.name.endswith(('.xlsx', '.xls')):
        return 'Поддерживаются только файлы .xlsx / .xls'
    return None


def _extract_emails_from_excel(excel_file) -> set[str]:
    """Collect lowercased email addresses from all string cells of the workbook."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(excel_file.read()), read_only=True)
    ws = wb.active

    email_re_pattern = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
    raw_emails = set()
    for row in ws.iter_rows(values_only=True):
        for cell in row:
            if cell and isinstance(cell, str):
                found = email_re_pattern.findall(cell.strip().lower())
                raw_emails.update(found)
    wb.close()
    return raw_emails


def _add_campaign_recipients(campaign, raw_emails: set[str]) -> tuple[int, int]:
    """Create EmailLog rows for new addresses; return (added, skipped_duplicates)."""
    existing = set(
        EmailLog.objects.filter(campaign=campaign)
        .values_list('recipient_email', flat=True)
    )
    new_emails = sorted(raw_emails - existing)

    if new_emails:
        logs = [EmailLog(campaign=campaign, recipient_email=e) for e in new_emails]
        EmailLog.objects.bulk_create(logs, batch_size=1000)

        campaign.total_recipients = EmailLog.objects.filter(campaign=campaign).count()
        campaign.save(update_fields=['total_recipients'])

    return len(new_emails), len(raw_emails) - len(new_emails)


def admin_campaign_upload_excel(request: HttpRequest, campaign_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny

    campaign = get_object_or_404(EmailCampaign, pk=campaign_id)
    back = redirect(f'/legacy-admin/campaigns/{campaign_id}/')

    if request.method != 'POST':
        return back

    excel_file = request.FILES.get('excel_file')
    error = _validate_excel_upload(excel_file)
    if error:
        request.session['campaign_upload_error'] = error
        return back

    try:
        raw_emails = _extract_emails_from_excel(excel_file)
    except Exception as e:
        request.session['campaign_upload_error'] = f'Ошибка чтения файла: {e}'
        return back

    if not raw_emails:
        request.session['campaign_upload_error'] = 'В файле не найдено email-адресов'
        return back

    added, skipped = _add_campaign_recipients(campaign, raw_emails)

    msg = f'Загружено {added} новых адресов из файла «{excel_file.name}».'
    if skipped:
        msg += f' Пропущено дублей: {skipped}.'
    request.session['campaign_upload_message'] = msg
    return back


def _mark_campaign_sending(campaign) -> None:
    if campaign.status in (EmailCampaign.STATUS_DRAFT, EmailCampaign.STATUS_PAUSED):
        campaign.status = EmailCampaign.STATUS_SENDING
        if not campaign.started_at:
            campaign.started_at = timezone.now()
        campaign.save(update_fields=['status', 'started_at'])


def _close_connection_quietly(connection) -> None:
    if connection:
        try:
            connection.close()
        except Exception:
            pass


def _reopen_connection_or_none():
    """Open a fresh SMTP connection; return None on failure (skip remaining sends)."""
    try:
        connection = get_connection()
        connection.open()
        return connection
    except Exception:
        return None


def _send_campaign_message(campaign, log, from_email, connection) -> None:
    """Send one campaign email and mark its log as sent. Raises on SMTP errors."""
    msg = EmailMultiAlternatives(
        subject=campaign.subject,
        body=campaign.body_text or campaign.subject,
        from_email=from_email,
        to=[log.recipient_email],
        connection=connection,
    )
    if campaign.body_html:
        msg.attach_alternative(campaign.body_html, 'text/html')
    msg.send(fail_silently=False)

    log.status = EmailLog.STATUS_SENT
    log.sent_at = timezone.now()
    log.save(update_fields=['status', 'sent_at'])


def _mark_log_failed(log, exc: Exception) -> None:
    log.status = EmailLog.STATUS_FAILED
    log.error_message = str(exc)[:500]
    log.save(update_fields=['status', 'error_message'])


def _send_campaign_batch(campaign, logs, connection) -> tuple[int, int]:
    """Send emails one by one (1/sec); reconnect after failures. Return (sent, failed)."""
    from_email = campaign.from_email or settings.DEFAULT_FROM_EMAIL
    sent = 0
    failed = 0
    for log in logs:
        try:
            _send_campaign_message(campaign, log, from_email, connection)
            sent += 1
        except Exception as e:
            _mark_log_failed(log, e)
            failed += 1
            # Try to reconnect for remaining emails
            _close_connection_quietly(connection)
            connection = _reopen_connection_or_none()
        time.sleep(1)  # 1 email/sec throttle
    _close_connection_quietly(connection)
    return sent, failed


def _update_campaign_counters(campaign) -> None:
    """Refresh sent/failed counters; mark campaign done when nothing is pending."""
    campaign.sent_count = EmailLog.objects.filter(campaign=campaign, status=EmailLog.STATUS_SENT).count()
    campaign.failed_count = EmailLog.objects.filter(campaign=campaign, status=EmailLog.STATUS_FAILED).count()
    remaining = EmailLog.objects.filter(campaign=campaign, status=EmailLog.STATUS_PENDING).count()
    if remaining == 0:
        campaign.status = EmailCampaign.STATUS_DONE
        campaign.finished_at = timezone.now()
    campaign.save(update_fields=['sent_count', 'failed_count', 'status', 'finished_at'])


def admin_campaign_send_batch(request: HttpRequest, campaign_id: int) -> JsonResponse:
    """AJAX: send one batch of emails (by log ID range)."""
    admin_user, deny = _require_admin(request)
    if deny:
        return JsonResponse({'ok': False, 'error': 'Forbidden'}, status=403)

    campaign = get_object_or_404(EmailCampaign, pk=campaign_id)

    first_id = int(request.POST.get('first_id', 0))
    last_id = int(request.POST.get('last_id', 0))
    if not first_id or not last_id:
        return JsonResponse({'ok': False, 'error': 'Missing batch IDs'})

    logs = list(
        EmailLog.objects.filter(
            campaign=campaign,
            id__gte=first_id,
            id__lte=last_id,
            status=EmailLog.STATUS_PENDING,
        ).order_by('id')
    )

    if not logs:
        return JsonResponse({'ok': True, 'sent': 0, 'failed': 0, 'message': 'Нет писем для отправки'})

    _mark_campaign_sending(campaign)

    try:
        connection = get_connection()
        connection.open()
    except Exception as e:
        return JsonResponse({'ok': False, 'error': f'SMTP connect error: {e}'})

    sent, failed = _send_campaign_batch(campaign, logs, connection)
    _update_campaign_counters(campaign)

    return JsonResponse({
        'ok': True,
        'sent': sent,
        'failed': failed,
        'message': f'Отправлено {sent}, ошибок {failed}',
    })
