from django.db.models import Q
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from ..models import Advert, LegacyUser, Message
from .helpers import (
    _get_current_legacy_user, _no_store, _send_email, logger,
)


def _send_new_message_email(recipient_email: str, sender_name: str, advert_title: str, inbox_url: str) -> bool:
    subject = 'Новое сообщение на сайте'
    body = (
        f'{sender_name} отправил вам сообщение'
        + (f' по объявлению «{advert_title}»' if advert_title else '')
        + f'.\n\nПрочитайте его в личном кабинете:\n{inbox_url}\n'
    )
    return _send_email(recipient_email, subject, body)


def messages_inbox(request: HttpRequest) -> HttpResponse:
    """Show inbox: list of conversations grouped by the other party."""
    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/?next=/messages/')

    # Get all messages involving this user, newest first
    qs = Message.objects.filter(
        Q(sender_id=user.id) | Q(recipient_id=user.id)
    ).select_related('sender', 'recipient', 'advert').order_by('-created_at')

    # Group into conversations by the other user
    conversations: dict[int, dict] = {}
    for msg in qs[:500]:
        other_id = msg.recipient_id if msg.sender_id == user.id else msg.sender_id
        if other_id not in conversations:
            other_user = msg.recipient if msg.sender_id == user.id else msg.sender
            conversations[other_id] = {
                'other_user': other_user,
                'last_message': msg,
                'unread_count': 0,
            }
        if msg.recipient_id == user.id and not msg.is_read:
            conversations[other_id]['unread_count'] += 1

    conv_list = sorted(conversations.values(), key=lambda c: c['last_message'].created_at, reverse=True)

    resp = render(request, 'legacy/messages_inbox.html', {
        'legacy_user': user,
        'conversations': conv_list,
    })
    return _no_store(resp)


def messages_thread(request: HttpRequest, user_id: int) -> HttpResponse:
    """Show conversation thread with a specific user."""
    user = _get_current_legacy_user(request)
    if not user:
        return redirect(f"/login/?next=/messages/{user_id}/")

    other_user = get_object_or_404(LegacyUser, pk=user_id)

    messages_qs = Message.objects.filter(
        (Q(sender_id=user.id, recipient_id=user_id) | Q(sender_id=user_id, recipient_id=user.id))
    ).select_related('sender', 'recipient', 'advert').order_by('created_at')

    messages_list = list(messages_qs[:200])

    # Mark unread messages as read
    unread_ids = [m.id for m in messages_list if m.recipient_id == user.id and not m.is_read]
    if unread_ids:
        Message.objects.filter(pk__in=unread_ids).update(is_read=True)

    resp = render(request, 'legacy/messages_thread.html', {
        'legacy_user': user,
        'other_user': other_user,
        'messages': messages_list,
    })
    return _no_store(resp)


def message_send(request: HttpRequest) -> HttpResponse:
    """Send a message to another user."""
    if request.method != 'POST':
        return redirect('/messages/')

    user = _get_current_legacy_user(request)
    if not user:
        return redirect('/login/')

    recipient_id_raw = (request.POST.get('recipient_id') or '').strip()
    text = (request.POST.get('text') or '').strip()
    advert_id_raw = (request.POST.get('advert_id') or '').strip()

    try:
        recipient_id = int(recipient_id_raw)
    except Exception:
        return redirect('/messages/')

    if recipient_id == user.id:
        return redirect('/messages/')

    recipient = LegacyUser.objects.filter(pk=recipient_id).first()
    if not recipient:
        return redirect('/messages/')

    if not text:
        return redirect(f"/messages/{recipient_id}/")

    if len(text) > 5000:
        text = text[:5000]

    advert_id = None
    advert_title = ''
    try:
        if advert_id_raw:
            advert_id = int(advert_id_raw)
            advert_obj = Advert.objects.filter(pk=advert_id).first()
            advert_title = (getattr(advert_obj, 'title', '') or '') if advert_obj else ''
    except Exception:
        advert_id = None

    now = timezone.now()
    Message.objects.create(
        sender_id=int(user.id),
        recipient_id=recipient_id,
        advert_id=advert_id,
        text=text,
        is_read=False,
        created_at=now,
    )

    # Email notification to recipient
    try:
        recipient_email = (getattr(recipient, 'email', '') or '').strip()
        sender_name = (getattr(user, 'name', '') or getattr(user, 'username', '') or '').strip()
        inbox_url = request.build_absolute_uri(f"/messages/{user.id}/")
        if recipient_email:
            _send_new_message_email(recipient_email, sender_name, advert_title, inbox_url)
    except Exception:
        logger.exception('Failed to send new message email notification')

    return redirect(f"/messages/{recipient_id}/")


def messages_unread_count_api(request: HttpRequest) -> JsonResponse:
    """API endpoint returning unread message count for the current user."""
    user = _get_current_legacy_user(request)
    if not user:
        return JsonResponse({'ok': False, 'count': 0})
    count = Message.objects.filter(recipient_id=user.id, is_read=False).count()
    return JsonResponse({'ok': True, 'count': count})
