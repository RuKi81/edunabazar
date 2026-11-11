"""
HTTP-слой чата поддержки (см. ``legacy/support_chat.py``).

Публичные API-эндпоинты ``csrf_exempt``: главная страница кэшируется целиком
для анонимов, поэтому встроить актуальный CSRF-токен в виджет нельзя.
Вместо этого проверяется same-origin (``Sec-Fetch-Site`` / ``Origin``), а
сам посетитель идентифицируется HttpOnly-cookie.
"""
import json
from urllib.parse import urlparse

from django.core.paginator import Paginator
from django.http import HttpRequest, HttpResponse, Http404, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .. import support_chat as svc
from ..models import SupportThread
from .helpers import _get_current_legacy_user, _no_store, _require_admin


def _same_origin(request: HttpRequest) -> bool:
    sfs = request.headers.get('Sec-Fetch-Site')
    if sfs:
        return sfs in ('same-origin', 'none')
    origin = request.headers.get('Origin') or request.headers.get('Referer')
    if not origin:
        return True
    return urlparse(origin).netloc.lower() == request.get_host().lower()


def _set_visitor_cookie(resp: HttpResponse, request: HttpRequest, key: str) -> HttpResponse:
    if request.COOKIES.get(svc.VISITOR_COOKIE) != key:
        resp.set_cookie(
            svc.VISITOR_COOKIE, key, max_age=svc.VISITOR_COOKIE_MAX_AGE,
            httponly=True, samesite='Lax', secure=request.is_secure(),
        )
    return resp


def _client_ip(request: HttpRequest) -> str | None:
    return request.META.get('HTTP_X_REAL_IP') or request.META.get('REMOTE_ADDR') or None


@csrf_exempt
@require_POST
def support_chat_send(request: HttpRequest) -> JsonResponse:
    if not _same_origin(request):
        return JsonResponse({'ok': False, 'error': 'forbidden'}, status=403)
    try:
        payload = json.loads(request.body.decode('utf-8') or '{}')
    except ValueError:
        return JsonResponse({'ok': False, 'error': 'bad_json'}, status=400)

    text = str(payload.get('text') or '').strip()
    if not text:
        return JsonResponse({'ok': False, 'error': 'empty'}, status=400)
    if len(text) > svc.MAX_TEXT_LEN:
        return JsonResponse({'ok': False, 'error': 'too_long'}, status=400)

    thread, key = svc.get_or_create_thread(
        request.COOKIES.get(svc.VISITOR_COOKIE),
        user=_get_current_legacy_user(request),
        page_url=str(payload.get('page') or '')[:500],
        user_agent=request.META.get('HTTP_USER_AGENT', ''),
        ip_address=_client_ip(request),
    )
    if svc.rate_limited(thread):
        resp = JsonResponse({'ok': False, 'error': 'rate_limited'}, status=429)
        return _set_visitor_cookie(resp, request, key)

    msg = svc.add_visitor_message(thread, text)
    resp = JsonResponse({'ok': True, 'message': svc.serialize_message(msg)})
    return _no_store(_set_visitor_cookie(resp, request, key))


@require_GET
def support_chat_messages(request: HttpRequest) -> JsonResponse:
    thread = svc.get_thread(request.COOKIES.get(svc.VISITOR_COOKIE))
    if thread is None:
        return _no_store(JsonResponse({'ok': True, 'messages': []}))
    try:
        after = int(request.GET.get('after') or 0)
    except ValueError:
        after = 0
    qs = thread.messages.filter(id__gt=after).order_by('id')[:200]
    return _no_store(JsonResponse({'ok': True, 'messages': [svc.serialize_message(m) for m in qs]}))


@csrf_exempt
@require_POST
def support_chat_telegram_webhook(request: HttpRequest, secret: str) -> HttpResponse:
    if secret != svc.webhook_secret():
        raise Http404
    try:
        update = json.loads(request.body.decode('utf-8') or '{}')
    except ValueError:
        return HttpResponse(status=400)
    svc.handle_telegram_update(update)
    return HttpResponse('ok')


# ── админка ───────────────────────────────────────────────────────────────

def admin_support_threads(request: HttpRequest) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    qs = SupportThread.objects.select_related('user').order_by('-updated_at')
    paginator = Paginator(qs, 50)
    page = paginator.get_page(request.GET.get('page') or 1)
    return _no_store(render(request, 'legacy/admin_support_threads.html', {
        'legacy_user': admin_user,
        'threads': page,
        'total_count': qs.count(),
        'tg_configured': svc.is_configured(),
    }))


def admin_support_thread(request: HttpRequest, thread_id: int) -> HttpResponse:
    admin_user, deny = _require_admin(request)
    if deny:
        return deny
    thread = get_object_or_404(SupportThread.objects.select_related('user'), pk=thread_id)
    if request.method == 'POST':
        text = (request.POST.get('text') or '').strip()
        if text:
            svc.add_operator_message(thread, text[:svc.MAX_TEXT_LEN])
        return redirect(f'/legacy-admin/support/{thread_id}/')
    return _no_store(render(request, 'legacy/admin_support_thread.html', {
        'legacy_user': admin_user,
        'thread': thread,
        'messages_list': list(thread.messages.all()),
    }))
