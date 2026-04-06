"""
Email unsubscribe views.

Unsubscribe link format:
    /unsubscribe/?email=<base64(email)>

No login required — the link itself serves as proof of ownership.
"""

import base64
import hashlib
import hmac

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from legacy.models import EmailUnsubscribe


def _make_token(email: str) -> str:
    """HMAC token to prevent forged unsubscribe requests."""
    key = (getattr(settings, 'SECRET_KEY', '') or 'fallback').encode()
    return hmac.new(key, email.lower().encode(), hashlib.sha256).hexdigest()[:16]


def make_unsubscribe_url(email: str) -> str:
    """Build full unsubscribe URL for a given email."""
    encoded = base64.urlsafe_b64encode(email.lower().encode()).decode()
    token = _make_token(email)
    return f'https://edunabazar.ru/unsubscribe/?e={encoded}&t={token}'


def email_unsubscribe(request: HttpRequest) -> HttpResponse:
    encoded = (request.GET.get('e') or '').strip()
    token = (request.GET.get('t') or '').strip()

    email = ''
    error = ''
    success = False

    if encoded:
        try:
            email = base64.urlsafe_b64decode(encoded).decode().lower().strip()
        except Exception:
            error = 'Неверная ссылка для отписки.'

    if email and not error:
        expected_token = _make_token(email)
        if not hmac.compare_digest(token, expected_token):
            error = 'Неверная ссылка для отписки.'

    if email and not error:
        _, created = EmailUnsubscribe.objects.get_or_create(email=email)
        success = True

    return render(request, 'legacy/email_unsubscribe.html', {
        'email': email,
        'success': success,
        'error': error,
    })
