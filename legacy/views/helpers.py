import hashlib
import logging
import time
import urllib.parse

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.utils import timezone
from django.core import signing
from django.core.mail import EmailMultiAlternatives
from django.core.validators import validate_email
from django.core.exceptions import ValidationError

from ..models import Advert, LegacyUser
from ..cache_utils import invalidate_advert_caches
from ..constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION,
    ADVERT_STATUS_PUBLISHED, ADVERT_STATUS_LABELS,
)


logger = logging.getLogger(__name__)


def _safe_localtime(dt):
    """Convert a possibly-naive datetime to local time safely."""
    if dt is None:
        return timezone.now()
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt)
    return timezone.localtime(dt)


def _no_store(resp: HttpResponse) -> HttpResponse:
    try:
        resp['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp['Pragma'] = 'no-cache'
        resp['Expires'] = '0'
    except Exception:
        pass
    return resp



def _get_current_legacy_user(request: HttpRequest):
    """Return the current legacy user or None.

    When ``LegacyUserMiddleware`` is active the user is already on
    ``request.legacy_user`` and this function simply returns it.
    Falls back to a session lookup for contexts without the middleware
    (e.g. management commands, tests).
    """
    _LOADED = '_cached_legacy_user_loaded'
    _ATTR = '_cached_legacy_user'
    if getattr(request, _LOADED, False):
        return getattr(request, _ATTR, None)
    legacy_user_id = request.session.get('legacy_user_id')
    user = LegacyUser.objects.filter(pk=legacy_user_id).first() if legacy_user_id else None
    setattr(request, _ATTR, user)
    setattr(request, _LOADED, True)
    return user


def _is_admin_user(user) -> bool:
    if not user:
        return False
    try:
        if bool(getattr(user, 'is_superuser', False)):
            return True
    except Exception:
        pass
    username = (getattr(user, 'username', '') or '').strip().lower()
    admin_usernames = getattr(settings, 'ADMIN_USERNAMES', {'admin'})
    return username in {u.lower() for u in admin_usernames}


def _get_admin_identity(request: HttpRequest):
    legacy_user = _get_current_legacy_user(request)
    if legacy_user is not None:
        return legacy_user
    try:
        django_user = getattr(request, 'user', None)
        if (
            django_user is not None
            and getattr(django_user, 'is_authenticated', False)
            and getattr(django_user, 'is_superuser', False)
        ):
            return django_user
    except Exception:
        return None


def _require_admin(request: HttpRequest):
    user = _get_admin_identity(request)
    if not user:
        return None, redirect(f"/login/?next={urllib.parse.quote(request.get_full_path())}")
    if not _is_admin_user(user):
        return None, redirect('/adverts/')
    return user, None


def _can_edit_advert(user, advert: Advert) -> bool:
    if not user:
        return False
    if _is_admin_user(user):
        return True
    try:
        return int(advert.author_id) == int(user.id) and int(advert.status) in (ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION, ADVERT_STATUS_PUBLISHED)
    except Exception:
        return False


def _can_manage_advert(user, advert: Advert) -> bool:
    if not user:
        return False
    if _is_admin_user(user):
        return True
    try:
        return int(advert.author_id) == int(user.id) and int(advert.status) in (ADVERT_STATUS_HIDDEN, ADVERT_STATUS_PUBLISHED)
    except Exception:
        return False


_STATUS_LABELS = ADVERT_STATUS_LABELS


def _annotate_adverts(adverts, user=None):
    for a in adverts:
        try:
            st = int(getattr(a, 'status', 0) or 0)
        except Exception:
            st = 0
        a.status_label = _STATUS_LABELS.get(st, f'Статус: {st}')
        if user is not None:
            a.can_edit = _can_edit_advert(user, a)
            a.can_manage = _can_manage_advert(user, a)
    return adverts


def _update_advert_status(advert_id: int, status: int) -> None:
    st = int(status)
    now = timezone.now()
    fields = {'status': st, 'updated_at': now}
    if st == ADVERT_STATUS_HIDDEN:
        fields['hidden_at'] = now
        fields['deleted_at'] = None
    elif st == ADVERT_STATUS_DELETED:
        fields['deleted_at'] = now
    elif st == ADVERT_STATUS_PUBLISHED:
        fields['hidden_at'] = None
        fields['deleted_at'] = None
    Advert.objects.filter(pk=int(advert_id)).update(**fields)
    invalidate_advert_caches()


_ANTISPAM_MIN_SECONDS = 3
_ANTISPAM_SECRET = 'eduna-antispam-2026'


def _antispam_token() -> tuple[str, str]:
    """Return (timestamp_str, hash) for embedding in a form."""
    ts = str(int(time.time()))
    h = hashlib.sha256(f'{ts}:{_ANTISPAM_SECRET}'.encode()).hexdigest()[:16]
    return ts, h


def _antispam_check(request) -> str | None:
    """
    Validate honeypot + time trap on POST.
    Returns error string if bot detected, else None.
    """
    # Honeypot: field "website" must be empty (hidden from humans via CSS)
    if (request.POST.get('website') or '').strip():
        logger.warning('Antispam: honeypot triggered from %s', request.META.get('REMOTE_ADDR'))
        return 'bot'

    ts_str = (request.POST.get('_ts') or '').strip()
    ts_hash = (request.POST.get('_th') or '').strip()
    if not ts_str or not ts_hash:
        logger.warning('Antispam: missing timestamp from %s', request.META.get('REMOTE_ADDR'))
        return 'bot'

    expected_hash = hashlib.sha256(f'{ts_str}:{_ANTISPAM_SECRET}'.encode()).hexdigest()[:16]
    if ts_hash != expected_hash:
        logger.warning('Antispam: invalid hash from %s', request.META.get('REMOTE_ADDR'))
        return 'bot'

    try:
        elapsed = int(time.time()) - int(ts_str)
    except (ValueError, TypeError):
        return 'bot'

    if elapsed < _ANTISPAM_MIN_SECONDS:
        logger.warning('Antispam: too fast (%ds) from %s', elapsed, request.META.get('REMOTE_ADDR'))
        return 'bot'

    return None


def _normalize_phone(phone: str) -> str:
    s = ''.join(ch for ch in (phone or '') if ch.isdigit() or ch == '+')
    s = s.strip()
    if not s:
        return ''
    if s.startswith('8') and len(s) == 11:
        s = '+7' + s[1:]
    if s.startswith('7') and len(s) == 11:
        s = '+7' + s[1:]
    if s.startswith('9') and len(s) == 10:
        s = '+7' + s
    if s.startswith('+'):
        return s
    return '+' + s


def _normalize_extra_contacts(contacts):
    if not contacts:
        return []
    if not isinstance(contacts, list):
        return []
    out = []
    for ec in contacts:
        ec = dict(ec)
        val = ec.get('value', '')
        if ec.get('type') in ('website', 'social') and val and not val.startswith(('http://', 'https://')):
            ec['href'] = 'https://' + val
        else:
            ec['href'] = val
        out.append(ec)
    return out


_EXTRA_CONTACT_LABELS = {
    'email': ('Email', 'email'),
    'telegram': ('Telegram', 'send'),
    'max': ('MAX', 'chat'),
    'social': ('Соцсеть', 'group'),
    'website': ('Сайт', 'language'),
}


def _send_email(to_email: str, subject: str, body: str) -> bool:
    to_email = (to_email or '').strip()
    if not to_email:
        return False
    try:
        validate_email(to_email)
    except ValidationError:
        logger.warning('Invalid email address: %s', to_email)
        return False

    from_email = (getattr(settings, 'DEFAULT_FROM_EMAIL', '') or '').strip() or None
    try:
        msg = EmailMultiAlternatives(
            subject=str(subject or '').strip(),
            body=str(body or ''),
            from_email=from_email,
            to=[to_email],
        )
        msg.send(fail_silently=False)
        return True
    except Exception:
        logger.exception('Failed to send email to=%s subject=%s', to_email, subject)
        return False


def _make_set_password_token(user_id: int, auth_key: str) -> str:
    payload = {
        'uid': int(user_id),
        'ak': str(auth_key or ''),
    }
    return signing.dumps(payload, salt='legacy-set-password')


def _send_registration_email(user_email: str, username: str, set_password_url: str) -> bool:
    subject = 'Регистрация на сайте'
    body = (
        'Вы зарегистрировали профиль.\n\n'
        f'Логин: {username}\n\n'
        'Установите пароль по ссылке (она одноразовая и скоро истечёт):\n'
        f'{set_password_url}\n'
    )
    return _send_email(user_email, subject, body)


def _send_advert_published_email(user_email: str, advert_title: str, advert_url: str) -> bool:
    subject = 'Ваше объявление опубликовано'
    body = (
        'Ваше объявление прошло модерацию и опубликовано.\n\n'
        f'Объявление: {advert_title}\n'
        f'Ссылка: {advert_url}\n'
    )
    return _send_email(user_email, subject, body)
