from datetime import datetime

from django.conf import settings

from .models import LegacyUser

_CACHE_ATTR = '_cached_legacy_user'
_CACHE_LOADED = '_cached_legacy_user_loaded'


def _is_admin(user) -> bool:
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


def _can_open_gis(user) -> bool:
    try:
        from access.services import can_open_gis_page
        return can_open_gis_page(user)
    except Exception:
        return _is_admin(user)


def _can_view_farmlands(user) -> bool:
    try:
        from access.services import can_view_farmlands
        return can_view_farmlands(user)
    except Exception:
        return _is_admin(user)


def _support_chat() -> dict:
    return {
        'support_chat_provider': getattr(settings, 'SUPPORT_CHAT_PROVIDER', 'telegram'),
        'support_telegram_url': getattr(settings, 'SUPPORT_TELEGRAM_URL', ''),
        'support_max_url': getattr(settings, 'SUPPORT_MAX_URL', ''),
    }


def _context(user) -> dict:
    return {
        'legacy_user': user, 'is_admin_user': _is_admin(user),
        'can_open_gis': _can_open_gis(user),
        'can_view_farmlands': _can_view_farmlands(user),
        'year': datetime.now().year,
        **_support_chat(),
    }


def legacy_user(request):
    if getattr(request, _CACHE_LOADED, False):
        return _context(getattr(request, _CACHE_ATTR, None))

    legacy_user_id = request.session.get('legacy_user_id')
    user = LegacyUser.objects.filter(pk=legacy_user_id).first() if legacy_user_id else None

    setattr(request, _CACHE_ATTR, user)
    setattr(request, _CACHE_LOADED, True)
    return _context(user)
