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


def legacy_user(request):
    if getattr(request, _CACHE_LOADED, False):
        user = getattr(request, _CACHE_ATTR, None)
        return {'legacy_user': user, 'is_admin_user': _is_admin(user), 'year': datetime.now().year}

    legacy_user_id = request.session.get('legacy_user_id')
    user = LegacyUser.objects.filter(pk=legacy_user_id).first() if legacy_user_id else None

    setattr(request, _CACHE_ATTR, user)
    setattr(request, _CACHE_LOADED, True)
    return {'legacy_user': user, 'is_admin_user': _is_admin(user), 'year': datetime.now().year}
