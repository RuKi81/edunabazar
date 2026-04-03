"""
Legacy auth middleware.

Attaches ``request.legacy_user`` (LegacyUser | None) on every request
so views no longer need to call ``_get_current_legacy_user()`` manually.

Also provides the ``@legacy_login_required`` decorator for views that
require an authenticated legacy user.
"""

import urllib.parse
from functools import wraps

from django.http import HttpRequest
from django.shortcuts import redirect

from .models import LegacyUser


class LegacyUserMiddleware:
    """
    Populate ``request.legacy_user`` from the session on every request.

    Must be placed **after** ``SessionMiddleware`` in MIDDLEWARE.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest):
        legacy_user_id = request.session.get('legacy_user_id')
        if legacy_user_id:
            request.legacy_user = (
                LegacyUser.objects.filter(pk=legacy_user_id).first()
            )
        else:
            request.legacy_user = None

        # Keep backward-compat cache attrs used by _get_current_legacy_user
        request._cached_legacy_user = request.legacy_user
        request._cached_legacy_user_loaded = True

        return self.get_response(request)


def legacy_login_required(view_func=None, *, login_url='/login/'):
    """
    Decorator for views that require an authenticated legacy user.

    Usage::

        @legacy_login_required
        def my_view(request):
            user = request.legacy_user  # guaranteed not None
            ...

        @legacy_login_required(login_url='/custom-login/')
        def other_view(request):
            ...
    """

    def decorator(func):
        @wraps(func)
        def _wrapped(request, *args, **kwargs):
            if getattr(request, 'legacy_user', None) is None:
                next_url = urllib.parse.quote(request.get_full_path())
                return redirect(f'{login_url}?next={next_url}')
            return func(request, *args, **kwargs)
        return _wrapped

    if view_func is not None:
        # Called as @legacy_login_required (without parens)
        return decorator(view_func)
    # Called as @legacy_login_required(...) (with parens)
    return decorator
