"""Shared utilities and constants for the agrocosmos views package."""
import functools
import math

from django.http import HttpResponse, JsonResponse
from django_ratelimit.decorators import ratelimit


MODIS_SATELLITES = ('modis_terra', 'modis_aqua')
RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')
FUSED_SATELLITES = ('hls_fused',)


def rate_limit(rate, binary=False):
    """IP-based rate limiting for public API endpoints.

    Args:
        rate: ``'<n>/<period>'`` e.g. ``'60/m'`` or ``'300/m'``.
        binary: if True, return an empty ``HttpResponse`` on 429 (suitable for
            tile endpoints consumed by map libraries). Otherwise return a
            JSON body.

    Uses the configured Django cache (Redis in production, LocMem in dev),
    so the limit is shared across gunicorn workers behind Redis and purely
    per-worker otherwise.
    """
    def decorator(view_func):
        @ratelimit(key='ip', rate=rate, block=False)
        @functools.wraps(view_func)
        def wrapper(request, *args, **kwargs):
            if getattr(request, 'limited', False):
                if binary:
                    return HttpResponse(status=429)
                return JsonResponse(
                    {'ok': False, 'error': 'rate limit exceeded', 'rate': rate},
                    status=429,
                )
            return view_func(request, *args, **kwargs)
        return wrapper
    return decorator


def _satellite_filter(source):
    """Return a dict suitable for ``.filter(**...)`` on VegetationIndex queryset."""
    if source == 'modis':
        return {'scene__satellite__in': MODIS_SATELLITES}
    if source == 'raster':
        return {'scene__satellite__in': RASTER_SATELLITES}
    if source == 'fused':
        return {'scene__satellite__in': FUSED_SATELLITES}
    return {}


def _safe_round(val, precision=4):
    """Round a float safely, returning 0 for None/NaN/Inf."""
    if val is None:
        return 0.0
    try:
        if math.isnan(val) or math.isinf(val):
            return 0.0
    except TypeError:
        return 0.0
    return round(val, precision)
