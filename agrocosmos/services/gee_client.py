"""
Wrapper around ``ee.data.computePixels`` that adds three things:

1. **Rate-limiter** (Redis-backed via Django cache) — prevents us from
   exceeding a configurable per-minute budget.  Earth Engine has no
   public quota endpoint, so we self-police.

2. **Retries with exponential backoff** for transient errors that look
   like quota/throttling (HTTP 429, "resource exhausted",
   "user memory limit", "computation timed out" — the last one often
   means we're being shaped).

3. **Daily metrics** in ``agro_gee_api_metric`` — call count, error
   count, throttled count, bytes downloaded.  Surfaced in the admin
   panel so we can spot a quota crunch early.

All other code paths (``services/gee_download.py``,
``services/satellite_modis_raster.py``) should route
``ee.data.computePixels`` through :func:`call_compute_pixels`.
"""
from __future__ import annotations

import logging
import time
from datetime import date

import ee
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import F
from django.utils import timezone

logger = logging.getLogger(__name__)


# Defaults; overridable via Django settings or env vars (see settings.py).
DEFAULT_CALLS_PER_MINUTE = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE_SEC = 2.0
DEFAULT_RATE_WAIT_SEC = 1.0        # how long to sleep when over budget
DEFAULT_RATE_MAX_WAIT_SEC = 30.0   # never sleep longer than this

# Substrings (lower-cased) that classify an exception as "throttled"
# rather than "errored".  We retry these; we raise "errored" immediately.
_THROTTLE_MARKERS = (
    '429',
    'resource exhausted',
    'quota',
    'rate limit',
    'rate_limit',
    'rate-limit',
    'user memory limit',
    'computation timed out',
    'too many requests',
)


class GeeRateLimitExceeded(Exception):
    """Raised when max retries are exhausted on throttling errors."""


def _cfg(name: str, default):
    return getattr(settings, name, default)


def _is_throttle_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(m in msg for m in _THROTTLE_MARKERS)


# ---------------------------------------------------------------------------
# Rate limiting (Redis-backed via Django cache)
# ---------------------------------------------------------------------------

def _acquire_slot():
    """Block until we're under the per-minute budget.

    Uses a cache key per wall-clock minute; increments atomically via
    ``cache.incr``.  Works with django-redis; on LocMemCache (dev) it
    still functions per worker process.
    """
    budget = _cfg('GEE_CALLS_PER_MINUTE', DEFAULT_CALLS_PER_MINUTE)
    wait = _cfg('GEE_RATE_WAIT_SEC', DEFAULT_RATE_WAIT_SEC)
    max_wait = _cfg('GEE_RATE_MAX_WAIT_SEC', DEFAULT_RATE_MAX_WAIT_SEC)

    waited = 0.0
    while True:
        minute = timezone.now().strftime('%Y%m%d%H%M')
        key = f'gee:rate:{minute}'
        # `add` only sets if missing (atomic), then `incr` bumps counter.
        cache.add(key, 0, timeout=90)
        try:
            n = cache.incr(key)
        except ValueError:
            # Key evicted between add() and incr(); retry loop.
            continue
        if n <= budget:
            return
        if waited >= max_wait:
            # Give up waiting and let the call through anyway; GEE will
            # respond with 429 and our retry logic handles it.  Safer
            # than blocking forever under cache outage.
            logger.warning(
                'GEE rate waiter: budget=%d/min exceeded for %.0fs, proceeding.',
                budget, waited,
            )
            return
        time.sleep(wait)
        waited += wait


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _bump(field: str, amount: int = 1, *, last_error: str | None = None) -> None:
    """Atomic F()-based increment on today's row.  Creates row if missing.

    Swallows any DB error — metrics must never break data pipelines.
    """
    from agrocosmos.models import GeeApiMetric  # local import to avoid cycle

    try:
        with transaction.atomic():
            obj, _ = GeeApiMetric.objects.get_or_create(day=date.today())
            update_kwargs = {field: F(field) + amount}
            if last_error is not None:
                update_kwargs['last_error'] = last_error[:2000]
            GeeApiMetric.objects.filter(pk=obj.pk).update(**update_kwargs)
    except Exception:
        logger.exception('Failed to record GEE metric (%s+=%s)', field, amount)


# ---------------------------------------------------------------------------
# Public: call_compute_pixels
# ---------------------------------------------------------------------------

def call_compute_pixels(params: dict) -> bytes:
    """Invoke ``ee.data.computePixels`` with rate limiting and retries.

    Returns raw response bytes (GeoTIFF).  Raises whatever EE raises on
    non-throttle errors, or :class:`GeeRateLimitExceeded` when retries
    on throttling errors are exhausted.
    """
    max_retries = _cfg('GEE_MAX_RETRIES', DEFAULT_MAX_RETRIES)
    base = _cfg('GEE_BACKOFF_BASE_SEC', DEFAULT_BACKOFF_BASE_SEC)

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        _acquire_slot()
        try:
            content = ee.data.computePixels(params)
            _bump('calls')
            _bump('bytes_downloaded', len(content) if content else 0)
            return content
        except Exception as exc:
            last_exc = exc
            if _is_throttle_error(exc):
                _bump('throttled', last_error=f'{type(exc).__name__}: {exc}')
                if attempt < max_retries:
                    sleep_for = base * (2 ** attempt)
                    logger.warning(
                        'GEE throttle (attempt %d/%d): %s — sleeping %.1fs',
                        attempt + 1, max_retries + 1, exc, sleep_for,
                    )
                    time.sleep(sleep_for)
                    continue
                # Retries exhausted.
                raise GeeRateLimitExceeded(
                    f'GEE throttling after {max_retries + 1} attempts: {exc}'
                ) from exc
            # Non-throttle error: count + re-raise immediately.
            _bump('errors', last_error=f'{type(exc).__name__}: {exc}')
            raise

    # Unreachable — loop either returns or raises.
    assert last_exc is not None
    raise last_exc
