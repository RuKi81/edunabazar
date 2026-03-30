"""
Cache helpers for invalidating view caches when data changes.
"""

from django.core.cache import cache

# Cache key prefixes
MAP_ADVERTS_PREFIX = 'map_adverts:'
MAP_CATEGORIES_KEY = 'map_categories'
HOME_PREFIX = 'home:'
SITEMAP_KEY = 'sitemap_xml'
TURBO_RSS_KEY = 'turbo_rss'

# Timeouts (seconds)
MAP_ADVERTS_TIMEOUT = 60       # 1 min — map data changes often
MAP_CATEGORIES_TIMEOUT = 300   # 5 min
HOME_TIMEOUT = 120             # 2 min
SITEMAP_TIMEOUT = 3600         # 1 hour
TURBO_RSS_TIMEOUT = 3600       # 1 hour


def invalidate_advert_caches():
    """Call after advert create/edit/delete/status change."""
    cache.delete(SITEMAP_KEY)
    cache.delete(TURBO_RSS_KEY)
    cache.delete(MAP_CATEGORIES_KEY)
    # Prefix-based keys are harder to delete individually;
    # we use a generation counter instead.
    _bump_generation('adverts')


def invalidate_home_cache():
    """Call after catalog/category/news changes."""
    _bump_generation('home')


def get_generation(name: str) -> int:
    key = f'_gen:{name}'
    val = cache.get(key)
    if val is None:
        cache.set(key, 1, timeout=None)
        return 1
    return int(val)


def _bump_generation(name: str):
    key = f'_gen:{name}'
    try:
        cache.incr(key)
    except ValueError:
        cache.set(key, 1, timeout=None)
