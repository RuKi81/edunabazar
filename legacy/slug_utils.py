"""
Russian-to-Latin transliteration and slug utilities for SEO-friendly URLs.
"""

import re
from django.core.cache import cache

_TRANSLIT = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
}

_RE_NON_ALNUM = re.compile(r'[^a-z0-9]+')


def slugify_ru(text: str) -> str:
    """Transliterate Russian text to a URL-safe slug."""
    text = text.lower().strip()
    result = []
    for ch in text:
        if ch in _TRANSLIT:
            result.append(_TRANSLIT[ch])
        else:
            result.append(ch)
    slug = ''.join(result)
    slug = _RE_NON_ALNUM.sub('-', slug).strip('-')
    return slug or 'item'


_CACHE_KEY = 'catalog_category_slug_map'
_CACHE_TIMEOUT = 3600


def _build_slug_map() -> dict:
    """Build bidirectional slug map: {catalog_slug: catalog_id, ...} and {(catalog_slug, category_slug): category_id}."""
    from .models import Catalog, Categories

    data = {
        'catalog_by_slug': {},
        'catalog_by_id': {},
        'category_by_slug': {},
        'category_by_id': {},
    }

    for cat in Catalog.objects.filter(active=1).order_by('sort'):
        slug = slugify_ru(cat.title)
        data['catalog_by_slug'][slug] = cat.id
        data['catalog_by_id'][cat.id] = slug

    for c in Categories.objects.filter(active=1).select_related('catalog'):
        catalog_slug = data['catalog_by_id'].get(c.catalog_id, '')
        cat_slug = slugify_ru(c.title)
        key = (catalog_slug, cat_slug)
        data['category_by_slug'][key] = c.id
        data['category_by_id'][c.id] = (catalog_slug, cat_slug)

    return data


def get_slug_map() -> dict:
    """Get cached slug map."""
    data = cache.get(_CACHE_KEY)
    if data is None:
        data = _build_slug_map()
        cache.set(_CACHE_KEY, data, _CACHE_TIMEOUT)
    return data


def invalidate_slug_map():
    """Call when catalogs or categories change."""
    cache.delete(_CACHE_KEY)
