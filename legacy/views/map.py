import hashlib
import json
import urllib.parse
import urllib.request
from functools import lru_cache

from django.core.cache import cache
from django.contrib.gis.geos import Polygon
from django.db.models import Prefetch
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render

from ..models import Advert, AdvertPhoto, Catalog, Categories
from ..cache_utils import (
    get_generation,
    MAP_ADVERTS_PREFIX, MAP_ADVERTS_TIMEOUT,
    MAP_CATEGORIES_KEY, MAP_CATEGORIES_TIMEOUT,
)
from ..constants import ADVERT_STATUS_DELETED, ADVERT_STATUS_PUBLISHED
from .helpers import (
    _get_current_legacy_user, _is_admin_user, _safe_localtime, _no_store,
)


def map_view(request: HttpRequest) -> HttpResponse:
    resp = render(
        request,
        'legacy/map.html',
        {
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
        },
    )
    return _no_store(resp)


def map_adverts_api(request: HttpRequest) -> JsonResponse:
    limit_raw = (request.GET.get('limit') or '5000').strip()
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 5000
    limit = max(1, min(limit, 10000))

    q = (request.GET.get('q') or '').strip()
    type_raw = (request.GET.get('type') or '').strip().lower()
    opt_raw = (request.GET.get('opt') or '').strip()
    delivery_raw = (request.GET.get('delivery') or '').strip()
    catalog_raw = (request.GET.get('catalog') or '').strip()
    category_raw = (request.GET.get('category') or '').strip()
    sort_raw = (request.GET.get('sort') or '').strip().lower()
    bbox_raw = (request.GET.get('bbox') or '').strip()

    user = _get_current_legacy_user(request)
    is_admin = _is_admin_user(user)

    # Cache for non-admin, non-search requests
    _use_cache = not is_admin and not q
    if _use_cache:
        gen = get_generation('adverts')
        raw = f'{gen}:{limit}:{type_raw}:{opt_raw}:{delivery_raw}:{catalog_raw}:{category_raw}:{sort_raw}:{bbox_raw}'
        ck = MAP_ADVERTS_PREFIX + hashlib.md5(raw.encode()).hexdigest()
        cached = cache.get(ck)
        if cached is not None:
            return JsonResponse(cached, safe=False)

    _photos_prefetch = Prefetch(
        'photos',
        queryset=AdvertPhoto.objects.order_by('sort', 'id'),
        to_attr='prefetched_photos',
    )
    qs = Advert.objects.select_related('category').prefetch_related(_photos_prefetch).exclude(status=ADVERT_STATUS_DELETED)
    if not is_admin:
        qs = qs.filter(status=ADVERT_STATUS_PUBLISHED)

    if q:
        qs = qs.extra(where=["search_vector @@ plainto_tsquery('russian', %s)"], params=[q])

    if type_raw in {'offer', 'demand'}:
        qs = qs.filter(type=0 if type_raw == 'offer' else 1)

    if opt_raw and opt_raw.strip().lower() in {'1', 'true', 'yes', 'on'}:
        qs = qs.filter(wholesale_price__gt=0)

    if delivery_raw and delivery_raw.strip().lower() in {'1', 'true', 'yes', 'on'}:
        qs = qs.filter(delivery=True)

    try:
        if category_raw:
            qs = qs.filter(category_id=int(category_raw))
    except Exception:
        pass

    try:
        if catalog_raw:
            qs = qs.filter(category__catalog_id=int(catalog_raw))
    except Exception:
        pass

    if bbox_raw:
        try:
            parts = bbox_raw.split(',')
            if len(parts) == 4:
                sw_lat, sw_lon, ne_lat, ne_lon = (float(p) for p in parts)
                bbox_poly = Polygon.from_bbox((sw_lon, sw_lat, ne_lon, ne_lat))
                bbox_poly.srid = 4326
                qs = qs.filter(location__within=bbox_poly)
        except (ValueError, TypeError):
            pass

    if sort_raw == 'price':
        qs = qs.order_by('price', '-updated_at', '-id')
    else:
        qs = qs.order_by('-updated_at', '-id')

    qs = qs[:limit]

    adverts = []
    for a in qs:
        try:
            loc = getattr(a, 'location', None)
            lat = float(loc.y) if loc else None
            lon = float(loc.x) if loc else None
        except Exception:
            lat = None
            lon = None

        text = (getattr(a, 'text', '') or '').strip()
        text_short = text
        if len(text_short) > 160:
            text_short = text_short[:160].rstrip() + '…'

        category_title = ''
        try:
            cat = getattr(a, 'category', None)
            category_title = (getattr(cat, 'title', '') or '').strip()
        except Exception:
            category_title = ''

        thumb_url = ''
        try:
            prefetched = getattr(a, 'prefetched_photos', None)
            photo = prefetched[0] if prefetched else None
            if photo:
                if getattr(photo, 'thumbnail', None) and photo.thumbnail:
                    thumb_url = getattr(photo.thumbnail, 'url', '') or ''
                elif getattr(photo, 'image', None):
                    thumb_url = getattr(photo.image, 'url', '') or ''
        except Exception:
            thumb_url = ''

        adverts.append(
            {
                'id': int(a.id),
                'title': getattr(a, 'title', '') or '',
                'lat': lat,
                'lon': lon,
                'category_id': int(a.category_id) if a.category_id is not None else None,
                'category_title': category_title,
                'price': getattr(a, 'price', None),
                'text_short': text_short,
                'url': f"/adverts/{int(a.id)}/",
                'thumb_url': thumb_url,
                'created_date': _safe_localtime(getattr(a, 'created_at', None)).strftime('%d.%m.%Y'),
                'is_opt': bool((getattr(a, 'wholesale_price', 0) or 0) > 0),
                'is_delivery': bool(getattr(a, 'delivery', False)),
            }
        )

    result = {'ok': True, 'adverts': adverts}
    if _use_cache:
        cache.set(ck, result, MAP_ADVERTS_TIMEOUT)
    return JsonResponse(result)


def map_categories_api(request: HttpRequest) -> JsonResponse:
    cached = cache.get(MAP_CATEGORIES_KEY)
    if cached is not None:
        return JsonResponse(cached, safe=False)

    limit_raw = (request.GET.get('limit') or '500').strip()
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 500
    limit = max(1, min(limit, 2000))
    qs = Categories.objects.filter(active=1).order_by('title')[:limit]
    result = {'ok': True, 'items': [{'id': int(c.id), 'title': c.title} for c in qs]}
    cache.set(MAP_CATEGORIES_KEY, result, MAP_CATEGORIES_TIMEOUT)
    return JsonResponse(result)


@lru_cache(maxsize=256)
def _geocode_ru(q: str):
    q = (q or '').strip()
    if not q:
        return None
    try:
        url = 'https://nominatim.openstreetmap.org/search?' + urllib.parse.urlencode(
            {'format': 'json', 'q': q, 'limit': 1}
        )
        req = urllib.request.Request(url, headers={'User-Agent': 'enb-legacy/1.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode('utf-8') or '[]')
        if not data:
            return None
        row = data[0]
        return float(row.get('lat')), float(row.get('lon')), (row.get('display_name') or '')
    except Exception:
        return None


def geocode_api(request: HttpRequest) -> JsonResponse:
    q = (request.GET.get('q') or '').strip()
    g = _geocode_ru(q)
    if not g:
        return JsonResponse({'ok': True, 'found': False})
    lat, lon, display_name = g
    return JsonResponse({'ok': True, 'found': True, 'lat': lat, 'lon': lon, 'display_name': display_name})


def reverse_geocode_api(request: HttpRequest) -> JsonResponse:
    lat_raw = (request.GET.get('lat') or '').strip().replace(',', '.')
    lon_raw = (request.GET.get('lon') or '').strip().replace(',', '.')
    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except Exception:
        return JsonResponse({'ok': False})
    try:
        url = 'https://nominatim.openstreetmap.org/reverse?' + urllib.parse.urlencode(
            {'format': 'json', 'lat': lat, 'lon': lon, 'zoom': 18}
        )
        req = urllib.request.Request(url, headers={'User-Agent': 'enb-legacy/1.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode('utf-8') or '{}')
        return JsonResponse({'ok': True, 'address': (data.get('display_name') or '')})
    except Exception:
        return JsonResponse({'ok': False})
