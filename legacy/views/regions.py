"""
Regional landing pages — one page per Russian region,
filtered by PostGIS bounding box.
"""

from django.contrib.gis.geos import Polygon
from django.core.paginator import Paginator
from django.db.models import Prefetch
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import render

from ..constants import ADVERT_STATUS_PUBLISHED
from ..models import Advert, AdvertPhoto, Catalog, Categories
from ..regions import REGIONS, REGIONS_LIST
from .helpers import _get_current_legacy_user, _is_admin_user


def region_detail(request: HttpRequest, region_slug: str) -> HttpResponse:
    region = REGIONS.get(region_slug)
    if region is None:
        raise Http404

    min_lon, min_lat, max_lon, max_lat = region['bbox']
    bbox_poly = Polygon.from_bbox((min_lon, min_lat, max_lon, max_lat))
    bbox_poly.srid = 4326

    _thumb_prefetch = Prefetch(
        'photos',
        queryset=AdvertPhoto.objects.order_by('sort', 'id'),
        to_attr='prefetched_photos',
    )

    qs = (
        Advert.objects
        .filter(status=ADVERT_STATUS_PUBLISHED, location__within=bbox_poly)
        .select_related('category__catalog', 'author')
        .prefetch_related(_thumb_prefetch)
        .order_by('-updated_at', '-id')
    )

    # Category counts for sidebar
    category_counts = {}
    for cat_id, cat_title in (
        qs.values_list('category_id', 'category__title')
        .distinct()
        .order_by('category__title')
    ):
        cnt = qs.filter(category_id=cat_id).count()
        if cnt > 0:
            category_counts[cat_title] = cnt

    paginator = Paginator(qs, 24)
    adverts_page = paginator.get_page(request.GET.get('page') or 1)
    page_range = paginator.get_elided_page_range(adverts_page.number)

    legacy_user = _get_current_legacy_user(request)

    return render(
        request,
        'legacy/region_detail.html',
        {
            'region': region,
            'adverts': adverts_page,
            'page_range': page_range,
            'total_count': paginator.count,
            'category_counts': category_counts,
            'legacy_user': legacy_user,
            'is_admin_user': _is_admin_user(legacy_user),
            'regions_list': REGIONS_LIST,
        },
    )


def region_list(request: HttpRequest) -> HttpResponse:
    legacy_user = _get_current_legacy_user(request)
    return render(
        request,
        'legacy/region_list.html',
        {
            'regions': REGIONS_LIST,
            'legacy_user': legacy_user,
        },
    )
