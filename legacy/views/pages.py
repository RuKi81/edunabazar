import json

from django.core.cache import cache
from django.core.paginator import Paginator
from django.db.models import Avg, Min, Max, Count
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, render

from ..models import Advert, Catalog, Categories, News
from ..cache_utils import get_generation, HOME_PREFIX, HOME_TIMEOUT
from ..constants import ADVERT_STATUS_MODERATION, ADVERT_STATUS_PUBLISHED
from .helpers import _get_current_legacy_user


def about(request: HttpRequest) -> HttpResponse:
    return render(request, 'legacy/about.html')


def contacts(request: HttpRequest) -> HttpResponse:
    return render(request, 'legacy/contacts.html')


def howto(request: HttpRequest) -> HttpResponse:
    return render(request, 'legacy/howto.html')


_PRICE_CATALOG_NAMES = [
    'Продукция с/х, сырье',
    'Продукты переработки',
    'Корма для с.х. животных и птиц',
    'Агрохимия',
]


def prices_page(request: HttpRequest) -> HttpResponse:
    catalogs_qs = Catalog.objects.filter(active=1, title__in=_PRICE_CATALOG_NAMES).order_by('sort', 'title')
    catalog_list = list(catalogs_qs)
    catalog_ids = [c.id for c in catalog_list]

    stats = (
        Advert.objects
        .filter(
            category__catalog_id__in=catalog_ids,
            price__gt=0,
            status__in=[ADVERT_STATUS_MODERATION, ADVERT_STATUS_PUBLISHED],
            deleted_at__isnull=True,
        )
        .values('category__title', 'category__catalog__title')
        .annotate(
            avg_price=Avg('price'),
            min_price=Min('price'),
            max_price=Max('price'),
            cnt=Count('id'),
        )
        .filter(cnt__gte=2)
        .order_by('category__catalog__title', '-cnt')
    )

    charts_data = {}
    for cat in catalog_list:
        charts_data[cat.title] = {
            'labels': [],
            'avg': [],
            'min': [],
            'max': [],
            'counts': [],
        }

    for row in stats:
        catalog_title = row['category__catalog__title']
        if catalog_title not in charts_data:
            continue
        d = charts_data[catalog_title]
        label = row['category__title']
        if len(label) > 25:
            label = label[:23] + '…'
        d['labels'].append(label)
        d['avg'].append(round(row['avg_price'], 1))
        d['min'].append(round(row['min_price'], 1))
        d['max'].append(round(row['max_price'], 1))
        d['counts'].append(row['cnt'])

    charts_json = json.dumps(charts_data, ensure_ascii=False)

    total_adverts = sum(sum(d['counts']) for d in charts_data.values())
    total_categories = sum(len(d['labels']) for d in charts_data.values())

    resp = render(
        request,
        'legacy/prices.html',
        {
            'legacy_user': _get_current_legacy_user(request),
            'charts_json': charts_json,
            'catalog_names': [c.title for c in catalog_list],
            'total_adverts': total_adverts,
            'total_categories': total_categories,
        },
    )
    return resp


def news_detail(request: HttpRequest, news_id: int) -> HttpResponse:
    item = get_object_or_404(News, pk=news_id, is_active=True)
    return render(request, 'legacy/news_detail.html', {'news': item})


def home(request: HttpRequest) -> HttpResponse:
    news_page_num = request.GET.get('news_page', 1)
    gen = get_generation('home')
    cache_key = f'{HOME_PREFIX}{gen}:p{news_page_num}'
    cached = cache.get(cache_key)
    if cached is not None:
        return HttpResponse(cached, content_type='text/html; charset=utf-8')

    news_qs = News.objects.filter(is_active=True)
    news_paginator = Paginator(news_qs, 3)
    news_page = news_paginator.get_page(news_page_num)
    resp = render(
        request,
        'legacy/home.html',
        {
            'catalogs': Catalog.objects.filter(active=1).order_by('sort', 'title', 'id'),
            'categories': Categories.objects.filter(active=1).select_related('catalog').order_by('title'),
            'news_page': news_page,
        },
    )
    cache.set(cache_key, resp.content.decode('utf-8'), HOME_TIMEOUT)
    return resp
