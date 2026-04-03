"""
SEO helpers: robots.txt, sitemap.xml, healthcheck, and context processors for meta tags.
"""

import json

from django.core.cache import cache
from django.db import connection
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.utils import timezone

from .models import Advert, News, Seller
from .cache_utils import SITEMAP_KEY, SITEMAP_TIMEOUT, TURBO_RSS_KEY, TURBO_RSS_TIMEOUT

SITEMAP_ADVERTS_KEY = 'sitemap_adverts_xml'
SITEMAP_SELLERS_KEY = 'sitemap_sellers_xml'
SITEMAP_NEWS_KEY = 'sitemap_news_xml'
SITEMAP_STATIC_KEY = 'sitemap_static_xml'


SITE_URL = 'https://edunabazar.ru'


YANDEX_VERIFICATION = (
    '<html>\n'
    '    <head>\n'
    '        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">\n'
    '    </head>\n'
    '    <body>Verification: 3d52c6e8c3e0f2e0</body>\n'
    '</html>'
)


def yandex_verification(request: HttpRequest) -> HttpResponse:
    return HttpResponse(YANDEX_VERIFICATION, content_type='text/html; charset=utf-8')


def robots_txt(request: HttpRequest) -> HttpResponse:
    lines = [
        'User-agent: *',
        'Allow: /',
        '',
        'Disallow: /admin/',
        'Disallow: /api/',
        'Disallow: /legacy-admin/',
        'Disallow: /login/',
        'Disallow: /register/',
        'Disallow: /set-password/',
        'Disallow: /logout/',
        'Disallow: /change-password/',
        'Disallow: /me/',
        'Disallow: /messages/',
        '',
        f'Host: edunabazar.ru',
        f'Sitemap: {SITE_URL}/sitemap.xml',
        f'Sitemap: {SITE_URL}/turbo-rss.xml',
    ]
    return HttpResponse('\n'.join(lines), content_type='text/plain; charset=utf-8')


def sitemap_xml(request: HttpRequest) -> HttpResponse:
    """Sitemap index pointing to sub-sitemaps."""
    cached = cache.get(SITEMAP_KEY)
    if cached is not None:
        return HttpResponse(cached, content_type='application/xml; charset=utf-8')

    now = timezone.now().strftime('%Y-%m-%dT%H:%M:%S+03:00')
    subs = [
        f'{SITE_URL}/sitemap-static.xml',
        f'{SITE_URL}/sitemap-adverts.xml',
        f'{SITE_URL}/sitemap-sellers.xml',
        f'{SITE_URL}/sitemap-news.xml',
    ]
    entries = []
    for loc in subs:
        entries.append(
            f'  <sitemap>\n'
            f'    <loc>{loc}</loc>\n'
            f'    <lastmod>{now}</lastmod>\n'
            f'  </sitemap>'
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + '\n'.join(entries)
        + '\n</sitemapindex>\n'
    )
    cache.set(SITEMAP_KEY, xml, SITEMAP_TIMEOUT)
    return HttpResponse(xml, content_type='application/xml; charset=utf-8')


def sitemap_static_xml(request: HttpRequest) -> HttpResponse:
    cached = cache.get(SITEMAP_STATIC_KEY)
    if cached is not None:
        return HttpResponse(cached, content_type='application/xml; charset=utf-8')

    from .slug_utils import get_slug_map

    now = timezone.now().strftime('%Y-%m-%d')
    urls = [
        _url(SITE_URL + '/', now, 'daily', '1.0'),
        _url(SITE_URL + '/adverts/', now, 'hourly', '0.9'),
        _url(SITE_URL + '/map/', now, 'daily', '0.8'),
        _url(SITE_URL + '/sellers/', now, 'daily', '0.6'),
        _url(SITE_URL + '/prices/', now, 'daily', '0.6'),
        _url(SITE_URL + '/about/', now, 'monthly', '0.5'),
        _url(SITE_URL + '/contacts/', now, 'monthly', '0.5'),
        _url(SITE_URL + '/howto/', now, 'monthly', '0.5'),
    ]

    slug_map = get_slug_map()
    for cat_slug in slug_map['catalog_by_slug']:
        urls.append(_url(f'{SITE_URL}/adverts/{cat_slug}/', now, 'daily', '0.8'))
    for (cat_slug, categ_slug) in slug_map['category_by_slug']:
        urls.append(_url(f'{SITE_URL}/adverts/{cat_slug}/{categ_slug}/', now, 'daily', '0.7'))

    xml = _wrap_urlset(urls)
    cache.set(SITEMAP_STATIC_KEY, xml, SITEMAP_TIMEOUT)
    return HttpResponse(xml, content_type='application/xml; charset=utf-8')


def sitemap_adverts_xml(request: HttpRequest) -> HttpResponse:
    cached = cache.get(SITEMAP_ADVERTS_KEY)
    if cached is not None:
        return HttpResponse(cached, content_type='application/xml; charset=utf-8')

    now = timezone.now().strftime('%Y-%m-%d')
    urls = []
    for a in Advert.objects.filter(status=10).order_by('-updated_at')[:5000]:
        lastmod = a.updated_at.strftime('%Y-%m-%d') if a.updated_at else now
        urls.append(_url(f'{SITE_URL}/adverts/{a.id}/', lastmod, 'weekly', '0.7'))

    xml = _wrap_urlset(urls)
    cache.set(SITEMAP_ADVERTS_KEY, xml, SITEMAP_TIMEOUT)
    return HttpResponse(xml, content_type='application/xml; charset=utf-8')


def sitemap_sellers_xml(request: HttpRequest) -> HttpResponse:
    cached = cache.get(SITEMAP_SELLERS_KEY)
    if cached is not None:
        return HttpResponse(cached, content_type='application/xml; charset=utf-8')

    now = timezone.now().strftime('%Y-%m-%d')
    urls = []
    for s in Seller.objects.filter(status=10).order_by('-updated_at')[:2000]:
        lastmod = s.updated_at.strftime('%Y-%m-%d') if s.updated_at else now
        urls.append(_url(f'{SITE_URL}/sellers/{s.id}/', lastmod, 'weekly', '0.6'))

    xml = _wrap_urlset(urls)
    cache.set(SITEMAP_SELLERS_KEY, xml, SITEMAP_TIMEOUT)
    return HttpResponse(xml, content_type='application/xml; charset=utf-8')


def sitemap_news_xml(request: HttpRequest) -> HttpResponse:
    cached = cache.get(SITEMAP_NEWS_KEY)
    if cached is not None:
        return HttpResponse(cached, content_type='application/xml; charset=utf-8')

    now = timezone.now().strftime('%Y-%m-%d')
    urls = []
    for n in News.objects.filter(is_active=True).order_by('-published_at')[:1000]:
        lastmod = n.published_at.strftime('%Y-%m-%d') if n.published_at else now
        urls.append(_url(f'{SITE_URL}/news/{n.id}/', lastmod, 'monthly', '0.5'))

    xml = _wrap_urlset(urls)
    cache.set(SITEMAP_NEWS_KEY, xml, SITEMAP_TIMEOUT)
    return HttpResponse(xml, content_type='application/xml; charset=utf-8')


def turbo_rss(request: HttpRequest) -> HttpResponse:
    """
    Yandex Turbo Pages RSS feed.
    https://yandex.ru/dev/turbo/doc/rss/markup.html
    """
    cached = cache.get(TURBO_RSS_KEY)
    if cached is not None:
        return HttpResponse(cached, content_type='application/rss+xml; charset=utf-8')

    from html import escape

    items = []

    # Published adverts (latest 1000)
    for a in (Advert.objects
              .filter(status=10)
              .select_related('category__catalog', 'author')
              .order_by('-updated_at')[:1000]):
        title = escape(a.title or '')
        link = f'{SITE_URL}/adverts/{a.id}/'
        pub_date = a.updated_at.strftime('%a, %d %b %Y %H:%M:%S +0300') if a.updated_at else ''
        author = escape(getattr(a.author, 'name', '') or getattr(a.author, 'username', '') or '')
        text = escape((a.text or '')[:5000])
        price_html = ''
        if a.price:
            price_html = f'<p><strong>Цена:</strong> {escape(str(a.price))} ₽</p>'
        category = ''
        cat = getattr(a, 'category', None)
        if cat:
            catalog = getattr(cat, 'catalog', None)
            parts = []
            if catalog:
                parts.append(escape(catalog.title or ''))
            parts.append(escape(cat.title or ''))
            category = ' / '.join(parts)

        breadcrumbs = (
            f'<div data-block="breadcrumblist">'
            f'<a href="{SITE_URL}/">Главная</a>'
            f'<a href="{SITE_URL}/adverts/">Объявления</a>'
        )
        if category:
            breadcrumbs += f'<a>{escape(category)}</a>'
        breadcrumbs += f'<a href="{link}">{title}</a></div>'

        content = (
            f'<header><h1>{title}</h1></header>'
            f'{breadcrumbs}'
            f'{price_html}'
            f'<p>{text}</p>'
        )
        if author:
            content += f'<p><strong>Контакт:</strong> {author}</p>'

        items.append(
            f'    <item turbo="true">\n'
            f'      <title>{title}</title>\n'
            f'      <link>{link}</link>\n'
            f'      <pubDate>{pub_date}</pubDate>\n'
            f'      <turbo:content><![CDATA[{content}]]></turbo:content>\n'
            f'    </item>'
        )

    # News (latest 200)
    for n in News.objects.filter(is_active=True).order_by('-published_at')[:200]:
        title = escape(n.title or '')
        link = f'{SITE_URL}/news/{n.id}/'
        pub_date = n.published_at.strftime('%a, %d %b %Y 00:00:00 +0300') if n.published_at else ''
        text = escape((n.text or '')[:5000])
        source = escape(n.source_name or '')

        content = (
            f'<header><h1>{title}</h1></header>'
            f'<div data-block="breadcrumblist">'
            f'<a href="{SITE_URL}/">Главная</a>'
            f'<a href="{link}">{title}</a></div>'
            f'<p>{text}</p>'
        )
        if source:
            content += f'<p><em>Источник: {source}</em></p>'

        items.append(
            f'    <item turbo="true">\n'
            f'      <title>{title}</title>\n'
            f'      <link>{link}</link>\n'
            f'      <pubDate>{pub_date}</pubDate>\n'
            f'      <turbo:content><![CDATA[{content}]]></turbo:content>\n'
            f'    </item>'
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss xmlns:yandex="http://news.yandex.ru" xmlns:media="http://search.yahoo.com/mrss/" '
        'xmlns:turbo="http://turbo.yandex.ru" version="2.0">\n'
        '  <channel>\n'
        f'    <title>Еду на базар</title>\n'
        f'    <link>{SITE_URL}</link>\n'
        '    <description>Доска объявлений сельскохозяйственной продукции, техники и услуг</description>\n'
        '    <language>ru</language>\n'
    )
    xml += '\n'.join(items)
    xml += '\n  </channel>\n</rss>\n'

    cache.set(TURBO_RSS_KEY, xml, TURBO_RSS_TIMEOUT)
    return HttpResponse(xml, content_type='application/rss+xml; charset=utf-8')


def healthcheck(request: HttpRequest) -> JsonResponse:
    """Lightweight health endpoint for uptime monitoring."""
    checks = {'app': 'ok'}
    status_code = 200
    try:
        with connection.cursor() as cur:
            cur.execute('SELECT 1')
        checks['db'] = 'ok'
    except Exception as e:
        checks['db'] = str(e)
        status_code = 503
    try:
        checks['adverts_count'] = Advert.objects.filter(status=10).count()
    except Exception:
        checks['adverts_count'] = None
    return JsonResponse(checks, status=status_code)


def _wrap_urlset(urls: list) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + '\n'.join(urls)
        + '\n</urlset>\n'
    )


def _url(loc: str, lastmod: str, changefreq: str, priority: str) -> str:
    return (
        '  <url>\n'
        f'    <loc>{loc}</loc>\n'
        f'    <lastmod>{lastmod}</lastmod>\n'
        f'    <changefreq>{changefreq}</changefreq>\n'
        f'    <priority>{priority}</priority>\n'
        '  </url>'
    )
