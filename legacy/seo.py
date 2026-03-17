"""
SEO helpers: robots.txt, sitemap.xml, and context processors for meta tags.
"""

from django.http import HttpRequest, HttpResponse
from django.utils import timezone

from .models import Advert, Catalog, Categories


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
    ]
    return HttpResponse('\n'.join(lines), content_type='text/plain; charset=utf-8')


def sitemap_xml(request: HttpRequest) -> HttpResponse:
    now = timezone.now().strftime('%Y-%m-%d')

    urls = []

    # Static pages
    urls.append(_url(SITE_URL + '/', now, 'daily', '1.0'))
    urls.append(_url(SITE_URL + '/adverts/', now, 'hourly', '0.9'))
    urls.append(_url(SITE_URL + '/map/', now, 'daily', '0.8'))
    urls.append(_url(SITE_URL + '/sellers/', now, 'daily', '0.6'))

    # Adverts (published only)
    for a in Advert.objects.filter(status=10).order_by('-updated_at')[:5000]:
        lastmod = a.updated_at.strftime('%Y-%m-%d') if a.updated_at else now
        urls.append(_url(f'{SITE_URL}/adverts/{a.id}/', lastmod, 'weekly', '0.7'))

    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    xml += '\n'.join(urls)
    xml += '\n</urlset>\n'

    return HttpResponse(xml, content_type='application/xml; charset=utf-8')


def _url(loc: str, lastmod: str, changefreq: str, priority: str) -> str:
    return (
        '  <url>\n'
        f'    <loc>{loc}</loc>\n'
        f'    <lastmod>{lastmod}</lastmod>\n'
        f'    <changefreq>{changefreq}</changefreq>\n'
        f'    <priority>{priority}</priority>\n'
        '  </url>'
    )
