from decimal import Decimal, InvalidOperation

from django import template
from django.templatetags.static import static

register = template.Library()


@register.simple_tag(takes_context=True)
def query_update(context, **kwargs):
    request = context.get('request')
    if not request:
        return ''

    q = request.GET.copy()
    for k, v in kwargs.items():
        if v is None or v == '':
            q.pop(k, None)
        else:
            q[k] = str(v)
    encoded = q.urlencode()
    return ('?' + encoded) if encoded else ''


@register.filter
def format_price(value):
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return value

    s = f"{d:,.2f}"
    return s.replace(",", " ")


@register.filter
def truncate_ellipsis(value, length=300):
    if value is None:
        return ''

    try:
        length_int = int(length)
    except (TypeError, ValueError):
        length_int = 300

    s = str(value)
    if length_int <= 0:
        return ''

    if len(s) <= length_int:
        return s

    return s[:length_int].rstrip() + '...'


@register.filter
def pick_thumb_url(title):
    t = (title or '').lower()
    rules = [
        (('пшениц', 'ячмен', 'кукуруз', 'овёс', 'рож'), 'legacy/images/main1.jpg'),
        (('подсолнеч', 'рапс', 'соя'), 'legacy/images/main4.jpg'),
        (('картоф', 'лук', 'морков', 'капуст'), 'legacy/images/img-big.jpg'),
        (('яблок', 'груш', 'ягод', 'виноград'), 'legacy/images/main2.jpg'),
        (('молок', 'сыр', 'творог', 'йогурт'), 'legacy/images/main3.jpg'),
        (('мёд', 'мед'), 'legacy/images/main2.jpg'),
        (('мяс', 'говядин', 'свинин', 'куриц', 'яйц'), 'legacy/images/main4.jpg'),
        (('мук', 'круп', 'отруб'), 'legacy/images/img-big.jpg'),
    ]
    for keywords, path in rules:
        for kw in keywords:
            if kw in t:
                return static(path)
    return static('legacy/images/no_photo_102_109.jpg')
