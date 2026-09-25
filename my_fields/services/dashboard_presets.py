"""Пресеты дашбордов: нормализация параметров, ссылка, письмо со сводкой.

Страница «Дашборды» (`/me/gis/dashboards/`) держит своё состояние в
query-string, поэтому «сохранённый отчёт» — это ровно тот же набор
параметров, что и в ссылке: слой, поле группировки, разрез, регион/район
и флаг малых диаграмм. Здесь эти параметры валидируются (поля — только из
``layer.attributes``, посторонние ключи отбрасываются), собираются в
абсолютную ссылку и разворачиваются в HTML-письмо.

Письмо отправляется СЕРВЕРНОЙ пересборкой сводки (тем же
``services.layer_summary.summary_by_field``), а не тем, что прислал
браузер: содержимое отчёта не должно зависеть от доверия к клиенту.

PDF намеренно не генерируется на сервере: это потребовало бы новой
зависимости (weasyprint/wkhtmltopdf) ради того, что браузер умеет сам —
страница печатается через ``window.print()`` с печатной таблицей стилей
(см. ``@media print`` в шаблоне dashboards.html).
"""
from __future__ import annotations

import logging
from urllib.parse import urlencode

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.core.validators import validate_email
from django.core.exceptions import ValidationError
from django.urls import reverse
from django.utils.html import escape

logger = logging.getLogger(__name__)

# Ключи, которые вообще могут храниться в пресете и попадать в ссылку.
PARAM_KEYS = ('region', 'district', 'group', 'group2', 'split', 'bysplit',
              'group_values', 'group2_values', 'split_values')

# Писем за один запрос: защита от превращения кнопки «отправить» в рассыльщик.
MAX_RECIPIENTS = 5

# В письме таблицу обрезаем: сводка может содержать до MAX_GROUPS категорий,
# а письмо должно остаться читаемым (полная таблица — по ссылке).
MAX_EMAIL_ROWS = 40
MAX_EMAIL_SPLITS = 8


class DashboardParamsError(ValueError):
    """Некорректные параметры пресета (текст пригоден для показа в UI)."""


def _int_or_none(value):
    try:
        out = int(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def normalize_params(layer, raw: dict | None) -> dict:
    """Привести параметры отчёта к каноническому виду.

    Args:
        layer: :class:`my_fields.models.GisLayer` — по его ``attributes``
            проверяются ``group``/``split``.
        raw: словарь от клиента (лишние ключи игнорируются).

    Returns:
        словарь по :data:`PARAM_KEYS`; ``region``/``district`` — ``int`` либо
        ``None``, ``group2``/``split`` — ``str`` либо ``None`` (повтор поля
        группировки = отсутствие второго уровня/разреза).

    Raises:
        DashboardParamsError: нет ``group`` либо поле не является атрибутом
            слоя.
    """
    raw = raw or {}
    names = {a.get('db') for a in (layer.attributes or []) if a.get('db')}

    group = str(raw.get('group') or '').strip()
    if not group:
        raise DashboardParamsError('Укажите поле группировки.')
    if group not in names:
        raise DashboardParamsError(f'Поле группировки не найдено в слое: {group}.')

    group2 = str(raw.get('group2') or '').strip() or None
    if group2 == group:
        group2 = None
    if group2 and group2 not in names:
        raise DashboardParamsError(
            f'Поле второй группировки не найдено в слое: {group2}.')

    split = str(raw.get('split') or '').strip() or None
    if split in (group, group2):
        split = None
    if split and split not in names:
        raise DashboardParamsError(f'Поле разреза не найдено в слое: {split}.')

    # Перечни значений (чекбоксы) валидирует сама сводка — там же, где
    # они попадают в SQL. Здесь только приводим тип и снимаем дубли.
    from .layer_summary import LayerSummaryError, clean_values
    try:
        gvals = clean_values(raw.get('group_values'), 'Значения группировки')
        g2vals = clean_values(raw.get('group2_values'),
                              'Значения 2-й группировки')
        svals = clean_values(raw.get('split_values'), 'Значения разреза')
    except LayerSummaryError as exc:
        raise DashboardParamsError(str(exc)) from None

    return {
        'region': _int_or_none(raw.get('region')),
        'district': _int_or_none(raw.get('district')),
        'group': group,
        'group2': group2,
        'split': split,
        'bysplit': bool(raw.get('bysplit', True)),
        'group_values': gvals,
        # Без самого поля его фильтр бессмыслен и только мусорит ссылку.
        'group2_values': g2vals if group2 else None,
        'split_values': svals if split else None,
    }


def _site_url() -> str:
    base = (getattr(settings, 'SITE_URL', '') or '').rstrip('/')
    if base:
        return base if base.startswith('http') else f'https://{base}'
    return 'https://edunabazar.ru'


def dashboard_url(layer, params: dict) -> str:
    """Абсолютная ссылка на дашборд с этим срезом (та же, что в адресной строке)."""
    query = {'layer': layer.pk}
    if params.get('region'):
        query['region'] = params['region']
    if params.get('district'):
        query['district'] = params['district']
    query['group'] = params['group']
    if params.get('group2'):
        query['group2'] = params['group2']
    if params.get('split'):
        query['split'] = params['split']
    if not params.get('bysplit', True):
        query['bysplit'] = '0'
    # gv/g2v/sv — ПОВТОРЯЮЩИЕСЯ параметры (doseq): сами значения
    # атрибутов содержат запятые, склеить их в одну строку нельзя.
    if params.get('group_values'):
        query['gv'] = list(params['group_values'])
    if params.get('group2') and params.get('group2_values'):
        query['g2v'] = list(params['group2_values'])
    if params.get('split') and params.get('split_values'):
        query['sv'] = list(params['split_values'])
    path = reverse('my_fields:ui_gis_dashboards')
    return f'{_site_url()}{path}?{urlencode(query, doseq=True)}'


def field_label(layer, db: str) -> str:
    """Человеческое имя атрибута слоя (db-имя — техническое)."""
    for attr in (layer.attributes or []):
        if attr.get('db') == db:
            return attr.get('name') or db
    return db


def _filter_note(layer, summary: dict) -> str:
    """Подпись о выборочности отчёта (чекбоксы значений).

    Без неё получатель письма принял бы усечённую выборку за полный
    итог по слою.
    """
    parts = []
    for field_key, values_key in (('group', 'group_values'),
                                  ('group2', 'group2_values'),
                                  ('split', 'split_values')):
        field = summary.get(field_key)
        values = summary.get(values_key)
        if not field or not values:
            continue
        parts.append(f'{field_label(layer, field)} — {len(values)} знач.')
    return ('выбраны значения: ' + '; '.join(parts)) if parts else ''


def _scope_names(params: dict) -> list[str]:
    """Названия региона/района для подзаголовка письма."""
    from agrocosmos.models import District, Region

    out = []
    if params.get('region'):
        name = (Region.objects.filter(pk=params['region'])
                .values_list('name', flat=True).first())
        if name:
            out.append(name)
    if params.get('district'):
        name = (District.objects.filter(pk=params['district'])
                .values_list('name', flat=True).first())
        if name:
            out.append(name)
    return out


def _num(value, digits=1) -> str:
    """Число в русском формате (пробел разрядов, запятая дроби)."""
    text = f'{float(value or 0):,.{digits}f}'
    return text.replace(',', '\u00a0').replace('.', ',')


def _cat(value) -> str:
    """NULL/пустая строка в атрибуте — «значение не заполнено»."""
    if value is None or value == '':
        return '(не задано)'
    return str(value)


def _table_html(layer, summary: dict) -> str:
    """Таблица сводки для письма (инлайновые стили — почтовики режут CSS)."""
    polygonal = summary.get('polygonal')
    splits = (summary.get('splits') or [])[:MAX_EMAIL_SPLITS]
    rows = (summary.get('rows') or [])[:MAX_EMAIL_ROWS]

    th = ('padding:6px 10px; border-bottom:2px solid #ddd; text-align:right;'
          ' font-size:13px; color:#555;')
    td = 'padding:6px 10px; border-bottom:1px solid #eee; text-align:right;'
    left = ' text-align:left;'

    head = f'<th style="{th}{left}">{escape(field_label(layer, summary["group"]))}</th>'
    group2 = summary.get('group2')
    if group2:
        head += f'<th style="{th}{left}">{escape(field_label(layer, group2))}</th>'
    if polygonal:
        head += f'<th style="{th}">Площадь, га</th>'
    head += f'<th style="{th}">Доля</th><th style="{th}">Объектов</th>'
    for sp in splits:
        unit = ', га' if polygonal else ', об.'
        head += f'<th style="{th}">{escape(_cat(sp["value"]) + unit)}</th>'

    body = ''
    for row in rows:
        body += f'<tr><td style="{td}{left}">{escape(_cat(row["value"]))}</td>'
        if group2:
            body += f'<td style="{td}{left}">{escape(_cat(row.get("value2")))}</td>'
        if polygonal:
            body += f'<td style="{td}">{_num(row["area_ha"])}</td>'
        body += f'<td style="{td}">{_num((row.get("share") or 0) * 100)}%</td>'
        body += f'<td style="{td}">{_num(row["count"], 0)}</td>'
        for sp in splits:
            cell = (row.get('splits') or {}).get(
                '' if sp['value'] is None else str(sp['value']))
            if not cell:
                body += f'<td style="{td}">—</td>'
            else:
                body += (f'<td style="{td}">'
                         f'{_num(cell["area_ha"]) if polygonal else _num(cell["count"], 0)}'
                         '</td>')
        body += '</tr>'

    total = summary.get('total') or {}
    foot = f'<tr><td style="{td}{left}"><strong>Итого</strong></td>'
    if group2:
        foot += f'<td style="{td}"></td>'
    if polygonal:
        foot += f'<td style="{td}"><strong>{_num(total.get("area_ha"))}</strong></td>'
    foot += f'<td style="{td}">100,0%</td>'
    foot += f'<td style="{td}"><strong>{_num(total.get("count"), 0)}</strong></td>'
    for sp in splits:
        foot += (f'<td style="{td}">'
                 f'{_num(sp["area_ha"]) if polygonal else _num(sp["count"], 0)}'
                 '</td>')
    foot += '</tr>'

    note = ''
    if len(summary.get('rows') or []) > len(rows):
        note = (f'<p style="color:#888; font-size:12px;">Показаны первые '
                f'{len(rows)} категорий из {len(summary["rows"])} — '
                f'полная таблица по ссылке.</p>')

    return ('<table style="border-collapse:collapse; width:100%;'
            ' font-family:Arial,sans-serif;">'
            f'<thead><tr>{head}</tr></thead><tbody>{body}</tbody>'
            f'<tfoot>{foot}</tfoot></table>{note}')


def _text_lines(layer, summary: dict, params: dict, url: str, note: str) -> str:
    """Текстовая версия письма (обязательна: не все клиенты рисуют HTML)."""
    polygonal = summary.get('polygonal')
    total = summary.get('total') or {}
    lines = [f'Отчёт по слою «{layer.title}»']
    scope = _scope_names(params)
    if scope:
        lines.append('Территория: ' + ', '.join(scope))
    lines.append('Группировка: ' + field_label(layer, summary['group']))
    if summary.get('group2'):
        lines.append('Вторая группировка: '
                     + field_label(layer, summary['group2']))
    if summary.get('split'):
        lines.append('Разрез: ' + field_label(layer, summary['split']))
    filt = _filter_note(layer, summary)
    if filt:
        lines.append('Фильтр: ' + filt)
    lines.append(f'Объектов: {_num(total.get("count"), 0)}')
    if polygonal:
        lines.append(f'Площадь: {_num(total.get("area_ha"))} га')
    lines.append('')
    for row in (summary.get('rows') or [])[:MAX_EMAIL_ROWS]:
        measure = (f'{_num(row["area_ha"])} га' if polygonal
                   else f'{_num(row["count"], 0)} об.')
        cat = _cat(row['value'])
        if summary.get('group2'):
            cat += ' / ' + _cat(row.get('value2'))
        lines.append(f'  {cat}: {measure} '
                     f'({_num((row.get("share") or 0) * 100)}%)')
    lines.append('')
    if note:
        lines.extend([note, ''])
    lines.append(f'Открыть дашборд: {url}')
    return '\n'.join(lines)


def build_email(layer, params: dict, summary: dict, note: str = '') -> tuple:
    """Собрать ``(subject, text, html)`` письма со сводкой."""
    url = dashboard_url(layer, params)
    scope = _scope_names(params)
    subject = f'Отчёт по слою «{layer.title}»'
    if scope:
        subject += ' — ' + ', '.join(scope)

    head = [f'<h2 style="margin:0 0 4px; font-size:18px;">{escape(layer.title)}</h2>']
    sub = list(scope)
    sub.append('группировка: ' + field_label(layer, summary['group']))
    if summary.get('group2'):
        sub.append('+ ' + field_label(layer, summary['group2']))
    if summary.get('split'):
        sub.append('разрез: ' + field_label(layer, summary['split']))
    filt = _filter_note(layer, summary)
    if filt:
        sub.append(filt)
    head.append('<p style="margin:0 0 14px; color:#888; font-size:13px;">'
                + escape(' · '.join(sub)) + '</p>')
    if note:
        head.append('<p style="margin:0 0 14px; font-size:14px;">'
                    + escape(note) + '</p>')

    html = (''.join(head) + _table_html(layer, summary) +
            f'<p style="margin-top:18px;"><a href="{escape(url)}"'
            ' style="padding:8px 14px; background:#2e7d32; color:#fff;'
            ' border-radius:4px; text-decoration:none;">Открыть дашборд</a></p>')
    return subject, _text_lines(layer, summary, params, url, note), html


def clean_recipients(raw) -> list[str]:
    """Разобрать адреса (список или строка через запятую/пробел/;).

    Raises:
        DashboardParamsError: пусто, адрес невалиден или их слишком много.
    """
    if isinstance(raw, str):
        items = [x for x in raw.replace(';', ',').replace(' ', ',').split(',')]
    elif isinstance(raw, (list, tuple)):
        items = [str(x) for x in raw]
    else:
        items = []

    out = []
    for item in items:
        addr = item.strip()
        if not addr or addr in out:
            continue
        try:
            validate_email(addr)
        except ValidationError:
            raise DashboardParamsError(f'Некорректный адрес: {addr}.') from None
        out.append(addr)

    if not out:
        raise DashboardParamsError('Укажите хотя бы один адрес получателя.')
    if len(out) > MAX_RECIPIENTS:
        raise DashboardParamsError(
            f'За один раз можно отправить не более {MAX_RECIPIENTS} адресатам.')
    return out


def send_summary_email(layer, params: dict, summary: dict,
                       recipients: list[str], note: str = '') -> int:
    """Отправить сводку адресатам. Возвращает число доставленных писем.

    Каждому адресату — отдельное письмо (адреса получателей не должны
    видеть друг друга). Ошибки SMTP логируются и не валят запрос: часть
    писем могла уйти.
    """
    subject, text, html = build_email(layer, params, summary, note=note)
    delivered = 0
    for addr in recipients:
        try:
            msg = EmailMultiAlternatives(
                subject=subject, body=text,
                from_email=getattr(settings, 'DEFAULT_FROM_EMAIL', None),
                to=[addr],
            )
            msg.attach_alternative(html, 'text/html')
            msg.send(fail_silently=False)
            delivered += 1
        except Exception:  # noqa: BLE001
            logger.exception('Dashboard email failed to=%s layer=%s',
                             addr, layer.pk)
    return delivered
