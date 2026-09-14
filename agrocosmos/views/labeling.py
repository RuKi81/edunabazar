"""Инструмент разметки обучающей выборки озимые/яровые (карта-разметчик).

Страница ``label/`` (админ-only) поверх карты угодий: эксперт кликает
угодье, смотрит профиль NDVI и фенологические фичи, ставит ИСТИННЫЙ класс.
Метки копятся в :class:`FarmlandTrainingLabel` и затем используются командой
``classify_winter_spring --reference-labels`` для калибровки порогов.

Active learning: по умолчанию отдаём «сомнительные» угодья — с пиком NDVI
у порога дня пика или с низкой уверенностью, где модель чаще ошибается.
"""
import json
from datetime import date

from django.db import connection
from django.db.models import Count
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from ..models import Farmland, FarmlandTrainingLabel, Region
from ._helpers import rate_limit
from .pages import _get_legacy_user
from .tiles import _is_admin_legacy

# Класс сезона, вокруг которого ведём разметку (пашня/сенокос).
_LABELABLE_CROP_TYPES = ('arable', 'hayfield')
_VALID_TRUE_CLASSES = {c.value for c in FarmlandTrainingLabel.TrueClass}

# Дефолты active learning: «сомнительное» угодье.
_DEFAULT_AMBIG_CONF = 0.60      # уверенность ниже — кандидат
_DEFAULT_AMBIG_DAYS = 12        # |peak_doy - порог| ≤ — кандидат
_DEFAULT_PEAK_THRESHOLD = 185   # фолбэк, если прогон был без калибровки
_MAX_CANDIDATES = 600


def label_dashboard(request: HttpRequest) -> HttpResponse:
    """Страница-разметчик обучающей выборки (только для админа)."""
    if not _is_admin_legacy(request):
        return render(request, 'agrocosmos/label.html', {
            'forbidden': True, 'legacy_user': _get_legacy_user(request),
            'regions': Region.objects.none(), 'active_page': 'label',
        }, status=403)

    regions = Region.objects.only('id', 'name', 'code')
    current_year = date.today().year
    return render(request, 'agrocosmos/label.html', {
        'forbidden': False,
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'region_id': request.GET.get('region') or '',
        'year': request.GET.get('year') or str(current_year),
        'years': list(range(current_year, current_year - 6, -1)),
        'active_page': 'label',
    })


def _labeler_username(request) -> str:
    user = getattr(request, 'legacy_user', None) or _get_legacy_user(request)
    return (getattr(user, 'username', '') or '')[:150]


@rate_limit('120/m')
@require_http_methods(['GET'])
def api_label_candidates(request: HttpRequest) -> JsonResponse:
    """Список угодий-кандидатов для разметки (GeoJSON-подобный).

    Параметры: ``region`` (обяз.), ``year``, ``source`` (fused|raster),
    ``mode`` (ambiguous|all), ``unlabeled`` (1 — скрыть уже размеченные),
    ``conf``/``days`` — пороги «сомнительности», ``limit``.
    """
    if not _is_admin_legacy(request):
        return JsonResponse({'ok': False, 'error': 'forbidden'}, status=403)

    region_id = request.GET.get('region')
    if not region_id or not str(region_id).isdigit():
        return JsonResponse({'ok': False, 'error': 'region required'}, status=400)
    try:
        year = int(request.GET.get('year') or 0)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year'}, status=400)

    source = request.GET.get('source') or 'fused'
    if source not in ('fused', 'raster'):
        source = 'fused'
    mode = request.GET.get('mode') or 'ambiguous'
    unlabeled = request.GET.get('unlabeled') in ('1', 'true', 'yes')
    conf = _safe_float(request.GET.get('conf'), _DEFAULT_AMBIG_CONF)
    days = _safe_float(request.GET.get('days'), _DEFAULT_AMBIG_DAYS)
    limit = min(_safe_int(request.GET.get('limit'), _MAX_CANDIDATES),
                _MAX_CANDIDATES)

    ct_ph = ', '.join(['%s'] * len(_LABELABLE_CROP_TYPES))
    where = [
        'cs.year = %s', 'cs.source = %s',
        f'f.crop_type IN ({ct_ph})',
        'f.district_id IN (SELECT id FROM agro_district WHERE region_id = %s)',
        "cs.season_class IN ('winter', 'spring')",
        'cs.peak_doy IS NOT NULL',
    ]
    params = [year, source, *_LABELABLE_CROP_TYPES, int(region_id)]

    if mode == 'ambiguous':
        where.append(
            '(cs.confidence < %s OR '
            'abs(cs.peak_doy - COALESCE(cs.peak_doy_threshold, %s)) <= %s)'
        )
        params += [conf, _DEFAULT_PEAK_THRESHOLD, days]
    if unlabeled:
        where.append('tl.id IS NULL')

    params.append(limit)
    sql = f"""
        SELECT f.id, f.area_ha, f.cadastral_number, f.crop_type,
               d.name AS district_name,
               cs.season_class, cs.confidence, cs.peak_doy,
               cs.peak_doy_threshold, cs.sos_doy, cs.early_spring_ndvi,
               cs.harvest_doy, cs.is_harvested,
               tl.true_class AS label,
               ST_AsGeoJSON(
                   ST_SimplifyPreserveTopology(
                       ST_Transform(f.geom, 4326), 0.0001), 6) AS geojson
        FROM agro_farmland_crop_season cs
        JOIN agro_farmland f ON f.id = cs.farmland_id
        LEFT JOIN agro_district d ON d.id = f.district_id
        LEFT JOIN agro_farmland_training_label tl
               ON tl.farmland_id = f.id AND tl.year = cs.year
        WHERE {' AND '.join(where)}
        ORDER BY abs(cs.peak_doy - COALESCE(cs.peak_doy_threshold, {int(_DEFAULT_PEAK_THRESHOLD)})) ASC,
                 cs.confidence ASC
        LIMIT %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    candidates = []
    for r in rows:
        geojson = r[14]
        if not geojson:
            continue
        candidates.append({
            'farmland_id': r[0],
            'area_ha': _round_or_none(r[1], 2),
            'cadastral': r[2] or '',
            'crop_type': r[3],
            'district': r[4] or '',
            'predicted_class': r[5],
            'confidence': _round_or_none(r[6]),
            'peak_doy': r[7],
            'peak_doy_threshold': _round_or_none(r[8], 1),
            'sos_doy': r[9],
            'early_spring_ndvi': _round_or_none(r[10]),
            'harvest_doy': r[11],
            'is_harvested': bool(r[12]),
            'label': r[13],
            'geometry': json.loads(geojson),
        })
    return JsonResponse({
        'ok': True, 'year': year, 'source': source, 'mode': mode,
        'count': len(candidates), 'candidates': candidates,
    })


@csrf_exempt
@require_http_methods(['POST', 'DELETE'])
def api_label_save(request: HttpRequest) -> JsonResponse:
    """POST — поставить/обновить метку; DELETE — снять метку."""
    if not _is_admin_legacy(request):
        return JsonResponse({'ok': False, 'error': 'forbidden'}, status=403)
    try:
        payload = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid json'}, status=400)

    fid = payload.get('farmland_id')
    year = payload.get('year')
    if not isinstance(fid, int) or not isinstance(year, int):
        return JsonResponse(
            {'ok': False, 'error': 'farmland_id and year (int) required'},
            status=400)
    if not Farmland.objects.filter(pk=fid).exists():
        return JsonResponse({'ok': False, 'error': 'farmland not found'},
                            status=404)

    if request.method == 'DELETE':
        FarmlandTrainingLabel.objects.filter(farmland_id=fid, year=year).delete()
        return JsonResponse({'ok': True, 'deleted': True})

    true_class = payload.get('true_class')
    if true_class not in _VALID_TRUE_CLASSES:
        return JsonResponse(
            {'ok': False, 'error': f'invalid true_class: {true_class}'},
            status=400)

    obj, created = FarmlandTrainingLabel.objects.update_or_create(
        farmland_id=fid, year=year,
        defaults={
            'true_class': true_class,
            'note': (payload.get('note') or '')[:255],
            'labeled_by': _labeler_username(request),
        },
    )
    return JsonResponse({
        'ok': True, 'created': created,
        'label': {'farmland_id': fid, 'year': year, 'true_class': true_class},
    })


@rate_limit('120/m')
@require_http_methods(['GET'])
def api_label_stats(request: HttpRequest) -> JsonResponse:
    """Счётчики набранных меток по классам за год (опц. по субъекту)."""
    if not _is_admin_legacy(request):
        return JsonResponse({'ok': False, 'error': 'forbidden'}, status=403)
    try:
        year = int(request.GET.get('year') or 0)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year'}, status=400)

    qs = FarmlandTrainingLabel.objects.filter(year=year)
    region_id = request.GET.get('region')
    if region_id and str(region_id).isdigit():
        qs = qs.filter(farmland__district__region_id=int(region_id))

    rows = qs.values('true_class').annotate(n=Count('id'))
    stats = {c.value: 0 for c in FarmlandTrainingLabel.TrueClass}
    for r in rows:
        stats[r['true_class']] = r['n']
    stats['total'] = sum(stats.values())
    return JsonResponse({'ok': True, 'year': year, 'stats': stats})


def _round_or_none(raw, precision=3):
    """Округление с сохранением ``None``.

    В отличие от ``_safe_round`` (даёт 0.0 для ``None``) здесь важно
    различать «нет данных» и «ноль»: разметчик показывает «—» вместо
    ложного нуля в фичах.
    """
    if raw is None:
        return None
    try:
        return round(float(raw), precision)
    except (TypeError, ValueError):
        return None


def _safe_float(raw, default):
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _safe_int(raw, default):
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default
