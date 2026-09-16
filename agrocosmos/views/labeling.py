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
from django.db.models import Count, Max, Min
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from ..models import (
    Farmland, FarmlandCropSeason, FarmlandPhenology, FarmlandTrainingLabel,
    Region, VegetationIndex,
)
from ._helpers import rate_limit
from .pages import _get_legacy_user
from .reports import _unused_signals
from .tiles import _is_admin_legacy

# Класс сезона, вокруг которого ведём разметку (пашня/сенокос).
_LABELABLE_CROP_TYPES = ('arable', 'hayfield')
_VALID_TRUE_CLASSES = {c.value for c in FarmlandTrainingLabel.TrueClass}

# Дефолты active learning: «сомнительное» угодье.
_DEFAULT_AMBIG_CONF = 0.60      # уверенность ниже — кандидат
_DEFAULT_AMBIG_DAYS = 12        # |peak_doy - порог| ≤ — кандидат
_DEFAULT_PEAK_THRESHOLD = 185   # фолбэк, если прогон был без калибровки
_MAX_CANDIDATES = 600

# Классы предсказания, доступные как пул кандидатов. ``unknown`` не входит:
# там не хватило наблюдений, проверять нечего. По умолчанию — озимые/яровые:
# именно этих эталонов мало для калибровки.
_CANDIDATE_CLASSES = ('winter', 'spring', 'hayfield', 'unused')
_DEFAULT_CANDIDATE_CLASSES = ('winter', 'spring')

# Порядок выдачи кандидатов:
# * ``ambiguity``  — сначала спорные (пик у порога), active learning:
#   максимум информации на метку, но разметчику труднее;
# * ``confidence`` — сначала уверенные: быстрое набивание объёма
#   (подтвердить очевидное одним нажатием);
# * ``area``       — сначала крупные: лучше видно на снимке и больше веса
#   в площадных сводках.
_ORDER_SQL = {
    'ambiguity': (
        'abs(cs.peak_doy - COALESCE(cs.peak_doy_threshold, '
        f'{int(_DEFAULT_PEAK_THRESHOLD)})) ASC NULLS LAST, cs.confidence ASC'
    ),
    'confidence': 'cs.confidence DESC, f.area_ha DESC NULLS LAST',
    'area': 'f.area_ha DESC NULLS LAST',
}

# Классы, напрямую сопоставимые с предсказанием классификатора. Подклассы
# зарастания (ДКР/сорная) сворачиваются в ``unused``:
# модель тип зарастания не предсказывает.
_COMPARABLE_CLASSES = ('winter', 'spring', 'unused')
# Сколько id расхождений отдавать для перехода к ним в разметчике.
_MAX_MISMATCH_IDS = 50


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

    Параметры: ``region`` (обяз.), ``district`` (сузить до района), ``year``,
    ``source`` (fused|raster), ``mode`` (ambiguous|all), ``unlabeled``
    (1 — скрыть уже размеченные), ``conf``/``days`` — пороги
    «сомнительности», ``classes`` — какие предсказанные классы брать в пул
    (csv из :data:`_CANDIDATE_CLASSES`), ``order`` — порядок выдачи
    (:data:`_ORDER_SQL`), ``hide_unused_like`` (1 — выкинуть угодья с
    NDVI-сигналами неиспользования), ``limit``.

    ``hide_unused_like`` — главный ускоритель набора эталонов озимых/яровых:
    в очередь не попадает залежь, на которой разметчик тратит время впустую.
    Фильтр считается по тем же сигналам, что в скрининге неиспользования, и
    применяется ПОСЛЕ SQL, поэтому при включённом флаге выбираем с запасом
    и обрезаем до ``limit`` уже после отсева.
    """
    if not _is_admin_legacy(request):
        return JsonResponse({'ok': False, 'error': 'forbidden'}, status=403)

    opts = _parse_candidates_request(request)
    if 'error' in opts:
        return JsonResponse({'ok': False, 'error': opts['error']}, status=400)

    year = opts['year']
    limit = opts['limit']
    hide_unused_like = opts['hide_unused_like']
    where, params = _candidates_where(opts)

    # С запасом: отсев залежи идёт после SQL и «съедает» часть строк.
    fetch_limit = (min(limit * 3, _MAX_CANDIDATES) if hide_unused_like
                   else limit)
    params.append(fetch_limit)
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
        ORDER BY {_ORDER_SQL[opts['order']]}
        LIMIT %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    candidates = [_row_to_candidate(r) for r in rows if r[14]]

    skipped_unused_like = 0
    if hide_unused_like and candidates:
        kept = _drop_unused_like(candidates, year)
        skipped_unused_like = len(candidates) - len(kept)
        candidates = kept
    candidates = candidates[:limit]

    return JsonResponse({
        'ok': True, 'year': year, 'source': opts['source'],
        'mode': opts['mode'], 'district': opts['district_id'],
        'classes': list(opts['classes']), 'order': opts['order'],
        'skipped_unused_like': skipped_unused_like,
        'count': len(candidates), 'candidates': candidates,
    })


def _parse_candidates_request(request) -> dict:
    """Разобрать и провалидировать query-параметры списка кандидатов.

    Возвращает словарь опций либо ``{'error': ...}`` при плохих параметрах.
    """
    region_id = request.GET.get('region')
    if not region_id or not str(region_id).isdigit():
        return {'error': 'region required'}
    try:
        year = int(request.GET.get('year') or 0)
    except (TypeError, ValueError):
        return {'error': 'invalid year'}

    source = request.GET.get('source') or 'fused'
    if source not in ('fused', 'raster'):
        source = 'fused'
    order = request.GET.get('order') or 'ambiguity'
    if order not in _ORDER_SQL:
        order = 'ambiguity'
    district_id = request.GET.get('district')
    district_id = int(district_id) if str(district_id or '').isdigit() else None
    return {
        'region_id': int(region_id),
        'year': year,
        'source': source,
        'order': order,
        'district_id': district_id,
        'mode': request.GET.get('mode') or 'ambiguous',
        'unlabeled': request.GET.get('unlabeled') in ('1', 'true', 'yes'),
        'hide_unused_like': (
            request.GET.get('hide_unused_like') in ('1', 'true', 'yes')
        ),
        'conf': _safe_float(request.GET.get('conf'), _DEFAULT_AMBIG_CONF),
        'days': _safe_float(request.GET.get('days'), _DEFAULT_AMBIG_DAYS),
        'limit': min(_safe_int(request.GET.get('limit'), _MAX_CANDIDATES),
                     _MAX_CANDIDATES),
        'classes': _parse_candidate_classes(request.GET.get('classes')),
    }


def _candidates_where(opts: dict):
    """Собрать список WHERE-условий и параметры запроса кандидатов."""
    classes = opts['classes']
    ct_ph = ', '.join(['%s'] * len(_LABELABLE_CROP_TYPES))
    cls_ph = ', '.join(['%s'] * len(classes))
    where = ['cs.year = %s', 'cs.source = %s', f'f.crop_type IN ({ct_ph})']
    params = [opts['year'], opts['source'], *_LABELABLE_CROP_TYPES]

    if opts['district_id'] is not None:
        where.append('f.district_id = %s')
        params.append(opts['district_id'])
    else:
        where.append(
            'f.district_id IN (SELECT id FROM agro_district WHERE region_id = %s)'
        )
        params.append(opts['region_id'])

    where.append(f'cs.season_class IN ({cls_ph})')
    params += list(classes)

    if opts['mode'] == 'ambiguous':
        where.append(
            '(cs.confidence < %s OR '
            'abs(cs.peak_doy - COALESCE(cs.peak_doy_threshold, %s)) <= %s)'
        )
        params += [opts['conf'], _DEFAULT_PEAK_THRESHOLD, opts['days']]
    if opts['unlabeled']:
        where.append('tl.id IS NULL')
    return where, params


def _row_to_candidate(r) -> dict:
    """Строка SQL-выдачи кандидатов → словарь для JSON-ответа."""
    return {
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
        'geometry': json.loads(r[14]),
    }


def _parse_candidate_classes(raw):
    """csv предсказанных классов → валидный кортеж (или дефолт)."""
    if not raw:
        return _DEFAULT_CANDIDATE_CLASSES
    picked = tuple(
        c for c in (p.strip() for p in str(raw).split(','))
        if c in _CANDIDATE_CLASSES
    )
    return picked or _DEFAULT_CANDIDATE_CLASSES


def _drop_unused_like(candidates, year):
    """Выкинуть кандидатов с NDVI-сигналами неиспользования (залежь).

    Использует те же сигналы, что скрининг неиспользования
    (:func:`agrocosmos.views.reports._unused_signals`): нет сомкнутого
    покрова за сезон либо нет вегетационного цикла. Угодья, по которым
    наблюдений недостаточно (``signals is None``), НЕ выкидываем — их всё
    равно можно разметить визуально по снимку.
    """
    ids = [c['farmland_id'] for c in candidates]
    stats = _season_stats_for(ids, year)
    sos_ids = _sos_ids_for(ids, year)
    kept = []
    for c in candidates:
        fid = c['farmland_id']
        signals = _unused_signals(stats.get(fid), fid in sos_ids)
        if signals:
            continue
        kept.append(c)
    return kept


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


@rate_limit('60/m')
@require_http_methods(['GET'])
def api_label_agreement(request: HttpRequest) -> JsonResponse:
    """Кросс-проверка ручных меток за год (опц. по субъекту).

    Две независимые сверки:

    1. ``model`` — матрица ошибок метка×предсказание
       :class:`FarmlandCropSeason` (по ``source``) и точность по
       сопоставимым классам (озимые/яровые/не обрабатывается).
    2. ``unused_check`` — сверка с NDVI-скринингом неиспользования по тем
       же порогам, что в отчёте ``api_report_unused`` (макс. NDVI,
       амплитуда, наличие SOS): метки «не обрабатывается» должны иметь
       сигналы неиспользования, метки культур — не иметь. Расхождения
       показывают либо ошибку разметчика, либо дыру в данных.
    """
    if not _is_admin_legacy(request):
        return JsonResponse({'ok': False, 'error': 'forbidden'}, status=403)
    try:
        year = int(request.GET.get('year') or 0)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year'}, status=400)

    source = request.GET.get('source') or 'fused'
    if source not in ('fused', 'raster'):
        source = 'fused'

    qs = FarmlandTrainingLabel.objects.filter(year=year)
    region_id = request.GET.get('region')
    if region_id and str(region_id).isdigit():
        qs = qs.filter(farmland__district__region_id=int(region_id))
    labels = dict(qs.values_list('farmland_id', 'true_class'))

    return JsonResponse({
        'ok': True, 'year': year, 'source': source,
        'model': _model_agreement(labels, year, source),
        'unused_check': _unused_agreement(labels, year),
    })


def _model_agreement(labels, year, source):
    """Матрица метка×предсказание и точность по сопоставимым классам."""
    predicted = dict(
        FarmlandCropSeason.objects.filter(
            farmland_id__in=labels.keys(), year=year, source=source,
        ).values_list('farmland_id', 'season_class')
    )
    matrix, agree, comparable = {}, 0, 0
    for fid, true_class in labels.items():
        pred = predicted.get(fid)
        if pred is None:
            continue
        # Матрица — по СЫРОЙ метке (видно, как модель ведёт себя
        # на ДКР и на сорняке порознь), а точность — по базовому
        # классу, иначе верная метка «ДКР» шла бы в ошибки.
        family = FarmlandTrainingLabel.family(true_class)
        matrix.setdefault(true_class, {})
        matrix[true_class][pred] = matrix[true_class].get(pred, 0) + 1
        if family in _COMPARABLE_CLASSES:
            comparable += 1
            if family == pred:
                agree += 1
    return {
        'labeled': len(labels),
        'with_model': len(predicted),
        'comparable': comparable,
        'agree': agree,
        'accuracy': round(agree / comparable, 3) if comparable else None,
        'matrix': matrix,
    }


def _unused_agreement(labels, year):
    """Сверка меток с NDVI-сигналами неиспользования (как в скрининге).

    ``confirmed`` — метка «не обрабатывается» и сигналы есть;
    ``contradicted`` — метка «не обрабатывается», но виден покров/цикл;
    ``suspicious_crop`` — метка культуры/сенокоса, но сигналы
    неиспользования есть (вероятная ошибка разметки);
    ``no_data`` — наблюдений меньше ``UNUSED_MIN_OBS``.
    """
    ids = list(labels.keys())
    stats = _season_stats_for(ids, year)
    sos_ids = _sos_ids_for(ids, year)

    out = {'checked': 0, 'confirmed': 0, 'contradicted': 0,
           'suspicious_crop': 0, 'no_data': 0,
           'contradicted_ids': [], 'suspicious_crop_ids': []}
    for fid, true_class in labels.items():
        if true_class == FarmlandTrainingLabel.TrueClass.IGNORE:
            continue
        signals = _unused_signals(stats.get(fid), fid in sos_ids)
        if signals is None:
            out['no_data'] += 1
            continue
        out['checked'] += 1
        if true_class in FarmlandTrainingLabel.UNUSED_CLASSES:
            if signals:
                out['confirmed'] += 1
            else:
                out['contradicted'] += 1
                out['contradicted_ids'].append(fid)
        elif signals:
            out['suspicious_crop'] += 1
            out['suspicious_crop_ids'].append(fid)
    for key in ('contradicted_ids', 'suspicious_crop_ids'):
        out[key] = sorted(out[key])[:_MAX_MISMATCH_IDS]
    return out


def _season_stats_for(farmland_ids, year):
    """farmland_id → {max, min, n_obs} NDVI за год по всем спутникам.

    Аналог ``reports._farmland_season_stats``, но с областью по списку
    угодий: меток обычно десятки, скан по району был бы избыточен.
    """
    if not farmland_ids:
        return {}
    rows = (
        VegetationIndex.objects.filter(
            farmland_id__in=farmland_ids,
            index_type='ndvi', is_outlier=False,
            mean__gte=-1, mean__lte=1,
            acquired_date__year=year,
        )
        .values('farmland_id')
        .annotate(
            max_ndvi=Max('mean'), min_ndvi=Min('mean'), n_obs=Count('id'),
        )
    )
    return {r['farmland_id']: r for r in rows}


def _sos_ids_for(farmland_ids, year):
    """Множество farmland_id с детектированным началом сезона за год."""
    if not farmland_ids:
        return set()
    return set(
        FarmlandPhenology.objects.filter(
            farmland_id__in=farmland_ids, year=year,
            sos_date__isnull=False,
        ).values_list('farmland_id', flat=True)
    )


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
