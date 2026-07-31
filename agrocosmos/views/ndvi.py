"""NDVI data endpoints: single-farmland series, aggregated stats, phenology,
and the list of available raster composites for the raster dashboard."""
from collections import defaultdict
from datetime import date, timedelta

from django.db.models import Avg, Count, F, FloatField, Sum, Value, CharField
from django.db.models.functions import Coalesce, Extract
from django.db.models.fields.json import KeyTextTransform
from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import cache_page

from ..models import (
    DistrictNdviSeries, Farmland, FarmlandPhenology, NdviBaseline,
    VegetationIndex,
)
from ..services.ndvi_stats import (
    compute_z_score, doy_to_mmdd, modis_last_period_end, weighted_mean,
)
from ._helpers import _satellite_filter, _safe_round, rate_limit


@rate_limit('60/m')
def api_farmland_ndvi(request: HttpRequest) -> JsonResponse:
    """NDVI time series for a single farmland. Optional ?year=2025 filter."""
    farmland_id = request.GET.get('farmland')
    if not farmland_id:
        return JsonResponse({'ok': False, 'error': 'farmland required'}, status=400)
    try:
        fid = int(farmland_id)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid farmland'}, status=400)

    source = request.GET.get('source')  # 'modis', 'raster', or empty
    qs = VegetationIndex.objects.filter(
        farmland_id=fid, index_type='ndvi',
        mean__gte=-1, mean__lte=1,
        **_satellite_filter(source),
    )

    year = request.GET.get('year')
    if year:
        try:
            qs = qs.filter(acquired_date__year=int(year))
        except (TypeError, ValueError):
            pass

    rows = qs.order_by('acquired_date').values(
        'acquired_date', 'mean', 'min_val', 'max_val', 'median',
        'mean_smooth', 'is_outlier',
    )
    data = []
    for r in rows:
        data.append({
            'date': str(r['acquired_date']),
            'mean': _safe_round(r['mean']),
            'min': _safe_round(r['min_val']),
            'max': _safe_round(r['max_val']),
            'median': _safe_round(r['median']),
            'mean_smooth': (None if r['mean_smooth'] is None else _safe_round(r['mean_smooth'])),
            'is_outlier': bool(r['is_outlier']),
        })
    # last_period_end for MODIS dashed extension line
    last_period_end = None
    if source == 'modis' and data:
        try:
            last_mid = date.fromisoformat(data[-1]['date'])
            last_period_end = str(last_mid + timedelta(days=8))
        except Exception:
            pass

    return JsonResponse({'ok': True, 'data': data, 'last_period_end': last_period_end})


# --- api_ndvi_stats helpers -------------------------------------------------
# Декомпозиция эндпоинта: парсинг → выбор источника (предагрегат/сырые VI)
# → накопление сумм → сборка ответа. Поведение зафиксировано страховочными
# тестами tests/test_ndvi_stats.py.

# Fixed UI ordering requested by the product side: пашня → сенокос →
# пастбище → многолетние насаждения → залежь, then any remaining crop
# types in NDVI-desc order (same ranking as ``by_crop_list``).
_CROP_ORDER_HEAD = ('arable', 'hayfield', 'pasture', 'perennial', 'fallow')


def _int_or_none(value):
    """int(value) либо None — для необязательных числовых GET-параметров."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class _WeightedAccs:
    """Аккумуляторы одного прохода агрегации (суммы area-weighted среднего).

    Три разреза: дата, культура, культура×дата (последний — только при
    ``breakdown=crop``, чтобы общий путь оставался без лишних аллокаций)
    плюс глобальные суммы для сводки.
    """

    def __init__(self, want_crop_breakdown: bool):
        self.want_crop_breakdown = want_crop_breakdown
        self.by_period = defaultdict(
            lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0, 'count': 0})
        self.by_crop = defaultdict(
            lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
        self.by_crop_period = defaultdict(
            lambda: defaultdict(
                lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0, 'count': 0}))
        self.global_ndvi_area = 0.0
        self.global_area = 0.0

    def add(self, crop_type, acq_date, sum_w, sum_area, count):
        if not sum_area:
            return
        p = self.by_period[acq_date]
        p['sum_ndvi_area'] += sum_w
        p['sum_area'] += sum_area
        p['count'] += count
        c = self.by_crop[crop_type]
        c['sum_ndvi_area'] += sum_w
        c['sum_area'] += sum_area
        if self.want_crop_breakdown:
            cp = self.by_crop_period[crop_type][acq_date]
            cp['sum_ndvi_area'] += sum_w
            cp['sum_area'] += sum_area
            cp['count'] += count
        self.global_ndvi_area += sum_w
        self.global_area += sum_area


def _filter_date_range(qs, year, date_from, date_to):
    """Фильтры по acquired_date: год (некорректный игнорируется) и диапазон."""
    if year:
        y = _int_or_none(year)
        if y is not None:
            qs = qs.filter(acquired_date__year=y)
    if date_from:
        qs = qs.filter(acquired_date__gte=date_from)
    if date_to:
        qs = qs.filter(acquired_date__lte=date_to)
    return qs


def _filter_district(qs, district_id, field='district_id'):
    """Фильтр по району: некорректный ID молча игнорируется (как раньше)."""
    if district_id:
        did = _int_or_none(district_id)
        if did is not None:
            qs = qs.filter(**{field: did})
    return qs


def _farmland_scope(region_id, district_id, crop_types, fact_isp_filter):
    """Queryset угодий + сводки для блока «Сводка».

    ``fl_summary``/``usage_summary`` считаются ДО фильтров crop_types и
    fact_isp — сводка в сайдбаре всегда показывает полный состав
    угодий региона/района.
    """
    fl_qs = Farmland.objects.filter(district__region_id=region_id)
    fl_qs = _filter_district(fl_qs, district_id)

    fl_summary = (
        fl_qs
        .values('crop_type')
        .annotate(count=Count('id'), total_area=Sum('area_ha'))
        .order_by('crop_type')
    )

    usage_summary_qs = (
        fl_qs
        .annotate(fi=Coalesce(KeyTextTransform('Fact_isp', 'properties'), Value(''), output_field=CharField()))
        .values('fi')
        .annotate(count=Count('id'), total_area=Sum('area_ha'))
        .order_by('fi')
    )
    usage_summary = [
        {
            'fact_isp': row['fi'],
            'count': row['count'],
            'area_ha': round(row['total_area'] or 0, 1),
        }
        for row in usage_summary_qs
    ]

    ct_list: list[str] = []
    if crop_types:
        ct_list = [ct.strip() for ct in crop_types.split(',') if ct.strip()]
        if ct_list:
            fl_qs = fl_qs.filter(crop_type__in=ct_list)

    if fact_isp_filter == 'used':
        fl_qs = fl_qs.filter(properties__Fact_isp='Используется')
    elif fact_isp_filter == 'unused':
        fl_qs = fl_qs.filter(properties__Fact_isp='Не используется')

    return fl_qs, fl_summary, usage_summary, ct_list


def _series_has_data(region_id, source, year):
    """Быстрая проверка предагрегата (индекс dns_district_src_date_idx).

    Если предагрегат для региона/источника ещё не наполнен (свежий
    деплой, новый регион), эндпоинт откатывается на сырые VI —
    медленно, но корректно.
    """
    probe = DistrictNdviSeries.objects.filter(
        district__region_id=region_id, source=source,
    )
    if year:
        y = _int_or_none(year)
        if y is not None:
            probe = probe.filter(acquired_date__year=y)
    return probe.exists()


def _aggregate_series(accs, region_id, district_id, source, year,
                      date_from, date_to, ct_list):
    """Быстрый путь: суммирование предагрегата DistrictNdviSeries.

    Максимум ``districts × composites × crop_types`` строк на источник —
    даже для Московской области ~7 k строк вместо ~14 M сырых VI.
    """
    series_qs = DistrictNdviSeries.objects.filter(
        district__region_id=region_id,
        source=source,
    )
    series_qs = _filter_district(series_qs, district_id)
    series_qs = _filter_date_range(series_qs, year, date_from, date_to)
    if ct_list:
        series_qs = series_qs.filter(crop_type__in=ct_list)

    agg = (
        series_qs
        .values('acquired_date', 'crop_type')
        .annotate(
            sum_w=Sum('sum_ndvi_area'),
            sum_a=Sum('sum_area'),
            cnt=Sum('obs_count'),
        )
    )
    for r in agg.iterator(chunk_size=2000):
        accs.add(
            r['crop_type'], r['acquired_date'],
            float(r['sum_w'] or 0), float(r['sum_a'] or 0),
            int(r['cnt'] or 0),
        )


def _aggregate_raw_vi(accs, fl_qs, source, year, date_from, date_to):
    """Медленный путь: сырые VegetationIndex (единственный путь для fact_isp)."""
    vi_qs = VegetationIndex.objects.filter(
        farmland__in=fl_qs, index_type='ndvi',
        mean__gte=-0.2, mean__lte=1,         # physical NDVI range
        is_outlier=False,                     # detected spikes (snow/cloud)
        **_satellite_filter(source),
    )
    vi_qs = _filter_date_range(vi_qs, year, date_from, date_to)

    weighted_ndvi = Sum(
        F('mean') * F('farmland__area_ha'),
        output_field=FloatField(),
    )
    agg = (
        vi_qs
        .values('acquired_date', 'farmland__crop_type')
        .annotate(
            sum_w_ndvi=weighted_ndvi,
            sum_area=Sum('farmland__area_ha'),
            count=Count('id'),
        )
    )
    for r in agg.iterator(chunk_size=2000):
        accs.add(
            r['farmland__crop_type'], r['acquired_date'],
            float(r['sum_w_ndvi'] or 0), float(r['sum_area'] or 0),
            r['count'],
        )


def _build_baseline(region_id, district_id):
    """(baseline_list, doy → (mean, std)) — исторический профиль NDVI."""
    baseline_qs = NdviBaseline.objects.filter(
        district__region_id=region_id,
        crop_type='',  # aggregated across all crop types
    )
    baseline_qs = _filter_district(baseline_qs, district_id)

    baseline_agg = (
        baseline_qs
        .values('day_of_year')
        .annotate(mean_ndvi=Avg('mean_ndvi'), std_ndvi=Avg('std_ndvi'))
        .order_by('day_of_year')
    )
    baseline_list = []
    baseline_lookup = {}  # doy → (mean, std) for z-score
    for row in baseline_agg:
        doy = row['day_of_year']
        bl_mean = row['mean_ndvi'] or 0
        bl_std = row['std_ndvi'] or 0
        baseline_list.append({
            'date': doy_to_mmdd(doy),
            'mean_ndvi': _safe_round(bl_mean),
            'std_ndvi': _safe_round(bl_std),
        })
        baseline_lookup[doy] = (bl_mean, bl_std)
    return baseline_list, baseline_lookup


def _attach_z_scores(by_period_list, baseline_lookup):
    """z-score каждой точки ряда против baseline её дня года."""
    for item in by_period_list:
        try:
            doy = date.fromisoformat(item['date']).timetuple().tm_yday
            bl_mean, bl_std = baseline_lookup.get(doy, (None, None))
            item['z_score'] = compute_z_score(
                item['mean_ndvi'], bl_mean, bl_std, precision=4,
            )
        except Exception:
            item['z_score'] = None


def _build_crop_breakdown(accs, region_id, district_id, crop_labels,
                          by_crop_list, fl_summary_list, baseline_list):
    """Per-crop серии для сайдбара (``breakdown=crop``).

    Baseline берётся per-crop из NdviBaseline; при отсутствии строк для
    культуры — fallback на общий ``baseline_list`` (осмысленная линия
    «архив» ещё до пересчёта per-crop baselines).
    """
    per_crop_bl_qs = NdviBaseline.objects.filter(
        district__region_id=region_id,
        crop_type__in=list(accs.by_crop_period.keys()),
    )
    per_crop_bl_qs = _filter_district(per_crop_bl_qs, district_id)
    per_crop_bl = defaultdict(dict)  # crop_type -> {doy: (mean, std)}
    for row in (
        per_crop_bl_qs
        .values('crop_type', 'day_of_year')
        .annotate(mean_ndvi=Avg('mean_ndvi'), std_ndvi=Avg('std_ndvi'))
    ):
        per_crop_bl[row['crop_type']][row['day_of_year']] = (
            row['mean_ndvi'] or 0,
            row['std_ndvi'] or 0,
        )

    present = set(accs.by_crop_period.keys())
    ordered_crops = [ct for ct in _CROP_ORDER_HEAD if ct in present]
    for c in by_crop_list:
        ct = c['crop_type']
        if ct in present and ct not in _CROP_ORDER_HEAD:
            ordered_crops.append(ct)

    breakdown_list = []
    for ct in ordered_crops:
        dates_acc = accs.by_crop_period[ct]
        period = []
        for acq_date in sorted(dates_acc.keys()):
            acc = dates_acc[acq_date]
            period.append({
                'date': str(acq_date),
                'mean_ndvi': _safe_round(weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])),
                'count': acc['count'],
            })

        crop_bl_map = per_crop_bl.get(ct) or {}
        if crop_bl_map:
            crop_bl = [
                {
                    'date': doy_to_mmdd(doy),
                    'mean_ndvi': _safe_round(crop_bl_map[doy][0]),
                    'std_ndvi': _safe_round(crop_bl_map[doy][1]),
                }
                for doy in sorted(crop_bl_map.keys())
            ]
        else:
            crop_bl = baseline_list

        # Area/count from ``fl_summary`` (consistent with ``by_crop_list``).
        fl_row = next(
            (r for r in fl_summary_list if r['crop_type'] == ct),
            {'count': 0, 'area_ha': 0},
        )
        breakdown_list.append({
            'crop_type': ct,
            'label': crop_labels.get(ct, ct),
            'count': fl_row['count'],
            'area_ha': fl_row['area_ha'],
            'mean_ndvi': next(
                (c['mean_ndvi'] for c in by_crop_list if c['crop_type'] == ct),
                None,
            ),
            'by_period': period,
            'baseline': crop_bl,
        })
    return breakdown_list


@rate_limit('30/m')
@cache_page(60 * 5)  # 5 min Redis cache; varies on full URL (incl. query string)
def api_ndvi_stats(request: HttpRequest) -> JsonResponse:
    """
    Aggregated NDVI statistics by crop type for a region/district and period.

    Params:
        region (required): region ID
        district (optional): district ID
        year (optional): filter by year (default: all)
        date_from / date_to (optional): date range filter

    Returns:
        {ok: true, stats: {
            by_crop_type: [{crop_type, label, count, mean_ndvi, min_ndvi, max_ndvi}, ...],
            by_period: [{date, mean_ndvi, count}, ...],
            summary: {total_farmlands, with_ndvi, mean_ndvi}
        }}
    """
    region_raw = request.GET.get('region')
    if not region_raw:
        return JsonResponse({'ok': False, 'error': 'region required'}, status=400)
    region_id = _int_or_none(region_raw)
    if region_id is None:
        return JsonResponse({'ok': False, 'error': 'invalid region'}, status=400)

    district_id = request.GET.get('district')
    year = request.GET.get('year')
    date_from = request.GET.get('date_from')
    date_to = request.GET.get('date_to')
    crop_types = request.GET.get('crop_types')  # comma-separated, e.g. 'arable,hayfield'
    fact_isp_filter = request.GET.get('fact_isp')  # 'used', 'unused', or empty for all
    source = request.GET.get('source')  # 'modis', 'raster', or empty
    # ``breakdown=crop`` — additionally emit a per-crop time series, so the
    # right-sidebar can render one small chart per crop type when a
    # district is clicked. The series is built from the same accumulators
    # we already maintain for ``by_crop_type``; no extra SQL.
    want_crop_breakdown = request.GET.get('breakdown', '') == 'crop'

    fl_qs, fl_summary, usage_summary, ct_list = _farmland_scope(
        region_id, district_id, crop_types, fact_isp_filter,
    )
    crop_labels = dict(Farmland.CropType.choices)

    # Предпочитаем предагрегат; fallback на сырые VI, когда он не может
    # ответить: задан fact_isp (это измерение намеренно не
    # материализовано) либо предагрегат пуст для региона/источника.
    use_series = (
        source in ('modis', 'raster', 'fused')
        and not fact_isp_filter
        and _series_has_data(region_id, source, year)
    )

    accs = _WeightedAccs(want_crop_breakdown)
    if use_series:
        _aggregate_series(
            accs, region_id, district_id, source, year,
            date_from, date_to, ct_list,
        )
    else:
        _aggregate_raw_vi(accs, fl_qs, source, year, date_from, date_to)

    # Per-crop farmland counts: reuse the cheap ``fl_summary`` (queried over
    # the small ``agro_farmland`` table). It counts *all* farmlands of that
    # crop in the region, not strictly those with NDVI data, but for the
    # sidebar widget the approximation is acceptable and removes a second
    # heavy ``COUNT(DISTINCT farmland_id)`` over millions of VI rows.
    fl_summary_counts = {row['crop_type']: row['count'] for row in fl_summary}

    by_crop_list = []
    for ct in sorted(accs.by_crop.keys()):
        acc = accs.by_crop[ct]
        by_crop_list.append({
            'crop_type': ct,
            'label': crop_labels.get(ct, ct),
            'count': fl_summary_counts.get(ct, 0),
            'mean_ndvi': _safe_round(weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])),
        })
    by_crop_list.sort(key=lambda r: r['mean_ndvi'] or 0, reverse=True)

    by_period_list = []
    for acq_date in sorted(accs.by_period.keys()):
        acc = accs.by_period[acq_date]
        by_period_list.append({
            'date': str(acq_date),
            'mean_ndvi': _safe_round(weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])),
            'count': acc['count'],
        })

    # Summary (area-weighted). ``with_ndvi`` is the number of farmlands of
    # the queried region/year that have at least one valid NDVI sample —
    # we approximate it as the total farmland count when any data exists,
    # because computing it exactly requires ``COUNT(DISTINCT farmland_id)``
    # over millions of VI rows (the previous implementation timed out for
    # large regions like Moscow Oblast).
    total_fl = fl_qs.count()
    with_ndvi = total_fl if accs.global_area > 0 else 0
    avg = weighted_mean(accs.global_ndvi_area, accs.global_area)

    # Farmland summary by crop type
    fl_summary_list = []
    for row in fl_summary:
        ct = row['crop_type']
        fl_summary_list.append({
            'crop_type': ct,
            'label': crop_labels.get(ct, ct),
            'count': row['count'],
            'area_ha': round(row['total_area'] or 0, 1),
        })

    # Baseline (historical average across all prior years) + z-scores
    baseline_list, baseline_lookup = _build_baseline(region_id, district_id)
    _attach_z_scores(by_period_list, baseline_lookup)

    # Конец последнего 16-дневного композита — для пунктирного «хвоста»
    # покрытия на графике.
    last_period_end = (
        modis_last_period_end(by_period_list) if source == 'modis' else None
    )

    crop_breakdown_list = []
    if want_crop_breakdown and accs.by_crop_period:
        crop_breakdown_list = _build_crop_breakdown(
            accs, region_id, district_id, crop_labels,
            by_crop_list, fl_summary_list, baseline_list,
        )

    response = {
        'ok': True,
        'stats': {
            'by_crop_type': by_crop_list,
            'by_period': by_period_list,
            'baseline': baseline_list,
            'last_period_end': last_period_end,
            'summary': {
                'total_farmlands': total_fl,
                'with_ndvi': with_ndvi,
                'mean_ndvi': _safe_round(avg),
            },
            'farmland_summary': fl_summary_list,
            'usage_summary': usage_summary,
        },
    }
    if want_crop_breakdown:
        response['stats']['crop_breakdown'] = crop_breakdown_list
    return JsonResponse(response)


@rate_limit('30/m')
def api_phenology(request: HttpRequest) -> JsonResponse:
    """Phenological metrics aggregated per district or region.

    Query params:
        region: region_id (required)
        year: year (required)
        district: optional district_id
        source: 'modis' (default) or 'raster'
    """
    region_id = request.GET.get('region')
    year = request.GET.get('year')
    district_id = request.GET.get('district')
    source = request.GET.get('source', 'modis')

    if not region_id or not year:
        return JsonResponse({'ok': False, 'error': 'region and year required'}, status=400)

    qs = FarmlandPhenology.objects.filter(
        farmland__district__region_id=region_id,
        year=int(year),
        source=source,
    )
    if district_id:
        try:
            qs = qs.filter(farmland__district_id=int(district_id))
        except (TypeError, ValueError):
            pass

    agg = qs.aggregate(
        count=Count('id'),
        avg_max_ndvi=Avg('max_ndvi'),
        avg_mean_ndvi=Avg('mean_ndvi'),
        avg_los=Avg('los_days'),
        avg_ti=Avg('total_ndvi'),
    )

    # Average SOS/EOS/POS as day-of-year
    date_agg = qs.aggregate(
        avg_sos=Avg(Extract('sos_date', 'doy')),
        avg_eos=Avg(Extract('eos_date', 'doy')),
        avg_pos=Avg(Extract('pos_date', 'doy')),
    )

    def doy_to_date(doy_val, yr):
        if doy_val is None:
            return None
        try:
            d = date(int(yr), 1, 1) + timedelta(days=int(round(doy_val)) - 1)
            return d.isoformat()
        except Exception:
            return None

    # Per-district breakdown
    by_district = (
        qs.values('farmland__district_id', 'farmland__district__name')
        .annotate(
            count=Count('id'),
            avg_max_ndvi=Avg('max_ndvi'),
            avg_mean_ndvi=Avg('mean_ndvi'),
            avg_los=Avg('los_days'),
            avg_sos=Avg(Extract('sos_date', 'doy')),
            avg_eos=Avg(Extract('eos_date', 'doy')),
        )
        .order_by('farmland__district__name')
    )

    districts_list = []
    for row in by_district:
        districts_list.append({
            'district_id': row['farmland__district_id'],
            'district': row['farmland__district__name'],
            'count': row['count'],
            'avg_max_ndvi': _safe_round(row['avg_max_ndvi']),
            'avg_mean_ndvi': _safe_round(row['avg_mean_ndvi']),
            'avg_los': round(row['avg_los']) if row['avg_los'] else None,
            'avg_sos': doy_to_date(row['avg_sos'], year),
            'avg_eos': doy_to_date(row['avg_eos'], year),
        })

    return JsonResponse({
        'ok': True,
        'phenology': {
            'count': agg['count'],
            'avg_max_ndvi': _safe_round(agg['avg_max_ndvi']),
            'avg_mean_ndvi': _safe_round(agg['avg_mean_ndvi']),
            'avg_los_days': round(agg['avg_los']) if agg['avg_los'] else None,
            'avg_total_ndvi': _safe_round(agg['avg_ti']),
            'avg_sos': doy_to_date(date_agg['avg_sos'], year),
            'avg_eos': doy_to_date(date_agg['avg_eos'], year),
            'avg_pos': doy_to_date(date_agg['avg_pos'], year),
            'by_district': districts_list,
        }
    })


def api_raster_composites(request: HttpRequest) -> JsonResponse:
    """List available raster composites for a sensor/scope/year.

    Query params:
        sensor: 's2' or 'l8'
        scope: region/district scope ID
        year: '2025'
    """
    from ..services.raster_tiles import list_available_composites

    sensor = request.GET.get('sensor', 's2')
    scope = request.GET.get('scope', '')
    year = request.GET.get('year', '')

    if not scope or not year:
        return JsonResponse({'ok': False, 'error': 'scope and year required'}, status=400)

    composites = list_available_composites(sensor, scope, year)
    return JsonResponse({'ok': True, 'composites': composites})
