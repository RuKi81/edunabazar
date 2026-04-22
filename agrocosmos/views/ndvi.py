"""NDVI data endpoints: single-farmland series, aggregated stats, phenology,
and the list of available raster composites for the raster dashboard."""
from collections import defaultdict
from datetime import date, timedelta

from django.db.models import Avg, Count, Sum, Value, CharField
from django.db.models.functions import Coalesce, Extract
from django.db.models.fields.json import KeyTextTransform
from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import cache_page

from ..models import (
    Farmland, FarmlandPhenology, NdviBaseline, VegetationIndex,
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
    region_id = request.GET.get('region')
    if not region_id:
        return JsonResponse({'ok': False, 'error': 'region required'}, status=400)

    try:
        region_id = int(region_id)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid region'}, status=400)

    district_id = request.GET.get('district')
    year = request.GET.get('year')
    date_from = request.GET.get('date_from')
    date_to = request.GET.get('date_to')
    crop_types = request.GET.get('crop_types')  # comma-separated, e.g. 'arable,hayfield'
    fact_isp_filter = request.GET.get('fact_isp')  # 'used', 'unused', or empty for all
    source = request.GET.get('source')  # 'modis', 'raster', or empty
    sat_kw = _satellite_filter(source)

    # Base queryset
    fl_qs = Farmland.objects.filter(district__region_id=region_id)
    if district_id:
        try:
            fl_qs = fl_qs.filter(district_id=int(district_id))
        except (TypeError, ValueError):
            pass

    # Farmland summary (before crop_types filter, for Сводка)
    fl_summary = (
        fl_qs
        .values('crop_type')
        .annotate(
            count=Count('id'),
            total_area=Sum('area_ha'),
        )
        .order_by('crop_type')
    )

    # Usage (Fact_isp) summary
    usage_summary_qs = (
        fl_qs
        .annotate(fi=Coalesce(KeyTextTransform('Fact_isp', 'properties'), Value(''), output_field=CharField()))
        .values('fi')
        .annotate(count=Count('id'), total_area=Sum('area_ha'))
        .order_by('fi')
    )
    usage_summary = []
    for row in usage_summary_qs:
        usage_summary.append({
            'fact_isp': row['fi'],
            'count': row['count'],
            'area_ha': round(row['total_area'] or 0, 1),
        })

    # Apply crop_types filter
    if crop_types:
        ct_list = [ct.strip() for ct in crop_types.split(',') if ct.strip()]
        if ct_list:
            fl_qs = fl_qs.filter(crop_type__in=ct_list)

    # Apply fact_isp filter
    if fact_isp_filter == 'used':
        fl_qs = fl_qs.filter(properties__Fact_isp='Используется')
    elif fact_isp_filter == 'unused':
        fl_qs = fl_qs.filter(properties__Fact_isp='Не используется')

    vi_qs = VegetationIndex.objects.filter(
        farmland__in=fl_qs, index_type='ndvi',
        mean__gte=-0.2, mean__lte=1,         # physical NDVI range
        is_outlier=False,                     # exclude detected spikes (snow/cloud)
        **sat_kw,
    )
    if year:
        try:
            vi_qs = vi_qs.filter(acquired_date__year=int(year))
        except (TypeError, ValueError):
            pass
    if date_from:
        vi_qs = vi_qs.filter(acquired_date__gte=date_from)
    if date_to:
        vi_qs = vi_qs.filter(acquired_date__lte=date_to)

    crop_labels = dict(Farmland.CropType.choices)

    # --- Single-pass aggregation ---
    # The four aggregates below (by_crop, by_period, with_ndvi, global avg)
    # all scan the same joined VegetationIndex + Farmland rows. For a large
    # region-year this join can be millions of rows, and running it four times
    # at the DB level is pointlessly slow. We instead fetch the raw tuples
    # once and aggregate in Python — one scan, O(n) memory, same results.
    rows = vi_qs.values_list(
        'acquired_date', 'mean', 'farmland_id',
        'farmland__crop_type', 'farmland__area_ha',
    )

    by_crop_acc = defaultdict(lambda: {'farmlands': set(), 'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    by_period_acc = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0, 'count': 0})
    global_ndvi_area = 0.0
    global_area = 0.0
    farmland_ids_with_ndvi = set()

    for acq_date, mean_v, fl_id, crop_type, area_ha in rows.iterator(chunk_size=5000):
        if mean_v is None or area_ha is None:
            continue
        area_f = float(area_ha)
        ndvi_area = float(mean_v) * area_f

        c = by_crop_acc[crop_type]
        c['farmlands'].add(fl_id)
        c['sum_ndvi_area'] += ndvi_area
        c['sum_area'] += area_f

        p = by_period_acc[acq_date]
        p['sum_ndvi_area'] += ndvi_area
        p['sum_area'] += area_f
        p['count'] += 1

        global_ndvi_area += ndvi_area
        global_area += area_f
        farmland_ids_with_ndvi.add(fl_id)

    # Materialise by_crop_list (sorted by mean_ndvi desc, same as before)
    by_crop_list = []
    for ct in sorted(by_crop_acc.keys()):
        acc = by_crop_acc[ct]
        s_area = acc['sum_area']
        weighted = (acc['sum_ndvi_area'] / s_area) if s_area else None
        by_crop_list.append({
            'crop_type': ct,
            'label': crop_labels.get(ct, ct),
            'count': len(acc['farmlands']),
            'mean_ndvi': _safe_round(weighted),
        })
    by_crop_list.sort(key=lambda r: r['mean_ndvi'] or 0, reverse=True)

    # Materialise by_period_list (chronological order, same as before)
    by_period_list = []
    for acq_date in sorted(by_period_acc.keys()):
        acc = by_period_acc[acq_date]
        s_area = acc['sum_area']
        weighted = (acc['sum_ndvi_area'] / s_area) if s_area else None
        by_period_list.append({
            'date': str(acq_date),
            'mean_ndvi': _safe_round(weighted),
            'count': acc['count'],
        })

    # Summary (area-weighted)
    total_fl = fl_qs.count()
    with_ndvi = len(farmland_ids_with_ndvi)
    avg = (global_ndvi_area / global_area) if global_area else None

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

    # Baseline (historical average across all prior years)
    baseline_qs = NdviBaseline.objects.filter(
        district__region_id=region_id,
        crop_type='',  # aggregated across all crop types
    )
    if district_id:
        try:
            baseline_qs = baseline_qs.filter(district_id=int(district_id))
        except (TypeError, ValueError):
            pass

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
        # Convert day-of-year to MM-DD
        try:
            d = date(2024, 1, 1) + timedelta(days=doy - 1)
            mm_dd = d.strftime('%m-%d')
        except Exception:
            mm_dd = f'{doy:03d}'
        bl_mean = row['mean_ndvi'] or 0
        bl_std = row['std_ndvi'] or 0
        baseline_list.append({
            'date': mm_dd,
            'mean_ndvi': _safe_round(bl_mean),
            'std_ndvi': _safe_round(bl_std),
        })
        baseline_lookup[doy] = (bl_mean, bl_std)

    # Enrich by_period with z-score relative to baseline
    for item in by_period_list:
        try:
            d = date.fromisoformat(item['date'])
            doy = d.timetuple().tm_yday
            bl_mean, bl_std = baseline_lookup.get(doy, (None, None))
            if bl_mean is not None and bl_std and bl_std > 0.01 and item['mean_ndvi'] is not None:
                item['z_score'] = _safe_round((item['mean_ndvi'] - bl_mean) / bl_std)
            else:
                item['z_score'] = None
        except Exception:
            item['z_score'] = None

    # For MODIS 16-day composites: expose the end date of the last chunk
    # so the frontend can draw a dashed "coverage" extension line.
    # mid_date = chunk_start + 7 days, chunk_end = chunk_start + 15 = mid + 8
    last_period_end = None
    if source == 'modis' and by_period_list:
        try:
            last_mid = date.fromisoformat(by_period_list[-1]['date'])
            last_period_end = str(last_mid + timedelta(days=8))
        except Exception:
            pass

    return JsonResponse({
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
    })


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
