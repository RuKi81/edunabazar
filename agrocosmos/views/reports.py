"""Report API endpoints: region/district/country MODIS reports and the
per-farmland detailed-monitoring «field passport» report."""
import json
from collections import defaultdict
from datetime import date

from django.db.models import Avg, Count, Max, Min, Q, Sum
from django.db.models.functions import Extract
from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import cache_page

from ..models import (
    Region, District, DistrictNdviSeries, Farmland, FarmlandPhenology,
    NdviBaseline, VegetationAlert, VegetationIndex,
)
# ``ndvi_assessment`` реэкспортируется под историческим именем: хелпер
# вынесен в сервис-слой, но импортируется извне через agrocosmos.views
# (см. views/__init__.py и tests/test_ndvi_assessment.py).
from ..services.ndvi_stats import (
    compute_z_score, doy_to_date, modis_last_period_end,
    ndvi_assessment as _ndvi_assessment, weighted_mean,
)
from ._helpers import (
    FUSED_SATELLITES, MODIS_SATELLITES, RASTER_SATELLITES,
    _safe_round, rate_limit,
)


# --- shared report helpers ---------------------------------------------------

def _parse_report_params(request: HttpRequest, key: str):
    """Parse required int GET-params ``key`` and ``year``; return (id, year, error)."""
    raw_id = request.GET.get(key)
    raw_year = request.GET.get('year')
    if not raw_id or not raw_year:
        return None, None, JsonResponse(
            {'ok': False, 'error': f'{key} and year required'}, status=400,
        )
    try:
        return int(raw_id), int(raw_year), None
    except (TypeError, ValueError):
        return None, None, JsonResponse(
            {'ok': False, 'error': 'invalid params'}, status=400,
        )


def _weighted_series(per_date):
    """date → {sum_ndvi_area, sum_area} → хронологический area-weighted NDVI-ряд."""
    series = []
    for acq_date in sorted(per_date.keys()):
        acc = per_date[acq_date]
        weighted = weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])
        series.append({
            'date': str(acq_date),
            'mean_ndvi': _safe_round(weighted),
        })
    return series


def _build_entity_rows(per_entity_date, names, bl_lookup, id_field, name_field):
    """Серии по сущностям (район/регион) с latest-значениями и z-score.

    per_entity_date: (entity_id, date) → {sum_ndvi_area, sum_area};
    bl_lookup: entity_id → {doy: (mean, std)}.
    """
    data = {}
    for (eid, d), acc in sorted(per_entity_date.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        weighted = weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])
        doy = d.timetuple().tm_yday

        bl_mean, bl_std = bl_lookup.get(eid, {}).get(doy, (None, None))
        z_score = compute_z_score(weighted, bl_mean, bl_std)

        if eid not in data:
            data[eid] = {
                id_field: eid,
                name_field: names.get(eid, ''),
                'series': [],
                'latest_ndvi': None,
                'latest_date': None,
                'latest_z_score': None,
            }
        row = data[eid]
        row['series'].append({
            'date': str(d),
            'mean_ndvi': _safe_round(weighted),
            'z_score': z_score,
        })
        if row['latest_date'] is None or d > date.fromisoformat(row['latest_date']):
            row['latest_ndvi'] = _safe_round(weighted)
            row['latest_date'] = str(d)
            row['latest_z_score'] = z_score
    return data


# --- api_report_region helpers -----------------------------------------------

def _region_series_accumulators(region_id, year):
    """Однопроходная агрегация предагрегата до (район, дата) и (регион, дата).

    NDVI time series per district (area-weighted mean per date) —
    read from the DistrictNdviSeries pre-aggregate instead of raw
    VegetationIndex. The pre-aggregate is built with the same filters
    (modis, is_outlier=false, mean∈[-0.2,1], index_type=ndvi) by
    services.district_ndvi_series.refresh_range, so results are
    byte-equivalent but ~3 orders of magnitude cheaper to read
    (≈6 500 rows / region / year vs. tens of millions of raw VI).
    """
    series_rows = DistrictNdviSeries.objects.filter(
        district__region_id=region_id,
        source=DistrictNdviSeries.Source.MODIS,
        acquired_date__year=year,
        sum_area__gt=0,
    ).values_list(
        'acquired_date', 'sum_ndvi_area', 'sum_area', 'district_id',
    )

    per_district_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    per_region_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})

    # Series rows are already aggregated per (district, date, crop_type) — we
    # sum across crop_types here to collapse to (district, date) and
    # (region, date) levels.
    for acq_date, sum_ndvi_area, sum_area, did in series_rows.iterator(chunk_size=5000):
        if not sum_area:
            continue
        dd = per_district_date[(did, acq_date)]
        dd['sum_ndvi_area'] += float(sum_ndvi_area)
        dd['sum_area'] += float(sum_area)

        rd = per_region_date[acq_date]
        rd['sum_ndvi_area'] += float(sum_ndvi_area)
        rd['sum_area'] += float(sum_area)
    return per_district_date, per_region_date


def _district_baseline_lookup(region_id):
    """Baseline lookup: district_id → {doy: (mean, std)}."""
    baseline_qs = NdviBaseline.objects.filter(
        district__region_id=region_id,
        crop_type='',
    ).values('district_id', 'day_of_year', 'mean_ndvi', 'std_ndvi')
    bl_lookup = {}
    for b in baseline_qs:
        bl_lookup.setdefault(b['district_id'], {})[b['day_of_year']] = (
            b['mean_ndvi'], b['std_ndvi']
        )
    return bl_lookup


def _region_avg_baseline(region_id, year):
    """Region-level baseline (average district baselines per DOY)."""
    region_bl_qs = (
        NdviBaseline.objects.filter(
            district__region_id=region_id,
            crop_type='',
        )
        .values('day_of_year')
        .annotate(avg_mean=Avg('mean_ndvi'), avg_std=Avg('std_ndvi'))
        .order_by('day_of_year')
    )
    region_baseline = []
    for b in region_bl_qs:
        d_date = doy_to_date(b['day_of_year'], year)
        region_baseline.append({
            'date': str(d_date),
            'mean_ndvi': _safe_round(b['avg_mean']),
            'std_ndvi': _safe_round(b['avg_std']),
        })
    return region_baseline


@rate_limit('30/m')
@cache_page(60 * 5)
def api_report_region(request: HttpRequest) -> JsonResponse:
    """Data for region-level MODIS report: NDVI time series per district.

    Query params:
        region (required): region_id
        year (required): year
    """
    region_id, year, error = _parse_report_params(request, 'region')
    if error:
        return error

    try:
        region = Region.objects.get(pk=region_id)
    except Region.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'region not found'}, status=404)

    districts = District.objects.filter(region=region).order_by('name')
    district_names = {d.pk: d.name for d in districts}

    per_district_date, per_region_date = _region_series_accumulators(region_id, year)

    bl_lookup = _district_baseline_lookup(region_id)

    # Build per-district data, sorted chronologically
    district_data = _build_entity_rows(
        per_district_date, district_names, bl_lookup,
        id_field='district_id', name_field='district_name',
    )

    # Build baseline series per district
    baseline_series = {
        did: _bl_to_series(doy_map, year) for did, doy_map in bl_lookup.items()
    }

    # Add assessment text
    result = []
    for d in districts:
        dd = district_data.get(d.pk, {
            'district_id': d.pk,
            'district_name': d.name,
            'series': [],
            'latest_ndvi': None,
            'latest_date': None,
            'latest_z_score': None,
        })
        dd['assessment'] = _ndvi_assessment(dd.get('latest_ndvi'), dd.get('latest_z_score'))
        dd['baseline'] = baseline_series.get(d.pk, [])
        result.append(dd)

    # Region-level overall NDVI series (built in the same single pass above)
    region_overall = _weighted_series(per_region_date)

    region_baseline = _region_avg_baseline(region_id, year)

    # last_period_end for dashed extension line (MODIS 16-day: mid + 8 days)
    last_period_end = modis_last_period_end(region_overall)

    return JsonResponse({
        'ok': True,
        'region': {'id': region.pk, 'name': region.name},
        'year': year,
        'districts': result,
        'region_overall_series': region_overall,
        'region_baseline': region_baseline,
        'last_period_end': last_period_end,
    })


def _country_category(z_score):
    """Bucket a region by its latest z-score vs its OWN baseline.

    Categorisation is always relative to the region's own multi-year
    norm (Kuban != Yakutia), never to the country-wide average.
    """
    if z_score is None:
        return 'nodata'
    if z_score <= -1.5:
        return 'anomaly'
    if z_score < -0.5:
        return 'below'
    return 'normal'


def _country_series_accumulators(year):
    """(region, date) aggregation done DB-side: ~73 regions × ~25 dates."""
    series_rows = (
        DistrictNdviSeries.objects.filter(
            source=DistrictNdviSeries.Source.MODIS,
            acquired_date__year=year,
            sum_area__gt=0,
        )
        .values('district__region_id', 'acquired_date')
        .annotate(s_ndvi_area=Sum('sum_ndvi_area'), s_area=Sum('sum_area'))
    )

    per_region_date = {}
    per_country_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    for row in series_rows.iterator(chunk_size=5000):
        s_area = float(row['s_area'] or 0)
        if not s_area:
            continue
        rid = row['district__region_id']
        d = row['acquired_date']
        per_region_date[(rid, d)] = {
            'sum_ndvi_area': float(row['s_ndvi_area']),
            'sum_area': s_area,
        }
        cd = per_country_date[d]
        cd['sum_ndvi_area'] += float(row['s_ndvi_area'])
        cd['sum_area'] += s_area
    return per_region_date, per_country_date


def _region_baseline_lookups():
    """Region baseline lookup: region_id → {doy: (avg_mean, avg_std)}
    (district baselines averaged per DOY, same trick as api_report_region),
    plus country-wide accumulator per DOY.
    """
    region_bl_qs = (
        NdviBaseline.objects.filter(crop_type='')
        .values('district__region_id', 'day_of_year')
        .annotate(avg_mean=Avg('mean_ndvi'), avg_std=Avg('std_ndvi'))
    )
    bl_lookup = {}
    country_bl = defaultdict(lambda: {'sum_mean': 0.0, 'sum_std': 0.0, 'n': 0})
    for b in region_bl_qs.iterator(chunk_size=5000):
        rid = b['district__region_id']
        doy = b['day_of_year']
        bl_lookup.setdefault(rid, {})[doy] = (b['avg_mean'], b['avg_std'])
        cb = country_bl[doy]
        cb['sum_mean'] += float(b['avg_mean'] or 0)
        cb['sum_std'] += float(b['avg_std'] or 0)
        cb['n'] += 1
    return bl_lookup, country_bl


def _country_baseline_series(country_bl, year):
    """Country baseline — VISUAL REFERENCE ONLY (see _country_category note)."""
    country_baseline = []
    for doy in sorted(country_bl.keys()):
        cb = country_bl[doy]
        if not cb['n']:
            continue
        d_date = doy_to_date(doy, year)
        country_baseline.append({
            'date': str(d_date),
            'mean_ndvi': _safe_round(cb['sum_mean'] / cb['n']),
            'std_ndvi': _safe_round(cb['sum_std'] / cb['n']),
        })
    return country_baseline


@rate_limit('30/m')
@cache_page(60 * 15)
def api_report_country(request: HttpRequest) -> JsonResponse:
    """Data for country-level MODIS report: NDVI time series per region.

    Reads the DistrictNdviSeries pre-aggregate summed up to
    (region, date) level — ≈2 000 points/year for the whole country —
    plus per-region baselines (district baselines averaged per DOY).

    Query params:
        year (required): year
    """
    year = request.GET.get('year')
    if not year:
        return JsonResponse({'ok': False, 'error': 'year required'}, status=400)
    try:
        year = int(year)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid params'}, status=400)

    regions = Region.objects.only('id', 'name').order_by('name')
    region_names = {r.pk: r.name for r in regions}

    per_region_date, per_country_date = _country_series_accumulators(year)

    bl_lookup, country_bl = _region_baseline_lookups()

    # Build per-region data, sorted chronologically
    region_data = _build_entity_rows(
        per_region_date, region_names, bl_lookup,
        id_field='region_id', name_field='region_name',
    )

    # Only regions with data — categorised by their OWN baseline z-score.
    result = []
    for rid in sorted(region_data.keys(), key=lambda r: region_names.get(r, '')):
        rd = region_data[rid]
        rd['category'] = _country_category(rd['latest_z_score'])
        rd['assessment'] = _ndvi_assessment(rd['latest_ndvi'], rd['latest_z_score'])
        result.append(rd)

    # Country-level overall NDVI series (area-weighted across all regions)
    country_overall = _weighted_series(per_country_date)

    country_baseline = _country_baseline_series(country_bl, year)

    # last_period_end for dashed extension line (MODIS 16-day: mid + 8 days)
    last_period_end = modis_last_period_end(country_overall)

    return JsonResponse({
        'ok': True,
        'year': year,
        'regions': result,
        'country_overall_series': country_overall,
        'country_baseline': country_baseline,
        'last_period_end': last_period_end,
    })


# --- api_report_district helpers --------------------------------------------

def _bl_to_series(doy_map, year):
    """doy → (mean, std) → хронологический список точек baseline-линии."""
    bl_list = []
    for doy in sorted(doy_map.keys()):
        m, s = doy_map[doy]
        d_date = doy_to_date(doy, year)
        bl_list.append({
            'date': str(d_date),
            'mean_ndvi': _safe_round(m),
            'std_ndvi': _safe_round(s),
        })
    return bl_list


def _district_baselines(district):
    """Baseline района: общий (crop_type='') и по-культурный словари doy-профилей."""
    all_bl_qs = NdviBaseline.objects.filter(
        district=district,
    ).values('day_of_year', 'mean_ndvi', 'std_ndvi', 'crop_type').order_by('crop_type', 'day_of_year')
    bl_lookup = {}        # overall: doy → (mean, std)
    bl_by_crop = {}       # crop_type → {doy: (mean, std)}
    for b in all_bl_qs:
        ct = b['crop_type']
        if ct == '':
            bl_lookup[b['day_of_year']] = (b['mean_ndvi'], b['std_ndvi'])
        else:
            bl_by_crop.setdefault(ct, {})[b['day_of_year']] = (b['mean_ndvi'], b['std_ndvi'])
    return bl_lookup, bl_by_crop


def _district_crop_rows(per_crop_date, fl_info, crop_labels, bl_lookup):
    """Строки по культурам: серия, latest-значения, z-score и оценка.

    В ответ попадают все культуры с угодьями в районе (count > 0),
    даже без NDVI-данных — тогда с пустой серией и «Нет данных».
    """
    crop_data = {}
    for (ct, d), acc in sorted(per_crop_date.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        weighted = weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])

        if ct not in crop_data:
            crop_data[ct] = {
                'crop_type': ct,
                'label': crop_labels.get(ct, ct),
                'count': fl_info.get(ct, {}).get('count', 0),
                'area_ha': fl_info.get(ct, {}).get('area_ha', 0),
                'series': [],
                'latest_ndvi': None,
                'latest_date': None,
            }
        crop_data[ct]['series'].append({
            'date': str(d),
            'mean_ndvi': _safe_round(weighted),
        })
        # Track latest
        if crop_data[ct]['latest_date'] is None or d > date.fromisoformat(crop_data[ct]['latest_date']):
            crop_data[ct]['latest_ndvi'] = _safe_round(weighted)
            crop_data[ct]['latest_date'] = str(d)

    result = []
    for ct_code, ct_label in Farmland.CropType.choices:
        if ct_code in crop_data:
            cd = crop_data[ct_code]
        else:
            cd = {
                'crop_type': ct_code,
                'label': ct_label,
                'count': fl_info.get(ct_code, {}).get('count', 0),
                'area_ha': fl_info.get(ct_code, {}).get('area_ha', 0),
                'series': [],
                'latest_ndvi': None,
                'latest_date': None,
            }
        # z-score for latest observation
        z = None
        if cd['latest_date'] and cd['latest_ndvi']:
            doy = date.fromisoformat(cd['latest_date']).timetuple().tm_yday
            bl_mean, bl_std = bl_lookup.get(doy, (None, None))
            z = compute_z_score(cd['latest_ndvi'], bl_mean, bl_std)
        cd['assessment'] = _ndvi_assessment(cd.get('latest_ndvi'), z)
        cd['latest_z_score'] = z
        if cd['count'] > 0:
            result.append(cd)
    return result


def _district_phenology_map(district, year):
    """Средние фенометрики района по культурам (SOS/EOS/POS/LOS и пр.)."""
    pheno_qs = (
        FarmlandPhenology.objects.filter(
            farmland__district=district,
            year=year,
            source='modis',
        )
        .values('farmland__crop_type')
        .annotate(
            count=Count('id'),
            avg_max_ndvi=Avg('max_ndvi'),
            avg_mean_ndvi=Avg('mean_ndvi'),
            avg_los=Avg('los_days'),
            avg_sos=Avg(Extract('sos_date', 'doy')),
            avg_eos=Avg(Extract('eos_date', 'doy')),
            avg_pos=Avg(Extract('pos_date', 'doy')),
        )
        .order_by('farmland__crop_type')
    )

    def _doy_to_str(doy_val):
        if doy_val is None:
            return None
        try:
            return doy_to_date(int(round(doy_val)), year).strftime('%d.%m')
        except Exception:
            return None

    pheno_map = {}
    for p in pheno_qs:
        ct = p['farmland__crop_type']
        pheno_map[ct] = {
            'count': p['count'],
            'avg_max_ndvi': _safe_round(p['avg_max_ndvi']),
            'avg_mean_ndvi': _safe_round(p['avg_mean_ndvi']),
            'avg_los': round(p['avg_los']) if p['avg_los'] else None,
            'avg_sos': _doy_to_str(p['avg_sos']),
            'avg_eos': _doy_to_str(p['avg_eos']),
            'avg_pos': _doy_to_str(p['avg_pos']),
        }
    return pheno_map


def _region_overall_series(region, year):
    """Area-weighted NDVI-ряд всего региона из предагрегата (для сравнения с районом)."""
    region_rows = DistrictNdviSeries.objects.filter(
        district__region=region,
        source=DistrictNdviSeries.Source.MODIS,
        acquired_date__year=year,
        sum_area__gt=0,
    ).values_list('acquired_date', 'sum_ndvi_area', 'sum_area')

    region_by_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    for acq_date, sum_ndvi_area, sum_area in region_rows.iterator(chunk_size=5000):
        if not sum_area:
            continue
        acc = region_by_date[acq_date]
        acc['sum_ndvi_area'] += float(sum_ndvi_area)
        acc['sum_area'] += float(sum_area)

    region_overall = []
    for acq_date in sorted(region_by_date.keys()):
        acc = region_by_date[acq_date]
        weighted = weighted_mean(acc['sum_ndvi_area'], acc['sum_area'])
        region_overall.append({
            'date': str(acq_date),
            'mean_ndvi': _safe_round(weighted),
        })
    return region_overall


def _farmland_info(district):
    """Farmland summary by crop type: count и площадь по каждой категории."""
    fl_summary = (
        Farmland.objects.filter(district=district)
        .values('crop_type')
        .annotate(count=Count('id'), total_area=Sum('area_ha'))
        .order_by('crop_type')
    )
    fl_info = {}
    for row in fl_summary:
        fl_info[row['crop_type']] = {
            'count': row['count'],
            'area_ha': round(row['total_area'] or 0, 1),
        }
    return fl_info


def _district_series_accumulators(district, year):
    """NDVI time series by crop type AND overall (area-weighted) —
    served from the DistrictNdviSeries pre-aggregate (same filters as
    raw VI, see api_report_region). ~120 rows / district / year.
    """
    series_rows = DistrictNdviSeries.objects.filter(
        district=district,
        source=DistrictNdviSeries.Source.MODIS,
        acquired_date__year=year,
        sum_area__gt=0,
    ).values_list('acquired_date', 'crop_type', 'sum_ndvi_area', 'sum_area')

    per_crop_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    per_overall_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})

    for acq_date, ct, sum_ndvi_area, sum_area in series_rows:
        if not sum_area:
            continue
        cd = per_crop_date[(ct, acq_date)]
        cd['sum_ndvi_area'] += float(sum_ndvi_area)
        cd['sum_area'] += float(sum_area)

        od = per_overall_date[acq_date]
        od['sum_ndvi_area'] += float(sum_ndvi_area)
        od['sum_area'] += float(sum_area)
    return per_crop_date, per_overall_date


@rate_limit('30/m')
@cache_page(60 * 5)
def api_report_district(request: HttpRequest) -> JsonResponse:
    """Data for district-level MODIS report: NDVI stats by crop type.

    Query params:
        district (required): district_id
        year (required): year
    """
    district_id, year, error = _parse_report_params(request, 'district')
    if error:
        return error

    try:
        district = District.objects.select_related('region').get(pk=district_id)
    except District.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'district not found'}, status=404)

    crop_labels = dict(Farmland.CropType.choices)

    fl_info = _farmland_info(district)

    per_crop_date, per_overall_date = _district_series_accumulators(district, year)

    overall_series = _weighted_series(per_overall_date)

    # Baseline for the district (all crop types + per crop type)
    bl_lookup, bl_by_crop = _district_baselines(district)

    # Per-crop rows: series, latest, z-score, assessment
    result = _district_crop_rows(per_crop_date, fl_info, crop_labels, bl_lookup)

    # Phenology + baselines per crop type
    pheno_map = _district_phenology_map(district, year)
    overall_baseline = _bl_to_series(bl_lookup, year)
    for cd in result:
        cd['phenology'] = pheno_map.get(cd['crop_type'])
        # Per-crop baseline; fallback to overall
        crop_bl = bl_by_crop.get(cd['crop_type'], bl_lookup)
        cd['baseline'] = _bl_to_series(crop_bl, year) if isinstance(crop_bl, dict) else []

    # Region-level overall NDVI series (area-weighted across ALL
    # districts in the same region) — read from the pre-aggregate.
    region_overall = _region_overall_series(district.region, year)

    # last_period_end for dashed extension line (MODIS 16-day: mid + 8 days)
    last_period_end = modis_last_period_end(overall_series)

    return JsonResponse({
        'ok': True,
        'district': {'id': district.pk, 'name': district.name},
        'region': {'id': district.region.pk, 'name': district.region.name},
        'year': year,
        'overall_series': overall_series,
        'overall_baseline': overall_baseline,
        'region_overall_series': region_overall,
        'crop_types': result,
        'last_period_end': last_period_end,
    })


# --- api_report_farmland (field passport) ------------------------------------

def _farmland_vi_series(fid, satellites, year):
    """NDVI-ряд угодья за год для набора спутников (хронологический).

    Включает сырое ``mean``, сглаженное ``mean_smooth``, флаг выброса и
    попиксельную гистограмму (5 бинов, может быть None для старых записей).
    """
    rows = VegetationIndex.objects.filter(
        farmland_id=fid, index_type='ndvi',
        mean__gte=-1, mean__lte=1,
        acquired_date__year=year,
        scene__satellite__in=satellites,
    ).order_by('acquired_date').values(
        'acquired_date', 'mean', 'mean_smooth', 'is_outlier', 'histogram',
    )
    series = []
    for r in rows:
        series.append({
            'date': str(r['acquired_date']),
            'mean_ndvi': _safe_round(r['mean']),
            'mean_smooth': (
                None if r['mean_smooth'] is None else _safe_round(r['mean_smooth'])
            ),
            'is_outlier': bool(r['is_outlier']),
            'histogram': r['histogram'],
        })
    return series


def _farmland_geometry(farmland):
    """Упрощённый GeoJSON контура для inline-SVG мини-карты.

    Поля из вектора ЗСН бывают с тысячами вершин — для миниатюры 200×200 px
    хватает допуска ~10 м (1e-4°). При сбое simplify отдаём оригинал.
    """
    geom = farmland.geom
    if geom is None:
        return None
    try:
        simplified = geom.simplify(0.0001, preserve_topology=True)
        if not simplified.empty:
            geom = simplified
    except Exception:
        pass
    try:
        gj = json.loads(geom.geojson)
        # simplify() может схлопнуть MultiPolygon из одного полигона в
        # Polygon — нормализуем тип, фронтенд рисует MultiPolygon.
        if gj.get('type') == 'Polygon':
            gj = {'type': 'MultiPolygon', 'coordinates': [gj['coordinates']]}
        return gj
    except Exception:
        return None


def _farmland_crop_baseline(farmland):
    """Baseline района по культуре угодья; fallback — общий (crop_type='')."""
    if farmland.district_id is None:
        return {}
    rows = NdviBaseline.objects.filter(
        district_id=farmland.district_id,
        crop_type__in=['', farmland.crop_type],
    ).values('day_of_year', 'mean_ndvi', 'std_ndvi', 'crop_type')
    overall, by_crop = {}, {}
    for b in rows:
        target = by_crop if b['crop_type'] else overall
        target[b['day_of_year']] = (b['mean_ndvi'], b['std_ndvi'])
    return by_crop or overall


def _farmland_phenology(farmland, year):
    """Фенометрики угодья за год (по источникам) + средние по району
    для той же культуры (MODIS) — контекст «раньше/позже нормы района»."""
    own = {}
    rows = FarmlandPhenology.objects.filter(farmland=farmland, year=year)
    for p in rows:
        own[p.source] = {
            'sos_date': str(p.sos_date) if p.sos_date else None,
            'eos_date': str(p.eos_date) if p.eos_date else None,
            'pos_date': str(p.pos_date) if p.pos_date else None,
            'max_ndvi': _safe_round(p.max_ndvi) if p.max_ndvi is not None else None,
            'mean_ndvi': _safe_round(p.mean_ndvi) if p.mean_ndvi is not None else None,
            'los_days': p.los_days,
            'total_ndvi': _safe_round(p.total_ndvi) if p.total_ndvi is not None else None,
        }

    district_avg = None
    if farmland.district_id is not None:
        agg = (
            FarmlandPhenology.objects.filter(
                farmland__district_id=farmland.district_id,
                farmland__crop_type=farmland.crop_type,
                year=year,
                source=FarmlandPhenology.Source.MODIS,
            )
            .aggregate(
                count=Count('id'),
                avg_max_ndvi=Avg('max_ndvi'),
                avg_los=Avg('los_days'),
                avg_ti=Avg('total_ndvi'),
                avg_sos=Avg(Extract('sos_date', 'doy')),
                avg_eos=Avg(Extract('eos_date', 'doy')),
                avg_pos=Avg(Extract('pos_date', 'doy')),
            )
        )
        if agg['count']:
            def _doy_str(doy_val):
                if doy_val is None:
                    return None
                try:
                    return str(doy_to_date(int(round(doy_val)), year))
                except Exception:
                    return None
            district_avg = {
                'count': agg['count'],
                'avg_max_ndvi': _safe_round(agg['avg_max_ndvi']),
                'avg_los': round(agg['avg_los']) if agg['avg_los'] else None,
                'avg_total_ndvi': _safe_round(agg['avg_ti']),
                'avg_sos': _doy_str(agg['avg_sos']),
                'avg_eos': _doy_str(agg['avg_eos']),
                'avg_pos': _doy_str(agg['avg_pos']),
            }
    return own, district_avg


def _farmland_alerts(farmland, year):
    """Алерты за сезон: per-farmland + district-level по культуре угодья."""
    scope = Q(farmland=farmland)
    if farmland.district_id is not None:
        scope |= Q(
            farmland__isnull=True,
            district_id=farmland.district_id,
            crop_type=farmland.crop_type,
        )
    qs = (
        VegetationAlert.objects.filter(scope, detected_on__year=year)
        .order_by('-detected_on')
    )
    alerts = []
    for a in qs[:50]:
        alerts.append({
            'scope': 'farmland' if a.farmland_id else 'district',
            'alert_type': a.alert_type,
            'alert_type_label': a.get_alert_type_display(),
            'severity': a.severity,
            'status': a.status,
            'status_label': a.get_status_display(),
            'detected_on': str(a.detected_on),
            'source': a.source,
            'message': a.message,
        })
    return alerts


def _latest_valid_point(series):
    """Последняя точка ряда без флага выброса (или None)."""
    for row in reversed(series):
        if not row['is_outlier']:
            return row
    return None


@rate_limit('30/m')
@cache_page(60 * 5)
def api_report_farmland(request: HttpRequest) -> JsonResponse:
    """Data for the per-farmland «field passport» report.

    Detailed-monitoring series (fused HLS if present, else raw S2/L8)
    plus the MODIS reference series, district baseline for the crop,
    phenology metrics vs district average, per-pixel NDVI histograms
    and season alerts.

    Query params:
        farmland (required): farmland id
        year (required): year
    """
    farmland_id, year, error = _parse_report_params(request, 'farmland')
    if error:
        return error

    try:
        farmland = Farmland.objects.select_related('district', 'region').get(pk=farmland_id)
    except Farmland.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'farmland not found'}, status=404)

    # Detailed series: fused composite preferred, raw S2/L8 as fallback.
    detailed_source = 'fused'
    detailed = _farmland_vi_series(farmland.pk, FUSED_SATELLITES, year)
    if not detailed:
        detailed_source = 'raster'
        detailed = _farmland_vi_series(farmland.pk, RASTER_SATELLITES, year)
    modis = _farmland_vi_series(farmland.pk, MODIS_SATELLITES, year)

    bl_doy_map = _farmland_crop_baseline(farmland)
    baseline = _bl_to_series(bl_doy_map, year)

    # Assessment from the latest non-outlier detailed observation
    # (fallback to MODIS when no detailed data exists at all).
    latest = _latest_valid_point(detailed) or _latest_valid_point(modis)
    latest_z = None
    if latest is not None:
        doy = date.fromisoformat(latest['date']).timetuple().tm_yday
        bl_mean, bl_std = bl_doy_map.get(doy, (None, None))
        latest_z = compute_z_score(latest['mean_ndvi'], bl_mean, bl_std)
    assessment = _ndvi_assessment(
        latest['mean_ndvi'] if latest else None, latest_z,
    )

    phenology, district_phenology = _farmland_phenology(farmland, year)
    alerts = _farmland_alerts(farmland, year)

    district = farmland.district
    region = farmland.region or (district.region if district else None)

    return JsonResponse({
        'ok': True,
        'farmland': {
            'id': farmland.pk,
            'crop_type': farmland.crop_type,
            'crop_type_label': farmland.get_crop_type_display(),
            'area_ha': _safe_round(farmland.area_ha, 2),
            'is_used': farmland.is_used,
            'cadastral_number': farmland.cadastral_number,
            'district': {'id': district.pk, 'name': district.name} if district else None,
            'region': {'id': region.pk, 'name': region.name} if region else None,
            'geometry': _farmland_geometry(farmland),
        },
        'year': year,
        'detailed_source': detailed_source if detailed else None,
        'detailed_series': detailed,
        'modis_series': modis,
        'baseline': baseline,
        'latest': {
            'date': latest['date'] if latest else None,
            'mean_ndvi': latest['mean_ndvi'] if latest else None,
            'z_score': latest_z,
            'assessment': assessment,
        },
        'phenology': phenology,
        'district_phenology': district_phenology,
        'alerts': alerts,
        'last_period_end': modis_last_period_end(modis),
    })


# --- api_report_screening (problem-fields screening) --------------------------

# Детальные источники скрининга: сырые S2/L8 + fused-композит.
_DETAILED_SATELLITES = RASTER_SATELLITES + FUSED_SATELLITES

SCREENING_DEFAULT_LIMIT = 20
SCREENING_MAX_LIMIT = 100


def _district_crop_baselines(district_id):
    """crop_type → {doy: (mean, std)}; '' — общерайонный fallback."""
    lookup = {}
    rows = NdviBaseline.objects.filter(district_id=district_id).values(
        'crop_type', 'day_of_year', 'mean_ndvi', 'std_ndvi',
    )
    for b in rows:
        lookup.setdefault(b['crop_type'], {})[b['day_of_year']] = (
            b['mean_ndvi'], b['std_ndvi'],
        )
    return lookup


def _latest_detailed_per_farmland(district_id, year):
    """Последнее детальное (S2/L8/fused) наблюдение по каждому угодью района.

    Postgres ``DISTINCT ON (farmland_id)`` по частичному индексу
    ``vi_ndvi_active_idx`` (farmland, acquired_date DESC) — один проход
    вместо N подзапросов.
    """
    return (
        VegetationIndex.objects.filter(
            farmland__district_id=district_id,
            index_type='ndvi', is_outlier=False,
            mean__gte=-1, mean__lte=1,
            acquired_date__year=year,
            scene__satellite__in=_DETAILED_SATELLITES,
        )
        .order_by('farmland_id', '-acquired_date')
        .distinct('farmland_id')
        .values(
            'farmland_id', 'acquired_date', 'mean', 'histogram',
            'farmland__crop_type', 'farmland__area_ha',
        )
    )


def _active_alert_counts(district_id, year):
    """farmland_id → число неразрешённых per-farmland алертов за сезон."""
    rows = (
        VegetationAlert.objects.filter(
            farmland__district_id=district_id,
            detected_on__year=year,
        )
        .exclude(status=VegetationAlert.Status.RESOLVED)
        .values('farmland_id')
        .annotate(n=Count('id'))
    )
    return {r['farmland_id']: r['n'] for r in rows}


def _histogram_low_pct(histogram):
    """Доля пикселей с NDVI < 0.4 (бины 0-0.2 и 0.2-0.4), % или None."""
    if not histogram or len(histogram) != 5:
        return None
    total = sum(histogram)
    if not total:
        return None
    return round((histogram[0] + histogram[1]) / total * 100, 1)


def _screening_score(z_score, low_pct, alerts):
    """Эвристический балл неблагополучия (больше = хуже).

    Компоненты:
    - глубина провала под норму: ``max(0, -z)`` — z-score симметричен,
      но выше нормы не проблема;
    - неоднородность/деградация: доля пикселей < 0.4 с весом 2
      (0..2 балла) — ловит частичную гибель, невидимую в среднем;
    - алерты: по баллу за каждый, с потолком 3, чтобы серия дублей
      одного события не выдавила остальные поля из топа.
    """
    score = 0.0
    if z_score is not None and z_score < 0:
        score += -z_score
    if low_pct is not None:
        score += (low_pct / 100.0) * 2
    score += min(alerts, 3)
    return round(score, 2)


def _screening_category(z_score, low_pct, alerts):
    """Категория поля для группировки в отчёте."""
    z = z_score if z_score is not None else 0
    lp = low_pct if low_pct is not None else 0
    if z <= -1.5 or lp >= 50 or alerts >= 2:
        return 'anomaly'
    if z <= -0.5 or lp >= 30 or alerts == 1:
        return 'below'
    return 'normal'


def _farmland_year_series(farmland_ids, year):
    """farmland_id → детальный NDVI-ряд за год (для sparkline топ-N полей)."""
    rows = (
        VegetationIndex.objects.filter(
            farmland_id__in=farmland_ids,
            index_type='ndvi', is_outlier=False,
            mean__gte=-1, mean__lte=1,
            acquired_date__year=year,
            scene__satellite__in=_DETAILED_SATELLITES,
        )
        .order_by('acquired_date')
        .values('farmland_id', 'acquired_date', 'mean')
    )
    series = defaultdict(list)
    for r in rows:
        series[r['farmland_id']].append({
            'date': str(r['acquired_date']),
            'mean_ndvi': _safe_round(r['mean']),
        })
    return series


@rate_limit('30/m')
@cache_page(60 * 10)
def api_report_screening(request: HttpRequest) -> JsonResponse:
    """Problem-fields screening for a district (detailed monitoring).

    Ranks the district's farmlands by a distress score combining the
    z-score of the latest detailed (S2/L8/fused) NDVI vs the district
    baseline, the share of low-NDVI pixels in the latest histogram and
    the number of unresolved alerts. Returns the top-N worst fields
    with sparkline series and links suitable for the field passport.

    Query params:
        district (required): district id
        year (required): year
        limit (optional): top-N size, default 20, max 100
    """
    district_id, year, error = _parse_report_params(request, 'district')
    if error:
        return error

    try:
        district = District.objects.select_related('region').get(pk=district_id)
    except District.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'district not found'}, status=404)

    try:
        limit = min(
            max(int(request.GET.get('limit', SCREENING_DEFAULT_LIMIT)), 1),
            SCREENING_MAX_LIMIT,
        )
    except (TypeError, ValueError):
        limit = SCREENING_DEFAULT_LIMIT

    bl_lookup = _district_crop_baselines(district.pk)
    alert_counts = _active_alert_counts(district.pk, year)

    rows = []
    with_data = 0
    for r in _latest_detailed_per_farmland(district.pk, year):
        with_data += 1
        doy = r['acquired_date'].timetuple().tm_yday
        crop = r['farmland__crop_type']
        crop_bl = bl_lookup.get(crop) or bl_lookup.get('') or {}
        bl_mean, bl_std = crop_bl.get(doy, (None, None))
        z = compute_z_score(r['mean'], bl_mean, bl_std)
        low_pct = _histogram_low_pct(r['histogram'])
        alerts = alert_counts.get(r['farmland_id'], 0)
        rows.append({
            'farmland_id': r['farmland_id'],
            'crop_type': crop,
            'crop_type_label': Farmland.CropType(crop).label if crop else '',
            'area_ha': _safe_round(r['farmland__area_ha'], 2),
            'latest_date': str(r['acquired_date']),
            'latest_ndvi': _safe_round(r['mean']),
            'z_score': z,
            'low_pct': low_pct,
            'active_alerts': alerts,
            'score': _screening_score(z, low_pct, alerts),
            'category': _screening_category(z, low_pct, alerts),
        })

    rows.sort(key=lambda x: -x['score'])
    top = rows[:limit]

    spark = _farmland_year_series([r['farmland_id'] for r in top], year)
    for r in top:
        r['series'] = spark.get(r['farmland_id'], [])

    return JsonResponse({
        'ok': True,
        'district': {'id': district.pk, 'name': district.name},
        'region': {'id': district.region.pk, 'name': district.region.name},
        'year': year,
        'farmlands_total': Farmland.objects.filter(district_id=district.pk).count(),
        'farmlands_with_data': with_data,
        'farmlands': top,
    })


# --- api_report_district_detailed (district detailed-monitoring summary) ------

def _district_detailed_series(district_id, year):
    """Детальные (S2/L8/fused) area-weighted ряды района: общий + по культурам.

    Один проход по VI района за год с аккумуляцией (date) и (crop, date) —
    как ``_region_series_accumulators``, но по сырым детальным записям
    (пре-агрегата для raster-источников нет).
    """
    rows = (
        VegetationIndex.objects.filter(
            farmland__district_id=district_id,
            index_type='ndvi', is_outlier=False,
            mean__gte=-1, mean__lte=1,
            acquired_date__year=year,
            scene__satellite__in=_DETAILED_SATELLITES,
            farmland__area_ha__gt=0,
        )
        .values_list('acquired_date', 'mean', 'farmland__crop_type', 'farmland__area_ha')
    )
    per_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    per_crop_date = defaultdict(lambda: {'sum_ndvi_area': 0.0, 'sum_area': 0.0})
    for acq_date, mean, crop, area in rows.iterator(chunk_size=5000):
        area = float(area)
        overall = per_date[acq_date]
        overall['sum_ndvi_area'] += mean * area
        overall['sum_area'] += area
        cd = per_crop_date[(crop, acq_date)]
        cd['sum_ndvi_area'] += mean * area
        cd['sum_area'] += area

    per_crop = defaultdict(dict)
    for (crop, acq_date), acc in per_crop_date.items():
        per_crop[crop][acq_date] = acc
    return (
        _weighted_series(per_date),
        {crop: _weighted_series(dates) for crop, dates in per_crop.items()},
    )


def _district_alerts_summary(district_id, year):
    """Неразрешённые алерты района за сезон: всего + разбивка по типам."""
    qs = (
        VegetationAlert.objects.filter(
            Q(farmland__district_id=district_id) | Q(district_id=district_id),
            detected_on__year=year,
        )
        .exclude(status=VegetationAlert.Status.RESOLVED)
    )
    by_type = [
        {
            'alert_type': r['alert_type'],
            'alert_type_label': VegetationAlert.AlertType(r['alert_type']).label,
            'count': r['n'],
        }
        for r in qs.values('alert_type').annotate(n=Count('id')).order_by('-n')
    ]
    return {'active_total': sum(t['count'] for t in by_type), 'by_type': by_type}


@rate_limit('30/m')
@cache_page(60 * 10)
def api_report_district_detailed(request: HttpRequest) -> JsonResponse:
    """District summary over detailed (S2/L8/fused) monitoring data.

    Complements the MODIS district report with: coverage of the detailed
    monitoring, category distribution of fields (screening rules), the
    area-weighted district series with per-crop split and an unresolved
    alerts summary.

    Query params:
        district (required): district id
        year (required): year
    """
    district_id, year, error = _parse_report_params(request, 'district')
    if error:
        return error

    try:
        district = District.objects.select_related('region').get(pk=district_id)
    except District.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'district not found'}, status=404)

    bl_lookup = _district_crop_baselines(district.pk)
    alert_counts = _active_alert_counts(district.pk, year)

    # Per-farmland latest observation → категории и агрегаты по культурам.
    categories = {
        key: {'count': 0, 'area_ha': 0.0}
        for key in ('anomaly', 'below', 'normal', 'nodata')
    }
    crop_agg = defaultdict(lambda: {
        'farmlands': 0, 'area_ha': 0.0, 'problem_count': 0,
        'sum_ndvi_area': 0.0, 'sum_area': 0.0, 'latest_date': None,
    })
    with_data = 0
    area_with_data = 0.0
    seen_ids = []
    for r in _latest_detailed_per_farmland(district.pk, year):
        with_data += 1
        seen_ids.append(r['farmland_id'])
        area = float(r['farmland__area_ha'] or 0)
        area_with_data += area

        doy = r['acquired_date'].timetuple().tm_yday
        crop = r['farmland__crop_type']
        crop_bl = bl_lookup.get(crop) or bl_lookup.get('') or {}
        bl_mean, bl_std = crop_bl.get(doy, (None, None))
        z = compute_z_score(r['mean'], bl_mean, bl_std)
        low_pct = _histogram_low_pct(r['histogram'])
        alerts = alert_counts.get(r['farmland_id'], 0)
        category = _screening_category(z, low_pct, alerts)

        categories[category]['count'] += 1
        categories[category]['area_ha'] += area

        ca = crop_agg[crop]
        ca['farmlands'] += 1
        ca['area_ha'] += area
        if category != 'normal':
            ca['problem_count'] += 1
        ca['sum_ndvi_area'] += r['mean'] * area
        ca['sum_area'] += area
        if ca['latest_date'] is None or r['acquired_date'] > ca['latest_date']:
            ca['latest_date'] = r['acquired_date']

    # Поля без детальных данных за год.
    nodata_agg = (
        Farmland.objects.filter(district_id=district.pk)
        .exclude(pk__in=seen_ids)
        .aggregate(n=Count('id'), area=Sum('area_ha'))
    )
    categories['nodata']['count'] = nodata_agg['n'] or 0
    categories['nodata']['area_ha'] = float(nodata_agg['area'] or 0)
    for cat in categories.values():
        cat['area_ha'] = _safe_round(cat['area_ha'], 1)

    overall_series, crop_series = _district_detailed_series(district.pk, year)

    crops = []
    for crop, ca in crop_agg.items():
        crops.append({
            'crop_type': crop,
            'crop_type_label': Farmland.CropType(crop).label if crop else '',
            'farmlands': ca['farmlands'],
            'area_ha': _safe_round(ca['area_ha'], 1),
            'problem_count': ca['problem_count'],
            'latest_ndvi': _safe_round(
                weighted_mean(ca['sum_ndvi_area'], ca['sum_area']),
            ),
            'latest_date': str(ca['latest_date']) if ca['latest_date'] else None,
            'series': crop_series.get(crop, []),
        })
    crops.sort(key=lambda c: -c['area_ha'])

    farmlands_total = Farmland.objects.filter(district_id=district.pk).count()

    return JsonResponse({
        'ok': True,
        'district': {'id': district.pk, 'name': district.name},
        'region': {'id': district.region.pk, 'name': district.region.name},
        'year': year,
        'coverage': {
            'farmlands_total': farmlands_total,
            'farmlands_with_data': with_data,
            'area_with_data_ha': _safe_round(area_with_data, 1),
        },
        'categories': categories,
        'overall_series': overall_series,
        'baseline': _bl_to_series(bl_lookup.get('') or {}, year),
        'crops': crops,
        'alerts_summary': _district_alerts_summary(district.pk, year),
    })


# --- api_report_unused (unused-lands screening) --------------------------------

# Пороги эвристик неиспользования. Обоснование:
# возделываемое поле в пике сезона даёт NDVI ≥ 0.5–0.6; максимум ниже
# 0.35 за весь год означает отсутствие сомкнутого растительного
# покрова (голая почва/деградация). Амплитуда < 0.15 без
# детектированного SOS — нет вегетационного цикла (залежь с
# постоянным покровом либо пустырь).
UNUSED_MAX_NDVI = 0.35
UNUSED_MIN_AMPLITUDE = 0.15
REACTIVATED_MAX_NDVI = 0.5
UNUSED_MIN_OBS = 3
UNUSED_DEFAULT_LIMIT = 50
UNUSED_MAX_LIMIT = 200


def _farmland_season_stats(district_id, year):
    """farmland_id → {max, min, n_obs} по всем источникам (вкл. MODIS).

    Для детекции неиспользования важна полнота покрытия, а не
    разрешение — берём все спутники, чтобы не пометить «подозрительным»
    поле, у которого просто нет безоблачных S2-сцен в пик сезона.
    """
    rows = (
        VegetationIndex.objects.filter(
            farmland__district_id=district_id,
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


def _farmlands_with_sos(district_id, year):
    """Множество farmland_id с детектированным началом сезона за год."""
    return set(
        FarmlandPhenology.objects.filter(
            farmland__district_id=district_id, year=year,
            sos_date__isnull=False,
        ).values_list('farmland_id', flat=True)
    )


def _unused_signals(stats, has_sos):
    """Список сигналов неиспользования для поля (пустой = чисто).

    stats — агрегат сезона {max_ndvi, min_ndvi, n_obs}; None или
    n_obs < UNUSED_MIN_OBS → данных недостаточно (возвращаем None).
    """
    if stats is None or stats['n_obs'] < UNUSED_MIN_OBS:
        return None
    signals = []
    if stats['max_ndvi'] < UNUSED_MAX_NDVI:
        signals.append('no_vegetation')
    amplitude = stats['max_ndvi'] - stats['min_ndvi']
    if amplitude < UNUSED_MIN_AMPLITUDE and not has_sos:
        signals.append('no_cycle')
    return signals


@rate_limit('30/m')
@cache_page(60 * 10)
def api_report_unused(request: HttpRequest) -> JsonResponse:
    """Unused-lands screening for a district (ЗСН control).

    Cross-checks the declared ``is_used`` flag against satellite
    signals of the season (max NDVI, amplitude, detected SOS):

    - suspects: declared used (or unknown) but no vegetation signal —
      candidates for ЗСН non-use inspection;
    - reactivated: declared unused but a clear crop cycle is present —
      likely returned to cultivation.

    Query params:
        district (required): district id
        year (required): year
        limit (optional): max rows per list, default 50, max 200
    """
    district_id, year, error = _parse_report_params(request, 'district')
    if error:
        return error

    try:
        district = District.objects.select_related('region').get(pk=district_id)
    except District.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'district not found'}, status=404)

    try:
        limit = min(
            max(int(request.GET.get('limit', UNUSED_DEFAULT_LIMIT)), 1),
            UNUSED_MAX_LIMIT,
        )
    except (TypeError, ValueError):
        limit = UNUSED_DEFAULT_LIMIT

    season_stats = _farmland_season_stats(district.pk, year)
    sos_ids = _farmlands_with_sos(district.pk, year)

    farmlands = Farmland.objects.filter(district_id=district.pk).values(
        'id', 'crop_type', 'area_ha', 'is_used', 'cadastral_number',
    )

    suspects, reactivated = [], []
    totals = {
        'farmlands_total': 0,
        'declared_unused': 0,
        'declared_unused_area': 0.0,
        'with_data': 0,
        'suspect_area': 0.0,
    }
    for f in farmlands:
        totals['farmlands_total'] += 1
        area = float(f['area_ha'] or 0)
        if f['is_used'] is False:
            totals['declared_unused'] += 1
            totals['declared_unused_area'] += area

        stats = season_stats.get(f['id'])
        has_sos = f['id'] in sos_ids
        signals = _unused_signals(stats, has_sos)
        if stats is not None and stats['n_obs'] >= UNUSED_MIN_OBS:
            totals['with_data'] += 1
        if signals is None:
            continue

        row = {
            'farmland_id': f['id'],
            'crop_type': f['crop_type'],
            'crop_type_label': (
                Farmland.CropType(f['crop_type']).label if f['crop_type'] else ''
            ),
            'area_ha': _safe_round(area, 2),
            'is_used': f['is_used'],
            'cadastral_number': f['cadastral_number'],
            'max_ndvi': _safe_round(stats['max_ndvi']),
            'amplitude': _safe_round(stats['max_ndvi'] - stats['min_ndvi']),
            'n_obs': stats['n_obs'],
            'has_sos': has_sos,
            'signals': signals,
        }
        if f['is_used'] is not False and signals:
            row['severity'] = 'high' if 'no_vegetation' in signals else 'medium'
            totals['suspect_area'] += area
            suspects.append(row)
        elif (
            f['is_used'] is False
            and not signals
            and stats['max_ndvi'] >= REACTIVATED_MAX_NDVI
            and has_sos
        ):
            reactivated.append(row)

    suspects.sort(
        key=lambda r: (r['severity'] != 'high', -(r['area_ha'] or 0)),
    )
    reactivated.sort(key=lambda r: -(r['area_ha'] or 0))

    totals['declared_unused_area'] = _safe_round(totals['declared_unused_area'], 1)
    totals['suspect_area'] = _safe_round(totals['suspect_area'], 1)

    return JsonResponse({
        'ok': True,
        'district': {'id': district.pk, 'name': district.name},
        'region': {'id': district.region.pk, 'name': district.region.name},
        'year': year,
        'totals': {**totals, 'suspects': len(suspects), 'reactivated': len(reactivated)},
        'thresholds': {
            'max_ndvi': UNUSED_MAX_NDVI,
            'min_amplitude': UNUSED_MIN_AMPLITUDE,
            'min_obs': UNUSED_MIN_OBS,
        },
        'suspects': suspects[:limit],
        'reactivated': reactivated[:limit],
    })
