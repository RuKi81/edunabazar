"""Report API endpoints: region-level and district-level MODIS NDVI reports."""
from collections import defaultdict
from datetime import date

from django.db.models import Avg, Count, Sum
from django.db.models.functions import Extract
from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import cache_page

from ..models import (
    Region, District, DistrictNdviSeries, Farmland, FarmlandPhenology,
    NdviBaseline,
)
# ``ndvi_assessment`` реэкспортируется под историческим именем: хелпер
# вынесен в сервис-слой, но импортируется извне через agrocosmos.views
# (см. views/__init__.py и tests/test_ndvi_assessment.py).
from ..services.ndvi_stats import (
    compute_z_score, doy_to_date, modis_last_period_end,
    ndvi_assessment as _ndvi_assessment, weighted_mean,
)
from ._helpers import _safe_round, rate_limit


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
