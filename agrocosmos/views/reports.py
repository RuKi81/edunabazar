"""Report API endpoints: region-level and district-level MODIS NDVI reports."""
from datetime import date, timedelta

from django.db.models import Avg, Count, F, Sum
from django.db.models.functions import Extract
from django.http import HttpRequest, JsonResponse

from ..models import (
    Region, District, Farmland, FarmlandPhenology, NdviBaseline, VegetationIndex,
)
from ._helpers import MODIS_SATELLITES, _safe_round


def _ndvi_assessment(mean_ndvi, z_score=None):
    """Return a short textual assessment of vegetation state."""
    if mean_ndvi is None:
        return 'Нет данных'
    if z_score is not None:
        if z_score < -2:
            return 'Критическое снижение вегетации'
        if z_score < -1:
            return 'Вегетация ниже нормы'
        if z_score > 2:
            return 'Вегетация значительно выше нормы'
        if z_score > 1:
            return 'Вегетация выше нормы'
    if mean_ndvi >= 0.6:
        return 'Активная вегетация'
    if mean_ndvi >= 0.4:
        return 'Умеренная вегетация'
    if mean_ndvi >= 0.2:
        return 'Слабая вегетация'
    return 'Вегетация практически отсутствует'


def api_report_region(request: HttpRequest) -> JsonResponse:
    """Data for region-level MODIS report: NDVI time series per district.

    Query params:
        region (required): region_id
        year (required): year
    """
    region_id = request.GET.get('region')
    year = request.GET.get('year')
    if not region_id or not year:
        return JsonResponse({'ok': False, 'error': 'region and year required'}, status=400)

    try:
        region_id = int(region_id)
        year = int(year)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid params'}, status=400)

    try:
        region = Region.objects.get(pk=region_id)
    except Region.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'region not found'}, status=404)

    districts = District.objects.filter(region=region).order_by('name')

    # NDVI time series per district (area-weighted mean per date)
    vi_qs = VegetationIndex.objects.filter(
        farmland__district__region_id=region_id,
        index_type='ndvi',
        acquired_date__year=year,
        is_anomaly=False,
        mean__gte=-0.2, mean__lte=1,
        scene__satellite__in=MODIS_SATELLITES,
    )

    by_district_date = (
        vi_qs
        .values('farmland__district_id', 'farmland__district__name', 'acquired_date')
        .annotate(
            _sum_ndvi_area=Sum(F('mean') * F('farmland__area_ha')),
            _sum_area=Sum('farmland__area_ha'),
            count=Count('id'),
        )
        .order_by('farmland__district_id', 'acquired_date')
    )

    # Baseline lookup: district_id → {doy: (mean, std)}
    baseline_qs = NdviBaseline.objects.filter(
        district__region_id=region_id,
        crop_type='',
    ).values('district_id', 'day_of_year', 'mean_ndvi', 'std_ndvi')
    bl_lookup = {}
    for b in baseline_qs:
        bl_lookup.setdefault(b['district_id'], {})[b['day_of_year']] = (
            b['mean_ndvi'], b['std_ndvi']
        )

    # Build per-district data
    district_data = {}
    for row in by_district_date:
        did = row['farmland__district_id']
        dname = row['farmland__district__name']
        s_area = row['_sum_area'] or 0
        weighted = (row['_sum_ndvi_area'] / s_area) if s_area else None
        d = row['acquired_date']
        doy = d.timetuple().tm_yday

        # z-score
        bl_mean, bl_std = bl_lookup.get(did, {}).get(doy, (None, None))
        z_score = None
        if bl_mean is not None and bl_std and bl_std > 0.01 and weighted is not None:
            z_score = round((weighted - bl_mean) / bl_std, 2)

        if did not in district_data:
            district_data[did] = {
                'district_id': did,
                'district_name': dname,
                'series': [],
                'latest_ndvi': None,
                'latest_date': None,
                'latest_z_score': None,
            }
        district_data[did]['series'].append({
            'date': str(d),
            'mean_ndvi': _safe_round(weighted),
            'z_score': z_score,
        })
        # Track latest
        if district_data[did]['latest_date'] is None or d > date.fromisoformat(district_data[did]['latest_date']):
            district_data[did]['latest_ndvi'] = _safe_round(weighted)
            district_data[did]['latest_date'] = str(d)
            district_data[did]['latest_z_score'] = z_score

    # Build baseline series per district: list of {doy, mean_ndvi, std_ndvi}
    baseline_series = {}
    for did, doy_map in bl_lookup.items():
        bl_list = []
        for doy in sorted(doy_map.keys()):
            m, s = doy_map[doy]
            d_date = date(year, 1, 1) + timedelta(days=doy - 1)
            bl_list.append({
                'date': str(d_date),
                'mean_ndvi': _safe_round(m),
                'std_ndvi': _safe_round(s),
            })
        baseline_series[did] = bl_list

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

    # Region-level overall NDVI series (area-weighted across ALL districts)
    region_overall_qs = (
        vi_qs
        .values('acquired_date')
        .annotate(
            _sum_ndvi_area=Sum(F('mean') * F('farmland__area_ha')),
            _sum_area=Sum('farmland__area_ha'),
        )
        .order_by('acquired_date')
    )
    region_overall = []
    for row in region_overall_qs:
        s_area = row['_sum_area'] or 0
        weighted = (row['_sum_ndvi_area'] / s_area) if s_area else None
        region_overall.append({
            'date': str(row['acquired_date']),
            'mean_ndvi': _safe_round(weighted),
        })

    # Region-level baseline (average district baselines per DOY)
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
        d_date = date(year, 1, 1) + timedelta(days=b['day_of_year'] - 1)
        region_baseline.append({
            'date': str(d_date),
            'mean_ndvi': _safe_round(b['avg_mean']),
            'std_ndvi': _safe_round(b['avg_std']),
        })

    # last_period_end for dashed extension line (MODIS 16-day: mid + 8 days)
    last_period_end = None
    if region_overall:
        try:
            last_mid = date.fromisoformat(region_overall[-1]['date'])
            last_period_end = str(last_mid + timedelta(days=8))
        except Exception:
            pass

    return JsonResponse({
        'ok': True,
        'region': {'id': region.pk, 'name': region.name},
        'year': year,
        'districts': result,
        'region_overall_series': region_overall,
        'region_baseline': region_baseline,
        'last_period_end': last_period_end,
    })


def api_report_district(request: HttpRequest) -> JsonResponse:
    """Data for district-level MODIS report: NDVI stats by crop type.

    Query params:
        district (required): district_id
        year (required): year
    """
    district_id = request.GET.get('district')
    year = request.GET.get('year')
    if not district_id or not year:
        return JsonResponse({'ok': False, 'error': 'district and year required'}, status=400)

    try:
        district_id = int(district_id)
        year = int(year)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid params'}, status=400)

    try:
        district = District.objects.select_related('region').get(pk=district_id)
    except District.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'district not found'}, status=404)

    crop_labels = dict(Farmland.CropType.choices)

    # Farmland summary by crop type
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

    # NDVI time series by crop type (area-weighted)
    vi_qs = VegetationIndex.objects.filter(
        farmland__district=district,
        index_type='ndvi',
        acquired_date__year=year,
        is_anomaly=False,
        mean__gte=-0.2, mean__lte=1,
        scene__satellite__in=MODIS_SATELLITES,
    )

    by_crop_date = (
        vi_qs
        .values('farmland__crop_type', 'acquired_date')
        .annotate(
            _sum_ndvi_area=Sum(F('mean') * F('farmland__area_ha')),
            _sum_area=Sum('farmland__area_ha'),
            count=Count('id'),
        )
        .order_by('farmland__crop_type', 'acquired_date')
    )

    # Overall time series (all crop types combined, area-weighted)
    overall_by_date = (
        vi_qs
        .values('acquired_date')
        .annotate(
            _sum_ndvi_area=Sum(F('mean') * F('farmland__area_ha')),
            _sum_area=Sum('farmland__area_ha'),
            count=Count('id'),
        )
        .order_by('acquired_date')
    )
    overall_series = []
    for row in overall_by_date:
        s_area = row['_sum_area'] or 0
        weighted = (row['_sum_ndvi_area'] / s_area) if s_area else None
        overall_series.append({
            'date': str(row['acquired_date']),
            'mean_ndvi': _safe_round(weighted),
        })

    # Baseline for the district (all crop types + per crop type)
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

    # Build per-crop data
    crop_data = {}
    for row in by_crop_date:
        ct = row['farmland__crop_type']
        s_area = row['_sum_area'] or 0
        weighted = (row['_sum_ndvi_area'] / s_area) if s_area else None
        d = row['acquired_date']

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

    # Add assessment + ensure all crop types present
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
            if bl_mean is not None and bl_std and bl_std > 0.01:
                z = round((cd['latest_ndvi'] - bl_mean) / bl_std, 2)
        cd['assessment'] = _ndvi_assessment(cd.get('latest_ndvi'), z)
        cd['latest_z_score'] = z
        if cd['count'] > 0:
            result.append(cd)

    # Phenology per crop type
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
            d = date(year, 1, 1) + timedelta(days=int(round(doy_val)) - 1)
            return d.strftime('%d.%m')
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

    # Build baseline series helper
    def _bl_to_series(doy_map):
        bl_list = []
        for doy in sorted(doy_map.keys()):
            m, s = doy_map[doy]
            d_date = date(year, 1, 1) + timedelta(days=doy - 1)
            bl_list.append({
                'date': str(d_date),
                'mean_ndvi': _safe_round(m),
                'std_ndvi': _safe_round(s),
            })
        return bl_list

    overall_baseline = _bl_to_series(bl_lookup)

    for cd in result:
        cd['phenology'] = pheno_map.get(cd['crop_type'])
        # Per-crop baseline; fallback to overall
        crop_bl = bl_by_crop.get(cd['crop_type'], bl_lookup)
        cd['baseline'] = _bl_to_series(crop_bl) if isinstance(crop_bl, dict) else []

    # Region-level overall NDVI series (area-weighted across ALL districts)
    region_overall_qs = (
        VegetationIndex.objects.filter(
            farmland__district__region=district.region,
            index_type='ndvi',
            acquired_date__year=year,
            is_anomaly=False,
            mean__gte=-0.2, mean__lte=1,
            scene__satellite__in=MODIS_SATELLITES,
        )
        .values('acquired_date')
        .annotate(
            _sum_ndvi_area=Sum(F('mean') * F('farmland__area_ha')),
            _sum_area=Sum('farmland__area_ha'),
        )
        .order_by('acquired_date')
    )
    region_overall = []
    for row in region_overall_qs:
        s_area = row['_sum_area'] or 0
        weighted = (row['_sum_ndvi_area'] / s_area) if s_area else None
        region_overall.append({
            'date': str(row['acquired_date']),
            'mean_ndvi': _safe_round(weighted),
        })

    # last_period_end for dashed extension line (MODIS 16-day: mid + 8 days)
    last_period_end = None
    if overall_series:
        try:
            last_mid = date.fromisoformat(overall_series[-1]['date'])
            last_period_end = str(last_mid + timedelta(days=8))
        except Exception:
            pass

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
