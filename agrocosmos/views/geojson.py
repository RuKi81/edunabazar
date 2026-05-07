"""GeoJSON API endpoints: regions, districts, farmlands."""
import json
from collections import defaultdict
from datetime import date, timedelta

from django.contrib.gis.db.models.functions import AsGeoJSON
from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import cache_page

from ..models import Region, District, Farmland, NdviBaseline, VegetationIndex
from ._helpers import MODIS_SATELLITES, _satellite_filter, rate_limit


def api_regions(request: HttpRequest) -> JsonResponse:
    """GeoJSON FeatureCollection of regions (simplified geometry).
    Optional ?id=<pk> to return a single region."""
    qs = Region.objects.all()
    region_id = request.GET.get('id')
    if region_id:
        try:
            qs = qs.filter(pk=int(region_id))
        except (TypeError, ValueError):
            pass
    rows = qs.annotate(geojson=AsGeoJSON('geom', precision=5)).values('id', 'name', 'code', 'geojson')
    features = []
    for r in rows:
        features.append({
            'type': 'Feature',
            'properties': {'id': r['id'], 'name': r['name'], 'code': r['code']},
            'geometry': json.loads(r['geojson']),
        })
    return JsonResponse({'type': 'FeatureCollection', 'features': features})


def api_districts(request: HttpRequest) -> JsonResponse:
    """GeoJSON districts filtered by region."""
    region_id = request.GET.get('region')
    if not region_id:
        return JsonResponse({'type': 'FeatureCollection', 'features': []})
    try:
        qs = District.objects.filter(region_id=int(region_id))
    except (TypeError, ValueError):
        return JsonResponse({'type': 'FeatureCollection', 'features': []})

    rows = qs.annotate(geojson=AsGeoJSON('geom', precision=5)).values('id', 'name', 'code', 'geojson')
    features = []
    for r in rows:
        features.append({
            'type': 'Feature',
            'properties': {'id': r['id'], 'name': r['name'], 'code': r['code']},
            'geometry': json.loads(r['geojson']),
        })
    return JsonResponse({'type': 'FeatureCollection', 'features': features})


@rate_limit('20/m')
@cache_page(60 * 60)
def api_districts_status(request: HttpRequest) -> JsonResponse:
    """All-Russia FeatureCollection of districts with current NDVI vs baseline.

    For each district: the latest available MODIS 16-day composite (within
    the last ~60 days) is aggregated as area-weighted mean of farmland NDVI
    on that date. Baseline is taken from ``NdviBaseline`` at the matching
    day-of-year (with ±8/±16 fallback to handle composite drift), and
    ``pct_of_baseline = current / baseline * 100``. Districts without data
    are returned with ``pct_of_baseline = null`` so the frontend can colour
    them grey.

    Geometry is simplified server-side via ``AsGeoJSON(precision=4)`` to keep
    the payload reasonable (≈2200 districts × geom). Response is cached for
    1 hour because the underlying data changes only every ~16 days.
    """
    cutoff = date.today() - timedelta(days=60)

    # Single pass over recent VI rows. We need: per-district latest date and
    # the area-weighted NDVI on that date.
    vi_qs = (
        VegetationIndex.objects
        .filter(
            index_type='ndvi',
            scene__satellite__in=MODIS_SATELLITES,
            is_outlier=False,
            mean__gte=-0.2, mean__lte=1,
            acquired_date__gte=cutoff,
        )
        .values_list(
            'acquired_date', 'mean', 'farmland__district_id', 'farmland__area_ha',
        )
    )

    # (district_id, acquired_date) -> {sum_ndvi*area, sum_area}
    per_dd = defaultdict(lambda: {'sum_w': 0.0, 'sum_a': 0.0})
    for acq_date, mean_v, did, area_ha in vi_qs.iterator(chunk_size=10000):
        if did is None or mean_v is None or area_ha is None:
            continue
        af = float(area_ha)
        if af <= 0:
            continue
        bucket = per_dd[(did, acq_date)]
        bucket['sum_w'] += float(mean_v) * af
        bucket['sum_a'] += af

    # Per district: pick the latest date with data
    latest = {}
    for (did, d), acc in per_dd.items():
        if acc['sum_a'] <= 0:
            continue
        cur = acc['sum_w'] / acc['sum_a']
        prev = latest.get(did)
        if prev is None or d > prev['date']:
            latest[did] = {'date': d, 'mean': cur}

    # Baseline (region-agnostic, all districts at once)
    bl_lookup = defaultdict(dict)
    for b in NdviBaseline.objects.filter(crop_type='').values(
        'district_id', 'day_of_year', 'mean_ndvi',
    ).iterator(chunk_size=10000):
        bl_lookup[b['district_id']][b['day_of_year']] = b['mean_ndvi']

    def _baseline_for(did, doy):
        m = bl_lookup.get(did)
        if not m:
            return None
        # Exact, then ±8/±16 to tolerate MODIS biweekly composite drift
        for off in (0, 8, -8, 16, -16):
            v = m.get(doy + off)
            if v is not None:
                return v
        return None

    rows = (
        District.objects
        .annotate(geojson=AsGeoJSON('geom', precision=4))
        .values('id', 'name', 'region__name', 'geojson')
    )

    features = []
    for r in rows:
        if not r['geojson']:
            continue
        cur_info = latest.get(r['id'])
        props = {
            'id': r['id'],
            'name': r['name'],
            'region': r['region__name'],
            'current_ndvi': None,
            'current_date': None,
            'baseline_ndvi': None,
            'pct_of_baseline': None,
        }
        if cur_info is not None:
            cur_d = cur_info['date']
            cur_v = cur_info['mean']
            doy = cur_d.timetuple().tm_yday
            bl = _baseline_for(r['id'], doy)
            props['current_ndvi'] = round(cur_v, 3)
            props['current_date'] = str(cur_d)
            if bl is not None and bl > 0.05:
                props['baseline_ndvi'] = round(bl, 3)
                props['pct_of_baseline'] = round(cur_v / bl * 100.0, 1)
        features.append({
            'type': 'Feature',
            'properties': props,
            'geometry': json.loads(r['geojson']),
        })

    return JsonResponse({'type': 'FeatureCollection', 'features': features})


@rate_limit('60/m')
def api_farmlands(request: HttpRequest) -> JsonResponse:
    """GeoJSON farmlands for a single district. For region overview use MVT tiles."""
    district_id = request.GET.get('district')
    if not district_id:
        return JsonResponse({'type': 'FeatureCollection', 'features': []})

    try:
        qs = Farmland.objects.filter(district_id=int(district_id))
    except (TypeError, ValueError):
        return JsonResponse({'type': 'FeatureCollection', 'features': []})

    # Get latest NDVI mean per farmland for coloring
    source = request.GET.get('source')  # 'modis', 'raster', or empty
    latest_ndvi = {}
    ndvi_rows = (
        VegetationIndex.objects
        .filter(farmland__in=qs, index_type='ndvi', **_satellite_filter(source))
        .order_by('farmland_id', '-acquired_date')
        .distinct('farmland_id')
        .values('farmland_id', 'mean', 'acquired_date')
    )
    for nr in ndvi_rows:
        latest_ndvi[nr['farmland_id']] = {'mean': nr['mean'], 'date': str(nr['acquired_date'])}

    rows = qs.annotate(
        geojson=AsGeoJSON('geom', precision=6)
    ).values(
        'id', 'crop_type', 'area_ha', 'cadastral_number', 'district__name', 'geojson',
    )

    crop_labels = dict(Farmland.CropType.choices)
    features = []
    for r in rows:
        ndvi_info = latest_ndvi.get(r['id'])
        props = {
            'id': r['id'],
            'crop_type': r['crop_type'],
            'crop_type_label': crop_labels.get(r['crop_type'], r['crop_type']),
            'area_ha': round(r['area_ha'], 2),
            'cadastral': r['cadastral_number'],
            'district': r['district__name'],
        }
        if ndvi_info:
            props['ndvi'] = round(ndvi_info['mean'], 3)
            props['ndvi_date'] = ndvi_info['date']
        geom = json.loads(r['geojson']) if r['geojson'] else None
        if geom:
            features.append({
                'type': 'Feature',
                'properties': props,
                'geometry': geom,
            })

    return JsonResponse({'type': 'FeatureCollection', 'features': features})
