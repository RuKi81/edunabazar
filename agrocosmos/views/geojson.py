"""GeoJSON API endpoints: regions, districts, farmlands."""
import json
import logging
import time

from django.contrib.gis.db.models.functions import AsGeoJSON
from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import cache_page

from ..models import Region, District, Farmland, VegetationIndex
from ._helpers import _satellite_filter, rate_limit

logger = logging.getLogger(__name__)


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

    Reads from the precomputed cache table ``agro_district_ndvi_status``
    (one row per district), populated by the management command
    ``recompute_district_ndvi_status`` at the tail of the MODIS pipeline.

    Computing this on-the-fly is infeasible: ``agro_vegetation_index``
    holds ~25M MODIS rows in any 60-day window, and even a single
    per-district aggregation takes >70 seconds. The cache table makes the
    endpoint ~constant-time (a single LEFT JOIN District ⟕ status with
    geometry serialisation).

    Geometry is simplified server-side via ``AsGeoJSON(precision=4)`` to
    keep the payload reasonable (≈2300 districts × geom ≈ 5-10 MB).
    Response is additionally cached at the HTTP layer for 1 hour.
    """
    overall_t = time.time()

    # Single query: every district + (optional) latest status. Districts
    # without a precomputed row appear in the response as `pct_of_baseline=null`
    # → coloured grey on the frontend, signalling "no recent data".
    rows = (
        District.objects
        .annotate(geojson=AsGeoJSON('geom', precision=4))
        .values(
            'id', 'name', 'region__name', 'geojson',
            'ndvi_status__latest_date',
            'ndvi_status__current_ndvi',
            'ndvi_status__baseline_ndvi',
            'ndvi_status__pct_of_baseline',
        )
    )

    features = []
    with_data = 0
    for r in rows.iterator(chunk_size=500):
        if not r['geojson']:
            continue
        cur_v = r['ndvi_status__current_ndvi']
        cur_d = r['ndvi_status__latest_date']
        bl_v = r['ndvi_status__baseline_ndvi']
        pct = r['ndvi_status__pct_of_baseline']
        if cur_v is not None:
            with_data += 1
        features.append({
            'type': 'Feature',
            'properties': {
                'id': r['id'],
                'name': r['name'],
                'region': r['region__name'],
                'current_ndvi': round(cur_v, 3) if cur_v is not None else None,
                'current_date': str(cur_d) if cur_d else None,
                'baseline_ndvi': round(bl_v, 3) if bl_v is not None else None,
                'pct_of_baseline': pct,
            },
            'geometry': json.loads(r['geojson']),
        })

    logger.info(
        'api_districts_status: districts=%d  with_data=%d  total=%.2fs',
        len(features), with_data, time.time() - overall_t,
    )
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
