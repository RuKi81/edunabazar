"""GeoJSON API endpoints: regions, districts, farmlands."""
import json
import logging
import time

from django.contrib.gis.db.models.functions import AsGeoJSON
from django.http import HttpRequest, JsonResponse

from ..models import Region, District, Farmland, VegetationIndex
from ..services import districts_status_geojson
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


@rate_limit('60/m')
def api_districts_status_timeline(request: HttpRequest) -> JsonResponse:
    """Timeline support for the all-Russia choropleth.

    Two modes:

    * ``?year=YYYY`` (no ``date``) — return the list of MODIS composite
      dates available within the year. Used by the dashboard to populate
      the timeline slider.
    * ``?date=YYYY-MM-DD`` — return a per-district NDVI snapshot for that
      composite date (``current_ndvi``, ``baseline_ndvi``,
      ``pct_of_baseline``). Used by the slider's ``change`` handler to
      recolour the choropleth in place.

    Both modes are cached (1 h for the dates list, eternally per date for
    snapshots — past biweekly composites are immutable).
    """
    from datetime import date as _date

    target_date = request.GET.get('date')
    year_param = request.GET.get('year')

    if target_date:
        try:
            payload = districts_status_geojson.build_snapshot(target_date)
        except ValueError:
            return JsonResponse(
                {'ok': False, 'error': 'invalid date'}, status=400,
            )
        # Surface the dates list for the same year so the slider can be
        # re-populated without a second round-trip.
        try:
            year = _date.fromisoformat(payload['date']).year
            payload['dates'] = districts_status_geojson.list_available_dates(year)
        except Exception:
            payload['dates'] = []
        payload['ok'] = True
        return JsonResponse(payload)

    # Dates list mode
    try:
        year = int(year_param) if year_param else _date.today().year
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year'}, status=400)
    dates = districts_status_geojson.list_available_dates(year)
    return JsonResponse({'ok': True, 'year': year, 'dates': dates})


@rate_limit('20/m')
def api_districts_status(request: HttpRequest) -> JsonResponse:
    """All-Russia FeatureCollection of districts with current NDVI vs baseline.

    The heavy GeoJSON build (district geometries + simplification +
    serialisation, ~20 s) is cached eternally in Redis under a dedicated
    key by ``agrocosmos.services.districts_status_geojson``. The cached
    payload is rotated:

    1. by ``recompute_district_ndvi_status`` after each NDVI refresh
       (daily, at the tail of the MODIS pipeline), and
    2. by the ``prewarm_agro_caches`` management command on deploy.

    Therefore this view is sub-millisecond on the hot path and never
    hammers PostgreSQL — keeping the choropleth from blocking gunicorn
    workers when traffic bursts (which previously took the site down).
    """
    overall_t = time.time()
    payload = districts_status_geojson.get_or_build()
    logger.info(
        'api_districts_status: features=%d  total=%.3fs',
        len(payload.get('features', [])), time.time() - overall_t,
    )
    return JsonResponse(payload)


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
