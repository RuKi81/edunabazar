"""GeoJSON API endpoints: regions, districts, farmlands."""
import json
import logging
import time

from django.contrib.gis.db.models.functions import AsGeoJSON
from django.core.cache import cache
from django.http import HttpRequest, HttpResponseNotModified, JsonResponse

from ..models import Region, District, Farmland, VegetationIndex
from ..services import districts_status_geojson
from ..services.districts_status_geojson import _SimplifyPreserveTopology
from ._helpers import _satellite_filter, rate_limit

logger = logging.getLogger(__name__)


# Static admin-boundary geometries are cached in Redis. Both keys are
# versioned (``:v3``) so a tolerance/precision change in code automatically
# invalidates the cache without manual ``cache.clear()`` on deploy.
_REGIONS_CACHE_KEY   = 'agro:regions:geojson:v3'
_DISTRICTS_CACHE_KEY = 'agro:districts:geojson:v3:region={region_id}'
_GEOJSON_CACHE_TTL   = 60 * 60 * 24 * 7  # 1 week — these geometries do not change

# Tolerances picked to match the existing choropleth (0.01° ≈ 1 km) for the
# country overview, and a tighter 0.005° (≈ 500 m) within a region. Precision
# 3 is plenty for Leaflet rendering at any zoom we expose on the map.
_REGION_SIMPLIFY_TOL   = 0.01
_DISTRICT_SIMPLIFY_TOL = 0.005
_GEOJSON_PRECISION     = 4


def _build_regions_payload(region_id: int | None) -> dict:
    qs = Region.objects.all()
    if region_id is not None:
        qs = qs.filter(pk=region_id)
    rows = qs.annotate(
        geojson=AsGeoJSON(
            _SimplifyPreserveTopology('geom', _REGION_SIMPLIFY_TOL),
            precision=_GEOJSON_PRECISION,
        ),
    ).values('id', 'name', 'code', 'geojson')
    features = []
    for r in rows:
        if not r['geojson']:
            continue
        features.append({
            'type': 'Feature',
            'properties': {'id': r['id'], 'name': r['name'], 'code': r['code']},
            'geometry': json.loads(r['geojson']),
        })
    return {'type': 'FeatureCollection', 'features': features}


def api_regions(request: HttpRequest) -> JsonResponse:
    """GeoJSON FeatureCollection of regions (topology-simplified).

    Previously this endpoint claimed to return simplified geometry but
    only trimmed coordinate precision (``AsGeoJSON(precision=5)``),
    producing a ~20 MB payload that took ~6 s to serialise and blocked
    page load. We now call ``ST_SimplifyPreserveTopology`` with a 1 km
    tolerance and cache the full FeatureCollection in Redis for a week
    — the geometries are static.

    Optional ``?id=<pk>`` returns a single region (also cached, but
    bypassing the all-regions key so the warm full payload is reused
    in-place when callers want everything).
    """
    region_id_raw = request.GET.get('id')
    region_id: int | None = None
    if region_id_raw:
        try:
            region_id = int(region_id_raw)
        except (TypeError, ValueError):
            region_id = None

    overall_t = time.time()
    if region_id is None:
        payload = cache.get(_REGIONS_CACHE_KEY)
        if payload is None:
            payload = _build_regions_payload(None)
            cache.set(_REGIONS_CACHE_KEY, payload, _GEOJSON_CACHE_TTL)
            logger.info(
                'api_regions: cold rebuild  features=%d  %.2fs',
                len(payload['features']), time.time() - overall_t,
            )
    else:
        # Single-region fetches are rare and tiny; serve them out of the
        # cached full payload to avoid a separate cache entry per id.
        full = cache.get(_REGIONS_CACHE_KEY)
        if full is None:
            full = _build_regions_payload(None)
            cache.set(_REGIONS_CACHE_KEY, full, _GEOJSON_CACHE_TTL)
        payload = {
            'type': 'FeatureCollection',
            'features': [
                f for f in full['features']
                if (f.get('properties') or {}).get('id') == region_id
            ],
        }
    return JsonResponse(payload)


def _build_districts_payload(region_id: int) -> dict:
    rows = (
        District.objects
        .filter(region_id=region_id)
        .annotate(geojson=AsGeoJSON(
            _SimplifyPreserveTopology('geom', _DISTRICT_SIMPLIFY_TOL),
            precision=_GEOJSON_PRECISION,
        ))
        .values('id', 'name', 'code', 'geojson')
    )
    features = []
    for r in rows:
        if not r['geojson']:
            continue
        features.append({
            'type': 'Feature',
            'properties': {'id': r['id'], 'name': r['name'], 'code': r['code']},
            'geometry': json.loads(r['geojson']),
        })
    return {'type': 'FeatureCollection', 'features': features}


def api_districts(request: HttpRequest) -> JsonResponse:
    """GeoJSON districts within a region (topology-simplified, cached).

    Same fix as ``api_regions`` — switch from coord-precision-only to
    ``ST_SimplifyPreserveTopology`` (500 m tolerance) + Redis cache.
    """
    region_id_raw = request.GET.get('region')
    if not region_id_raw:
        return JsonResponse({'type': 'FeatureCollection', 'features': []})
    try:
        region_id = int(region_id_raw)
    except (TypeError, ValueError):
        return JsonResponse({'type': 'FeatureCollection', 'features': []})

    cache_key = _DISTRICTS_CACHE_KEY.format(region_id=region_id)
    payload = cache.get(cache_key)
    if payload is None:
        overall_t = time.time()
        payload = _build_districts_payload(region_id)
        cache.set(cache_key, payload, _GEOJSON_CACHE_TTL)
        logger.info(
            'api_districts: cold rebuild region=%d features=%d  %.2fs',
            region_id, len(payload['features']), time.time() - overall_t,
        )
    return JsonResponse(payload)


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

    # Optional ``?region=<id>`` filter: re-use the cached all-Russia
    # FeatureCollection by sub-setting it in memory (~2300 dict checks,
    # microseconds) so the per-region choropleth shares the same warm
    # cache as the all-regions view. No second heavy PostGIS query.
    region_id = request.GET.get('region')
    if region_id:
        try:
            r_id = int(region_id)
        except (TypeError, ValueError):
            r_id = None
        if r_id is not None:
            features = [
                f for f in payload.get('features', [])
                if (f.get('properties') or {}).get('region_id') == r_id
            ]
            payload = {'type': 'FeatureCollection', 'features': features}

    # Browser-side caching via ETag.
    #
    # The choropleth payload only changes once a day, when the MODIS
    # pipeline finishes and ``recompute_district_ndvi_status`` rotates
    # the per-district status table. Within that window the payload is
    # byte-identical for every visitor → ideal for an HTTP cache.
    #
    # ETag fingerprint = (max latest_date across districts,
    #                     features count, region filter).
    # Changes the moment fresh NDVI data is published; otherwise stable.
    # We use a *weak* ETag (W/) because JsonResponse may add whitespace
    # variations across Django versions and we only need semantic match.
    features = payload.get('features', [])
    latest = ''
    for f in features:
        d = (f.get('properties') or {}).get('current_date') or ''
        if d > latest:
            latest = d
    etag = 'W/"agro-ds-{date}-{n}-{rg}"'.format(
        date=latest or 'none',
        n=len(features),
        rg=region_id or 'all',
    )
    if request.headers.get('If-None-Match') == etag:
        # 304 ≈ 100 bytes, RTT-bound. Saves the entire 1 MB transfer
        # *and* the JsonResponse encode cost on the server.
        resp = HttpResponseNotModified()
        resp['ETag'] = etag
        resp['Cache-Control'] = 'public, max-age=3600, stale-while-revalidate=86400'
        logger.info(
            'api_districts_status: 304 not-modified region=%s in %.3fs',
            region_id or '-', time.time() - overall_t,
        )
        return resp

    logger.info(
        'api_districts_status: features=%d  total=%.3fs region=%s',
        len(features), time.time() - overall_t, region_id or '-',
    )
    resp = JsonResponse(payload)
    resp['ETag'] = etag
    # Public so an upstream CDN/Nginx can also cache it. 1-hour fresh
    # window covers the typical user session; ``stale-while-revalidate``
    # lets the browser serve the stale copy instantly on the next visit
    # while it revalidates in the background.
    resp['Cache-Control'] = 'public, max-age=3600, stale-while-revalidate=86400'
    return resp


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
