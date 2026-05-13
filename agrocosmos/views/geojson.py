"""GeoJSON API endpoints: regions, districts, farmlands."""
import json
import logging
import time

from django.contrib.gis.db.models.functions import AsGeoJSON
from django.core.cache import cache
from django.http import HttpRequest, HttpResponse, HttpResponseNotModified, JsonResponse

from ..models import Region, District, Farmland, VegetationIndex
from ..services import districts_status_geojson
from ..services.districts_status_geojson import _SimplifyPreserveTopology
from ._helpers import _satellite_filter, rate_limit

logger = logging.getLogger(__name__)


# Static admin-boundary geometries are cached in Redis. Both keys are
# versioned (``:v4``) so a tolerance/precision change in code automatically
# invalidates the cache without manual ``cache.clear()`` on deploy.
_REGIONS_CACHE_KEY   = 'agro:regions:geojson:v4'
_DISTRICTS_CACHE_KEY = 'agro:districts:geojson:v4:region={region_id}'
_GEOJSON_CACHE_TTL   = 60 * 60 * 24 * 7  # 1 week — these geometries do not change

# Tolerances tightened after ETag/browser caching was added: payload is now
# only paid once per visitor per day, so we can afford crisper outlines.
# 0.002° (≈ 200 m) for regions and 0.001° (≈ 100 m) for districts within a
# region — both well below one screen pixel at the zoom levels they render
# at, so the boundary looks like the true geometry to the eye.
_REGION_SIMPLIFY_TOL   = 0.002
_DISTRICT_SIMPLIFY_TOL = 0.001
_GEOJSON_PRECISION     = 4


def _conditional_json(
    request: HttpRequest,
    payload: dict,
    *,
    etag: str,
    cache_control: str,
):
    """Return a JsonResponse with ETag/Cache-Control, honouring If-None-Match.

    Centralises the HTTP-cache plumbing so each GeoJSON endpoint just has
    to pass a stable fingerprint string and a freshness policy. On a
    cache hit on the client side we return ``304 Not Modified`` (~50
    bytes, no body, no JSON encode), otherwise the full JsonResponse.
    """
    if request.headers.get('If-None-Match') == etag:
        resp = HttpResponseNotModified()
        resp['ETag'] = etag
        resp['Cache-Control'] = cache_control
        return resp
    resp = JsonResponse(payload)
    resp['ETag'] = etag
    resp['Cache-Control'] = cache_control
    return resp


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
    # Region geometries are static (cache key bumps on schema changes),
    # so a strong long-lived browser cache is safe. The fingerprint is
    # tied to the Redis cache key version + filter so it auto-rotates
    # whenever simplification settings change.
    etag = 'W/"{key}-{rg}-{n}"'.format(
        key=_REGIONS_CACHE_KEY,
        rg=region_id if region_id is not None else 'all',
        n=len(payload.get('features', [])),
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=604800, stale-while-revalidate=86400',
    )


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
    etag = 'W/"{key}-{n}"'.format(
        key=cache_key,
        n=len(payload.get('features', [])),
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=604800, stale-while-revalidate=86400',
    )


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
        # Per-date snapshots are immutable: a 16-day MODIS composite,
        # once published, never changes. Long-lived strong cache, and
        # the ETag fingerprint includes the date so any dates-list
        # revision still revalidates correctly.
        etag = 'W/"agro-snap-v2-{d}-{n}"'.format(
            d=payload.get('date') or target_date,
            n=len(payload.get('districts') or {}),
        )
        return _conditional_json(
            request, payload,
            etag=etag,
            cache_control='public, max-age=86400, stale-while-revalidate=604800',
        )

    # Dates list mode
    try:
        year = int(year_param) if year_param else _date.today().year
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year'}, status=400)
    dates = districts_status_geojson.list_available_dates(year)
    payload = {'ok': True, 'year': year, 'dates': dates}
    # Dates list grows by one entry every 16 days. Hourly browser cache
    # is plenty; the fingerprint flips the moment a new composite lands.
    etag = 'W/"agro-dates-v1-{y}-{n}-{last}"'.format(
        y=year,
        n=len(dates),
        last=(dates[-1] if dates else 'none'),
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=3600, stale-while-revalidate=86400',
    )


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
    region_id = request.GET.get('region')
    r_id: int | None = None
    if region_id:
        try:
            r_id = int(region_id)
        except (TypeError, ValueError):
            r_id = None

    # Fast path: no ?region= filter → serve the pre-encoded JSON bytes
    # directly. Bypasses both pickle.loads of the 3 MB dict (~300 ms)
    # and the JsonResponse json.dumps re-encoding (~1.5 s) — roughly
    # 50× speedup on the hot path measured on prod (TTFB 2.7 s → 50 ms).
    #
    # Cache invariant: ``refresh_cache`` always writes both the dict
    # and this pre-encoded blob atomically, so the ETag in the blob
    # is in sync with the data the slow path would compute.
    if r_id is None:
        etag, body = districts_status_geojson.get_fast_blob()
        if request.headers.get('If-None-Match') == etag:
            resp = HttpResponseNotModified()
        else:
            resp = HttpResponse(body, content_type='application/json')
        resp['ETag'] = etag
        resp['Cache-Control'] = 'public, max-age=3600, stale-while-revalidate=86400'
        logger.info(
            'api_districts_status: fast-blob region=- in %.3fs',
            time.time() - overall_t,
        )
        return resp

    # Slow path (per-region filter): unpickle the dict, sub-set it in
    # memory (~2300 dict checks, microseconds), encode the slice. Per-
    # region requests are rare enough that pre-encoding 86 separate
    # blobs would waste Redis memory for little benefit.
    payload = districts_status_geojson.get_or_build()
    features = [
        f for f in payload.get('features', [])
        if (f.get('properties') or {}).get('region_id') == r_id
    ]
    payload = {'type': 'FeatureCollection', 'features': features}

    # ETag for the per-region slice — fingerprint matches the all-Russia
    # one but with the region id substituted, so 304s are scoped per
    # region without colliding with the unfiltered cache entry.
    latest = ''
    for f in features:
        d = (f.get('properties') or {}).get('current_date') or ''
        if d > latest:
            latest = d
    etag = 'W/"agro-ds-{date}-{n}-{rg}"'.format(
        date=latest or 'none',
        n=len(features),
        rg=r_id,
    )
    logger.info(
        'api_districts_status: slow-path features=%d total=%.3fs region=%s',
        len(features), time.time() - overall_t, r_id,
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=3600, stale-while-revalidate=86400',
    )


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
