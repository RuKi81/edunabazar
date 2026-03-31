import json

from django.db import connection
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.db.models import Count, Sum, Avg, Q

from .models import Region, District, Farmland, VegetationIndex


def _get_legacy_user(request):
    """Reuse legacy session auth."""
    from legacy.models import LegacyUser
    uid = request.session.get('legacy_user_id')
    if not uid:
        return None
    try:
        return LegacyUser.objects.get(pk=int(uid))
    except LegacyUser.DoesNotExist:
        return None


def dashboard(request: HttpRequest) -> HttpResponse:
    """Main Agrocosmos map page."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')

    districts = District.objects.none()
    if region_id:
        try:
            districts = District.objects.filter(region_id=int(region_id)).order_by('name')
        except (TypeError, ValueError):
            pass

    # Summary stats
    farmland_qs = Farmland.objects.all()
    if district_id:
        try:
            farmland_qs = farmland_qs.filter(district_id=int(district_id))
        except (TypeError, ValueError):
            pass
    elif region_id:
        try:
            farmland_qs = farmland_qs.filter(district__region_id=int(region_id))
        except (TypeError, ValueError):
            pass

    summary = farmland_qs.aggregate(
        total_count=Count('id'),
        total_area=Sum('area_ha'),
    )

    crop_stats = (
        farmland_qs
        .values('crop_type')
        .annotate(cnt=Count('id'), area=Sum('area_ha'))
        .order_by('-area')
    )

    return render(request, 'agrocosmos/dashboard.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'summary': summary,
        'crop_stats': list(crop_stats),
        'crop_type_labels': dict(Farmland.CropType.choices),
    })


# ── GeoJSON API endpoints ──────────────────────────────────────────

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
    latest_ndvi = {}
    ndvi_rows = (
        VegetationIndex.objects
        .filter(farmland__in=qs, index_type='ndvi')
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


def _tile_bbox(z, x, y):
    """Convert tile coords to EPSG:3857 bounding box."""
    import math
    n = 2.0 ** z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    # Convert to EPSG:3857
    def to_3857(lon, lat):
        x = lon * 20037508.34 / 180.0
        y = math.log(math.tan((90 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
        y = y * 20037508.34 / 180.0
        return x, y
    xmin, ymin = to_3857(lon_min, lat_min)
    xmax, ymax = to_3857(lon_max, lat_max)
    return xmin, ymin, xmax, ymax


def api_tile(request: HttpRequest, z: int, x: int, y: int) -> HttpResponse:
    """Mapbox Vector Tile (MVT) endpoint for farmland polygons.
    Uses PostGIS ST_AsMVT for on-the-fly tile generation.
    """
    import logging
    logger = logging.getLogger('agrocosmos')

    region_id = request.GET.get('region')
    district_id = request.GET.get('district')

    where_clauses = []
    params = []

    if district_id:
        try:
            where_clauses.append("f.district_id = %s")
            params.append(int(district_id))
        except (TypeError, ValueError):
            pass
    elif region_id:
        try:
            where_clauses.append("d.region_id = %s")
            params.append(int(region_id))
        except (TypeError, ValueError):
            pass

    where_sql = ("AND " + " AND ".join(where_clauses)) if where_clauses else ""

    xmin, ymin, xmax, ymax = _tile_bbox(z, x, y)

    sql = f"""
        WITH
        bounds AS (
            SELECT ST_MakeEnvelope(%s, %s, %s, %s, 3857) AS envelope
        ),
        tile_data AS (
            SELECT
                f.id,
                f.crop_type,
                f.area_ha,
                f.cadastral_number,
                d.name AS district,
                ST_AsMVTGeom(
                    ST_Transform(f.geom, 3857),
                    b.envelope,
                    4096,
                    256,
                    true
                ) AS geom
            FROM agro_farmland f
            JOIN agro_district d ON d.id = f.district_id
            CROSS JOIN bounds b
            WHERE f.geom && ST_Transform(b.envelope, 4326)
            {where_sql}
        )
        SELECT ST_AsMVT(tile_data, 'farmlands', 4096, 'geom')
        FROM tile_data
        WHERE geom IS NOT NULL;
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, [xmin, ymin, xmax, ymax] + params)
            row = cursor.fetchone()
            raw = row[0] if row and row[0] else b''
            # psycopg may return memoryview
            tile_bytes = bytes(raw) if not isinstance(raw, bytes) else raw
    except Exception as e:
        logger.error('MVT tile error z=%s x=%s y=%s: %s', z, x, y, e)
        tile_bytes = b''

    return HttpResponse(
        tile_bytes,
        content_type='application/x-protobuf',
    )


def api_farmland_ndvi(request: HttpRequest) -> JsonResponse:
    """NDVI time series for a single farmland."""
    farmland_id = request.GET.get('farmland')
    if not farmland_id:
        return JsonResponse({'ok': False, 'error': 'farmland required'}, status=400)
    try:
        fid = int(farmland_id)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid farmland'}, status=400)

    rows = (
        VegetationIndex.objects
        .filter(farmland_id=fid, index_type='ndvi')
        .order_by('acquired_date')
        .values('acquired_date', 'mean', 'min_val', 'max_val', 'median')
    )
    data = []
    for r in rows:
        data.append({
            'date': str(r['acquired_date']),
            'mean': round(r['mean'], 4),
            'min': round(r['min_val'], 4),
            'max': round(r['max_val'], 4),
            'median': round(r['median'], 4),
        })
    return JsonResponse({'ok': True, 'data': data})
