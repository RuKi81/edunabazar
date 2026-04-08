import json
import math

from django.db import connection
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.contrib.gis.db.models.functions import AsGeoJSON
from datetime import date

from django.db.models import Count, Sum, Avg, Q
from django.views.decorators.cache import cache_page

from .models import Region, District, Farmland, VegetationIndex, MonitoringTask


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

    # Available years: from NDVI data + current year
    current_year = date.today().year
    data_years = (
        VegetationIndex.objects
        .filter(index_type='ndvi')
        .values_list('acquired_date__year', flat=True)
        .distinct()
        .order_by('-acquired_date__year')
    )
    years = sorted(set(list(data_years) + [current_year]), reverse=True)

    return render(request, 'agrocosmos/dashboard.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'summary': summary,
        'crop_stats': list(crop_stats),
        'crop_type_labels': dict(Farmland.CropType.choices),
        'years': years,
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


@cache_page(60 * 10)  # 10 min in Redis
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

    resp = HttpResponse(tile_bytes, content_type='application/x-protobuf')
    resp['Cache-Control'] = 'public, max-age=600'
    resp['Access-Control-Allow-Origin'] = '*'
    return resp


def api_farmland_ndvi(request: HttpRequest) -> JsonResponse:
    """NDVI time series for a single farmland. Optional ?year=2025 filter."""
    farmland_id = request.GET.get('farmland')
    if not farmland_id:
        return JsonResponse({'ok': False, 'error': 'farmland required'}, status=400)
    try:
        fid = int(farmland_id)
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid farmland'}, status=400)

    qs = VegetationIndex.objects.filter(
        farmland_id=fid, index_type='ndvi',
        mean__gte=-1, mean__lte=1,
    )

    year = request.GET.get('year')
    if year:
        try:
            qs = qs.filter(acquired_date__year=int(year))
        except (TypeError, ValueError):
            pass

    rows = qs.order_by('acquired_date').values(
        'acquired_date', 'mean', 'min_val', 'max_val', 'median',
    )
    data = []
    for r in rows:
        data.append({
            'date': str(r['acquired_date']),
            'mean': _safe_round(r['mean']),
            'min': _safe_round(r['min_val']),
            'max': _safe_round(r['max_val']),
            'median': _safe_round(r['median']),
        })
    return JsonResponse({'ok': True, 'data': data})


def _safe_round(val, precision=4):
    """Round a float safely, returning 0 for None/NaN/Inf."""
    if val is None:
        return 0.0
    try:
        if math.isnan(val) or math.isinf(val):
            return 0.0
    except TypeError:
        return 0.0
    return round(val, precision)


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

    # Apply crop_types filter
    if crop_types:
        ct_list = [ct.strip() for ct in crop_types.split(',') if ct.strip()]
        if ct_list:
            fl_qs = fl_qs.filter(crop_type__in=ct_list)

    vi_qs = VegetationIndex.objects.filter(
        farmland__in=fl_qs, index_type='ndvi',
        mean__gte=-1, mean__lte=1,          # exclude NaN / Inf
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

    # Stats by crop type (average of all periods)
    by_crop = (
        vi_qs
        .values('farmland__crop_type')
        .annotate(
            count=Count('farmland_id', distinct=True),
            mean_ndvi=Avg('mean'),
        )
        .order_by('-mean_ndvi')
    )
    by_crop_list = []
    for row in by_crop:
        ct = row['farmland__crop_type']
        by_crop_list.append({
            'crop_type': ct,
            'label': crop_labels.get(ct, ct),
            'count': row['count'],
            'mean_ndvi': _safe_round(row['mean_ndvi']),
        })

    # Stats by period (time series, aggregated across all farmlands)
    by_period = (
        vi_qs
        .values('acquired_date')
        .annotate(
            mean_ndvi=Avg('mean'),
            count=Count('id'),
        )
        .order_by('acquired_date')
    )
    by_period_list = []
    for row in by_period:
        by_period_list.append({
            'date': str(row['acquired_date']),
            'mean_ndvi': _safe_round(row['mean_ndvi']),
            'count': row['count'],
        })

    # Summary
    total_fl = fl_qs.count()
    with_ndvi = vi_qs.values('farmland_id').distinct().count()
    avg = vi_qs.aggregate(avg=Avg('mean'))['avg']

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

    return JsonResponse({
        'ok': True,
        'stats': {
            'by_crop_type': by_crop_list,
            'by_period': by_period_list,
            'summary': {
                'total_farmlands': total_fl,
                'with_ndvi': with_ndvi,
                'mean_ndvi': _safe_round(avg),
            },
            'farmland_summary': fl_summary_list,
        },
    })
