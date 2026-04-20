"""Tile serving endpoints: MVT vector tiles for farmlands + NDVI raster PNG tiles."""
import logging
import math

from django.db import connection
from django.http import HttpRequest, HttpResponse
from django.views.decorators.cache import cache_page

from ._helpers import rate_limit


def _tile_bbox(z, x, y):
    """Convert tile coords to EPSG:3857 bounding box."""
    n = 2.0 ** z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))

    def to_3857(lon, lat):
        x_m = lon * 20037508.34 / 180.0
        y_m = math.log(math.tan((90 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
        y_m = y_m * 20037508.34 / 180.0
        return x_m, y_m

    xmin, ymin = to_3857(lon_min, lat_min)
    xmax, ymax = to_3857(lon_max, lat_max)
    return xmin, ymin, xmax, ymax


@rate_limit('300/m', binary=True)
@cache_page(60 * 10)  # 10 min in Redis
def api_tile(request: HttpRequest, z: int, x: int, y: int) -> HttpResponse:
    """Mapbox Vector Tile (MVT) endpoint for farmland polygons.
    Uses PostGIS ST_AsMVT for on-the-fly tile generation.
    """
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
                COALESCE(f.properties->>'Fact_isp', '') AS fact_isp,
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


@rate_limit('300/m', binary=True)
def api_raster_tile(request: HttpRequest, z: int, x: int, y: int) -> HttpResponse:
    """Serve NDVI pseudocolor PNG tile from a GeoTIFF composite.

    Query params:
        sensor: 's2' or 'l8'
        scope: region/district scope ID, e.g. 'd1' or '37'
        date: 'YYYY-MM-DD_YYYY-MM-DD'
    """
    from ..services.raster_tiles import find_raster_path, render_tile

    sensor = request.GET.get('sensor', 's2')
    scope = request.GET.get('scope', '')
    date_range = request.GET.get('date', '')

    if not scope or not date_range:
        return HttpResponse(b'', content_type='image/png', status=204)

    tif_path = find_raster_path(sensor, scope, date_range)
    if not tif_path:
        return HttpResponse(b'', content_type='image/png', status=204)

    png_bytes = render_tile(tif_path, z, x, y)
    if not png_bytes:
        return HttpResponse(b'', content_type='image/png', status=204)

    resp = HttpResponse(png_bytes, content_type='image/png')
    resp['Cache-Control'] = 'public, max-age=3600'
    return resp
