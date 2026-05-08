"""Cached GeoJSON for the all-Russia districts NDVI choropleth.

This is the heavy part of ``api_districts_status`` — building a 4-7 MB
FeatureCollection out of ~2300 districts, each with a topology-preserved
simplified geometry. PostGIS ``ST_SimplifyPreserveTopology`` plus
``AsGeoJSON`` over the country bbox takes ~20 seconds on a cold cache,
which during traffic bursts pinned all gunicorn workers and effectively
took the site down.

The payload only changes when ``recompute_district_ndvi_status`` writes
new rows into ``agro_district_ndvi_status`` (once a day). So we
materialise the full FeatureCollection into Redis under a single key
with no TTL and only invalidate it from the recompute command. The view
just `cache.get()`s it — sub-millisecond read of an already-encoded
JSON byte string.

Falling back to an inline build is kept for the case where the cache
was flushed (e.g. ``redis-cli FLUSHDB`` after a Redis restart) and the
recompute command hasn't run yet.
"""
from __future__ import annotations

import json
import logging
import time

from django.contrib.gis.db.models import GeometryField
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.core.cache import cache
from django.db.models import F, Func, Value

from ..models import District

logger = logging.getLogger(__name__)


# Cache key. Bump the version suffix if the GeoJSON schema changes
# (extra properties, different precision, etc.) — old cached payloads
# from previous deploys then become unreachable and are eventually
# evicted by Redis LRU.
CACHE_KEY = 'agro:districts_status:geojson:v2'


class _SimplifyPreserveTopology(Func):
    """PostGIS ``ST_SimplifyPreserveTopology(geom, tolerance)``.

    Duplicates the helper in ``views/geojson.py`` to keep this module
    self-contained — both can be removed once Django ships a built-in.
    """
    function = 'ST_SimplifyPreserveTopology'
    output_field = GeometryField()

    def __init__(self, expression, tolerance):
        super().__init__(
            F(expression) if isinstance(expression, str) else expression,
            Value(tolerance),
        )


def build_geojson_payload() -> dict:
    """Run the heavy query and return a FeatureCollection dict.

    ~20 seconds on a cold PostgreSQL cache. Always reads from
    ``agro_district_ndvi_status`` (populated by the recompute command),
    so it does not itself touch the 25M-row vegetation index table.
    """
    overall_t = time.time()
    rows = (
        District.objects
        .annotate(geojson=AsGeoJSON(
            _SimplifyPreserveTopology('geom', 0.01),
            precision=3,
        ))
        .values(
            'id', 'name', 'region_id', 'region__name', 'geojson',
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
                'region_id': r['region_id'],
                'region': r['region__name'],
                'current_ndvi': round(cur_v, 3) if cur_v is not None else None,
                'current_date': str(cur_d) if cur_d else None,
                'baseline_ndvi': round(bl_v, 3) if bl_v is not None else None,
                'pct_of_baseline': pct,
            },
            'geometry': json.loads(r['geojson']),
        })

    logger.info(
        'districts_status_geojson.build: districts=%d with_data=%d in %.2fs',
        len(features), with_data, time.time() - overall_t,
    )
    return {'type': 'FeatureCollection', 'features': features}


def refresh_cache() -> dict:
    """Rebuild the GeoJSON and atomically replace the cached copy.

    Called by ``recompute_district_ndvi_status`` after the SQL upsert,
    and by the ``prewarm_agro_caches`` management command on deploy.
    Returns the freshly built payload so callers can log / inspect it.
    """
    payload = build_geojson_payload()
    # ``timeout=None`` → no expiry; only the recompute command rotates it.
    cache.set(CACHE_KEY, payload, timeout=None)
    return payload


def get_or_build() -> dict:
    """Read the cached payload; lazily rebuild on a miss.

    A cache miss is rare (Redis restart / FLUSHDB) but should not 500
    the page — we transparently rebuild and warm the cache.
    """
    payload = cache.get(CACHE_KEY)
    if payload is not None:
        return payload
    logger.warning('districts_status_geojson cache miss — rebuilding inline')
    return refresh_cache()
