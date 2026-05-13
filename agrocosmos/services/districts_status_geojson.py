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


# ── Timeline support: per-date snapshots & list of available dates ───────
#
# The choropleth above shows "current" status (latest available MODIS
# composite). For the timeline UI we additionally need to colour the
# same polygons by NDVI on an arbitrary past biweekly date.
#
# Building a full FeatureCollection per date would 16x the Redis usage
# and the load time. Instead the snapshot endpoint returns only the
# {district_id -> {current, baseline, pct}} mapping (~50 KB JSON), and
# the frontend recolours the layer it already loaded once.

_AVAILABLE_DATES_KEY = 'agro:districts_status:dates:v1:{year}'
# v2: snapshot now uses as-of semantics (carry-forward over 60 days)
# instead of exact date match — old v1 payloads (with grey holes for
# any district missing on the exact composite date) must not be served.
_SNAPSHOT_KEY = 'agro:districts_status:snapshot:v2:{date}'


def list_available_dates(year: int) -> list[str]:
    """List of distinct MODIS NDVI ``acquired_date`` values within ``year``
    that have data in the district pre-aggregate.

    Reads from ``agro_district_ndvi_series`` via the
    ``dns_src_date_idx`` index on ``(source, acquired_date)`` —
    millisecond-level even on cold cache. The previous implementation
    walked the raw ``agro_vegetation_index`` table (~10⁸ rows) with
    ``EXTRACT(YEAR FROM acquired_date) = %s`` which defeats the index
    on ``acquired_date`` and timed out at >120 s on cold Redis (the
    1 h Redis cache merely hid the bug).

    Cached for 1 hour — new dates appear once per biweekly cycle.
    """
    key = _AVAILABLE_DATES_KEY.format(year=int(year))
    cached = cache.get(key)
    if cached is not None:
        return cached

    from datetime import date as _date
    # Lazy import: this module is imported during Django startup, the
    # model layer is fine to import at function-call time.
    from ..models import DistrictNdviSeries

    rows = (
        DistrictNdviSeries.objects
        .filter(
            source=DistrictNdviSeries.Source.MODIS,
            acquired_date__gte=_date(int(year), 1, 1),
            acquired_date__lte=_date(int(year), 12, 31),
            sum_area__gt=0,
        )
        .order_by('acquired_date')
        .values_list('acquired_date', flat=True)
        .distinct()
    )
    dates = [str(d) for d in rows]

    cache.set(key, dates, timeout=3600)
    return dates


def build_snapshot(target_date: str) -> dict:
    """Per-district NDVI snapshot for one MODIS composite date.

    Returns ``{ "date": "YYYY-MM-DD",
                "districts": { district_id: {current_ndvi, baseline_ndvi,
                                              pct_of_baseline}, ...} }``.

    Cached eternally per date — past biweekly composites never mutate
    once written. ~5-15 s SQL on cold cache, sub-ms on warm.
    """
    from datetime import date as _date
    # Normalise / validate
    d = _date.fromisoformat(target_date)
    target_iso = d.isoformat()

    key = _SNAPSHOT_KEY.format(date=target_iso)
    cached = cache.get(key)
    if cached is not None:
        return cached

    from django.db import connection
    # As-of semantics: for each district, pick the most recent MODIS
    # composite with NDVI data on or before ``target_date`` within a
    # 60-day look-back. This avoids grey holes when a single composite
    # was cloud-masked over a district — the choropleth carries forward
    # the previous reading until a fresher one is available. The 60-day
    # window matches the existing ``recompute_district_ndvi_status`` so
    # the slider's last position is identical to the always-on cached
    # GeoJSON.
    sql = """
        WITH latest_per_district AS (
            SELECT f.district_id, MAX(vi.acquired_date) AS as_of
            FROM   agro_vegetation_index vi
            JOIN   agro_farmland f         ON f.id = vi.farmland_id
            JOIN   agro_satellite_scene s  ON s.id = vi.scene_id
            WHERE  vi.index_type = 'ndvi'
              AND  vi.is_outlier = false
              AND  vi.mean BETWEEN -0.2 AND 1
              AND  s.satellite IN ('modis_terra', 'modis_aqua')
              AND  vi.acquired_date <= %s
              AND  vi.acquired_date >= %s::date - INTERVAL '60 days'
              AND  f.district_id IS NOT NULL
            GROUP BY f.district_id
        ),
        current_ndvi AS (
            SELECT  l.district_id,
                    l.as_of,
                    SUM(vi.mean * f.area_ha)
                      / NULLIF(SUM(f.area_ha), 0) AS w_ndvi
            FROM   latest_per_district l
            JOIN   agro_farmland f ON f.district_id = l.district_id
            JOIN   agro_vegetation_index vi
                       ON vi.farmland_id   = f.id
                      AND vi.acquired_date = l.as_of
            JOIN   agro_satellite_scene s ON s.id = vi.scene_id
            WHERE  vi.index_type = 'ndvi'
              AND  vi.is_outlier = false
              AND  vi.mean BETWEEN -0.2 AND 1
              AND  s.satellite IN ('modis_terra', 'modis_aqua')
            GROUP BY l.district_id, l.as_of
        ),
        matched_baseline AS (
            SELECT DISTINCT ON (cn.district_id)
                   cn.district_id,
                   bl.mean_ndvi AS baseline_ndvi
            FROM   current_ndvi cn
            LEFT JOIN agro_ndvi_baseline bl
                   ON bl.district_id = cn.district_id
                  AND bl.crop_type = ''
                  AND bl.day_of_year BETWEEN
                          EXTRACT(DOY FROM cn.as_of)::int - 16
                      AND EXTRACT(DOY FROM cn.as_of)::int + 16
            ORDER BY cn.district_id,
                     ABS(bl.day_of_year - EXTRACT(DOY FROM cn.as_of)::int) NULLS LAST
        )
        SELECT  cn.district_id,
                cn.as_of,
                ROUND(cn.w_ndvi::numeric, 3) AS current_ndvi,
                CASE WHEN mb.baseline_ndvi IS NOT NULL
                     THEN ROUND(mb.baseline_ndvi::numeric, 3)
                     ELSE NULL END AS baseline_ndvi,
                CASE WHEN mb.baseline_ndvi IS NOT NULL
                      AND mb.baseline_ndvi > 0.05
                     THEN ROUND((cn.w_ndvi / mb.baseline_ndvi * 100.0)::numeric, 1)
                     ELSE NULL END AS pct_of_baseline
        FROM    current_ndvi cn
        LEFT JOIN matched_baseline mb USING (district_id)
        WHERE   cn.w_ndvi IS NOT NULL
    """
    overall_t = time.time()
    districts = {}
    with connection.cursor() as cur:
        cur.execute(sql, [target_iso, target_iso])
        for d_id, as_of, cur_v, bl_v, pct in cur.fetchall():
            districts[d_id] = {
                'as_of': str(as_of) if as_of else None,
                'current_ndvi': float(cur_v) if cur_v is not None else None,
                'baseline_ndvi': float(bl_v) if bl_v is not None else None,
                'pct_of_baseline': float(pct) if pct is not None else None,
            }

    payload = {'date': target_iso, 'districts': districts}
    logger.info(
        'districts_status_geojson.build_snapshot date=%s districts=%d in %.2fs',
        target_iso, len(districts), time.time() - overall_t,
    )
    # Eternal cache — past composites are immutable.
    cache.set(key, payload, timeout=None)
    return payload


def invalidate_available_dates(year: int) -> None:
    """Drop the cached list-of-dates for ``year``.

    Called by prewarm jobs after fresh MODIS data has been ingested so the
    next ``list_available_dates`` call rebuilds the list and includes the
    newly-arrived composite.
    """
    cache.delete(_AVAILABLE_DATES_KEY.format(year=int(year)))


def prewarm_snapshots(
    dates: list[str],
    force: bool = False,
) -> tuple[int, int, float]:
    """Pre-build per-date timeline snapshots so users never wait on a miss.

    Iterates ``dates`` sequentially (the underlying SQL is already heavy on
    a single CPU; running them in parallel just thrashes the same indexes).

    Returns ``(built, skipped, elapsed_s)``:
      * ``built``   — snapshots actually rebuilt (cache miss or ``force``).
      * ``skipped`` — already in cache, left untouched.
      * ``elapsed_s`` — total wall time.
    """
    t0 = time.time()
    built = skipped = 0
    for d in dates:
        key = _SNAPSHOT_KEY.format(date=str(d))
        if not force and cache.get(key) is not None:
            skipped += 1
            continue
        if force:
            cache.delete(key)
        try:
            build_snapshot(str(d))
            built += 1
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning(
                'prewarm_snapshots: build_snapshot(%s) failed: %s', d, exc,
            )
    return built, skipped, time.time() - t0
