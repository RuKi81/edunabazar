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
CACHE_KEY = 'agro:districts_status:geojson:v4'
# Companion key holding the *pre-encoded* JSON body + matching ETag for the
# unfiltered all-Russia choropleth. The view that serves the no-?region=
# fast path skips both pickle.loads of the dict (~300 ms for 3 MB) and the
# subsequent json.dumps re-encoding (~1.5 s) and just streams the bytes
# straight into an HttpResponse.
#
# Tuple shape: ``(etag: str, body: bytes)``. ``body`` is the same JSON the
# slow ``JsonResponse(payload)`` path would produce.
CACHE_KEY_FAST = 'agro:districts_status:geojson:v4:fast'


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
        # Tolerance 0.003° ≈ 300 m — fine enough that district outlines
        # look smooth even on a desktop overview of European Russia (one
        # screen pixel ≈ 5-10 km at zoom 5). The previous 0.01° (~1 km)
        # was visible as jagged zigzags on the Cabinet → Map transition.
        # Payload grows roughly 3× (1.1 → ~3 MB) but with browser ETag
        # caching that cost is paid once per day per visitor.
        .annotate(geojson=AsGeoJSON(
            _SimplifyPreserveTopology('geom', 0.003),
            precision=4,
        ))
        .values(
            'id', 'name', 'region_id', 'region__name', 'geojson',
            'ndvi_status__latest_date',
            'ndvi_status__current_ndvi',
            'ndvi_status__baseline_ndvi',
            'ndvi_status__pct_of_baseline',
            # Coverage counters — exposed in every payload so the
            # admin overlay can compute the trust ratio client-side
            # without an extra round-trip. Negligible JSON cost
            # (~12 bytes per district) but only rendered when the
            # user is an administrator (gated in the template).
            'ndvi_status__farmlands_with_data',
            'ndvi_status__farmlands_total',
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
        fl_with = r['ndvi_status__farmlands_with_data']
        fl_total = r['ndvi_status__farmlands_total']
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
                # Coverage. ``None`` for districts without a status
                # row at all (e.g. zero farmlands ingested), 0 when
                # the row exists but the latest composite gave no
                # valid pixels in the district.
                'farmlands_with_data': int(fl_with) if fl_with is not None else None,
                'farmlands_total': int(fl_total) if fl_total is not None else None,
            },
            'geometry': json.loads(r['geojson']),
        })

    logger.info(
        'districts_status_geojson.build: districts=%d with_data=%d in %.2fs',
        len(features), with_data, time.time() - overall_t,
    )
    return {'type': 'FeatureCollection', 'features': features}


def _build_fast_blob(payload: dict) -> tuple[str, bytes]:
    """Pre-encode JSON body + matching ETag for the unfiltered fast path.

    Done once per refresh so that on each request the view can stream the
    bytes directly without paying for ``json.dumps`` of a 3 MB structure.
    """
    # ``separators`` mirrors what JsonResponse would produce by default
    # apart from a couple of whitespace characters (negligible). We use
    # the compact form to shave ~3 % off the wire size.
    body = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    features = payload.get('features', [])
    latest = ''
    for f in features:
        d = (f.get('properties') or {}).get('current_date') or ''
        if d > latest:
            latest = d
    # Same fingerprint formula the view's slow path uses, so a client
    # that warmed the cache on the slow path keeps a valid ETag here.
    etag = 'W/"agro-ds-{date}-{n}-all"'.format(
        date=latest or 'none',
        n=len(features),
    )
    return etag, body


def refresh_cache() -> dict:
    """Rebuild the GeoJSON and atomically replace the cached copy.

    Called by ``recompute_district_ndvi_status`` after the SQL upsert,
    and by the ``prewarm_agro_caches`` management command on deploy.
    Returns the freshly built payload so callers can log / inspect it.
    Populates *two* Redis entries:

    * ``CACHE_KEY``      — the dict (used for ``?region=<id>`` filter).
    * ``CACHE_KEY_FAST`` — pre-encoded ``(etag, bytes)`` for the
      no-filter fast path that bypasses pickle + json round-tripping.
    """
    payload = build_geojson_payload()
    # ``timeout=None`` → no expiry; only the recompute command rotates it.
    cache.set(CACHE_KEY, payload, timeout=None)
    cache.set(CACHE_KEY_FAST, _build_fast_blob(payload), timeout=None)
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


def get_fast_blob() -> tuple[str, bytes]:
    """Pre-encoded ``(etag, json_bytes)`` for the unfiltered choropleth.

    Used by the view's fast path. Lazily rebuilds on a cache miss; the
    caller can rely on always getting a ready-to-stream tuple.
    """
    blob = cache.get(CACHE_KEY_FAST)
    if blob is not None:
        return blob
    logger.warning('districts_status_geojson fast-blob cache miss — rebuilding inline')
    refresh_cache()
    blob = cache.get(CACHE_KEY_FAST)
    if blob is None:
        # Defensive: refresh_cache wrote both keys, but in case of a
        # truly broken cache backend (e.g. write succeeds, read returns
        # None) build the blob inline from the just-built dict instead
        # of crashing the request.
        return _build_fast_blob(get_or_build())
    return blob


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
# v3: payload now carries ``farmlands_with_data`` / ``farmlands_total``
# per district for the admin coverage overlay; v2 entries lack those
# keys and would render an empty trust ratio in the popup.
# v4: build_snapshot now reads from the pre-aggregated
# ``agro_district_ndvi_series`` (1000× faster) instead of the raw
# ``agro_vegetation_index``; ``farmlands_with_data`` is now an
# observation-count proxy rather than a unique-farmland count, so
# v3 entries (with the old exact counter) must not be served to
# avoid mismatched trust ratios across the slider range.
_SNAPSHOT_KEY = 'agro:districts_status:snapshot:v4:{date}'


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

    # Dogpile guard: the SQL below scans up to 60 days of
    # ``agro_vegetation_index`` (~25 M rows) and joins to ~2 200
    # districts; on a cold buffer pool it takes 30-90 s. Without a
    # mutex, N concurrent web hits during/after a fresh MODIS ingest
    # spawn N copies of this query, each thrashing the same pages
    # and slowing every other request to a crawl (observed: 12
    # parallel queries waiting on ``BufferIO`` for 30+ minutes).
    # We use ``cache.add`` (atomic SET-if-not-exists) as the lock;
    # the loser waits for the winner's payload to land in the
    # snapshot cache instead of running its own SQL.
    lock_key = key + ':build_lock'
    # Worst case observed in prod: 9-15 min when buffer pool is cold
    # and the sort spills to disk (BufFileWrite). Set the TTL high
    # enough to cover that so a slow winner does not get bypassed
    # by impatient followers running the same heavy SQL in parallel.
    # If the winner truly dies, we still recover via the polling
    # fallback below.
    LOCK_TTL = 1200         # 20 min — safe ceiling for cold-cache builds
    POLL_INTERVAL = 1.0
    # Web-side poll budget: most builds finish in 30-90 s; gunicorn
    # times out long requests around 60-120 s anyway, so any longer
    # wait would just produce 502s. After this we fall through and
    # build it ourselves — bounded extra work, never N copies.
    POLL_MAX = 60
    if not cache.add(lock_key, '1', timeout=LOCK_TTL):
        # Another worker is already building this snapshot. Poll the
        # snapshot cache (not the lock — the lock auto-expires) until
        # the payload appears or we time out. On timeout we fall
        # through and build it ourselves; the duplicated work is
        # bounded to one extra query, not N.
        import time as _time
        for _ in range(POLL_MAX):
            _time.sleep(POLL_INTERVAL)
            cached = cache.get(key)
            if cached is not None:
                return cached
        # Winner died / timed out — fall through and build it.

    from django.db import connection
    # As-of semantics: for each district, pick the most recent MODIS
    # composite with NDVI data on or before ``target_date`` within a
    # 60-day look-back. This avoids grey holes when a single composite
    # was cloud-masked over a district — the choropleth carries forward
    # the previous reading until a fresher one is available. The 60-day
    # window matches the existing ``recompute_district_ndvi_status`` so
    # the slider's last position is identical to the always-on cached
    # GeoJSON.
    #
    # Implementation: read pre-aggregated per-district × per-crop sums
    # from ``agro_district_ndvi_series`` (~9 k rows / date) instead of
    # the raw ``agro_vegetation_index`` (~6-8 M rows / 60-day window).
    # ``recompute_district_ndvi_series`` rebuilds the series at the end
    # of every MODIS pipeline run, so the data is always in sync with
    # the always-on choropleth's ``agro_district_ndvi_status``. The
    # district-level NDVI is recovered by summing per-crop sum-of-NDVI
    # × area and dividing by sum-of-area — algebraically identical to
    # the area-weighted mean we used to compute on the raw VI table,
    # but ~1000× faster (1.3 s vs 20+ min cold).
    sql = """
        WITH latest_per_district AS (
            SELECT district_id, MAX(acquired_date) AS as_of
            FROM   agro_district_ndvi_series
            WHERE  source = 'modis'
              AND  sum_area > 0
              AND  acquired_date <= %s
              AND  acquired_date >= %s::date - INTERVAL '60 days'
            GROUP BY district_id
        ),
        current_ndvi AS (
            SELECT  s.district_id,
                    l.as_of,
                    SUM(s.sum_ndvi_area)
                      / NULLIF(SUM(s.sum_area), 0) AS w_ndvi,
                    -- ``obs_count`` is per-crop MODIS observations;
                    -- summing across crops gives a coverage proxy
                    -- (not exact "unique farmlands with data" — that
                    -- would require a re-scan of the VI table — but
                    -- proportional and good enough for the popup
                    -- trust badge).
                    SUM(s.obs_count) AS fl_with_data
            FROM   latest_per_district l
            JOIN   agro_district_ndvi_series s
                       ON s.district_id   = l.district_id
                      AND s.acquired_date = l.as_of
                      AND s.source        = 'modis'
            GROUP BY s.district_id, l.as_of
        ),
        farmlands_total AS (
            SELECT f.district_id, COUNT(*) AS total
            FROM   agro_farmland f
            WHERE  f.district_id IN (SELECT district_id FROM current_ndvi)
            GROUP BY f.district_id
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
                     ELSE NULL END AS pct_of_baseline,
                COALESCE(cn.fl_with_data, 0) AS farmlands_with_data,
                COALESCE(ft.total, 0)        AS farmlands_total
        FROM    current_ndvi cn
        LEFT JOIN matched_baseline mb  USING (district_id)
        LEFT JOIN farmlands_total  ft  USING (district_id)
        WHERE   cn.w_ndvi IS NOT NULL
    """
    overall_t = time.time()
    districts = {}
    with connection.cursor() as cur:
        cur.execute(sql, [target_iso, target_iso])
        for d_id, as_of, cur_v, bl_v, pct, fl_with, fl_total in cur.fetchall():
            districts[d_id] = {
                'as_of': str(as_of) if as_of else None,
                'current_ndvi': float(cur_v) if cur_v is not None else None,
                'baseline_ndvi': float(bl_v) if bl_v is not None else None,
                'pct_of_baseline': float(pct) if pct is not None else None,
                'farmlands_with_data': int(fl_with) if fl_with is not None else None,
                'farmlands_total': int(fl_total) if fl_total is not None else None,
            }

    payload = {'date': target_iso, 'districts': districts}
    logger.info(
        'districts_status_geojson.build_snapshot date=%s districts=%d in %.2fs',
        target_iso, len(districts), time.time() - overall_t,
    )
    # Eternal cache — past composites are immutable.
    cache.set(key, payload, timeout=None)
    # Release the dogpile lock so we don't keep it for ``LOCK_TTL``
    # seconds longer than necessary; harmless if it's already gone
    # (e.g. expired because the SQL took longer than expected).
    cache.delete(lock_key)
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
            # Also clear any stale build_lock left behind by a previous
            # crashed worker; otherwise we would wait the full 20-min
            # poll fallback before rebuilding.
            cache.delete(key + ':build_lock')
        try:
            build_snapshot(str(d))
            built += 1
        except Exception as exc:  # noqa: BLE001 — log + continue
            logger.warning(
                'prewarm_snapshots: build_snapshot(%s) failed: %s', d, exc,
            )
    return built, skipped, time.time() - t0
