"""Maintenance of the ``agro_district_ndvi_series`` pre-aggregate.

The raw ``agro_vegetation_index`` table holds ~1 B rows (one per
farmland × composite), which makes per-region live aggregation
impractical for large subjects — Moscow Oblast alone requires
~14 M row scans and regularly exceeded the 120 s gateway timeout.

This module owns the reconciliation of a compact district-level
time series (see :class:`agrocosmos.models.DistrictNdviSeries`) from
the raw VI rows, using a single ``INSERT … ON CONFLICT DO UPDATE``
so each refresh is idempotent.

Public entry points:

* :func:`refresh_recent` — incremental, meant for the daily cron.
* :func:`rebuild` — full backfill; safe to run ad-hoc but slow
  (hundreds of seconds for all history).
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

from django.db import connection, transaction
from django.db.models import Max, Min

logger = logging.getLogger(__name__)


# (source_code, tuple of satellite codes in SatelliteScene.satellite)
_SOURCE_SATELLITES: dict[str, tuple[str, ...]] = {
    'modis': ('modis_terra', 'modis_aqua'),
    'raster': ('sentinel2', 'landsat8', 'landsat9'),
    'fused': ('hls_fused',),
}


def _scope_sql(column: str, country: str | None,
               region_id: int | None) -> tuple[str, list]:
    """``(sql, params)`` limiting a district-id ``column`` to one region or
    one country; empty for the unscoped (all districts) refresh."""
    if region_id is not None:
        return (
            f' AND {column} IN ('
            'SELECT id FROM agro_district WHERE region_id = %s)',
            [region_id],
        )
    if country:
        return (
            f' AND {column} IN ('
            'SELECT d.id FROM agro_district d '
            'JOIN agro_region r ON r.id = d.region_id WHERE r.country = %s)',
            [country],
        )
    return '', []


def _upsert_sql(source: str, scope_sql: str = '') -> tuple[str, list]:
    """Return ``(sql, params)`` for upserting series rows in a date range.

    ``params`` holds the source and each satellite code; the caller
    appends ``date_from`` (inclusive), ``date_to`` (inclusive) and the
    parameters of ``scope_sql`` — in that order.
    """
    sats = _SOURCE_SATELLITES[source]
    placeholders = ', '.join(['%s'] * len(sats))
    sql = f"""
    INSERT INTO agro_district_ndvi_series AS target (
        district_id, acquired_date, crop_type, source,
        sum_ndvi_area, sum_area, obs_count, computed_at
    )
    SELECT
        f.district_id,
        vi.acquired_date,
        f.crop_type,
        %s AS source,
        SUM(vi.mean * f.area_ha) AS sum_w,
        SUM(f.area_ha)           AS sum_a,
        COUNT(*)                  AS cnt,
        NOW()
    FROM agro_vegetation_index vi
    JOIN agro_farmland          f  ON f.id = vi.farmland_id
    JOIN agro_satellite_scene   s  ON s.id = vi.scene_id
    WHERE vi.index_type    = 'ndvi'
      AND vi.is_outlier    = false
      AND vi.mean BETWEEN -0.2 AND 1
      AND s.satellite IN ({placeholders})
      AND f.district_id IS NOT NULL
      AND vi.acquired_date >= %s
      AND vi.acquired_date <= %s{scope_sql}
    GROUP BY f.district_id, vi.acquired_date, f.crop_type
    ON CONFLICT (district_id, acquired_date, crop_type, source) DO UPDATE
        SET sum_ndvi_area = EXCLUDED.sum_ndvi_area,
            sum_area       = EXCLUDED.sum_area,
            obs_count      = EXCLUDED.obs_count,
            computed_at    = NOW();
    """
    params: list = [source, *sats]
    return sql, params


def _delete_in_range_sql(source: str, scope_sql: str = '') -> str:
    return (
        "DELETE FROM agro_district_ndvi_series "
        "WHERE source = %s AND acquired_date >= %s AND acquired_date <= %s"
        + scope_sql
    )


@transaction.atomic
def refresh_range(
    date_from: date, date_to: date, source: str = 'modis', *,
    country: str | None = None, region_id: int | None = None,
) -> dict:
    """Rebuild the series rows for one date range end-to-end.

    Deletes existing rows in that range first (covers farmlands whose
    ``crop_type`` changed, farmlands deleted, etc.), then re-inserts.
    The two statements share a transaction so readers never see a
    half-empty range.

    ``region_id`` or ``country`` limits both statements to the districts
    of one region / one country: backfilling the archive of a newly
    added region must not rebuild years of every other region's rows.
    """
    if source not in _SOURCE_SATELLITES:
        raise ValueError(f'unknown source: {source}')
    if date_to < date_from:
        raise ValueError('date_to must be >= date_from')

    t0 = time.time()
    with connection.cursor() as cur:
        scope_sql, scope_params = _scope_sql('district_id', country, region_id)
        cur.execute(
            _delete_in_range_sql(source, scope_sql),
            [source, date_from, date_to, *scope_params],
        )
        deleted = cur.rowcount
        scope_sql, scope_params = _scope_sql('f.district_id', country, region_id)
        sql, params = _upsert_sql(source, scope_sql)
        cur.execute(sql, params + [date_from, date_to, *scope_params])
        inserted = cur.rowcount
    elapsed = time.time() - t0
    logger.info(
        'district_ndvi_series.refresh_range source=%s %s..%s region=%s '
        'country=%s deleted=%d inserted=%d in %.1fs',
        source, date_from, date_to, region_id, country,
        deleted, inserted, elapsed,
    )
    return {
        'source': source,
        'date_from': str(date_from),
        'date_to': str(date_to),
        'deleted': deleted,
        'inserted': inserted,
        'elapsed_s': round(elapsed, 2),
    }


def refresh_recent(days: int = 60, source: str = 'modis', *,
                   country: str | None = None,
                   region_id: int | None = None) -> dict:
    """Incremental refresh — the last ``days`` days of MODIS composites.

    Called by ``recompute_district_ndvi_status`` (daily cron) right
    after the status-row upsert, so the series stays in sync with the
    choropleth.
    """
    today = date.today()
    return refresh_range(
        today - timedelta(days=days), today, source=source,
        country=country, region_id=region_id,
    )


def rebuild(source: str = 'modis') -> dict:
    """Full backfill across all MODIS history.

    Uses the MIN/MAX of ``agro_satellite_scene.acquired_date`` for the
    requested source to bound the range — much cheaper than scanning
    ``agro_vegetation_index``.
    """
    from ..models import SatelliteScene

    sats = _SOURCE_SATELLITES[source]
    agg = (
        SatelliteScene.objects
        .filter(satellite__in=sats)
        .aggregate(first_dt=Min('acquired_date'), last_dt=Max('acquired_date'))
    )
    first = agg.get('first_dt')
    last = agg.get('last_dt')
    if not first or not last:
        logger.info('district_ndvi_series.rebuild: no scenes for source=%s', source)
        return {'source': source, 'deleted': 0, 'inserted': 0, 'elapsed_s': 0.0}
    return refresh_range(first, last, source=source)
