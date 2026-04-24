"""
OSM bulk boundary fetcher for Region / District import.

Uses two services, both free & public:
  * Overpass API (https://overpass-api.de/api/interpreter) to list all
    OSM relation ids with a given admin_level inside Russia.
  * https://polygons.openstreetmap.fr/get_geojson.py — returns a ready
    multipolygon GeoJSON for any single OSM relation id, which avoids
    reimplementing OSM's multipolygon-assembly rules from ways+nodes.

No extra Python dependencies required — just ``requests``.

Important limits:
  * Overpass has a per-IP quota (~10k req/day, ~180s of CPU time per query);
    the one-shot admin-level query used here is cheap (<30 s, <1 MB).
  * polygons.openstreetmap.fr is slower — about one polygon every
    2-3 seconds sustainably. For 85 regions ~4 minutes, for ~2k districts
    ~2 hours. Use the ``sleep`` knob in management commands to tune.
"""
from __future__ import annotations

import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

# Allow switching to mirrors (e.g. https://overpass.kumi.systems/api/interpreter
# or https://lz4.overpass-api.de/api/interpreter) without a redeploy.
OVERPASS_URL = os.environ.get(
    'OVERPASS_URL',
    'https://overpass-api.de/api/interpreter',
)
POLYGONS_URL = 'https://polygons.openstreetmap.fr/get_geojson.py'

# Overpass QL's own timeout (seconds). The HTTP timeout we use for the
# request is a bit bigger so we get a clean QL timeout error back rather
# than a connection reset.
OVERPASS_QL_TIMEOUT = 900

# Both Overpass and polygons.osm.fr reject requests without a sensible
# User-Agent (Overpass returns 406 Not Acceptable for the default
# "python-requests/*"). Identify ourselves per OSM etiquette.
_UA = 'agrocosmos-import/1.0 (+https://edunabazar.ru)'
_HEADERS = {'User-Agent': _UA, 'Accept': 'application/json'}

# Transient HTTP statuses from Overpass we're willing to retry on.
# 429 = rate limit, 502/503/504 = upstream/temporary, 500 = server error.
_RETRY_STATUSES = (429, 500, 502, 503, 504)


def _overpass_post(query: str, *, retries: int = 3) -> dict:
    """POST a QL query to ``OVERPASS_URL`` with retry/backoff on 5xx/timeouts.

    Why: public Overpass mirrors (kumi.systems, lz4, main) intermittently
    return 504/502 for a few seconds when they're under load. A single
    retry loop here avoids skipping entire regions in the bulk importers.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                OVERPASS_URL,
                data={'data': query},
                headers=_HEADERS,
                timeout=OVERPASS_QL_TIMEOUT + 60,
            )
            if resp.status_code in _RETRY_STATUSES:
                raise requests.HTTPError(
                    f'{resp.status_code} {resp.reason}', response=resp,
                )
            resp.raise_for_status()
            return resp.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = min(30, 2 ** attempt)  # 2s, 4s, 8s, ... capped at 30s
            logger.warning(
                'Overpass %s (attempt %d/%d); retrying in %ds…',
                exc, attempt, retries, sleep_s,
            )
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def fetch_admin_relations_in(parent_osm_id: int, admin_level: int) -> list[dict]:
    """
    Return OSM admin_level=N relations inside a given parent relation.

    Useful for per-region district imports: ``area[ISO3166-1=RU]`` +
    admin_level=6 is too heavy for Overpass (times out at 15 min), but
    scoping to one subject at a time runs in a few seconds.

    ``parent_osm_id`` is the OSM *relation* id (e.g. 3795586 for
    Republic of Crimea). We map it to an area id with Overpass's
    standard ``rel(id); map_to_area;`` trick.
    """
    # Overpass area ids = relation_id + 3_600_000_000 for relations
    area_id = parent_osm_id + 3_600_000_000
    query = f"""
    [out:json][timeout:{OVERPASS_QL_TIMEOUT}];
    area({area_id})->.parent;
    relation
      ["boundary"="administrative"]
      ["admin_level"="{admin_level}"]
      (area.parent);
    out tags;
    """
    logger.info(
        'Overpass: fetching admin_level=%d relations inside parent rel %d…',
        admin_level, parent_osm_id,
    )
    data = _overpass_post(query)

    out: list[dict] = []
    for el in data.get('elements', []):
        if el.get('type') != 'relation':
            continue
        tags = el.get('tags', {})
        name = tags.get('name:ru') or tags.get('name') or ''
        out.append({
            'osm_id': el['id'],
            'name': name.strip(),
            'tags': tags,
        })
    logger.info(
        'Overpass: got %d relations at admin_level=%d inside rel %d',
        len(out), admin_level, parent_osm_id,
    )
    return out


def fetch_russia_admin_relations(admin_level: int) -> list[dict]:
    """
    Return a list of OSM relations with a given admin_level inside Russia.

    Each element is ``{"osm_id": int, "name": str, "tags": dict}``.

    Only tags are returned — geometry is fetched per-relation via
    :func:`fetch_polygon_geojson` because Overpass's inline geometry
    output for thousands of relations blows past response size limits.

    We filter by the ``ISO3166-2`` tag (``^RU-*``) instead of
    ``area[ISO3166-1=RU]`` because the area-scoped query forces Overpass
    to materialise Russia's full multipolygon on the fly, which has been
    timing out on the public servers. Tag-filtering is O(index) and
    completes in <2 s on mirrors like overpass.kumi.systems.

    For admin_level=4 (federal subjects) every RU subject carries an
    ISO3166-2 tag, so this is complete. For other levels (where the tag
    doesn't exist), prefer :func:`fetch_admin_relations_in` with a
    parent relation id.
    """
    query = f"""
    [out:json][timeout:{OVERPASS_QL_TIMEOUT}];
    relation
      ["boundary"="administrative"]
      ["admin_level"="{admin_level}"]
      ["ISO3166-2"~"^RU-"];
    out tags;
    """
    logger.info('Overpass: fetching admin_level=%d relations…', admin_level)
    data = _overpass_post(query)

    out: list[dict] = []
    for el in data.get('elements', []):
        if el.get('type') != 'relation':
            continue
        tags = el.get('tags', {})
        name = tags.get('name:ru') or tags.get('name') or ''
        out.append({
            'osm_id': el['id'],
            'name': name.strip(),
            'tags': tags,
        })
    logger.info('Overpass: got %d relations at admin_level=%d', len(out), admin_level)
    return out


def fetch_polygon_geojson(osm_id: int, retries: int = 3,
                          delay: float = 3.0) -> dict | None:
    """
    Fetch a multipolygon GeoJSON for a single OSM relation id.

    Returns a dict like ``{"type": "MultiPolygon", "coordinates": [...]}``
    on success, or ``None`` if the service can't build the polygon
    (invalid boundary, tags missing, etc.).

    polygons.openstreetmap.fr sometimes returns the literal string "None"
    when a polygon is still being built on their side — we treat that as
    a transient failure and retry with exponential-ish backoff.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(
                POLYGONS_URL,
                params={'id': osm_id, 'params': 0},
                headers=_HEADERS,
                timeout=120,
            )
            body = resp.text.strip()
            if resp.status_code == 200 and body and body.lower() != 'none':
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    pass  # transient — fall through to retry
        except requests.RequestException as exc:
            logger.warning('polygons.osm.fr request failed for %s: %s',
                           osm_id, exc)
        time.sleep(delay * (attempt + 1))
    return None
