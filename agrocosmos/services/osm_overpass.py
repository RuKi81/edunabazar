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
import time

import requests

logger = logging.getLogger(__name__)

OVERPASS_URL = 'https://overpass-api.de/api/interpreter'
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


def fetch_russia_admin_relations(admin_level: int) -> list[dict]:
    """
    Return a list of OSM relations with a given admin_level inside Russia.

    Each element is ``{"osm_id": int, "name": str, "tags": dict}``.

    Only tags are returned — geometry is fetched per-relation via
    :func:`fetch_polygon_geojson` because Overpass's inline geometry
    output for thousands of relations blows past response size limits.
    """
    query = f"""
    [out:json][timeout:{OVERPASS_QL_TIMEOUT}];
    area["ISO3166-1"="RU"]->.ru;
    relation
      ["boundary"="administrative"]
      ["admin_level"="{admin_level}"]
      (area.ru);
    out tags;
    """
    logger.info('Overpass: fetching admin_level=%d relations…', admin_level)
    resp = requests.post(
        OVERPASS_URL,
        data={'data': query},
        headers=_HEADERS,
        timeout=OVERPASS_QL_TIMEOUT + 60,
    )
    resp.raise_for_status()
    data = resp.json()

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
