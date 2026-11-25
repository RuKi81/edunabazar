"""
OSM bulk boundary fetcher for Region / District import.

Uses free & public services:
  * Overpass API (https://overpass-api.de/api/interpreter) to list all
    OSM relation ids with a given admin_level inside a country.
  * https://polygons.openstreetmap.fr/get_geojson.py — returns a ready
    multipolygon GeoJSON for any single OSM relation id, which avoids
    reimplementing OSM's multipolygon-assembly rules from ways+nodes.
  * https://nominatim.openstreetmap.org/lookup — fallback polygon source
    for when polygons.openstreetmap.fr is down.

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
import re
import time
import unicodedata

import requests

logger = logging.getLogger(__name__)

# Overpass endpoints, tried in turn: the public instances go down or hang
# independently of each other (502 from the main one, timeouts on a
# mirror), and one healthy mirror is enough for our tag-only queries.
# ``OVERPASS_URL`` in the environment puts a preferred endpoint first
# without a redeploy.
OVERPASS_URL = os.environ.get(
    'OVERPASS_URL',
    'https://overpass-api.de/api/interpreter',
)
OVERPASS_MIRRORS = (
    'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
    'https://overpass.kumi.systems/api/interpreter',
    'https://overpass.private.coffee/api/interpreter',
)
POLYGONS_URL = 'https://polygons.openstreetmap.fr/get_geojson.py'
# Second source of relation polygons, used when polygons.osm.fr is down or
# cannot build one. Usage policy: at most 1 request per second — the
# importers already pause longer than that between relations.
NOMINATIM_LOOKUP_URL = 'https://nominatim.openstreetmap.org/lookup'

# Overpass QL's own timeout (seconds).
OVERPASS_QL_TIMEOUT = 900
# HTTP (connect, read) timeout of one attempt. Our queries return tags
# only and finish in seconds; a mirror silent for 2 minutes is hung, and
# waiting out the full QL timeout on it would stall the import.
_OVERPASS_HTTP_TIMEOUT = (20, 120)

# Both Overpass and polygons.osm.fr reject requests without a sensible
# User-Agent (Overpass returns 406 Not Acceptable for the default
# "python-requests/*"). Identify ourselves per OSM etiquette.
_UA = 'agrocosmos-import/1.0 (+https://edunabazar.ru)'
_HEADERS = {'User-Agent': _UA, 'Accept': 'application/json'}

# Transient HTTP statuses from Overpass we're willing to retry on.
# 429 = rate limit, 502/503/504 = upstream/temporary, 500 = server error.
_RETRY_STATUSES = (429, 500, 502, 503, 504)


def _overpass_endpoints() -> list[str]:
    return [OVERPASS_URL] + [u for u in OVERPASS_MIRRORS if u != OVERPASS_URL]


def _overpass_post(query: str, *, retries: int = 8) -> dict:
    """POST a QL query to Overpass with retry/backoff on 5xx/timeouts,
    moving to the next endpoint of ``_overpass_endpoints()`` each attempt
    (two passes over the list by default).

    Why: public Overpass instances (main, kumi.systems, …) intermittently
    return 504/502 or hang when they're under load. Rotating endpoints
    in the retry loop avoids skipping entire regions in the bulk importers.
    """
    endpoints = _overpass_endpoints()
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        url = endpoints[(attempt - 1) % len(endpoints)]
        try:
            resp = requests.post(
                url,
                data={'data': query},
                headers=_HEADERS,
                timeout=_OVERPASS_HTTP_TIMEOUT,
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
                'Overpass %s at %s (attempt %d/%d); retrying in %ds…',
                exc, url, attempt, retries, sleep_s,
            )
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def _is_readable(name: str) -> bool:
    """Название написано кириллицей или латиницей (а не вязью и т.п.)."""
    letters = [ch for ch in name if ch.isalpha()]
    return bool(letters) and all(
        'LATIN' in unicodedata.name(ch, '')
        or 'CYRILLIC' in unicodedata.name(ch, '')
        for ch in letters
    )


def display_name(tags: dict) -> str:
    """Название объекта OSM для русского интерфейса.

    ``name:ru``; если его нет — местное ``name``, когда оно читается
    (кириллица/латиница: турецкие ильче), иначе ``name:en`` (иранские
    шахрестаны подписаны только по-персидски и по-английски).
    """
    name_ru = (tags.get('name:ru') or '').strip()
    if name_ru:
        return name_ru
    name = (tags.get('name') or '').strip()
    if name and _is_readable(name):
        return name
    return (tags.get('name:en') or '').strip() or name


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
        name = display_name(tags)
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


def fetch_country_admin_relations(country_code: str, admin_level: int) -> list[dict]:
    """
    Return a list of OSM relations with a given admin_level inside a country.

    ``country_code`` is the ISO 3166-1 alpha-2 code (``RU``, ``KZ``, …).
    Each element is ``{"osm_id": int, "name": str, "tags": dict}``.

    Only tags are returned — geometry is fetched per-relation via
    :func:`fetch_polygon_geojson` because Overpass's inline geometry
    output for thousands of relations blows past response size limits.

    We filter by the ``ISO3166-2`` tag (``^RU-*``) instead of
    ``area[ISO3166-1=RU]`` because the area-scoped query forces Overpass
    to materialise Russia's full multipolygon on the fly, which has been
    timing out on the public servers. Tag-filtering is O(index) and
    completes in <2 s on mirrors like overpass.kumi.systems.

    For admin_level=4 (first-level subjects) every subject of the
    supported countries carries an ISO3166-2 tag, so this is complete.
    For other levels (where the tag doesn't exist), prefer
    :func:`fetch_admin_relations_in` with a parent relation id.
    """
    if not re.fullmatch(r'[A-Z]{2}', country_code or ''):
        raise ValueError(f'bad country code: {country_code!r}')
    query = f"""
    [out:json][timeout:{OVERPASS_QL_TIMEOUT}];
    relation
      ["boundary"="administrative"]
      ["admin_level"="{admin_level}"]
      ["ISO3166-2"~"^{country_code}-"];
    out tags;
    """
    logger.info('Overpass: fetching %s admin_level=%d relations…',
                country_code, admin_level)
    data = _overpass_post(query)

    out: list[dict] = []
    for el in data.get('elements', []):
        if el.get('type') != 'relation':
            continue
        tags = el.get('tags', {})
        name = display_name(tags)
        out.append({
            'osm_id': el['id'],
            'name': name.strip(),
            'tags': tags,
        })
    logger.info('Overpass: got %d relations at admin_level=%d', len(out), admin_level)
    return out


# polygons.osm.fr outage breaker: after this many relations in a row that
# got a 5xx / network error we stop asking it for the rest of the process
# and go straight to Nominatim.
_POLYGONS_DOWN_AFTER = 3
_polygons_failures = 0


def _fetch_polygon_polygons_fr(osm_id: int, retries: int,
                               delay: float) -> dict | None:
    """polygons.openstreetmap.fr — the primary polygon source.

    It sometimes returns the literal string "None" when a polygon is
    still being built on their side — we treat that as a transient
    failure and retry with exponential-ish backoff. A 5xx or a network
    error means the service itself is down: no point in retrying.
    """
    global _polygons_failures
    if _polygons_failures >= _POLYGONS_DOWN_AFTER:
        return None
    for attempt in range(retries):
        try:
            resp = requests.get(
                POLYGONS_URL,
                params={'id': osm_id, 'params': 0},
                headers=_HEADERS,
                timeout=120,
            )
        except requests.RequestException as exc:
            logger.warning('polygons.osm.fr request failed for %s: %s',
                           osm_id, exc)
            _polygons_failures += 1
            return None
        if resp.status_code >= 500:
            logger.warning('polygons.osm.fr returned %s for %s',
                           resp.status_code, osm_id)
            _polygons_failures += 1
            return None
        _polygons_failures = 0
        body = resp.text.strip()
        if resp.status_code == 200 and body and body.lower() != 'none':
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                pass  # transient — fall through to retry
        time.sleep(delay * (attempt + 1))
    return None


def _fetch_polygon_nominatim(osm_id: int) -> dict | None:
    """Nominatim ``lookup`` with ``polygon_geojson`` — the fallback source."""
    try:
        resp = requests.get(
            NOMINATIM_LOOKUP_URL,
            params={
                'osm_ids': f'R{osm_id}',
                'format': 'geojson',
                'polygon_geojson': 1,
            },
            headers=_HEADERS,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning('Nominatim lookup failed for %s: %s', osm_id, exc)
        return None
    for feature in data.get('features') or []:
        geometry = feature.get('geometry') or {}
        # A relation Nominatim has no area for comes back as a Point.
        if geometry.get('type') in ('Polygon', 'MultiPolygon'):
            return geometry
    return None


def fetch_polygon_geojson(osm_id: int, retries: int = 3,
                          delay: float = 3.0) -> dict | None:
    """
    Fetch a multipolygon GeoJSON for a single OSM relation id.

    Returns a dict like ``{"type": "MultiPolygon", "coordinates": [...]}``
    on success, or ``None`` if neither source can build the polygon
    (invalid boundary, tags missing, etc.).

    polygons.openstreetmap.fr is asked first; if it is down or has no
    polygon, the same relation is looked up in Nominatim.
    """
    return (
        _fetch_polygon_polygons_fr(osm_id, retries, delay)
        or _fetch_polygon_nominatim(osm_id)
    )
