"""
Copernicus Data Space Ecosystem (CDSE) — Sentinel Hub Statistical API client.

Fetches NDVI zonal statistics for a given polygon and date range
directly from Sentinel-2 L2A data, no raster download needed.

Required env vars:
    CDSE_CLIENT_ID      — OAuth2 client_id from https://dataspace.copernicus.eu
    CDSE_CLIENT_SECRET  — OAuth2 client_secret

Docs:
    https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Statistical.html
"""
import json
import logging
from datetime import date, timedelta

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

TOKEN_URL = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
CATALOG_URL = 'https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search'
STATISTICAL_URL = 'https://sh.dataspace.copernicus.eu/api/v1/statistics'

# Sentinel-2 L2A collection on CDSE
S2_COLLECTION = 'sentinel-2-l2a'
S2_DATA_COLLECTION = 'S2L2A'

# Evalscript: compute NDVI from B04 (Red) and B08 (NIR)
NDVI_EVALSCRIPT = """
//VERSION=3
function setup() {
  return {
    input: [{
      bands: ["B04", "B08", "SCL"],
      units: "DN"
    }],
    output: [
      { id: "ndvi", bands: 1, sampleType: "FLOAT32" },
      { id: "dataMask", bands: 1 }
    ]
  };
}

function evaluatePixel(samples) {
  // SCL cloud/shadow mask: keep only vegetation, soil, water pixels
  // 4=vegetation, 5=bare soil, 6=water, 7=low cloud prob
  var scl = samples.SCL;
  var valid = (scl === 4 || scl === 5 || scl === 6 || scl === 7);

  var nir = samples.B08;
  var red = samples.B04;
  var ndvi = (nir + red) !== 0 ? (nir - red) / (nir + red) : 0;

  return {
    ndvi: [valid ? ndvi : NaN],
    dataMask: [valid ? 1 : 0]
  };
}
"""


class CDSEError(Exception):
    """Raised when CDSE API returns an error."""
    pass


def _get_credentials():
    """Read CDSE OAuth2 credentials from Django settings / env."""
    client_id = getattr(settings, 'CDSE_CLIENT_ID', None)
    client_secret = getattr(settings, 'CDSE_CLIENT_SECRET', None)
    if not client_id or not client_secret:
        import os
        client_id = os.environ.get('CDSE_CLIENT_ID', '')
        client_secret = os.environ.get('CDSE_CLIENT_SECRET', '')
    if not client_id or not client_secret:
        raise CDSEError(
            'CDSE_CLIENT_ID and CDSE_CLIENT_SECRET must be set in .env or settings'
        )
    return client_id, client_secret


def get_access_token():
    """Obtain short-lived Bearer token via OAuth2 client_credentials flow."""
    client_id, client_secret = _get_credentials()
    resp = requests.post(TOKEN_URL, data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }, timeout=15)
    if resp.status_code != 200:
        raise CDSEError(f'Token request failed ({resp.status_code}): {resp.text}')
    return resp.json()['access_token']


def search_scenes(geometry_geojson, date_from, date_to, cloud_max=30, limit=20):
    """
    Search Sentinel-2 L2A scenes intersecting the given geometry.

    Args:
        geometry_geojson: GeoJSON geometry dict (Polygon or MultiPolygon)
        date_from: start date (date or str 'YYYY-MM-DD')
        date_to: end date
        cloud_max: max cloud cover %
        limit: max results

    Returns:
        list of dicts with keys: scene_id, acquired_date, cloud_cover, bbox_geojson
    """
    token = get_access_token()

    if isinstance(date_from, date):
        date_from = date_from.isoformat()
    if isinstance(date_to, date):
        date_to = date_to.isoformat()

    body = {
        'collections': [S2_COLLECTION],
        'datetime': f'{date_from}T00:00:00Z/{date_to}T23:59:59Z',
        'intersects': geometry_geojson,
        'limit': limit,
        'filter': f'eo:cloud_cover < {cloud_max}',
        'filter-lang': 'cql2-text',
    }

    resp = requests.post(
        CATALOG_URL,
        json=body,
        headers={'Authorization': f'Bearer {token}'},
        timeout=30,
    )
    if resp.status_code != 200:
        raise CDSEError(f'Catalog search failed ({resp.status_code}): {resp.text}')

    results = []
    for feat in resp.json().get('features', []):
        props = feat.get('properties', {})
        results.append({
            'scene_id': feat.get('id', ''),
            'acquired_date': props.get('datetime', '')[:10],
            'cloud_cover': props.get('eo:cloud_cover', 0),
            'bbox_geojson': feat.get('geometry'),
        })
    return results


def fetch_ndvi_stats(geometry_geojson, date_from, date_to, cloud_max=30,
                     min_valid_ratio=0.95):
    """
    Compute NDVI zonal statistics for a polygon using CDSE Statistical API.

    Args:
        geometry_geojson: GeoJSON geometry dict (Polygon or MultiPolygon)
        date_from: start date
        date_to: end date
        cloud_max: max cloud cover %
        min_valid_ratio: minimum ratio of valid (non-cloud/nodata) pixels
            to total pixels. Dates below this threshold are skipped.
            Default 0.95 means skip if >5% of polygon has no data.

    Returns:
        list of dicts, one per date:
        [
            {
                'date': '2025-06-15',
                'mean': 0.65,
                'median': 0.68,
                'min': 0.12,
                'max': 0.89,
                'std': 0.11,
                'pixel_count': 1200,
                'valid_pixel_count': 980,
                'valid_ratio': 0.817,
            },
            ...
        ]
    """
    token = get_access_token()

    if isinstance(date_from, date):
        date_from = date_from.isoformat()
    if isinstance(date_to, date):
        date_to = date_to.isoformat()

    body = {
        'input': {
            'bounds': {
                'geometry': geometry_geojson,
                'properties': {'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326'},
            },
            'data': [{
                'type': S2_DATA_COLLECTION,
                'dataFilter': {
                    'timeRange': {
                        'from': f'{date_from}T00:00:00Z',
                        'to': f'{date_to}T23:59:59Z',
                    },
                    'maxCloudCoverage': cloud_max,
                    'mosaickingOrder': 'leastCC',
                },
            }],
        },
        'aggregation': {
            'timeRange': {
                'from': f'{date_from}T00:00:00Z',
                'to': f'{date_to}T23:59:59Z',
            },
            'aggregationInterval': {'of': 'P1D'},
            'evalscript': NDVI_EVALSCRIPT,
            'resx': 10,
            'resy': 10,
        },
        'calculations': {
            'default': {
                'statistics': {
                    'default': {
                        'percentiles': {'k': [50]},
                    }
                }
            }
        },
    }

    resp = requests.post(
        STATISTICAL_URL,
        json=body,
        headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        },
        timeout=120,
    )

    if resp.status_code != 200:
        raise CDSEError(f'Statistical API failed ({resp.status_code}): {resp.text}')

    data = resp.json()
    results = []

    for interval in data.get('data', []):
        ts_from = interval.get('interval', {}).get('from', '')[:10]
        outputs = interval.get('outputs', {})
        ndvi_out = outputs.get('ndvi', {})
        bands = ndvi_out.get('bands', {})
        b0 = bands.get('B0', {})
        stats = b0.get('stats', {})
        no_data = b0.get('noDataCount', 0)
        sample_count = b0.get('sampleCount', 0)
        valid_count = sample_count - no_data

        if valid_count <= 0 or sample_count <= 0:
            continue

        ratio = valid_count / sample_count
        if ratio < min_valid_ratio:
            logger.debug(
                'Skipping %s: valid ratio %.1f%% < %.1f%%',
                ts_from, ratio * 100, min_valid_ratio * 100,
            )
            continue

        percentiles = stats.get('percentiles', {}).get('50.0', stats.get('mean', 0))

        results.append({
            'date': ts_from,
            'mean': round(stats.get('mean', 0), 4),
            'median': round(percentiles if isinstance(percentiles, (int, float)) else 0, 4),
            'min': round(stats.get('min', 0), 4),
            'max': round(stats.get('max', 0), 4),
            'std': round(stats.get('stDev', 0), 4),
            'pixel_count': sample_count,
            'valid_pixel_count': valid_count,
            'valid_ratio': round(ratio, 4),
        })

    logger.info('NDVI stats: %d intervals for %s..%s', len(results), date_from, date_to)
    return results
