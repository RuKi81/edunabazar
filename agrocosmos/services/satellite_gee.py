"""
Google Earth Engine — Sentinel-2 NDVI zonal statistics.

Alternative to CDSE Sentinel Hub for regions where CDSE is geo-blocked.

Authentication:
    Option 1 (local/interactive):
        pip install earthengine-api
        earthengine authenticate --project YOUR_PROJECT_ID

    Option 2 (server/service account):
        Set env var GEE_SERVICE_ACCOUNT_KEY to path of JSON key file
        Set env var GEE_PROJECT to Google Cloud project ID

Setup:
    1. Go to https://code.earthengine.google.com/ — register
    2. Create a Google Cloud project and enable Earth Engine API
    3. Run: earthengine authenticate --project YOUR_PROJECT_ID
"""
import logging
import os
from datetime import date

import ee
from django.conf import settings

logger = logging.getLogger(__name__)

_initialized = False


class GEEError(Exception):
    """Raised when GEE API returns an error."""
    pass


def initialize():
    """Initialize Earth Engine. Handles both interactive and service account auth."""
    global _initialized
    if _initialized:
        return

    project = (
        getattr(settings, 'GEE_PROJECT', None)
        or os.environ.get('GEE_PROJECT', '')
    )
    sa_key = (
        getattr(settings, 'GEE_SERVICE_ACCOUNT_KEY', None)
        or os.environ.get('GEE_SERVICE_ACCOUNT_KEY', '')
    )

    try:
        if sa_key and os.path.exists(sa_key):
            credentials = ee.ServiceAccountCredentials(None, sa_key)
            ee.Initialize(credentials, project=project or None)
            logger.info('GEE initialized with service account')
        else:
            ee.Initialize(project=project or None)
            logger.info('GEE initialized with default credentials')
        _initialized = True
    except Exception as e:
        raise GEEError(
            f'Failed to initialize GEE: {e}\n'
            f'Run: earthengine authenticate --project YOUR_PROJECT_ID'
        )


def fetch_ndvi_stats(geometry_geojson, date_from, date_to, cloud_max=30,
                     min_valid_ratio=0.95):
    """
    Compute NDVI zonal statistics using Google Earth Engine + Sentinel-2 L2A.

    Same interface as CDSE version in satellite.py.

    Args:
        geometry_geojson: GeoJSON geometry dict (Polygon or MultiPolygon)
        date_from: start date (date or str 'YYYY-MM-DD')
        date_to: end date
        cloud_max: max scene-level cloud cover %
        min_valid_ratio: minimum ratio of valid (cloud-free) pixels to total.
            Dates below this threshold are skipped.
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
    initialize()

    if isinstance(date_from, date):
        date_from = date_from.isoformat()
    if isinstance(date_to, date):
        date_to = date_to.isoformat()

    try:
        geometry = ee.Geometry(geometry_geojson)

        # Sentinel-2 Surface Reflectance (Harmonized)
        s2 = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
              .filterDate(date_from, date_to)
              .filterBounds(geometry)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_max)))

        def _process_image(image):
            """Cloud-mask via SCL, compute NDVI, reduce over polygon."""
            # SCL: 4=vegetation, 5=bare_soil, 6=water, 7=low_cloud_prob
            scl = image.select('SCL')
            cloud_mask = (scl.eq(4).Or(scl.eq(5))
                          .Or(scl.eq(6)).Or(scl.eq(7)))

            # NDVI = (NIR - Red) / (NIR + Red)
            ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
            ndvi_masked = ndvi.updateMask(cloud_mask)

            # Total pixel count (all data pixels before cloud mask)
            total = image.select('B4').reduceRegion(
                reducer=ee.Reducer.count(),
                geometry=geometry,
                scale=10,
                maxPixels=1e9,
            ).get('B4')

            # NDVI stats on valid (cloud-free) pixels
            stats = ndvi_masked.reduceRegion(
                reducer=(ee.Reducer.mean()
                         .combine(ee.Reducer.median(), '', True)
                         .combine(ee.Reducer.min(), '', True)
                         .combine(ee.Reducer.max(), '', True)
                         .combine(ee.Reducer.stdDev(), '', True)
                         .combine(ee.Reducer.count(), '', True)),
                geometry=geometry,
                scale=10,
                maxPixels=1e9,
            )

            return ee.Feature(None, {
                'date': image.date().format('YYYY-MM-dd'),
                'mean': stats.get('NDVI_mean'),
                'median': stats.get('NDVI_median'),
                'min': stats.get('NDVI_min'),
                'max': stats.get('NDVI_max'),
                'std': stats.get('NDVI_stdDev'),
                'valid_count': stats.get('NDVI_count'),
                'total_count': total,
            })

        features = s2.map(_process_image)
        data = features.getInfo()

    except Exception as e:
        raise GEEError(f'GEE error: {e}')

    # Parse server-side results, apply min_valid_ratio filter
    results = []
    for feat in (data or {}).get('features', []):
        props = feat.get('properties', {})
        total = props.get('total_count') or 0
        valid = props.get('valid_count') or 0
        mean = props.get('mean')

        if not total or not valid or mean is None:
            continue

        ratio = valid / total
        if ratio < min_valid_ratio:
            logger.debug(
                'Skipping %s: valid ratio %.1f%% < %.1f%%',
                props.get('date', '?'), ratio * 100, min_valid_ratio * 100,
            )
            continue

        results.append({
            'date': props['date'],
            'mean': round(mean, 4),
            'median': round(props.get('median', 0) or 0, 4),
            'min': round(props.get('min', 0) or 0, 4),
            'max': round(props.get('max', 0) or 0, 4),
            'std': round(props.get('std', 0) or 0, 4),
            'pixel_count': int(total),
            'valid_pixel_count': int(valid),
            'valid_ratio': round(ratio, 4),
        })

    logger.info('GEE NDVI stats: %d dates for %s..%s', len(results), date_from, date_to)
    return results


def fetch_ndvi_batch(farmlands, date_from, date_to, cloud_max=30,
                     min_valid_ratio=0.95):
    """
    Batch NDVI via monthly median composite + single reduceRegions() call.

    Creates a cloud-free median NDVI composite for the date range, then
    computes zonal stats for all polygons in ONE reduceRegions() call.
    The management command iterates monthly, so each call produces one
    composite data point per farmland per month.

    Much faster than per-image approach because:
    - 1 reduceRegions call instead of N (one per Sentinel-2 image)
    - Median composite is computed server-side efficiently

    Best performance when farmlands are spatially grouped (same district)
    so filterBounds returns fewer images.

    Args:
        farmlands: list of dicts, each with 'id' (int) and 'geometry' (GeoJSON)
        date_from: str 'YYYY-MM-DD' or date object
        date_to: str 'YYYY-MM-DD' or date object
        cloud_max: max scene cloud cover %
        min_valid_ratio: skip polygon where valid/total < this

    Returns:
        dict: {farmland_id: [{'date', 'mean', 'min', 'max', 'std',
               'pixel_count', 'valid_pixel_count', 'valid_ratio'}, ...]}
    """
    initialize()

    if isinstance(date_from, date):
        date_from = date_from.isoformat()
    if isinstance(date_to, date):
        date_to = date_to.isoformat()

    # Midpoint date for the composite record
    d1 = date.fromisoformat(date_from)
    d2 = date.fromisoformat(date_to)
    mid_date = (d1 + (d2 - d1) / 2).isoformat()

    # Build ee.FeatureCollection from farmland polygons
    ee_features = []
    for fl in farmlands:
        ee_features.append(
            ee.Feature(ee.Geometry(fl['geometry']), {'fl_id': fl['id']})
        )
    fc = ee.FeatureCollection(ee_features)

    try:
        aoi = fc.geometry().bounds()

        s2 = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
              .filterDate(date_from, date_to)
              .filterBounds(aoi)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_max)))

        n_images = s2.size().getInfo()
        if n_images == 0:
            logger.info('No images for batch %s..%s', date_from, date_to)
            return {}

        # Cloud-mask + NDVI for every image
        def _add_ndvi(image):
            scl = image.select('SCL')
            clear = (scl.eq(4).Or(scl.eq(5))
                     .Or(scl.eq(6)).Or(scl.eq(7)))
            ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
            return ndvi.updateMask(clear)

        ndvi_col = s2.map(_add_ndvi)

        # Median composite — one clean image from all observations
        composite = ndvi_col.median().rename('NDVI')

        # Total pixel coverage (any S2 data, before cloud mask)
        total_band = s2.select('B4').mosaic().rename('total')

        stacked = composite.addBands(total_band)

        # Single reduceRegions call for ALL polygons
        reducer = (ee.Reducer.mean()
                   .combine(ee.Reducer.count(), '', True)
                   .combine(ee.Reducer.min(), '', True)
                   .combine(ee.Reducer.max(), '', True)
                   .combine(ee.Reducer.stdDev(), '', True))

        result_fc = stacked.reduceRegions(
            collection=fc,
            reducer=reducer,
            scale=10,
        )

        data = result_fc.getInfo()

    except Exception as e:
        raise GEEError(f'GEE batch error: {e}')

    # Parse and group by farmland_id
    results = {}
    for feat in (data or {}).get('features', []):
        props = feat.get('properties', {})
        fl_id = props.get('fl_id')
        if fl_id is None:
            continue

        total = props.get('total_count') or 0
        valid = props.get('NDVI_count') or 0
        mean = props.get('NDVI_mean')

        if not total or not valid or mean is None:
            continue

        ratio = valid / total
        if ratio < min_valid_ratio:
            continue

        if fl_id not in results:
            results[fl_id] = []

        results[fl_id].append({
            'date': mid_date,
            'mean': round(mean, 4),
            'median': round(mean, 4),
            'min': round(props.get('NDVI_min', 0) or 0, 4),
            'max': round(props.get('NDVI_max', 0) or 0, 4),
            'std': round(props.get('NDVI_stdDev', 0) or 0, 4),
            'pixel_count': int(total),
            'valid_pixel_count': int(valid),
            'valid_ratio': round(ratio, 4),
        })

    logger.info(
        'GEE composite: %d images → median, %d/%d polygons pass filter (%s..%s)',
        n_images, len(results), len(farmlands), date_from, date_to,
    )
    return results
