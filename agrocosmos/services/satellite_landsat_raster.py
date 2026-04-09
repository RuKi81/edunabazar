"""
Landsat 8 / 9 NDVI raster download + local zonal statistics.

Downloads cloud-free NDVI composites from GEE as GeoTIFF (30m resolution),
then delegates zonal statistics to the shared zonal_stats module.

Approach:
- 16-day median composites (Landsat revisit = 16 days, L8+L9 combined = 8 days)
- Cloud mask via QA_PIXEL band (bit 3 = cloud, bit 4 = cloud shadow)
- NDVI = (SR_B5 - SR_B4) / (SR_B5 + SR_B4)  — Collection 2, Level 2
- Harmonization to S2-equivalent NDVI: NDVI_h = 0.9589 * NDVI_L + 0.0029
  (Roy et al., 2016, doi:10.1016/j.rse.2015.12.024)

Storage: /data/landsat/{region_id}/{year}/
    landsat_ndvi_{region_id}_{date_from}_{date_to}.tif

GEE collections:
    L8: LANDSAT/LC08/C02/T1_L2  (2013-04 → present)
    L9: LANDSAT/LC09/C02/T1_L2  (2021-10 → present)
"""
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import ee
from django.conf import settings

from .satellite_gee import GEEError, initialize

logger = logging.getLogger(__name__)

# Storage root
RASTER_DIR = os.environ.get(
    'LANDSAT_RASTER_DIR',
    getattr(settings, 'LANDSAT_RASTER_DIR', '/data/landsat'),
)

SCALE_M = 30
SCALE_DEG = SCALE_M / 111320
COMPOSITE_DAYS = 16  # L8 revisit; L8+L9 = 8 days overlap

# Harmonization coefficients (Roy et al. 2016)
# NDVI_S2 ≈ a * NDVI_Landsat + b
HARMONIZE_A = 0.9589
HARMONIZE_B = 0.0029


# ---------------------------------------------------------------------------
# Temporal helpers
# ---------------------------------------------------------------------------

def landsat_chunks(date_from, date_to, days=COMPOSITE_DAYS):
    """
    Split date range into N-day periods anchored to Jan 1.
    """
    epoch = date(date_from.year, 1, 1)
    chunks = []
    cursor = epoch
    while cursor <= date_to:
        end = cursor + timedelta(days=days - 1)
        if end >= date_from:
            chunks.append((max(cursor, date_from), min(end, date_to)))
        cursor = end + timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _raster_path(region_id, date_from, date_to):
    d = Path(RASTER_DIR) / str(region_id) / str(date_from.year)
    d.mkdir(parents=True, exist_ok=True)
    fname = f'landsat_ndvi_{region_id}_{date_from.isoformat()}_{date_to.isoformat()}.tif'
    return str(d / fname)


def download_composite(region_geom_extent, region_id, date_from, date_to,
                       cloud_max=30, harmonize=True, overwrite=False):
    """
    Download a cloud-free L8+L9 median NDVI composite from GEE as GeoTIFF.

    Args:
        region_geom_extent: (xmin, ymin, xmax, ymax) in EPSG:4326
        region_id: int
        date_from, date_to: date
        cloud_max: max scene cloud cover %
        harmonize: apply Roy et al. correction to S2-equivalent NDVI
        overwrite: re-download if exists

    Returns:
        str: path to GeoTIFF, or None if no data
    """
    initialize()

    out_path = _raster_path(region_id, date_from, date_to)
    if os.path.exists(out_path) and not overwrite:
        logger.info('Raster exists, skipping: %s', out_path)
        return out_path

    xmin, ymin, xmax, ymax = region_geom_extent
    aoi = ee.Geometry.Rectangle([xmin, ymin, xmax, ymax])

    df = date_from.isoformat()
    dt = date_to.isoformat()

    try:
        # Merge L8 + L9
        l8 = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
              .filterDate(df, dt)
              .filterBounds(aoi)
              .filter(ee.Filter.lt('CLOUD_COVER', cloud_max)))

        l9 = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
              .filterDate(df, dt)
              .filterBounds(aoi)
              .filter(ee.Filter.lt('CLOUD_COVER', cloud_max)))

        merged = l8.merge(l9)

        n_images = merged.size().getInfo()
        if n_images == 0:
            logger.info('No Landsat images for %s..%s', df, dt)
            return None

        def _add_ndvi(image):
            # QA_PIXEL: bit 3 = cloud, bit 4 = cloud shadow
            qa = image.select('QA_PIXEL')
            cloud_mask = (qa.bitwiseAnd(1 << 3).eq(0)
                          .And(qa.bitwiseAnd(1 << 4).eq(0)))

            # Scale factors for Collection 2 Level 2
            sr = image.select(['SR_B4', 'SR_B5']).multiply(0.0000275).add(-0.2)
            ndvi = sr.normalizedDifference(['SR_B5', 'SR_B4']).rename('NDVI')
            return ndvi.updateMask(cloud_mask)

        ndvi_col = merged.map(_add_ndvi)
        composite = ndvi_col.median().rename('NDVI').toFloat()

        # Harmonize Landsat NDVI → S2-equivalent
        if harmonize:
            composite = composite.multiply(HARMONIZE_A).add(HARMONIZE_B).rename('NDVI')

        width = int((xmax - xmin) / SCALE_DEG) + 1
        height = int((ymax - ymin) / SCALE_DEG) + 1

        content = ee.data.computePixels({
            'expression': composite,
            'fileFormat': 'GEO_TIFF',
            'grid': {
                'crsCode': 'EPSG:4326',
                'affineTransform': {
                    'scaleX': SCALE_DEG,
                    'shearX': 0,
                    'translateX': xmin,
                    'shearY': 0,
                    'scaleY': -SCALE_DEG,
                    'translateY': ymax,
                },
                'dimensions': {
                    'width': width,
                    'height': height,
                },
            },
        })

        with open(out_path, 'wb') as f:
            f.write(content)

        import rasterio
        with rasterio.open(out_path) as ds:
            logger.info(
                'Downloaded Landsat: %s (%d×%d, %.1f MB, %d images → median, harmonize=%s)',
                out_path, ds.width, ds.height,
                os.path.getsize(out_path) / 1e6, n_images, harmonize,
            )

        return out_path

    except Exception as e:
        if os.path.exists(out_path):
            os.remove(out_path)
        raise GEEError(f'Landsat download error: {e}')


def download_period(region_geom_extent, region_id, date_from, date_to,
                    cloud_max=30, harmonize=True, overwrite=False):
    """
    Download all Landsat composites for a date range.

    Returns:
        list of (date_from, date_to, tif_path) tuples
    """
    chunks = landsat_chunks(date_from, date_to)
    results = []
    for i, (cf, ct) in enumerate(chunks):
        logger.info(
            'Downloading Landsat composite %d/%d: %s..%s (region %d)',
            i + 1, len(chunks), cf, ct, region_id,
        )
        path = download_composite(
            region_geom_extent, region_id, cf, ct,
            cloud_max=cloud_max, harmonize=harmonize, overwrite=overwrite,
        )
        results.append((cf, ct, path))
    return results
