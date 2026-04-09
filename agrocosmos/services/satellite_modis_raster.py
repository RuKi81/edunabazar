"""
MODIS NDVI raster download + local zonal statistics.

Downloads cloud-free 16-day NDVI composites from GEE as GeoTIFF,
then computes zonal statistics locally using rasterio + rasterstats.

Much faster than GEE reduceRegions for large numbers of polygons:
- Download: ~5-10 min per region per year (23 GeoTIFFs × ~1 MB)
- Zonal stats: ~1 min per region per composite (133K polygons)
- Total: ~30 min vs ~12 hours via API

Storage: /data/modis/{region_id}/{year}/
    modis_ndvi_{region_id}_{date_from}_{date_to}.tif
"""
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import ee
import rasterio
from django.conf import settings

from .satellite_gee import GEEError, initialize
from .zonal_stats import compute_zonal_stats  # noqa: F401 — re-export

logger = logging.getLogger(__name__)

# Default storage root — override with MODIS_RASTER_DIR env var
RASTER_DIR = os.environ.get(
    'MODIS_RASTER_DIR',
    getattr(settings, 'MODIS_RASTER_DIR', '/data/modis'),
)


def _biweekly_chunks(date_from, date_to):
    """
    Split date range into 16-day periods aligned to Jan 1 of the year.

    Anchoring to Jan 1 ensures that any --date-from value produces the
    same chunk boundaries, preventing duplicate records when re-running
    with different date ranges.
    """
    # Build grid anchored to Jan 1
    epoch = date(date_from.year, 1, 1)
    chunks = []
    cursor = epoch
    while cursor <= date_to:
        end = cursor + timedelta(days=15)
        # Only include chunks that overlap with [date_from, date_to]
        if end >= date_from:
            chunks.append((max(cursor, date_from), min(end, date_to)))
        cursor = end + timedelta(days=1)
    return chunks


def _raster_path(region_id, date_from, date_to):
    """Return local file path for a MODIS composite GeoTIFF."""
    d = Path(RASTER_DIR) / str(region_id) / str(date_from.year)
    d.mkdir(parents=True, exist_ok=True)
    fname = f'modis_ndvi_{region_id}_{date_from.isoformat()}_{date_to.isoformat()}.tif'
    return str(d / fname)


def download_composite(region_geom_extent, region_id, date_from, date_to,
                       overwrite=False):
    """
    Download a cloud-free MODIS NDVI composite from GEE as GeoTIFF.

    Args:
        region_geom_extent: tuple (xmin, ymin, xmax, ymax) in EPSG:4326
        region_id: int, used for file naming
        date_from: date
        date_to: date
        overwrite: re-download if file exists

    Returns:
        str: path to downloaded GeoTIFF, or None if no data
    """
    initialize()

    out_path = _raster_path(region_id, date_from, date_to)
    if os.path.exists(out_path) and not overwrite:
        logger.info('Raster exists, skipping: %s', out_path)
        return out_path

    xmin, ymin, xmax, ymax = region_geom_extent
    aoi = ee.Geometry.Rectangle([xmin, ymin, xmax, ymax])

    date_from_str = date_from.isoformat()
    date_to_str = date_to.isoformat()

    try:
        # Terra + Aqua merged
        terra = (ee.ImageCollection('MODIS/061/MOD13Q1')
                 .filterDate(date_from_str, date_to_str)
                 .filterBounds(aoi))
        aqua = (ee.ImageCollection('MODIS/061/MYD13Q1')
                .filterDate(date_from_str, date_to_str)
                .filterBounds(aoi))
        modis = terra.merge(aqua)

        n_images = modis.size().getInfo()
        if n_images == 0:
            logger.info('No MODIS images for %s..%s', date_from_str, date_to_str)
            return None

        # Quality-filter + scale NDVI
        def _process(image):
            ndvi = image.select('NDVI').multiply(0.0001).rename('NDVI')
            qa = image.select('SummaryQA')
            good = qa.lte(1)  # 0=good, 1=marginal
            return ndvi.updateMask(good)

        ndvi_col = modis.map(_process)

        # Median composite — cloud-free
        composite = ndvi_col.median().rename('NDVI').toFloat()

        # Download as GeoTIFF via computePixels (uses compute API, no
        # extra IAM permissions needed unlike getDownloadURL)
        content = ee.data.computePixels({
            'expression': composite,
            'fileFormat': 'GEO_TIFF',
            'grid': {
                'crsCode': 'EPSG:4326',
                'affineTransform': {
                    'scaleX': 250 / 111320,   # ~250m in degrees at equator
                    'shearX': 0,
                    'translateX': xmin,
                    'shearY': 0,
                    'scaleY': -250 / 111320,
                    'translateY': ymax,
                },
                'dimensions': {
                    'width': int((xmax - xmin) / (250 / 111320)) + 1,
                    'height': int((ymax - ymin) / (250 / 111320)) + 1,
                },
            },
        })

        with open(out_path, 'wb') as f:
            f.write(content)

        # Verify the file is readable
        with rasterio.open(out_path) as ds:
            logger.info(
                'Downloaded: %s (%d×%d, %.1f MB, %d images → median)',
                out_path, ds.width, ds.height,
                os.path.getsize(out_path) / 1e6, n_images,
            )

        return out_path

    except Exception as e:
        # Clean up partial file
        if os.path.exists(out_path):
            os.remove(out_path)
        raise GEEError(f'MODIS download error: {e}')


def download_year(region_geom_extent, region_id, date_from, date_to,
                  overwrite=False):
    """
    Download all 16-day composites for a date range.

    Returns:
        list of (date_from, date_to, tif_path) tuples
    """
    chunks = _biweekly_chunks(date_from, date_to)
    results = []
    for i, (cf, ct) in enumerate(chunks):
        logger.info(
            'Downloading composite %d/%d: %s..%s (region %d)',
            i + 1, len(chunks), cf, ct, region_id,
        )
        path = download_composite(
            region_geom_extent, region_id, cf, ct,
            overwrite=overwrite,
        )
        results.append((cf, ct, path))
    return results


# compute_zonal_stats re-exported from .zonal_stats for backward compatibility
