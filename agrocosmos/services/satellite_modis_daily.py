"""
MODIS daily NDVI raster download (MOD09GQ + MYD09GQ) + local zonal statistics.

Uses daily surface reflectance to compute NDVI manually:
    NDVI = (NIR - Red) / (NIR + Red)

Provides near-real-time data (1-2 day publication lag) compared to
MOD13Q1/MYD13Q1 16-day composites (2-3 week lag).

Uses 5-day median composites to reduce cloud contamination while
maintaining high temporal resolution (~73 composites/year).

Storage: /data/modis_daily/{region_id}/{year}/
    modis_daily_ndvi_{region_id}_{date_from}_{date_to}.tif

NOTE: Old MOD13Q1 pipeline in satellite_modis_raster.py is kept intact
      for easy rollback if this approach doesn't work well.
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

# Default storage root — override with MODIS_DAILY_RASTER_DIR env var
RASTER_DIR = os.environ.get(
    'MODIS_DAILY_RASTER_DIR',
    getattr(settings, 'MODIS_DAILY_RASTER_DIR', '/data/modis_daily'),
)


def _daily_chunks(date_from, date_to):
    """
    Split date range into 5-day periods aligned to Jan 1 of the year.

    5-day windows with daily data (Terra + Aqua = ~4 overpasses/day)
    give ~20-30 clear observations per window, enough for a clean
    median composite in most weather conditions.

    Anchoring to Jan 1 ensures that any --date-from value produces the
    same chunk boundaries, preventing duplicate records when re-running
    with different date ranges.
    """
    epoch = date(date_from.year, 1, 1)
    chunks = []
    cursor = epoch
    while cursor <= date_to:
        end = cursor + timedelta(days=4)
        if end >= date_from:
            chunks.append((max(cursor, date_from), min(end, date_to)))
        cursor = end + timedelta(days=1)
    return chunks


def _raster_path(region_id, date_from, date_to):
    """Return local file path for a MODIS daily composite GeoTIFF."""
    d = Path(RASTER_DIR) / str(region_id) / str(date_from.year)
    d.mkdir(parents=True, exist_ok=True)
    fname = f'modis_daily_ndvi_{region_id}_{date_from.isoformat()}_{date_to.isoformat()}.tif'
    return str(d / fname)


def download_composite(region_geom_extent, region_id, date_from, date_to,
                       overwrite=False):
    """
    Download a MODIS daily NDVI composite from GEE as GeoTIFF.

    Uses MOD09GQ (Terra) + MYD09GQ (Aqua) daily surface reflectance
    at 250m. NDVI computed from Red (b01) and NIR (b02) bands.
    Cloud filtering via QC_250m quality band.

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

    # GEE filterDate is exclusive on end: [start, end)
    # Add 1 day so the end date is included
    date_from_str = date_from.isoformat()
    date_to_str = (date_to + timedelta(days=1)).isoformat()

    try:
        # Terra + Aqua daily surface reflectance, 250m
        terra = (ee.ImageCollection('MODIS/061/MOD09GQ')
                 .filterDate(date_from_str, date_to_str)
                 .filterBounds(aoi))
        aqua = (ee.ImageCollection('MODIS/061/MYD09GQ')
                .filterDate(date_from_str, date_to_str)
                .filterBounds(aoi))
        merged = terra.merge(aqua)

        n_images = merged.size().getInfo()
        if n_images == 0:
            logger.info('No MODIS daily images for %s..%s', date_from_str, date_to_str)
            return None

        logger.info(
            'MODIS daily: %d images for %s..%s',
            n_images, date_from_str, date_to_str,
        )

        def _compute_ndvi(image):
            """Compute NDVI from surface reflectance with QC mask."""
            # Scale factor for MOD09GQ: 0.0001
            red = image.select('sur_refl_b01').multiply(0.0001)
            nir = image.select('sur_refl_b02').multiply(0.0001)

            # QC_250m band — bits 0-1: MODLAND QA
            #   00 = corrected product produced at ideal quality
            #   01 = corrected product produced at less than ideal quality
            #   10 = not produced due to cloud
            #   11 = not produced for other reason
            qc = image.select('QC_250m')
            good_quality = qc.bitwiseAnd(3).lte(1)  # 00 or 01

            # Additional: reject negative reflectance (sensor errors)
            valid_red = red.gte(0).And(red.lte(1))
            valid_nir = nir.gte(0).And(nir.lte(1))

            ndvi = nir.subtract(red).divide(nir.add(red))

            # Mask: good QC + valid reflectance + physical NDVI range
            mask = good_quality.And(valid_red).And(valid_nir)
            return ndvi.updateMask(mask).rename('NDVI').toFloat()

        ndvi_col = merged.map(_compute_ndvi)

        # Median composite — reduces cloud/noise contamination
        composite = ndvi_col.median().rename('NDVI').toFloat()

        # Download as GeoTIFF via computePixels
        content = ee.data.computePixels({
            'expression': composite,
            'fileFormat': 'GEO_TIFF',
            'grid': {
                'crsCode': 'EPSG:4326',
                'affineTransform': {
                    'scaleX': 250 / 111320,
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
        raise GEEError(f'MODIS daily download error: {e}')


def download_year(region_geom_extent, region_id, date_from, date_to,
                  overwrite=False):
    """
    Download all 5-day daily composites for a date range.

    Returns:
        list of (date_from, date_to, tif_path) tuples
    """
    chunks = _daily_chunks(date_from, date_to)
    results = []
    for i, (cf, ct) in enumerate(chunks):
        logger.info(
            'Downloading daily composite %d/%d: %s..%s (region %d)',
            i + 1, len(chunks), cf, ct, region_id,
        )
        path = download_composite(
            region_geom_extent, region_id, cf, ct,
            overwrite=overwrite,
        )
        results.append((cf, ct, path))
    return results
