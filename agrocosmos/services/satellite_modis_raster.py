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
import io
import logging
import os
import warnings
import zipfile
from datetime import date, timedelta
from pathlib import Path

import ee
import numpy as np
import rasterio
import rasterio.features
import rasterio.mask
from django.conf import settings

from .satellite_gee import GEEError, initialize

logger = logging.getLogger(__name__)

# Default storage root — override with MODIS_RASTER_DIR env var
RASTER_DIR = os.environ.get(
    'MODIS_RASTER_DIR',
    getattr(settings, 'MODIS_RASTER_DIR', '/data/modis'),
)


def _biweekly_chunks(date_from, date_to):
    """Split date range into 16-day periods (matches MODIS MOD13Q1 cadence)."""
    chunks = []
    cursor = date_from
    while cursor <= date_to:
        end = min(cursor + timedelta(days=15), date_to)
        chunks.append((cursor, end))
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


def compute_zonal_stats(tif_path, farmland_geometries, min_valid_ratio=0.5,
                        progress_callback=None):
    """
    Compute NDVI zonal statistics for all farmlands from a local GeoTIFF.

    Uses rasterio + numpy directly (no rasterstats) for speed:
    1. Read entire raster into memory (~1 MB for MODIS)
    2. Rasterize all polygons into a label array (pixel → farmland index)
    3. Compute stats per label using numpy — vectorized, no per-polygon loop

    Args:
        tif_path: str, path to MODIS NDVI GeoTIFF
        farmland_geometries: list of dicts with 'id' and 'geometry' (GeoJSON)
        min_valid_ratio: min ratio of valid (non-nodata) pixels
        progress_callback: optional callable(done, total) for progress

    Returns:
        dict: {farmland_id: {'mean', 'min', 'max', 'std',
               'pixel_count', 'valid_pixel_count', 'valid_ratio'}}
    """
    if not tif_path or not os.path.exists(tif_path):
        return {}

    if not farmland_geometries:
        return {}

    try:
        with rasterio.open(tif_path) as ds:
            ndvi = ds.read(1)  # shape: (H, W)
            transform = ds.transform
            nodata = ds.nodata

        # Build valid mask
        if nodata is not None and not np.isnan(nodata):
            valid_mask = ndvi != nodata
        else:
            valid_mask = ~np.isnan(ndvi)

        # Prepare shapes for rasterize: (geometry, label_index)
        # Use 1-based indices (0 = no polygon)
        shapes = []
        id_list = []
        for i, fl in enumerate(farmland_geometries):
            shapes.append((fl['geometry'], i + 1))
            id_list.append(fl['id'])

        max_label = len(farmland_geometries)

        # Rasterize all polygons at once → label array
        labels = rasterio.features.rasterize(
            shapes,
            out_shape=ndvi.shape,
            transform=transform,
            fill=0,
            dtype='int32',
            all_touched=True,
        )

        # Flatten for vectorized processing
        flat_labels = labels.ravel()
        flat_ndvi = ndvi.ravel()
        flat_valid = valid_mask.ravel()

        # Total pixel count per label (including nodata)
        all_mask = flat_labels > 0
        total_per_label = np.bincount(
            flat_labels[all_mask], minlength=max_label + 1
        )

        # Valid pixels only: sort by label for fast groupby
        valid_px_mask = all_mask & flat_valid
        valid_lbls = flat_labels[valid_px_mask]
        valid_vals = flat_ndvi[valid_px_mask]

        if len(valid_lbls) == 0:
            return {}

        order = np.argsort(valid_lbls, kind='mergesort')
        sorted_lbls = valid_lbls[order]
        sorted_vals = valid_vals[order]

        # Split points for each label
        split_pts = np.searchsorted(
            sorted_lbls, np.arange(1, max_label + 2)
        )

        results = {}
        for lbl in range(1, max_label + 1):
            start = split_pts[lbl - 1]
            end = split_pts[lbl]
            valid_count = end - start
            if valid_count == 0:
                continue

            total_count = int(total_per_label[lbl])
            ratio = valid_count / total_count if total_count else 0
            if ratio < min_valid_ratio:
                continue

            vals = sorted_vals[start:end]
            fl_id = id_list[lbl - 1]

            results[fl_id] = {
                'mean': round(float(vals.mean()), 4),
                'median': round(float(np.median(vals)), 4),
                'min': round(float(vals.min()), 4),
                'max': round(float(vals.max()), 4),
                'std': round(float(vals.std()), 4),
                'pixel_count': total_count,
                'valid_pixel_count': int(valid_count),
                'valid_ratio': round(ratio, 4),
            }

        if progress_callback:
            progress_callback(len(results), len(farmland_geometries))

    except Exception as e:
        logger.error('Zonal stats error for %s: %s', tif_path, e)
        return {}

    logger.info(
        'Zonal stats: %d/%d farmlands with valid data from %s',
        len(results), len(farmland_geometries), os.path.basename(tif_path),
    )
    return results
