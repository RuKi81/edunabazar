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
import zipfile
from datetime import date, timedelta
from pathlib import Path

import ee
import rasterio
import rasterstats
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

        # Download as GeoTIFF via getDownloadURL
        url = composite.getDownloadURL({
            'name': 'modis_ndvi',
            'region': aoi,
            'scale': 250,
            'format': 'GEO_TIFF',
            'crs': 'EPSG:4326',
        })

        import requests
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()

        # GEE returns a zip with the GeoTIFF inside
        content = resp.content
        if content[:2] == b'PK':  # ZIP file
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                tif_names = [n for n in zf.namelist() if n.endswith('.tif')]
                if not tif_names:
                    logger.error('No .tif in downloaded zip')
                    return None
                content = zf.read(tif_names[0])

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


def compute_zonal_stats(tif_path, farmland_geometries, min_valid_ratio=0.5):
    """
    Compute NDVI zonal statistics for all farmlands from a local GeoTIFF.

    Args:
        tif_path: str, path to MODIS NDVI GeoTIFF
        farmland_geometries: list of dicts with 'id' and 'geometry' (GeoJSON)
        min_valid_ratio: min ratio of valid (non-nodata) pixels

    Returns:
        dict: {farmland_id: {'mean', 'min', 'max', 'std',
               'pixel_count', 'valid_pixel_count', 'valid_ratio'}}
    """
    if not tif_path or not os.path.exists(tif_path):
        return {}

    # Prepare geometries for rasterstats
    geojson_features = []
    id_map = []
    for fl in farmland_geometries:
        geojson_features.append(fl['geometry'])
        id_map.append(fl['id'])

    try:
        stats = rasterstats.zonal_stats(
            geojson_features,
            tif_path,
            stats=['mean', 'min', 'max', 'std', 'count'],
            nodata=float('nan'),
            all_touched=True,
        )
    except Exception as e:
        logger.error('Zonal stats error for %s: %s', tif_path, e)
        return {}

    # Also get total pixel count (including nodata) via count with nodata=None
    try:
        total_stats = rasterstats.zonal_stats(
            geojson_features,
            tif_path,
            stats=['count'],
            all_touched=True,
        )
    except Exception:
        total_stats = [{'count': 0}] * len(stats)

    results = {}
    for i, (fl_id, st, tst) in enumerate(zip(id_map, stats, total_stats)):
        valid = st.get('count') or 0
        total = tst.get('count') or 0
        mean = st.get('mean')

        if not total or not valid or mean is None:
            continue

        ratio = valid / total if total else 0
        if ratio < min_valid_ratio:
            continue

        results[fl_id] = {
            'mean': round(mean, 4),
            'median': round(mean, 4),
            'min': round(st.get('min', 0) or 0, 4),
            'max': round(st.get('max', 0) or 0, 4),
            'std': round(st.get('std', 0) or 0, 4),
            'pixel_count': int(total),
            'valid_pixel_count': int(valid),
            'valid_ratio': round(ratio, 4),
        }

    logger.info(
        'Zonal stats: %d/%d farmlands with valid data from %s',
        len(results), len(farmland_geometries), os.path.basename(tif_path),
    )
    return results
