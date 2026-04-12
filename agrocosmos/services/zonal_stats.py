"""
Shared zonal statistics for raster NDVI composites.

Used by S2, Landsat and MODIS raster pipelines.
Downloads produce GeoTIFF files; this module computes per-polygon
statistics from those files using rasterio + numpy.

Performance: ~1 min per composite for 133K polygons (MODIS 250m).
"""
import logging
import os

import numpy as np
import rasterio
import rasterio.features

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10_000  # polygons per rasterize batch


def _stats_for_chunk(ndvi, valid_mask, transform, chunk_geoms, chunk_ids,
                     min_valid_ratio):
    """Rasterize a chunk of polygons and compute stats via numpy groupby."""
    n = len(chunk_geoms)
    shapes = [(g, i + 1) for i, g in enumerate(chunk_geoms)]

    labels = rasterio.features.rasterize(
        shapes,
        out_shape=ndvi.shape,
        transform=transform,
        fill=0,
        dtype='int32',
        all_touched=True,
    )

    flat_labels = labels.ravel()
    flat_ndvi = ndvi.ravel()
    flat_valid = valid_mask.ravel()

    all_mask = flat_labels > 0
    if not all_mask.any():
        return {}

    total_per = np.bincount(flat_labels[all_mask], minlength=n + 1)

    valid_px = all_mask & flat_valid
    v_lbls = flat_labels[valid_px]
    v_vals = flat_ndvi[valid_px]

    if len(v_lbls) == 0:
        return {}

    order = np.argsort(v_lbls, kind='mergesort')
    sorted_lbls = v_lbls[order]
    sorted_vals = v_vals[order]
    split_pts = np.searchsorted(sorted_lbls, np.arange(1, n + 2))

    results = {}
    for lbl in range(1, n + 1):
        s, e = split_pts[lbl - 1], split_pts[lbl]
        vc = e - s
        if vc == 0:
            continue
        tc = int(total_per[lbl])
        ratio = vc / tc if tc else 0
        if ratio < min_valid_ratio:
            continue
        vals = sorted_vals[s:e]
        results[chunk_ids[lbl - 1]] = {
            'mean': round(float(vals.mean()), 4),
            'median': round(float(np.median(vals)), 4),
            'min': round(float(vals.min()), 4),
            'max': round(float(vals.max()), 4),
            'std': round(float(vals.std()), 4),
            'pixel_count': tc,
            'valid_pixel_count': int(vc),
            'valid_ratio': round(ratio, 4),
        }
    return results


def compute_zonal_stats(tif_path, farmland_geometries, min_valid_ratio=0.5,
                        progress_callback=None):
    """
    Compute NDVI zonal statistics for all farmlands from a local GeoTIFF.

    Processes polygons in chunks of CHUNK_SIZE (10K) to keep rasterize fast.
    Each chunk: rasterize → numpy groupby → stats.

    Args:
        tif_path: str, path to NDVI GeoTIFF (any sensor)
        farmland_geometries: list of dicts with 'id' and 'geometry' (GeoJSON)
        min_valid_ratio: min ratio of valid (non-nodata) pixels
        progress_callback: optional callable(done, total) for progress

    Returns:
        dict: {farmland_id: {'mean', 'median', 'min', 'max', 'std',
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

        # Build valid mask (nodata + physical NDVI range)
        if nodata is not None and not np.isnan(nodata):
            valid_mask = ndvi != nodata
        else:
            valid_mask = ~np.isnan(ndvi)
        # Reject physically impossible values
        valid_mask &= (ndvi >= -0.2) & (ndvi <= 1.0)

        total_fl = len(farmland_geometries)
        results = {}

        for offset in range(0, total_fl, CHUNK_SIZE):
            chunk = farmland_geometries[offset:offset + CHUNK_SIZE]
            chunk_geoms = [fl['geometry'] for fl in chunk]
            chunk_ids = [fl['id'] for fl in chunk]

            chunk_results = _stats_for_chunk(
                ndvi, valid_mask, transform,
                chunk_geoms, chunk_ids, min_valid_ratio,
            )
            results.update(chunk_results)

            done = min(offset + CHUNK_SIZE, total_fl)
            if progress_callback:
                progress_callback(done, total_fl)

    except Exception as e:
        logger.error('Zonal stats error for %s: %s', tif_path, e)
        return {}

    logger.info(
        'Zonal stats: %d/%d farmlands with valid data from %s',
        len(results), len(farmland_geometries), os.path.basename(tif_path),
    )
    return results
