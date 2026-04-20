"""Shared utilities and constants for the agrocosmos views package."""
import math


MODIS_SATELLITES = ('modis_terra', 'modis_aqua')
RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')


def _satellite_filter(source):
    """Return a dict suitable for ``.filter(**...)`` on VegetationIndex queryset."""
    if source == 'modis':
        return {'scene__satellite__in': MODIS_SATELLITES}
    if source == 'raster':
        return {'scene__satellite__in': RASTER_SATELLITES}
    return {}


def _safe_round(val, precision=4):
    """Round a float safely, returning 0 for None/NaN/Inf."""
    if val is None:
        return 0.0
    try:
        if math.isnan(val) or math.isinf(val):
            return 0.0
    except TypeError:
        return 0.0
    return round(val, precision)
