"""Agrocosmos views package.

Historically ``agrocosmos.views`` was a single 1165-line module. It has been
split into focused submodules; this ``__init__`` re-exports every public name
so that existing imports (``views.dashboard``, ``views.api_ndvi_stats``, ...)
and ``urls.py`` keep working without changes.
"""
from ._helpers import (
    MODIS_SATELLITES,
    RASTER_SATELLITES,
    _satellite_filter,
    _safe_round,
)
from .pages import (
    dashboard,
    raster_dashboard,
    report_region,
    report_district,
    _get_legacy_user,
)
from .geojson import (
    api_regions,
    api_districts,
    api_farmlands,
)
from .tiles import (
    api_tile,
    api_raster_tile,
    _tile_bbox,
)
from .ndvi import (
    api_farmland_ndvi,
    api_ndvi_stats,
    api_phenology,
    api_raster_composites,
)
from .reports import (
    api_report_region,
    api_report_district,
    _ndvi_assessment,
)
from .cabinet import me_agrocosmos

__all__ = [
    # constants
    'MODIS_SATELLITES', 'RASTER_SATELLITES',
    # pages
    'dashboard', 'raster_dashboard', 'report_region', 'report_district',
    # GeoJSON
    'api_regions', 'api_districts', 'api_farmlands',
    # tiles
    'api_tile', 'api_raster_tile',
    # NDVI data APIs
    'api_farmland_ndvi', 'api_ndvi_stats', 'api_phenology',
    'api_raster_composites',
    # reports
    'api_report_region', 'api_report_district',
    # cabinet
    'me_agrocosmos',
    # private helpers (exported for tests / legacy callers)
    '_satellite_filter', '_safe_round', '_get_legacy_user',
    '_tile_bbox', '_ndvi_assessment',
]
