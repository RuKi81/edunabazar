"""URL-конфигурация ``my_fields``.

Монтируется в корневом ``enb_django/urls.py`` без префикса, поэтому
все маршруты здесь идут с полным абсолютным путём (``api/...``,
``me/...``).
"""
from __future__ import annotations

from django.urls import path

from . import api, views

app_name = 'my_fields'

urlpatterns = [
    # ── REST API ──
    path('api/my/fields/', api.fields_collection, name='api_fields'),
    path('api/my/fields/<int:pk>/', api.field_detail, name='api_field_detail'),
    path('api/my/fields/monitoring/', api.monitoring_collection, name='api_monitoring'),
    path('api/my/fields/<int:pk>/monitoring/', api.field_monitoring, name='api_field_monitoring'),
    path('api/my/fields/<int:pk>/events/', api.events_collection, name='api_events'),
    path('api/my/fields/<int:pk>/events/<int:eid>/', api.event_detail, name='api_event_detail'),
    path('api/my/fields/<int:pk>/seasons/', api.seasons_collection, name='api_seasons'),
    path('api/my/fields/<int:pk>/seasons/<int:sid>/', api.season_detail, name='api_season_detail'),

    # ── Паспорт поля (NDVI-снимки + зоны неоднородности) ──
    path('api/my/fields/<int:pk>/passport/frames/', api.field_passport_frames, name='api_passport_frames'),
    path('api/my/fields/<int:pk>/passport/preview/', api.field_passport_preview, name='api_passport_preview'),
    path('api/my/fields/<int:pk>/passport/zones/', api.field_passport_zones, name='api_passport_zones'),
    path('api/my/fields/<int:pk>/passport/zones/kml/', api.field_passport_zones_kml, name='api_passport_zones_kml'),
    path('api/my/fields/<int:pk>/passport/zones/shp/', api.field_passport_zones_shp, name='api_passport_zones_shp'),

    # ── ГИС-слои: загрузка SHP (ZIP) → таблица PostGIS на каждый .shp ──
    path('me/gis/api/layers/', api.gis_layers_collection, name='api_gis_layers'),
    path('me/gis/api/layers/create/', api.gis_layer_create, name='api_gis_layer_create'),
    path('me/gis/api/layers/reorder/', api.gis_layers_reorder, name='api_gis_layers_reorder'),
    path('me/gis/api/folders/', api.gis_folders_collection, name='api_gis_folders'),
    path('me/gis/api/folders/<int:pk>/', api.gis_folder_detail, name='api_gis_folder_detail'),
    path('me/gis/api/layers/<int:pk>/', api.gis_layer_detail, name='api_gis_layer_detail'),
    path('me/gis/api/layers/<int:pk>/features/', api.gis_layer_features, name='api_gis_layer_features'),
    path('me/gis/api/layers/<int:pk>/query/', api.gis_layer_query, name='api_gis_layer_query'),
    path('me/gis/api/overlay/', api.gis_overlay_create, name='api_gis_overlay_create'),
    path('me/gis/api/overlay/<int:run_id>/', api.gis_overlay_status, name='api_gis_overlay_status'),
    path('me/gis/api/layers/<int:pk>/export/', api.gis_layer_export, name='api_gis_layer_export'),
    path('me/gis/api/layers/<int:pk>/field-stats/', api.gis_layer_field_stats, name='api_gis_layer_field_stats'),
    path('me/gis/api/layers/<int:pk>/features/<int:fid>/',
         api.gis_layer_feature_detail, name='api_gis_layer_feature_detail'),
    path('me/gis/api/layers/<int:pk>/tiles/<int:z>/<int:x>/<int:y>.pbf',
         api.gis_layer_tiles, name='api_gis_layer_tiles'),

    # ── Растровые слои (GeoTIFF → COG в MinIO/S3) ──
    path('me/gis/api/rasters/', api.raster_layers_collection, name='api_raster_layers'),
    path('me/gis/api/rasters/upload/init/', api.raster_upload_init, name='api_raster_upload_init'),
    path('me/gis/api/rasters/upload/sign/', api.raster_upload_sign, name='api_raster_upload_sign'),
    path('me/gis/api/rasters/upload/complete/', api.raster_upload_complete, name='api_raster_upload_complete'),
    path('me/gis/api/rasters/upload/abort/', api.raster_upload_abort, name='api_raster_upload_abort'),
    path('me/gis/api/rasters/<int:pk>/tiles/<int:z>/<int:x>/<int:y>.png',
         api.raster_tile, name='api_raster_tile'),
    path('me/gis/api/rasters/<int:pk>/reprocess/', api.raster_reprocess, name='api_raster_reprocess'),
    path('me/gis/api/rasters/<int:pk>/', api.raster_layer_detail, name='api_raster_layer_detail'),

    # ── UI ──
    path('me/fields/', views.fields_list_page, name='ui_fields_list'),
    path('me/fields/<int:pk>/', views.field_detail_page, name='ui_field_detail'),
    path('me/fields/<int:pk>/passport/', views.field_passport_page, name='ui_field_passport'),
    # Admin-only experimental MapLibre + MVT GIS page (см. views.gis_page).
    path('me/gis/', views.gis_page, name='ui_gis'),
]
