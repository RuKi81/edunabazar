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

    # ── UI ──
    path('me/fields/', views.fields_list_page, name='ui_fields_list'),
    path('me/fields/<int:pk>/', views.field_detail_page, name='ui_field_detail'),
    path('me/fields/<int:pk>/passport/', views.field_passport_page, name='ui_field_passport'),
    # Admin-only experimental MapLibre + MVT GIS page (см. views.gis_page).
    path('me/gis/', views.gis_page, name='ui_gis'),
]
