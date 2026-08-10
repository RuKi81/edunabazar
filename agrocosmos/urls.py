from django.urls import path
from . import views

app_name = 'agrocosmos'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('raster/', views.raster_dashboard, name='raster_dashboard'),
    path('report/region/', views.report_region, name='report_region'),
    path('report/farmland/', views.report_farmland, name='report_farmland'),
    path('report/screening/', views.report_screening, name='report_screening'),
    path('report/district-detailed/', views.report_district_detailed, name='report_district_detailed'),
    path('report/unused/', views.report_unused, name='report_unused'),
    path('api/regions/', views.api_regions, name='api_regions'),
    path('api/districts/', views.api_districts, name='api_districts'),
    path('api/districts/status/', views.api_districts_status, name='api_districts_status'),
    path('api/districts/status/timeline/', views.api_districts_status_timeline, name='api_districts_status_timeline'),
    path('api/farmlands/', views.api_farmlands, name='api_farmlands'),
    path('api/farmland/', views.api_farmland_collection, name='api_farmland_collection'),
    path('api/farmland/<int:pk>/', views.api_farmland_detail, name='api_farmland_detail'),
    path('api/tiles/<int:z>/<int:x>/<int:y>.pbf', views.api_tile, name='api_tile'),
    path('api/farmland/ndvi/', views.api_farmland_ndvi, name='api_farmland_ndvi'),
    path('api/ndvi-stats/', views.api_ndvi_stats, name='api_ndvi_stats'),
    path('api/raster-tile/<int:z>/<int:x>/<int:y>.png', views.api_raster_tile, name='api_raster_tile'),
    path('api/raster-composites/', views.api_raster_composites, name='api_raster_composites'),
    path('api/raster-preview/', views.api_raster_preview, name='api_raster_preview'),
    path('api/farmland/raster-frames/', views.api_farmland_raster_frames, name='api_farmland_raster_frames'),
    path('api/farmland/zones/', views.api_farmland_zones, name='api_farmland_zones'),
    path('api/phenology/', views.api_phenology, name='api_phenology'),
    path('api/report/region/', views.api_report_region, name='api_report_region'),
    path('api/report/district/', views.api_report_district, name='api_report_district'),
    path('api/report/country/', views.api_report_country, name='api_report_country'),
    path('api/report/farmland/', views.api_report_farmland, name='api_report_farmland'),
    path('api/report/screening/', views.api_report_screening, name='api_report_screening'),
    path('api/report/district-detailed/', views.api_report_district_detailed, name='api_report_district_detailed'),
    path('api/report/unused/', views.api_report_unused, name='api_report_unused'),
    path('api/yield/forecast/', views.api_yield_forecast, name='api_yield_forecast'),
    path(
        'api/yield/forecast/region/<int:region_id>/',
        views.api_yield_forecast_region, name='api_yield_forecast_region',
    ),
    path('api/yield/models/', views.api_yield_models, name='api_yield_models'),
]
