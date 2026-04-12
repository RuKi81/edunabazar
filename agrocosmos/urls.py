from django.urls import path
from . import views

app_name = 'agrocosmos'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('raster/', views.raster_dashboard, name='raster_dashboard'),
    path('api/regions/', views.api_regions, name='api_regions'),
    path('api/districts/', views.api_districts, name='api_districts'),
    path('api/farmlands/', views.api_farmlands, name='api_farmlands'),
    path('api/tiles/<int:z>/<int:x>/<int:y>.pbf', views.api_tile, name='api_tile'),
    path('api/farmland/ndvi/', views.api_farmland_ndvi, name='api_farmland_ndvi'),
    path('api/ndvi-stats/', views.api_ndvi_stats, name='api_ndvi_stats'),
    path('api/raster-tile/<int:z>/<int:x>/<int:y>.png', views.api_raster_tile, name='api_raster_tile'),
    path('api/raster-composites/', views.api_raster_composites, name='api_raster_composites'),
]
