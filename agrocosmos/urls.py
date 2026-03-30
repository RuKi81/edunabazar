from django.urls import path
from . import views

app_name = 'agrocosmos'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('api/regions/', views.api_regions, name='api_regions'),
    path('api/districts/', views.api_districts, name='api_districts'),
    path('api/farmlands/', views.api_farmlands, name='api_farmlands'),
    path('api/farmland/ndvi/', views.api_farmland_ndvi, name='api_farmland_ndvi'),
]
