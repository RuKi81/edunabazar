from django.contrib import admin
from .models import Region, District, Farmland, SatelliteScene, VegetationIndex


@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'created_at')
    search_fields = ('name', 'code')


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    list_display = ('name', 'region', 'code', 'created_at')
    list_filter = ('region',)
    search_fields = ('name', 'code')


@admin.register(Farmland)
class FarmlandAdmin(admin.ModelAdmin):
    list_display = ('id', 'district', 'crop_type', 'area_ha', 'cadastral_number')
    list_filter = ('crop_type', 'district__region')
    search_fields = ('cadastral_number',)


@admin.register(SatelliteScene)
class SatelliteSceneAdmin(admin.ModelAdmin):
    list_display = ('scene_id', 'satellite', 'acquired_date', 'cloud_cover', 'processed')
    list_filter = ('satellite', 'processed')
    search_fields = ('scene_id',)


@admin.register(VegetationIndex)
class VegetationIndexAdmin(admin.ModelAdmin):
    list_display = ('farmland', 'index_type', 'acquired_date', 'mean', 'median')
    list_filter = ('index_type', 'acquired_date')
