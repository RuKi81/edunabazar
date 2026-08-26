"""Django-admin регистрация ``my_fields``.

Минимальный набор для оперативной отладки — без OSMGeoAdmin (тяжеловат,
тянет JS-зависимости). Геометрию редактируем через JSON, либо позже
прикрутим GeoDjango-виджет, когда понадобится менять полигоны из админки.
"""
from __future__ import annotations

from django.contrib import admin

from .models import (
    FieldEvent, FieldPhoto, FieldSeason, GisFolder, GisLayer, Plan, RasterLayer,
    UserField, UserPlan,
)


@admin.register(Plan)
class PlanAdmin(admin.ModelAdmin):
    list_display = (
        'code', 'name', 'monthly_price_rub', 'max_fields',
        'max_total_area_ha', 'is_active', 'sort_order',
    )
    list_editable = ('is_active', 'sort_order')
    search_fields = ('code', 'name')


@admin.register(UserPlan)
class UserPlanAdmin(admin.ModelAdmin):
    list_display = ('user', 'plan', 'activated_at', 'expires_at')
    autocomplete_fields = ('user', 'plan')
    raw_id_fields = ()
    list_select_related = ('user', 'plan')


@admin.register(UserField)
class UserFieldAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'name', 'owner', 'area_ha', 'crop_type',
        'region', 'district', 'is_archived', 'updated_at',
    )
    list_filter = ('crop_type', 'is_archived', 'region')
    search_fields = ('name', 'cadastral_number', 'owner__username', 'owner__email')
    autocomplete_fields = ('owner', 'region', 'district')
    readonly_fields = ('area_ha', 'created_at', 'updated_at')
    list_select_related = ('owner', 'region', 'district')


@admin.register(FieldSeason)
class FieldSeasonAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'field', 'year', 'crop', 'variety',
        'sowing_date', 'actual_harvest_date', 'actual_yield_t_per_ha',
    )
    list_filter = ('crop', 'year')
    search_fields = ('field__name', 'variety')
    autocomplete_fields = ('field',)
    list_select_related = ('field',)


@admin.register(FieldEvent)
class FieldEventAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'event_date', 'event_type', 'field', 'season',
        'product_name', 'quantity', 'quantity_unit', 'cost_rub',
    )
    list_filter = ('event_type', 'event_date')
    search_fields = ('title', 'description', 'product_name', 'field__name')
    autocomplete_fields = ('field', 'season', 'created_by')
    date_hierarchy = 'event_date'
    list_select_related = ('field', 'season')


@admin.register(FieldPhoto)
class FieldPhotoAdmin(admin.ModelAdmin):
    list_display = ('id', 'field', 'event', 'taken_at', 'uploaded_at')
    autocomplete_fields = ('field', 'event', 'uploaded_by')
    list_select_related = ('field', 'event')


@admin.register(GisFolder)
class GisFolderAdmin(admin.ModelAdmin):
    """Организационные папки-группы для ГИС-слоёв (UI-группировка)."""
    list_display = ('id', 'name', 'sort_order', 'collapsed', 'visible', 'owner', 'created_at')
    list_filter = ('collapsed', 'visible', 'created_at')
    search_fields = ('name',)
    list_select_related = ('owner',)


@admin.register(GisLayer)
class GisLayerAdmin(admin.ModelAdmin):
    """Реестр загруженных SHP-слоёв.

    Удаление здесь дропает и физическую таблицу PostGIS (через
    ``services.shp_import.drop_layer``), а не только строку реестра.
    """
    list_display = (
        'id', 'title', 'geom_kind', 'feature_count', 'table_name',
        'srid_original', 'owner', 'created_at',
    )
    list_filter = ('geom_kind', 'created_at')
    search_fields = ('title', 'table_name', 'original_filename', 'source_archive')
    readonly_fields = (
        'table_name', 'original_filename', 'source_archive', 'geom_kind',
        'geom_type', 'srid_original', 'feature_count', 'attributes',
        'extent', 'created_at',
    )
    list_select_related = ('owner',)

    def has_add_permission(self, request):
        # Слои появляются только через импорт SHP, не вручную.
        return False

    def delete_model(self, request, obj):
        from .services.shp_import import drop_layer
        drop_layer(obj)

    def delete_queryset(self, request, queryset):
        from .services.shp_import import drop_layer
        for obj in queryset:
            drop_layer(obj)


@admin.register(RasterLayer)
class RasterLayerAdmin(admin.ModelAdmin):
    """Реестр растровых слоёв (GeoTIFF/COG в MinIO/S3).

    Файлы живут в объектном хранилище; здесь только мета-реестр. Удаление
    S3-объектов (оригинал + COG) при удалении строки будет добавлено в
    Фазе 5 — пока строка удаляется без чистки хранилища.
    """
    list_display = (
        'id', 'title', 'status', 'band_count', 'srid', 'size_bytes',
        'owner', 'created_at',
    )
    list_filter = ('status', 'created_at')
    search_fields = ('title', 'original_filename', 'upload_key', 'cog_key')
    readonly_fields = (
        'status', 'original_filename', 'upload_key', 'cog_key', 'size_bytes',
        'srid', 'bounds', 'band_count', 'nodata', 'stats', 'error',
        'created_at', 'updated_at',
    )
    list_select_related = ('owner',)

    def has_add_permission(self, request):
        # Слои появляются только через загрузку в ГИС-окне, не вручную.
        return False
