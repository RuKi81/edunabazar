"""Django-admin регистрация ``my_fields``.

Минимальный набор для оперативной отладки — без OSMGeoAdmin (тяжеловат,
тянет JS-зависимости). Геометрию редактируем через JSON, либо позже
прикрутим GeoDjango-виджет, когда понадобится менять полигоны из админки.
"""
from __future__ import annotations

from django.contrib import admin

from .models import (
    FieldEvent, FieldPhoto, FieldSeason, Plan, UserField, UserPlan,
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
