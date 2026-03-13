from django.contrib import admin

from .models import Catalog, Categories

# Register your models here.


@admin.register(Catalog)
class CatalogAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'sort', 'active')
    list_editable = ('sort', 'active')
    ordering = ('sort', 'title', 'id')
    search_fields = ('title',)
    list_filter = ('active',)


@admin.register(Categories)
class CategoriesAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'catalog', 'active')
    list_filter = ('active', 'catalog')
    search_fields = ('title',)
    ordering = ('title', 'id')
