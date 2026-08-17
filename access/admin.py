from django.contrib import admin

from .models import ResourceGrant


@admin.register(ResourceGrant)
class ResourceGrantAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'legacy_user', 'resource_type', 'resource_id', 'level',
        'granted_by', 'created_at',
    )
    list_filter = ('resource_type', 'level')
    search_fields = (
        'legacy_user__username', 'legacy_user__email', 'note',
        'resource_id',
    )
    raw_id_fields = ('legacy_user', 'granted_by')
    readonly_fields = ('created_at',)
    ordering = ('-created_at',)
