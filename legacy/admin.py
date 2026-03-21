from django.contrib import admin
from django.utils.html import format_html

from .models import (
    Advert, AdvertPhoto, Catalog, Categories, LegacyUser, Message,
    News, NewsFeedSource, NewsKeyword, Review, Seller,
)

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


class AdvertPhotoInline(admin.TabularInline):
    model = AdvertPhoto
    extra = 0
    readonly_fields = ('image_preview',)
    fields = ('image', 'image_preview', 'sort')

    def image_preview(self, obj):
        if obj.image:
            return format_html('<img src="{}" style="max-height:80px;">', obj.image.url)
        return '-'
    image_preview.short_description = 'Превью'


@admin.register(Advert)
class AdvertAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'author', 'category', 'price', 'status', 'created_at')
    list_filter = ('status', 'type', 'category__catalog', 'delivery')
    search_fields = ('title', 'text', 'address')
    readonly_fields = ('created_at', 'updated_at')
    ordering = ('-created_at',)
    raw_id_fields = ('author', 'category')
    inlines = [AdvertPhotoInline]


@admin.register(AdvertPhoto)
class AdvertPhotoAdmin(admin.ModelAdmin):
    list_display = ('id', 'advert', 'sort', 'created_at')
    raw_id_fields = ('advert',)
    ordering = ('-created_at',)


@admin.register(LegacyUser)
class LegacyUserAdmin(admin.ModelAdmin):
    list_display = ('id', 'username', 'email', 'phone', 'name', 'status', 'created_at')
    list_filter = ('status', 'type')
    search_fields = ('username', 'email', 'phone', 'name')
    readonly_fields = ('created_at', 'updated_at', 'auth_key', 'password_hash')
    ordering = ('-created_at',)


@admin.register(Seller)
class SellerAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'user', 'status', 'created_at')
    list_filter = ('status',)
    search_fields = ('name',)
    readonly_fields = ('created_at', 'updated_at')
    raw_id_fields = ('user',)
    ordering = ('-created_at',)


@admin.register(Review)
class ReviewAdmin(admin.ModelAdmin):
    list_display = ('id', 'type', 'object_id', 'author', 'points', 'status', 'created_at')
    list_filter = ('status', 'type', 'points')
    search_fields = ('text',)
    readonly_fields = ('created_at', 'updated_at')
    raw_id_fields = ('author',)
    ordering = ('-created_at',)


@admin.register(Message)
class MessageAdmin(admin.ModelAdmin):
    list_display = ('id', 'sender', 'recipient', 'advert', 'is_read', 'created_at')
    list_filter = ('is_read',)
    search_fields = ('text',)
    readonly_fields = ('created_at',)
    raw_id_fields = ('sender', 'recipient', 'advert')
    ordering = ('-created_at',)


@admin.register(News)
class NewsAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'source_name', 'published_at', 'is_active', 'created_at')
    list_filter = ('is_active', 'source_name')
    search_fields = ('title', 'text', 'source_title')
    list_editable = ('is_active',)
    readonly_fields = ('created_at',)
    ordering = ('-published_at', '-created_at')


@admin.register(NewsKeyword)
class NewsKeywordAdmin(admin.ModelAdmin):
    list_display = ('keyword', 'keyword_type', 'is_active')
    list_filter = ('keyword_type', 'is_active')
    list_editable = ('keyword_type', 'is_active')
    search_fields = ('keyword',)
    ordering = ('keyword_type', 'keyword')


@admin.register(NewsFeedSource)
class NewsFeedSourceAdmin(admin.ModelAdmin):
    list_display = ('name', 'url', 'is_active')
    list_editable = ('is_active',)
    search_fields = ('name', 'url')
