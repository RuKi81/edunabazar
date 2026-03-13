from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .api import (
    AdvertViewSet, CatalogViewSet, CategoryViewSet,
    SellerViewSet, ReviewViewSet, MessageViewSet,
)

router = DefaultRouter()
router.register('adverts', AdvertViewSet, basename='api-adverts')
router.register('catalogs', CatalogViewSet, basename='api-catalogs')
router.register('categories', CategoryViewSet, basename='api-categories')
router.register('sellers', SellerViewSet, basename='api-sellers')
router.register('reviews', ReviewViewSet, basename='api-reviews')
router.register('messages', MessageViewSet, basename='api-messages')

urlpatterns = router.urls
