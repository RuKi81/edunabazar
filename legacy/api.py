from django.db.models import Q, Prefetch
from django.utils import timezone
from django.contrib.gis.geos import Point

from rest_framework import viewsets, mixins, status, permissions
from rest_framework.decorators import action
from rest_framework.response import Response

from .models import Advert, AdvertPhoto, Categories, Catalog, Seller, Review, Message, LegacyUser
from .serializers import (
    AdvertListSerializer, AdvertDetailSerializer, AdvertWriteSerializer,
    CategorySerializer, CatalogSerializer, SellerSerializer,
    ReviewSerializer, ReviewCreateSerializer,
    MessageSerializer,
)
from .views import _get_current_legacy_user, _is_admin_user


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------

class IsLegacyAuthenticated(permissions.BasePermission):
    """Allow access only to users with a legacy session."""
    def has_permission(self, request, view):
        return _get_current_legacy_user(request) is not None


class IsLegacyAuthenticatedOrReadOnly(permissions.BasePermission):
    """Read-only for anonymous, full access for legacy-authenticated."""
    def has_permission(self, request, view):
        if request.method in permissions.SAFE_METHODS:
            return True
        return _get_current_legacy_user(request) is not None


# ---------------------------------------------------------------------------
# Adverts
# ---------------------------------------------------------------------------

class AdvertViewSet(viewsets.GenericViewSet,
                    mixins.ListModelMixin,
                    mixins.RetrieveModelMixin,
                    mixins.CreateModelMixin):
    permission_classes = [IsLegacyAuthenticatedOrReadOnly]

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return AdvertDetailSerializer
        if self.action == 'create':
            return AdvertWriteSerializer
        return AdvertListSerializer

    def get_queryset(self):
        user = _get_current_legacy_user(self.request)
        is_admin = _is_admin_user(user)

        _thumb_prefetch = Prefetch(
            'photos',
            queryset=AdvertPhoto.objects.order_by('sort', 'id'),
            to_attr='prefetched_photos',
        )
        qs = Advert.objects.select_related('category', 'author').prefetch_related(_thumb_prefetch)

        if is_admin:
            qs = qs.exclude(status=0)
        else:
            qs = qs.filter(status=10)

        # Filters
        q = (self.request.query_params.get('q') or '').strip()
        if q:
            qs = qs.filter(Q(title__icontains=q) | Q(text__icontains=q))

        type_raw = (self.request.query_params.get('type') or '').strip().lower()
        if type_raw == 'offer':
            qs = qs.filter(type=0)
        elif type_raw == 'demand':
            qs = qs.filter(type=1)

        catalog_raw = (self.request.query_params.get('catalog') or '').strip()
        category_raw = (self.request.query_params.get('category') or '').strip()
        try:
            if category_raw:
                qs = qs.filter(category_id=int(category_raw))
        except (TypeError, ValueError):
            pass
        try:
            if catalog_raw:
                qs = qs.filter(category__catalog_id=int(catalog_raw))
        except (TypeError, ValueError):
            pass

        return qs.order_by('-created_at', '-id')

    def create(self, request, *args, **kwargs):
        user = _get_current_legacy_user(request)
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        d = serializer.validated_data
        now = timezone.now()

        advert = Advert.objects.create(
            type=d['type'],
            category_id=d['category'],
            author_id=int(user.id),
            address=d['address'],
            location=Point(float(d['lon']), float(d['lat']), srid=4326),
            delivery=d['delivery'],
            contacts=d['contacts'],
            title=d['title'],
            text=d['text'],
            price=d['price'],
            price_unit=d['price_unit'],
            wholesale_price=d['wholesale_price'],
            min_volume=d['min_volume'],
            wholesale_volume=d['wholesale_volume'],
            volume=d['volume'],
            priority=0,
            created_at=now,
            updated_at=now,
            status=5,
        )
        out = AdvertDetailSerializer(advert, context={'request': request})
        return Response(out.data, status=status.HTTP_201_CREATED)


# ---------------------------------------------------------------------------
# Catalogs & Categories (read-only)
# ---------------------------------------------------------------------------

class CatalogViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    serializer_class = CatalogSerializer
    queryset = Catalog.objects.filter(active=1).order_by('sort', 'title', 'id')
    permission_classes = [permissions.AllowAny]
    pagination_class = None


class CategoryViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    serializer_class = CategorySerializer
    queryset = Categories.objects.filter(active=1).select_related('catalog').order_by('title')
    permission_classes = [permissions.AllowAny]
    pagination_class = None


# ---------------------------------------------------------------------------
# Sellers (read-only)
# ---------------------------------------------------------------------------

class SellerViewSet(viewsets.GenericViewSet,
                    mixins.ListModelMixin,
                    mixins.RetrieveModelMixin):
    serializer_class = SellerSerializer
    permission_classes = [permissions.AllowAny]

    def get_queryset(self):
        qs = Seller.objects.select_related('user').order_by('-created_at')
        q = (self.request.query_params.get('q') or '').strip()
        if q:
            qs = qs.filter(Q(name__icontains=q) | Q(about__icontains=q))
        return qs


# ---------------------------------------------------------------------------
# Reviews
# ---------------------------------------------------------------------------

class ReviewViewSet(viewsets.GenericViewSet,
                    mixins.ListModelMixin,
                    mixins.CreateModelMixin):
    serializer_class = ReviewSerializer
    permission_classes = [IsLegacyAuthenticatedOrReadOnly]

    def get_queryset(self):
        qs = Review.objects.select_related('author').filter(status=10)
        review_type = self.request.query_params.get('review_type')
        object_id = self.request.query_params.get('object_id')
        try:
            if review_type is not None:
                qs = qs.filter(type=int(review_type))
        except (TypeError, ValueError):
            pass
        try:
            if object_id is not None:
                qs = qs.filter(object_id=int(object_id))
        except (TypeError, ValueError):
            pass
        return qs.order_by('-created_at', '-id')

    def get_serializer_class(self):
        if self.action == 'create':
            return ReviewCreateSerializer
        return ReviewSerializer

    def create(self, request, *args, **kwargs):
        user = _get_current_legacy_user(request)
        serializer = ReviewCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        d = serializer.validated_data

        existing = Review.objects.filter(
            type=d['review_type'], object_id=d['object_id'], author_id=user.id,
        ).exclude(status=0).exists()
        if existing:
            return Response({'detail': 'Вы уже оставили отзыв'}, status=status.HTTP_400_BAD_REQUEST)

        now = timezone.now()
        review = Review.objects.create(
            type=d['review_type'],
            object_id=d['object_id'],
            points=d['points'],
            author_id=int(user.id),
            text=d['text'],
            created_at=now,
            updated_at=now,
            status=5,
        )
        out = ReviewSerializer(review, context={'request': request})
        return Response(out.data, status=status.HTTP_201_CREATED)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

class MessageViewSet(viewsets.GenericViewSet,
                     mixins.ListModelMixin,
                     mixins.CreateModelMixin):
    serializer_class = MessageSerializer
    permission_classes = [IsLegacyAuthenticated]

    def get_queryset(self):
        user = _get_current_legacy_user(self.request)
        if not user:
            return Message.objects.none()
        return Message.objects.filter(
            Q(sender_id=user.id) | Q(recipient_id=user.id)
        ).select_related('sender', 'recipient', 'advert').order_by('-created_at')

    def create(self, request, *args, **kwargs):
        user = _get_current_legacy_user(request)
        recipient_id = request.data.get('recipient')
        text = (request.data.get('text') or '').strip()
        advert_id = request.data.get('advert') or None

        if not recipient_id or not text:
            return Response({'detail': 'recipient and text are required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            recipient_id = int(recipient_id)
        except (TypeError, ValueError):
            return Response({'detail': 'Invalid recipient'}, status=status.HTTP_400_BAD_REQUEST)

        if recipient_id == user.id:
            return Response({'detail': 'Cannot message yourself'}, status=status.HTTP_400_BAD_REQUEST)

        if not LegacyUser.objects.filter(pk=recipient_id).exists():
            return Response({'detail': 'Recipient not found'}, status=status.HTTP_404_NOT_FOUND)

        if len(text) > 5000:
            text = text[:5000]

        try:
            advert_id = int(advert_id) if advert_id else None
        except (TypeError, ValueError):
            advert_id = None

        msg = Message.objects.create(
            sender_id=int(user.id),
            recipient_id=recipient_id,
            advert_id=advert_id,
            text=text,
            is_read=False,
            created_at=timezone.now(),
        )
        out = MessageSerializer(msg, context={'request': request})
        return Response(out.data, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=['get'], url_path='unread-count')
    def unread_count(self, request):
        user = _get_current_legacy_user(request)
        if not user:
            return Response({'count': 0})
        count = Message.objects.filter(recipient_id=user.id, is_read=False).count()
        return Response({'count': count})
