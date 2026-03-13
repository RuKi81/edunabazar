from rest_framework import serializers

from .models import Advert, AdvertPhoto, Categories, Catalog, Seller, Review, Message, LegacyUser


class CatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = Catalog
        fields = ['id', 'title', 'sort', 'active']
        read_only_fields = fields


class CategorySerializer(serializers.ModelSerializer):
    catalog_title = serializers.CharField(source='catalog.title', read_only=True)

    class Meta:
        model = Categories
        fields = ['id', 'catalog', 'catalog_title', 'title', 'active']
        read_only_fields = fields


class AdvertPhotoSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()

    class Meta:
        model = AdvertPhoto
        fields = ['id', 'url', 'sort']
        read_only_fields = fields

    def get_url(self, obj) -> str:
        if obj.image:
            request = self.context.get('request')
            url = obj.image.url
            return request.build_absolute_uri(url) if request else url
        return ''


class AdvertListSerializer(serializers.ModelSerializer):
    category_title = serializers.CharField(source='category.title', read_only=True, default='')
    author_name = serializers.SerializerMethodField()
    thumb_url = serializers.SerializerMethodField()
    lat = serializers.SerializerMethodField()
    lon = serializers.SerializerMethodField()

    class Meta:
        model = Advert
        fields = [
            'id', 'type', 'title', 'category', 'category_title',
            'author', 'author_name', 'price', 'price_unit',
            'wholesale_price', 'delivery', 'volume', 'min_volume',
            'lat', 'lon', 'address', 'thumb_url',
            'status', 'created_at', 'updated_at',
        ]
        read_only_fields = fields

    def get_author_name(self, obj) -> str:
        author = getattr(obj, 'author', None)
        if author:
            return getattr(author, 'name', '') or getattr(author, 'username', '') or ''
        return ''

    def get_thumb_url(self, obj) -> str:
        url = obj.thumb_url
        if url:
            request = self.context.get('request')
            return request.build_absolute_uri(url) if request else url
        return ''

    def get_lat(self, obj) -> float | None:
        loc = getattr(obj, 'location', None)
        return float(loc.y) if loc else None

    def get_lon(self, obj) -> float | None:
        loc = getattr(obj, 'location', None)
        return float(loc.x) if loc else None


class AdvertDetailSerializer(AdvertListSerializer):
    photos = serializers.SerializerMethodField()
    text = serializers.CharField()
    contacts = serializers.CharField()

    class Meta(AdvertListSerializer.Meta):
        fields = AdvertListSerializer.Meta.fields + [
            'text', 'contacts', 'wholesale_volume', 'photos',
        ]
        read_only_fields = fields

    def get_photos(self, obj) -> list:
        photos = AdvertPhoto.objects.filter(advert_id=obj.id).order_by('sort', 'id')
        return AdvertPhotoSerializer(photos, many=True, context=self.context).data


class AdvertWriteSerializer(serializers.Serializer):
    type = serializers.IntegerField(default=0)
    category = serializers.IntegerField()
    title = serializers.CharField(max_length=255)
    text = serializers.CharField()
    contacts = serializers.CharField()
    address = serializers.CharField(max_length=255)
    price = serializers.FloatField(default=0)
    price_unit = serializers.CharField(max_length=10, default='кг')
    wholesale_price = serializers.FloatField(default=0)
    min_volume = serializers.FloatField(default=0)
    wholesale_volume = serializers.FloatField(default=0)
    volume = serializers.FloatField(default=0)
    delivery = serializers.BooleanField(default=False)
    lat = serializers.FloatField()
    lon = serializers.FloatField()

    def validate_type(self, value):
        if value not in (0, 1):
            raise serializers.ValidationError('type must be 0 (offer) or 1 (demand)')
        return value

    def validate_category(self, value):
        if not Categories.objects.filter(pk=value, active=1).exists():
            raise serializers.ValidationError('Invalid category')
        return value

    def validate_lat(self, value):
        if not (-90 <= value <= 90):
            raise serializers.ValidationError('lat must be between -90 and 90')
        return value

    def validate_lon(self, value):
        if not (-180 <= value <= 180):
            raise serializers.ValidationError('lon must be between -180 and 180')
        return value


class SellerSerializer(serializers.ModelSerializer):
    user_name = serializers.SerializerMethodField()

    class Meta:
        model = Seller
        fields = [
            'id', 'user', 'user_name', 'name', 'location',
            'contacts', 'links', 'about',
            'created_at', 'updated_at', 'status',
        ]
        read_only_fields = fields

    def get_user_name(self, obj) -> str:
        user = getattr(obj, 'user', None)
        if user:
            return getattr(user, 'name', '') or getattr(user, 'username', '') or ''
        return ''


class ReviewSerializer(serializers.ModelSerializer):
    author_name = serializers.SerializerMethodField()

    class Meta:
        model = Review
        fields = [
            'id', 'type', 'object_id', 'points',
            'author', 'author_name', 'text',
            'created_at', 'status',
        ]
        read_only_fields = ['id', 'author', 'author_name', 'created_at', 'status']

    def get_author_name(self, obj) -> str:
        author = getattr(obj, 'author', None)
        if author:
            return getattr(author, 'name', '') or getattr(author, 'username', '') or ''
        return ''


class ReviewCreateSerializer(serializers.Serializer):
    review_type = serializers.IntegerField()
    object_id = serializers.IntegerField()
    points = serializers.IntegerField(min_value=1, max_value=5)
    text = serializers.CharField(max_length=2000)

    def validate_review_type(self, value):
        if value not in (0, 1):
            raise serializers.ValidationError('review_type must be 0 (advert) or 1 (seller)')
        return value


class MessageSerializer(serializers.ModelSerializer):
    sender_name = serializers.SerializerMethodField()

    class Meta:
        model = Message
        fields = [
            'id', 'sender', 'sender_name', 'recipient', 'advert',
            'text', 'is_read', 'created_at',
        ]
        read_only_fields = ['id', 'sender', 'sender_name', 'is_read', 'created_at']

    def get_sender_name(self, obj) -> str:
        sender = getattr(obj, 'sender', None)
        if sender:
            return getattr(sender, 'name', '') or getattr(sender, 'username', '') or ''
        return ''
