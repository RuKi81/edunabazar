from django.contrib.gis.db import models

# Create your models here.

class Advert(models.Model):
    id = models.AutoField(primary_key=True)
    type = models.PositiveSmallIntegerField(db_comment='0- яЁхфыюцхэшх, 1-ёяЁюё')
    category = models.ForeignKey('Categories', models.DO_NOTHING, db_column='category', db_comment='╩рЄхюЁш ')
    author = models.ForeignKey('LegacyUser', models.DO_NOTHING, db_column='author', db_comment='└тЄюЁ')
    address = models.CharField(max_length=255, blank=True, null=True)
    location = models.PointField(srid=4326)
    delivery = models.BooleanField(default=False, db_comment='Доставка (0/1)')
    contacts = models.TextField(db_comment='╩юэЄръЄ√')
    title = models.CharField(max_length=255, db_comment='╟руюыютюъ')
    text = models.TextField(db_comment='╥хъёЄ юс· тыхэш ')
    price = models.FloatField(db_comment='╨ючэшўэр  Ўхэр')
    wholesale_price = models.FloatField(db_comment='╬яЄютр  Ўхэр')
    min_volume = models.FloatField(db_comment='╠шэьшры№э√щ юс·хь')
    wholesale_volume = models.FloatField(db_comment='╬с·хь фы  юяЄютющ Ўхэ√')
    volume = models.FloatField(db_comment='╬с·хь тёхую')
    priority = models.IntegerField(db_comment='╧ЁшюЁшЄхЄ юс· тыхэш ')
    created_at = models.DateTimeField(db_comment='┬Ёхь  ёючфрэш ')
    updated_at = models.DateTimeField(db_comment='┬Ёхь  юсэютыхэш ')
    price_unit = models.CharField(max_length=10, blank=True, default='кг')
    hidden_at = models.DateTimeField(blank=True, null=True)
    deleted_at = models.DateTimeField(blank=True, null=True)
    status = models.SmallIntegerField(db_comment='╤ЄрЄєё юс· тыхэш ')

    def __str__(self):
        return f'{self.id}: {self.title}'

    @property
    def thumb_url(self) -> str:
        prefetched = getattr(self, 'prefetched_photos', None)
        if prefetched is None:
            photo = AdvertPhoto.objects.filter(advert_id=self.id).order_by('sort', 'id').first()
        else:
            photo = prefetched[0] if prefetched else None
        try:
            return photo.image.url if photo and photo.image else ''
        except Exception:
            return ''

    class Meta:
        managed = False
        db_table = 'advert'


class AdvertPhoto(models.Model):
    id = models.AutoField(primary_key=True)
    advert = models.ForeignKey(Advert, on_delete=models.CASCADE, related_name='photos')
    image = models.FileField(upload_to='adverts/photos/')
    sort = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'Photo #{self.id} for advert {self.advert_id}'

    class Meta:
        db_table = 'advert_photo'
        indexes = [
            models.Index(fields=['advert', 'sort']),
        ]

class Catalog(models.Model):
    title = models.CharField(unique=True, max_length=255, db_comment='╚ь  ╩рЄрыюур')
    sort = models.IntegerField(default=0)
    active = models.SmallIntegerField()

    def __str__(self):
        return self.title or f'Catalog #{self.pk}'

    class Meta:
        managed = False
        db_table = 'catalog'

class Categories(models.Model):
    catalog = models.ForeignKey(Catalog, models.DO_NOTHING, db_column='catalog', db_comment='id ърЄрыюур')
    title = models.CharField(unique=True, max_length=255, db_comment='╚ь  ărЄхюЁшш')
    active = models.SmallIntegerField()

    def __str__(self):
        return self.title or f'Category #{self.pk}'

    class Meta:
        managed = False
        db_table = 'categories'


class Review(models.Model):
    type = models.PositiveSmallIntegerField(db_comment='0-яю юс· тыхэш■, 1-яю яЁюфртЎє, 2...')
    object_id = models.PositiveIntegerField(db_column='object', db_comment='╬с·хъЄ юЄч√тр')
    points = models.IntegerField(db_comment='┴рыы')
    author = models.ForeignKey('LegacyUser', models.DO_NOTHING, db_column='author', db_comment='└тЄюЁ')
    text = models.TextField(db_comment='╥хъёЄ юЄч√тр')
    created_at = models.DateTimeField(db_comment='┬Ёхь  фюсртыхэш ')
    updated_at = models.DateTimeField(db_comment='┬Ёхь  юсэютыхэш ')
    status = models.SmallIntegerField(db_comment='╤ЄрЄєё юЄч√тр')

    REVIEW_TYPE_ADVERT = 0
    REVIEW_TYPE_SELLER = 1

    def __str__(self):
        return f'Review #{self.pk} (type={self.type}, object={self.object_id})'

    class Meta:
        managed = False
        db_table = 'review'


class Seller(models.Model):
    user = models.ForeignKey('LegacyUser', models.DO_NOTHING, db_column='user', db_comment='╧юы№чютрЄхы№')
    name = models.CharField(unique=True, max_length=255, db_comment='╚ь  яЁюфртЎр')
    logo = models.PositiveIntegerField(db_comment='╦юуюЄшя')
    location = models.TextField(db_comment='╠хёЄюэрїюцфхэшх')  # This field type is a guess.
    contacts = models.JSONField(db_comment='╩юэЄръЄ√')
    price_list = models.PositiveIntegerField(db_comment='╘рщы яЁрщё-ышёЄр')
    links = models.TextField(db_comment='╤ё√ыъш эр тэх°эшх ЁхёєЁё√ яЁюфртЎр')
    about = models.TextField(db_comment='╬яшёрэшх яЁюфртЎр')
    created_at = models.DateTimeField(db_comment='┬Ёхь  ёючфрэш ')
    updated_at = models.DateTimeField(db_comment='┬Ёхь  юсэютыхэш ')
    status = models.SmallIntegerField(db_comment='╤ЄрЄєё яЁюфртЎр')

    def __str__(self):
        return self.name or f'Seller #{self.pk}'

    class Meta:
        managed = False
        db_table = 'seller'

class Message(models.Model):
    sender = models.ForeignKey('LegacyUser', models.CASCADE, related_name='sent_messages')
    recipient = models.ForeignKey('LegacyUser', models.CASCADE, related_name='received_messages')
    advert = models.ForeignKey('Advert', models.SET_NULL, blank=True, null=True, related_name='messages')
    text = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField()

    def __str__(self):
        return f'Message #{self.pk} from {self.sender_id} to {self.recipient_id}'

    class Meta:
        db_table = 'message'
        indexes = [
            models.Index(fields=['recipient', '-created_at']),
            models.Index(fields=['sender', '-created_at']),
        ]


class News(models.Model):
    title = models.CharField(max_length=500)
    text = models.TextField(blank=True, default='')
    source_url = models.URLField(max_length=1000)
    source_name = models.CharField(max_length=200, blank=True, default='')
    source_title = models.CharField(max_length=500, blank=True, default='')
    published_at = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f'{self.published_at}: {self.title[:80]}'

    class Meta:
        db_table = 'news'
        ordering = ['-published_at', '-created_at']
        verbose_name = 'Новость'
        verbose_name_plural = 'Новости'


class LegacyUser(models.Model):
    type = models.PositiveSmallIntegerField(db_comment='0- ЇшчышЎю, 1-■ЁышЎю')
    username = models.CharField(unique=True, max_length=255, db_comment='╚ь  яюы№чютрЄхы ')
    auth_key = models.CharField(max_length=32)
    password_hash = models.CharField(max_length=255, db_comment='ярЁюы№')
    password_reset_token = models.CharField(unique=True, max_length=255, blank=True, null=True)
    email = models.CharField(unique=True, max_length=255, db_comment='яюўЄют√щ  ∙шъ')
    currency = models.CharField(max_length=5, db_comment='тры■Єр')
    name = models.CharField(max_length=255, db_comment='╘╚╬ шыш шь  ■ЁышЎр')
    address = models.CharField(max_length=255, db_comment='рфЁхё')
    phone = models.CharField(max_length=255, db_comment='ЄхыхЇюэ')
    inn = models.CharField(max_length=20, db_comment='╚══')
    status = models.SmallIntegerField()
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    location = models.PointField(srid=4326, blank=True, null=True)
    contacts = models.TextField(db_comment='╩юэЄръЄ√')

    def __str__(self):
        return self.username or f'User #{self.pk}'

    class Meta:
        managed = False
        db_table = 'legacy_user'

