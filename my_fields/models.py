"""Модели приложения ``my_fields`` — пользовательские угодья + журнал.

Связи с уже существующими таблицами:

* ``Region`` / ``District`` (``agrocosmos.models``) — для авто-резолва
  географической привязки поля по геометрии. На сами FK ссылаемся, но
  при удалении региона зануляем (``SET_NULL``) — поле пользователя не
  должно исчезать, если справочник перетряхнули.
* ``settings.AUTH_USER_MODEL`` (``auth.User``) — владелец поля. Внутри
  проекта ``auth.User`` создаётся прозрачно из ``legacy_user`` при первом
  логине через ``LegacyUserBackend``, так что для FMS-кейса (поле может
  завести только залогиненный пользователь) shadow-user уже гарантированно
  существует.

В одном файле — пока всего 6 моделей. Когда суммарный объём перевалит
за ~800 строк, разделим на подмодули ``models/field.py``, ``journal.py``,
``billing.py``.
"""
from __future__ import annotations

from django.conf import settings
from django.contrib.gis.db import models


# ─────────────────────────────────────────────────────────────────────
# Биллинг — заготовка под фазу тарификации.
# В MVP-1 создаём единственный план ``free`` через data-migration и
# никаких реальных списаний. Helpers в ``services/quotas.py`` уже
# смотрят сюда, чтобы переключение на платные тарифы не требовало
# рефакторинга views.
# ─────────────────────────────────────────────────────────────────────
class Plan(models.Model):
    code = models.CharField(max_length=20, unique=True, verbose_name='Код')
    name = models.CharField(max_length=80, verbose_name='Название')
    monthly_price_rub = models.IntegerField(default=0, verbose_name='Цена/мес, ₽')

    # Лимиты. ``None`` ⇒ безлимит. Бесплатный план держим консервативно.
    max_fields = models.IntegerField(null=True, blank=True, verbose_name='Макс. полей')
    max_total_area_ha = models.FloatField(null=True, blank=True, verbose_name='Макс. площадь, га')
    ndvi_history_years = models.IntegerField(default=1, verbose_name='Глубина NDVI-истории, лет')
    weather_forecast_enabled = models.BooleanField(default=False, verbose_name='Прогноз погоды')
    alerts_enabled = models.BooleanField(default=True, verbose_name='Алерты по полю')

    is_active = models.BooleanField(default=True)
    sort_order = models.IntegerField(default=0)

    class Meta:
        db_table = 'myf_plan'
        ordering = ['sort_order', 'monthly_price_rub']
        verbose_name = 'Тариф'
        verbose_name_plural = 'Тарифы'

    def __str__(self):
        return f'{self.name} ({self.code})'


class UserPlan(models.Model):
    """Текущий тариф пользователя. Отсутствие записи трактуется как ``free``."""
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='myf_plan',
    )
    plan = models.ForeignKey(Plan, on_delete=models.PROTECT)
    activated_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    # Платёжная мета — заполняется интеграцией с провайдером в V2.
    last_payment_provider = models.CharField(max_length=40, blank=True, default='')
    last_payment_id = models.CharField(max_length=120, blank=True, default='')
    last_payment_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'myf_user_plan'
        verbose_name = 'Тариф пользователя'
        verbose_name_plural = 'Тарифы пользователей'


# ─────────────────────────────────────────────────────────────────────
# Поля и сезоны.
# ─────────────────────────────────────────────────────────────────────
class UserField(models.Model):
    """Угодье пользователя.

    Геометрия хранится строго как ``MultiPolygon`` (даже если в момент
    создания пользователь нарисовал один полигон — приводим к мульти на
    уровне сервиса). Это упрощает SQL-агрегацию NDVI и совместимо со
    схемой ``Farmland`` (``agrocosmos.models``).

    ``area_ha`` пересчитывается из геометрии (см. ``services/geometry.py``)
    и хранится копией, чтобы быстрые фильтры по площади не требовали
    ``ST_Area`` на каждом запросе.

    ``region`` / ``district`` резолвятся при сохранении по точке
    centroid'а — пользователь руками выбирать субъект не обязан.
    """

    class CropType(models.TextChoices):
        ARABLE = 'arable', 'Пашня'
        FALLOW = 'fallow', 'Залежь'
        HAYFIELD = 'hayfield', 'Сенокос'
        PASTURE = 'pasture', 'Пастбище'
        PERENNIAL = 'perennial', 'Многолетние насаждения'
        GARDEN = 'garden', 'Сад / огород'
        OTHER = 'other', 'Прочее'

    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='myf_fields',
        verbose_name='Владелец',
    )
    name = models.CharField(max_length=120, verbose_name='Название')
    geom = models.MultiPolygonField(srid=4326, verbose_name='Границы')
    area_ha = models.FloatField(default=0, verbose_name='Площадь, га')

    # Авто-резолв из геометрии. ``on_delete=SET_NULL``, чтобы перетряска
    # справочника не разрушала пользовательские данные.
    region = models.ForeignKey(
        'agrocosmos.Region', null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        verbose_name='Субъект',
    )
    district = models.ForeignKey(
        'agrocosmos.District', null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        verbose_name='Район',
    )
    crop_type = models.CharField(
        max_length=20, choices=CropType.choices, default=CropType.ARABLE,
        verbose_name='Тип угодья',
    )
    cadastral_number = models.CharField(
        max_length=50, blank=True, default='', db_index=True,
        verbose_name='Кадастровый номер',
    )
    notes = models.TextField(blank=True, default='', verbose_name='Заметки')

    is_archived = models.BooleanField(
        default=False, verbose_name='Архив',
        help_text='Архивные поля не показываются на основной карте и не учитываются в квотах.',
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'myf_field'
        ordering = ['-updated_at']
        verbose_name = 'Поле пользователя'
        verbose_name_plural = 'Поля пользователей'
        indexes = [
            models.Index(fields=['owner', 'is_archived']),
            models.Index(fields=['region', 'crop_type']),
        ]

    def __str__(self):
        return f'{self.name} ({self.area_ha:.1f} га)'


class FieldSeason(models.Model):
    """Сезон возделывания конкретной культуры на поле.

    Уникальность по ``(field, year, crop)`` — на одном поле в одном году
    может быть несколько культур (двойной севооборот), но не дубликат
    одной и той же.
    """

    class Crop(models.TextChoices):
        # Совпадает с ``agrocosmos.models.YieldCrop``, но дублируем здесь
        # сознательно: список FMS-культур шире (овощи, ягоды и т.п.).
        WHEAT = 'wheat', 'Пшеница'
        BARLEY = 'barley', 'Ячмень'
        RYE = 'rye', 'Рожь'
        OATS = 'oats', 'Овёс'
        CORN = 'corn', 'Кукуруза'
        SUNFLOWER = 'sunflower', 'Подсолнечник'
        SOYBEAN = 'soybean', 'Соя'
        RAPESEED = 'rapeseed', 'Рапс'
        SUGAR_BEET = 'sugar_beet', 'Сахарная свёкла'
        POTATO = 'potato', 'Картофель'
        VEGETABLES = 'vegetables', 'Овощи'
        FRUITS = 'fruits', 'Плодовые'
        BERRIES = 'berries', 'Ягоды'
        GRASS = 'grass', 'Травы (кормовые)'
        FALLOW = 'fallow', 'Пар'
        OTHER = 'other', 'Прочее'

    field = models.ForeignKey(
        UserField, on_delete=models.CASCADE, related_name='seasons',
        verbose_name='Поле',
    )
    year = models.IntegerField(verbose_name='Год')
    crop = models.CharField(max_length=20, choices=Crop.choices, verbose_name='Культура')
    variety = models.CharField(max_length=120, blank=True, default='', verbose_name='Сорт / гибрид')

    sowing_date = models.DateField(null=True, blank=True, verbose_name='Дата сева')
    planned_harvest_date = models.DateField(null=True, blank=True, verbose_name='План. уборка')
    actual_harvest_date = models.DateField(null=True, blank=True, verbose_name='Факт. уборка')

    planned_yield_t_per_ha = models.FloatField(null=True, blank=True, verbose_name='План, т/га')
    actual_yield_t_per_ha = models.FloatField(null=True, blank=True, verbose_name='Факт, т/га')
    gross_t = models.FloatField(null=True, blank=True, verbose_name='Валовой сбор, т')

    notes = models.TextField(blank=True, default='')

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'myf_field_season'
        ordering = ['-year', '-created_at']
        verbose_name = 'Сезон'
        verbose_name_plural = 'Сезоны'
        constraints = [
            models.UniqueConstraint(
                fields=['field', 'year', 'crop'],
                name='myf_season_unique_field_year_crop',
            ),
        ]

    def __str__(self):
        return f'{self.field.name} / {self.year} / {self.get_crop_display()}'


# ─────────────────────────────────────────────────────────────────────
# Журнал событий и фото.
# ─────────────────────────────────────────────────────────────────────
class FieldEvent(models.Model):
    """Запись агрожурнала.

    Может быть привязана к ``FieldSeason`` (большинство случаев — сев,
    удобрение, СЗР), но не обязана: межсезонные осмотры и заметки
    хранятся без ``season``.

    ``weather_snapshot`` — JSON-снимок погоды на момент события
    (например, заполняется при создании из API Open-Meteo). Помогает
    задним числом понять, почему обработка дала эффект или нет.
    """

    class Type(models.TextChoices):
        SOWING = 'sowing', 'Сев'
        FERTILIZE = 'fertilize', 'Внесение удобрений'
        PROTECT = 'protect', 'Обработка СЗР'
        TILLAGE = 'tillage', 'Обработка почвы'
        IRRIGATE = 'irrigate', 'Полив'
        SCOUT = 'scout', 'Осмотр / разведка'
        ISSUE = 'issue', 'Проблема'
        HARVEST = 'harvest', 'Уборка'
        SOIL_TEST = 'soil_test', 'Анализ почвы'
        OTHER = 'other', 'Прочее'

    field = models.ForeignKey(
        UserField, on_delete=models.CASCADE, related_name='events',
        verbose_name='Поле',
    )
    season = models.ForeignKey(
        FieldSeason, null=True, blank=True,
        on_delete=models.SET_NULL, related_name='events',
        verbose_name='Сезон',
    )
    event_type = models.CharField(max_length=20, choices=Type.choices, verbose_name='Тип события')
    event_date = models.DateField(verbose_name='Дата')

    title = models.CharField(max_length=180, blank=True, default='', verbose_name='Заголовок')
    description = models.TextField(blank=True, default='', verbose_name='Описание')

    # Количественные параметры — необязательны, но полезны для дальнейших
    # отчётов и аналитики по затратам.
    quantity = models.FloatField(null=True, blank=True, verbose_name='Количество')
    quantity_unit = models.CharField(
        max_length=20, blank=True, default='',
        verbose_name='Ед. изм.', help_text='кг/га, л/га, т/га, шт., м³',
    )
    product_name = models.CharField(
        max_length=180, blank=True, default='',
        verbose_name='Препарат / удобрение',
    )
    cost_rub = models.FloatField(null=True, blank=True, verbose_name='Затраты, ₽')

    weather_snapshot = models.JSONField(
        null=True, blank=True, verbose_name='Снимок погоды',
        help_text='Заполняется автоматически при создании из Open-Meteo.',
    )

    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name='+',
    )

    class Meta:
        db_table = 'myf_field_event'
        ordering = ['-event_date', '-created_at']
        verbose_name = 'Событие журнала'
        verbose_name_plural = 'Журнал событий'
        indexes = [
            models.Index(fields=['field', '-event_date']),
            models.Index(fields=['season']),
            models.Index(fields=['event_type', '-event_date']),
        ]

    def __str__(self):
        return f'{self.get_event_type_display()} @ {self.field.name} ({self.event_date})'


class FieldPhoto(models.Model):
    """Фотография поля или события. GPS извлекается из EXIF при загрузке."""

    field = models.ForeignKey(
        UserField, on_delete=models.CASCADE, related_name='photos',
        verbose_name='Поле',
    )
    event = models.ForeignKey(
        FieldEvent, null=True, blank=True,
        on_delete=models.CASCADE, related_name='photos',
        verbose_name='Событие',
    )
    image = models.ImageField(
        upload_to='my_fields/photos/%Y/%m/',
        verbose_name='Файл',
    )
    taken_at = models.DateTimeField(null=True, blank=True, verbose_name='Снято')
    geo_lat = models.FloatField(null=True, blank=True)
    geo_lon = models.FloatField(null=True, blank=True)
    caption = models.CharField(max_length=300, blank=True, default='', verbose_name='Подпись')

    uploaded_at = models.DateTimeField(auto_now_add=True)
    uploaded_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name='+',
    )

    class Meta:
        db_table = 'myf_field_photo'
        ordering = ['-taken_at', '-uploaded_at']
        verbose_name = 'Фото поля'
        verbose_name_plural = 'Фото полей'
        indexes = [
            models.Index(fields=['field', '-taken_at']),
            models.Index(fields=['event']),
        ]

    def __str__(self):
        return f'Фото #{self.pk} @ {self.field.name}'
