from django.contrib.gis.db import models


class Region(models.Model):
    """Субъект РФ (например, Республика Крым)."""
    name = models.CharField(max_length=255, verbose_name='Название')
    code = models.CharField(max_length=100, unique=True, verbose_name='Код субъекта')
    geom = models.MultiPolygonField(srid=4326, verbose_name='Границы')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'agro_region'
        ordering = ['name']
        verbose_name = 'Регион'
        verbose_name_plural = 'Регионы'

    def __str__(self):
        return self.name


class District(models.Model):
    """Муниципальный район."""
    region = models.ForeignKey(Region, on_delete=models.CASCADE, related_name='districts')
    name = models.CharField(max_length=255, verbose_name='Название')
    code = models.CharField(max_length=150, blank=True, default='', verbose_name='Код района')
    geom = models.MultiPolygonField(srid=4326, verbose_name='Границы')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'agro_district'
        ordering = ['name']
        verbose_name = 'Район'
        verbose_name_plural = 'Районы'

    def __str__(self):
        return f'{self.name} ({self.region.name})'


class Farmland(models.Model):
    """Полигон сельхоз угодья."""

    class CropType(models.TextChoices):
        ARABLE = 'arable', 'Пашня'
        HAYFIELD = 'hayfield', 'Сенокос'
        PASTURE = 'pasture', 'Пастбище'
        PERENNIAL = 'perennial', 'Многолетнее насаждение'
        OTHER = 'other', 'Прочее'

    district = models.ForeignKey(District, on_delete=models.CASCADE, related_name='farmlands')
    crop_type = models.CharField(
        max_length=20,
        choices=CropType.choices,
        default=CropType.ARABLE,
        verbose_name='Вид угодья',
    )
    cadastral_number = models.CharField(max_length=50, blank=True, default='', verbose_name='Кадастровый номер')
    area_ha = models.FloatField(default=0, verbose_name='Площадь, га')
    geom = models.MultiPolygonField(srid=4326, verbose_name='Границы')
    properties = models.JSONField(blank=True, null=True, verbose_name='Доп. атрибуты')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'agro_farmland'
        ordering = ['district', 'crop_type']
        verbose_name = 'Угодье'
        verbose_name_plural = 'Угодья'
        indexes = [
            models.Index(fields=['district', 'crop_type']),
        ]

    def __str__(self):
        return f'{self.get_crop_type_display()} #{self.pk} ({self.area_ha:.1f} га)'


class SatelliteScene(models.Model):
    """Метаданные спутникового снимка."""

    class Satellite(models.TextChoices):
        SENTINEL2 = 'sentinel2', 'Sentinel-2'
        LANDSAT8 = 'landsat8', 'Landsat 8'
        LANDSAT9 = 'landsat9', 'Landsat 9'
        MODIS_TERRA = 'modis_terra', 'MODIS Terra'
        MODIS_AQUA = 'modis_aqua', 'MODIS Aqua'

    satellite = models.CharField(max_length=20, choices=Satellite.choices, verbose_name='Спутник')
    scene_id = models.CharField(max_length=255, unique=True, verbose_name='ID снимка')
    acquired_date = models.DateField(verbose_name='Дата съёмки')
    cloud_cover = models.FloatField(default=0, verbose_name='Облачность, %')
    bbox = models.PolygonField(srid=4326, blank=True, null=True, verbose_name='Охват')
    file_path = models.CharField(max_length=500, blank=True, default='', verbose_name='Путь к файлу')
    metadata = models.JSONField(blank=True, null=True, verbose_name='Метаданные')
    processed = models.BooleanField(default=False, verbose_name='Обработан')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'agro_satellite_scene'
        ordering = ['-acquired_date']
        verbose_name = 'Снимок'
        verbose_name_plural = 'Снимки'

    def __str__(self):
        return f'{self.get_satellite_display()} {self.acquired_date} ({self.scene_id})'


class VegetationIndex(models.Model):
    """Зональная статистика вегетационного индекса по угодью."""

    class IndexType(models.TextChoices):
        NDVI = 'ndvi', 'NDVI'
        EVI = 'evi', 'EVI'
        MSAVI = 'msavi', 'MSAVI'
        NDWI = 'ndwi', 'NDWI'
        NDMI = 'ndmi', 'NDMI'

    farmland = models.ForeignKey(Farmland, on_delete=models.CASCADE, related_name='indices')
    scene = models.ForeignKey(SatelliteScene, on_delete=models.CASCADE, related_name='indices')
    index_type = models.CharField(max_length=10, choices=IndexType.choices, verbose_name='Тип индекса')
    acquired_date = models.DateField(verbose_name='Дата съёмки')

    mean = models.FloatField(verbose_name='Среднее')
    median = models.FloatField(default=0, verbose_name='Медиана')
    min_val = models.FloatField(default=0, verbose_name='Минимум')
    max_val = models.FloatField(default=0, verbose_name='Максимум')
    std_val = models.FloatField(default=0, verbose_name='Ст. отклонение')
    pixel_count = models.IntegerField(default=0, verbose_name='Кол-во пикселей')
    valid_pixel_count = models.IntegerField(default=0, verbose_name='Валидных пикселей')

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'agro_vegetation_index'
        ordering = ['-acquired_date']
        verbose_name = 'Вег. индекс'
        verbose_name_plural = 'Вег. индексы'
        unique_together = [('farmland', 'scene', 'index_type')]
        indexes = [
            models.Index(fields=['farmland', 'index_type', 'acquired_date']),
            models.Index(fields=['acquired_date', 'index_type']),
        ]

    def __str__(self):
        return f'{self.index_type.upper()} {self.acquired_date} → {self.mean:.3f}'
