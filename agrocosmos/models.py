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
        HLS_FUSED = 'hls_fused', 'HLS Fused (S2+L)'

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
        indexes = [
            # Speeds up JOIN ... WHERE satellite IN (...) used in every
            # dashboard/report query that distinguishes MODIS vs raster sources.
            models.Index(fields=['satellite', 'acquired_date'], name='scene_sat_date_idx'),
        ]

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

    # ``is_outlier`` — spike in the time series (snow, cloud haze, SCL mask
    # bleed-through). Excluded before Savitzky-Golay smoothing. This is NOT a
    # biological anomaly — real vegetation alerts live in a separate model.
    is_outlier = models.BooleanField(
        default=False,
        verbose_name='Выброс (снег/облако, исключён из сглаживания)',
    )
    mean_smooth = models.FloatField(null=True, blank=True, verbose_name='NDVI сглаженное')

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
            # Partial index covering the hot dashboard/report path:
            # WHERE index_type='ndvi' AND is_outlier=false, grouped by date/farmland.
            # ~95% of rows satisfy is_outlier=false, so the partial index
            # is roughly the same size as a full one but skips the filter step.
            models.Index(
                fields=['farmland', 'acquired_date'],
                condition=models.Q(index_type='ndvi', is_outlier=False),
                name='vi_ndvi_active_idx',
            ),
        ]

    def __str__(self):
        return f'{self.index_type.upper()} {self.acquired_date} → {self.mean:.3f}'


class FarmlandPhenology(models.Model):
    """Фенологические метрики по угодью на сезон (из сглаженного ряда NDVI)."""

    class Source(models.TextChoices):
        MODIS = 'modis', 'MODIS'
        RASTER = 'raster', 'S2/L8'

    farmland = models.ForeignKey(Farmland, on_delete=models.CASCADE, related_name='phenology')
    year = models.IntegerField(verbose_name='Год')
    source = models.CharField(max_length=10, choices=Source.choices, default=Source.MODIS, verbose_name='Источник')

    sos_date = models.DateField(null=True, blank=True, verbose_name='Начало сезона (SOS)')
    eos_date = models.DateField(null=True, blank=True, verbose_name='Конец сезона (EOS)')
    pos_date = models.DateField(null=True, blank=True, verbose_name='Пик сезона (POS)')
    max_ndvi = models.FloatField(null=True, blank=True, verbose_name='Макс. NDVI')
    mean_ndvi = models.FloatField(null=True, blank=True, verbose_name='Средн. NDVI за сезон')
    los_days = models.IntegerField(null=True, blank=True, verbose_name='Длит. сезона (дней)')
    total_ndvi = models.FloatField(null=True, blank=True, verbose_name='Интеграл NDVI (TI)')

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'agro_farmland_phenology'
        ordering = ['-year']
        verbose_name = 'Фенология угодья'
        verbose_name_plural = 'Фенология угодий'
        unique_together = [('farmland', 'year', 'source')]
        indexes = [
            models.Index(fields=['farmland', 'year']),
        ]

    def __str__(self):
        return f'Phenology {self.farmland_id} {self.year} ({self.source})'


class MonitoringTask(models.Model):
    """Задача мониторинга NDVI для региона (опционально — конкретного района)."""

    class Status(models.TextChoices):
        ACTIVE = 'active', 'Активный'
        PAUSED = 'paused', 'Приостановлен'
        COMPLETED = 'completed', 'Завершён'

    class TaskType(models.TextChoices):
        MODIS = 'modis', 'MODIS (16-дн. архив)'
        RASTER = 'raster', 'Sentinel-2 + Landsat (оперативно)'

    task_type = models.CharField(
        max_length=20, choices=TaskType.choices,
        default=TaskType.MODIS, verbose_name='Тип мониторинга',
    )
    region = models.ForeignKey(
        Region, on_delete=models.CASCADE, related_name='monitoring_tasks',
    )
    district = models.ForeignKey(
        District, on_delete=models.CASCADE,
        related_name='monitoring_tasks', null=True, blank=True,
        verbose_name='Район (опц.)',
    )
    year = models.IntegerField(verbose_name='Год')
    status = models.CharField(
        max_length=20, choices=Status.choices,
        default=Status.ACTIVE, verbose_name='Статус',
    )
    last_check = models.DateTimeField(blank=True, null=True, verbose_name='Последняя проверка')
    last_date_to = models.DateField(
        blank=True, null=True,
        verbose_name='Последний обработанный период (конец)',
    )
    records_total = models.IntegerField(default=0, verbose_name='Записей всего')
    log = models.TextField(blank=True, default='', verbose_name='Лог')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'agro_monitoring_task'
        ordering = ['-created_at']
        constraints = [
            models.UniqueConstraint(
                fields=['task_type', 'region', 'district', 'year'],
                name='uniq_monitoring_task_scope',
            ),
        ]
        verbose_name = 'Задача мониторинга'
        verbose_name_plural = 'Задачи мониторинга'

    def __str__(self):
        return f'{self.region.name} — {self.year} ({self.get_status_display()})'


class NdviBaseline(models.Model):
    """Среднее историческое значение NDVI по району на дату (день года).

    Рассчитывается по всем годам, кроме текущего. Пересчёт 7 января.
    """
    district = models.ForeignKey(District, on_delete=models.CASCADE, related_name='ndvi_baselines')
    day_of_year = models.SmallIntegerField(verbose_name='День года (1‑366)')
    mean_ndvi = models.FloatField(verbose_name='Среднее NDVI')
    std_ndvi = models.FloatField(default=0, verbose_name='Ст. отклонение NDVI')
    years_count = models.SmallIntegerField(default=0, verbose_name='Кол-во лет')
    crop_type = models.CharField(
        max_length=20,
        choices=Farmland.CropType.choices,
        blank=True, default='',
        verbose_name='Вид угодья (пусто = все)',
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'agro_ndvi_baseline'
        unique_together = [('district', 'day_of_year', 'crop_type')]
        indexes = [
            models.Index(fields=['district', 'crop_type', 'day_of_year']),
        ]
        verbose_name = 'Базовая линия NDVI'
        verbose_name_plural = 'Базовые линии NDVI'

    def __str__(self):
        return f'Baseline d={self.district_id} doy={self.day_of_year} ndvi={self.mean_ndvi:.3f}'


class PipelineRun(models.Model):
    """Лог запуска любого процесса пайплайна."""

    class TaskType(models.TextChoices):
        UPLOAD_REGION = 'upload_region', 'Загрузка региона'
        UPLOAD_DISTRICTS = 'upload_districts', 'Загрузка районов'
        UPLOAD_FARMLANDS = 'upload_farmlands', 'Загрузка угодий'
        ARCHIVE_NDVI = 'archive_ndvi', 'Архивные данные NDVI (MODIS)'
        RASTER_NDVI = 'raster_ndvi', 'Растровые данные NDVI (S2/L8)'
        MONITORING = 'monitoring', 'Мониторинг NDVI'

    class Status(models.TextChoices):
        RUNNING = 'running', 'Выполняется'
        COMPLETED = 'completed', 'Завершён'
        FAILED = 'failed', 'Ошибка'

    task_type = models.CharField(max_length=30, choices=TaskType.choices, verbose_name='Тип процесса')
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RUNNING, verbose_name='Статус')
    region = models.ForeignKey(
        Region, on_delete=models.SET_NULL, null=True, blank=True,
        verbose_name='Регион',
    )
    year = models.IntegerField(null=True, blank=True, verbose_name='Год')
    description = models.CharField(max_length=500, blank=True, default='', verbose_name='Описание')
    log = models.TextField(blank=True, default='', verbose_name='Лог')
    records_count = models.IntegerField(default=0, verbose_name='Записей')
    started_at = models.DateTimeField(auto_now_add=True, verbose_name='Начало')
    finished_at = models.DateTimeField(null=True, blank=True, verbose_name='Окончание')
    # ── detached subprocess tracking ──
    pid = models.IntegerField(null=True, blank=True, verbose_name='PID')
    log_file = models.CharField(
        max_length=255, blank=True, default='',
        verbose_name='Путь к файлу лога',
    )
    heartbeat_at = models.DateTimeField(
        null=True, blank=True, verbose_name='Последний heartbeat',
    )

    class Meta:
        db_table = 'agro_pipeline_run'
        ordering = ['-started_at']
        verbose_name = 'Запуск пайплайна'
        verbose_name_plural = 'Запуски пайплайна'

    def __str__(self):
        return f'{self.get_task_type_display()} — {self.get_status_display()} ({self.started_at:%Y-%m-%d %H:%M})'

    @property
    def duration(self):
        if self.finished_at and self.started_at:
            delta = self.finished_at - self.started_at
            minutes = int(delta.total_seconds() // 60)
            seconds = int(delta.total_seconds() % 60)
            return f'{minutes}м {seconds}с'
        return '—'


class GeeApiMetric(models.Model):
    """Дневной агрегат вызовов Google Earth Engine API.

    Инкрементируется через ``services.gee_client`` для каждого
    ``computePixels`` вызова. Используется для мониторинга расхода
    квоты GEE в админ-панели (нет публичного quota API у Earth Engine,
    поэтому мы считаем вызовы сами).
    """
    day = models.DateField(unique=True, verbose_name='Дата')
    calls = models.BigIntegerField(default=0, verbose_name='Успешных вызовов')
    errors = models.IntegerField(default=0, verbose_name='Ошибок')
    throttled = models.IntegerField(
        default=0,
        verbose_name='Переповторов из-за лимита',
    )
    bytes_downloaded = models.BigIntegerField(
        default=0,
        verbose_name='Байт скачано',
    )
    last_error = models.TextField(blank=True, default='', verbose_name='Последняя ошибка')
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'agro_gee_api_metric'
        ordering = ['-day']
        verbose_name = 'Метрика GEE API'
        verbose_name_plural = 'Метрики GEE API'

    def __str__(self):
        return f'{self.day}: {self.calls} calls, {self.errors} err'
