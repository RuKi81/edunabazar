from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0036_alter_pipelinerun_task_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelinerun',
            name='task_type',
            field=models.CharField(choices=[('upload_region', 'Загрузка региона'), ('upload_districts', 'Загрузка районов'), ('upload_farmlands', 'Загрузка угодий'), ('archive_ndvi', 'Архивные данные NDVI (MODIS)'), ('raster_ndvi', 'Растровые данные NDVI (S2/L8)'), ('monitoring', 'Мониторинг NDVI'), ('gis_overlay', 'Оверлей ГИС-слоёв'), ('raster_ingest', 'Конвертация растра (COG)')], max_length=30, verbose_name='Тип процесса'),
        ),
    ]
