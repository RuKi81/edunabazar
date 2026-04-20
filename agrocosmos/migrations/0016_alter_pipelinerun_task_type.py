"""Sync PipelineRun.task_type choices with the models.py state.

Adds ``upload_districts`` to the enumeration. Schema-level this is a no-op
(``CharField(max_length=30)`` doesn't change); it only refreshes Django's
internal migration state so ``makemigrations`` stops complaining.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0015_perf_indexes'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelinerun',
            name='task_type',
            field=models.CharField(
                choices=[
                    ('upload_region', 'Загрузка региона'),
                    ('upload_districts', 'Загрузка районов'),
                    ('upload_farmlands', 'Загрузка угодий'),
                    ('archive_ndvi', 'Архивные данные NDVI (MODIS)'),
                    ('raster_ndvi', 'Растровые данные NDVI (S2/L8)'),
                    ('monitoring', 'Мониторинг NDVI'),
                ],
                max_length=30,
                verbose_name='Тип процесса',
            ),
        ),
    ]
