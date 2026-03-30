import django.contrib.gis.db.models.fields
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='Region',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, verbose_name='Название')),
                ('code', models.CharField(max_length=10, unique=True, verbose_name='Код субъекта')),
                ('geom', django.contrib.gis.db.models.fields.MultiPolygonField(srid=4326, verbose_name='Границы')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'verbose_name': 'Регион',
                'verbose_name_plural': 'Регионы',
                'db_table': 'agro_region',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='District',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255, verbose_name='Название')),
                ('code', models.CharField(blank=True, default='', max_length=20, verbose_name='Код района')),
                ('geom', django.contrib.gis.db.models.fields.MultiPolygonField(srid=4326, verbose_name='Границы')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('region', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='districts', to='agrocosmos.region')),
            ],
            options={
                'verbose_name': 'Район',
                'verbose_name_plural': 'Районы',
                'db_table': 'agro_district',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='Farmland',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('crop_type', models.CharField(choices=[('arable', 'Пашня'), ('hayfield', 'Сенокос'), ('pasture', 'Пастбище'), ('perennial', 'Многолетнее насаждение'), ('other', 'Прочее')], default='arable', max_length=20, verbose_name='Вид угодья')),
                ('cadastral_number', models.CharField(blank=True, default='', max_length=50, verbose_name='Кадастровый номер')),
                ('area_ha', models.FloatField(default=0, verbose_name='Площадь, га')),
                ('geom', django.contrib.gis.db.models.fields.MultiPolygonField(srid=4326, verbose_name='Границы')),
                ('properties', models.JSONField(blank=True, null=True, verbose_name='Доп. атрибуты')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('district', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='farmlands', to='agrocosmos.district')),
            ],
            options={
                'verbose_name': 'Угодье',
                'verbose_name_plural': 'Угодья',
                'db_table': 'agro_farmland',
                'ordering': ['district', 'crop_type'],
            },
        ),
        migrations.AddIndex(
            model_name='farmland',
            index=models.Index(fields=['district', 'crop_type'], name='agro_farmla_distric_idx'),
        ),
        migrations.CreateModel(
            name='SatelliteScene',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('satellite', models.CharField(choices=[('sentinel2', 'Sentinel-2'), ('landsat8', 'Landsat 8'), ('landsat9', 'Landsat 9'), ('modis_terra', 'MODIS Terra'), ('modis_aqua', 'MODIS Aqua')], max_length=20, verbose_name='Спутник')),
                ('scene_id', models.CharField(max_length=255, unique=True, verbose_name='ID снимка')),
                ('acquired_date', models.DateField(verbose_name='Дата съёмки')),
                ('cloud_cover', models.FloatField(default=0, verbose_name='Облачность, %')),
                ('bbox', django.contrib.gis.db.models.fields.PolygonField(blank=True, null=True, srid=4326, verbose_name='Охват')),
                ('file_path', models.CharField(blank=True, default='', max_length=500, verbose_name='Путь к файлу')),
                ('metadata', models.JSONField(blank=True, null=True, verbose_name='Метаданные')),
                ('processed', models.BooleanField(default=False, verbose_name='Обработан')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'verbose_name': 'Снимок',
                'verbose_name_plural': 'Снимки',
                'db_table': 'agro_satellite_scene',
                'ordering': ['-acquired_date'],
            },
        ),
        migrations.CreateModel(
            name='VegetationIndex',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('index_type', models.CharField(choices=[('ndvi', 'NDVI'), ('evi', 'EVI'), ('msavi', 'MSAVI'), ('ndwi', 'NDWI'), ('ndmi', 'NDMI')], max_length=10, verbose_name='Тип индекса')),
                ('acquired_date', models.DateField(verbose_name='Дата съёмки')),
                ('mean', models.FloatField(verbose_name='Среднее')),
                ('median', models.FloatField(default=0, verbose_name='Медиана')),
                ('min_val', models.FloatField(default=0, verbose_name='Минимум')),
                ('max_val', models.FloatField(default=0, verbose_name='Максимум')),
                ('std_val', models.FloatField(default=0, verbose_name='Ст. отклонение')),
                ('pixel_count', models.IntegerField(default=0, verbose_name='Кол-во пикселей')),
                ('valid_pixel_count', models.IntegerField(default=0, verbose_name='Валидных пикселей')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('farmland', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='indices', to='agrocosmos.farmland')),
                ('scene', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='indices', to='agrocosmos.satellitescene')),
            ],
            options={
                'verbose_name': 'Вег. индекс',
                'verbose_name_plural': 'Вег. индексы',
                'db_table': 'agro_vegetation_index',
                'ordering': ['-acquired_date'],
                'unique_together': {('farmland', 'scene', 'index_type')},
            },
        ),
        migrations.AddIndex(
            model_name='vegetationindex',
            index=models.Index(fields=['farmland', 'index_type', 'acquired_date'], name='agro_vegidx_farm_type_date'),
        ),
        migrations.AddIndex(
            model_name='vegetationindex',
            index=models.Index(fields=['acquired_date', 'index_type'], name='agro_vegidx_date_type'),
        ),
    ]
