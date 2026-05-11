from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0028_district_ndvi_status'),
    ]

    operations = [
        migrations.CreateModel(
            name='DistrictNdviSeries',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('acquired_date', models.DateField(verbose_name='Дата композиты')),
                ('crop_type', models.CharField(
                    choices=[
                        ('arable', 'Пашня'),
                        ('hayfield', 'Сенокос'),
                        ('pasture', 'Пастбище'),
                        ('perennial', 'Многолетние насаждения'),
                        ('other', 'Прочее'),
                    ],
                    max_length=20,
                    verbose_name='Тип угодья',
                )),
                ('source', models.CharField(
                    choices=[
                        ('modis', 'MODIS'),
                        ('raster', 'Sentinel-2 / Landsat'),
                        ('fused', 'HLS Fused'),
                    ],
                    default='modis', max_length=10, verbose_name='Источник',
                )),
                ('sum_ndvi_area', models.FloatField(
                    help_text='Числитель area-weighted среднего NDVI.',
                    verbose_name='Σ (mean_ndvi × area_ha)',
                )),
                ('sum_area', models.FloatField(
                    help_text='Знаменатель area-weighted среднего NDVI.',
                    verbose_name='Σ area_ha угодий с данными',
                )),
                ('obs_count', models.IntegerField(default=0, verbose_name='Количество VI-строк в агрегате')),
                ('computed_at', models.DateTimeField(auto_now=True)),
                ('district', models.ForeignKey(
                    on_delete=models.deletion.CASCADE,
                    related_name='ndvi_series',
                    to='agrocosmos.district',
                )),
            ],
            options={
                'db_table': 'agro_district_ndvi_series',
                'verbose_name': 'Временной ряд NDVI по району',
                'verbose_name_plural': 'Временные ряды NDVI по районам',
            },
        ),
        migrations.AlterUniqueTogether(
            name='districtndviseries',
            unique_together={('district', 'acquired_date', 'crop_type', 'source')},
        ),
        migrations.AddIndex(
            model_name='districtndviseries',
            index=models.Index(
                fields=['district', 'source', 'acquired_date'],
                name='dns_district_src_date_idx',
            ),
        ),
        migrations.AddIndex(
            model_name='districtndviseries',
            index=models.Index(
                fields=['source', 'acquired_date'],
                name='dns_src_date_idx',
            ),
        ),
    ]
