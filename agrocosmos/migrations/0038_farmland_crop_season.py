import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0037_alter_pipelinerun_task_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='FarmlandCropSeason',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.IntegerField(verbose_name='Год')),
                ('source', models.CharField(choices=[('raster', 'S2/L8'), ('fused', 'HLS Fused')], default='raster', max_length=10, verbose_name='Источник')),
                ('season_class', models.CharField(choices=[('winter', 'Озимые'), ('spring', 'Яровые'), ('unknown', 'Не определено')], default='unknown', max_length=10, verbose_name='Класс сезона')),
                ('confidence', models.FloatField(default=0, verbose_name='Уверенность (0..1)')),
                ('early_spring_ndvi', models.FloatField(blank=True, help_text='Ключевой признак озимых: рост в апреле до всходов яровых.', null=True, verbose_name='Средний NDVI ранней весны')),
                ('winter_baseline', models.FloatField(blank=True, null=True, verbose_name='Зимний baseline NDVI')),
                ('sos_doy', models.IntegerField(blank=True, null=True, verbose_name='SOS (день года)')),
                ('peak_doy', models.IntegerField(blank=True, null=True, verbose_name='Пик (день года)')),
                ('peak_ndvi', models.FloatField(blank=True, null=True, verbose_name='Пиковый NDVI')),
                ('is_reference', models.BooleanField(default=False, verbose_name='Опорное (по разметке)')),
                ('reference_crop', models.CharField(blank=True, default='', max_length=120, verbose_name='Культура по разметке')),
                ('threshold', models.FloatField(blank=True, null=True, verbose_name='Порог early_spring_ndvi на прогоне')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('farmland', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='crop_seasons', to='agrocosmos.farmland')),
            ],
            options={
                'verbose_name': 'Озимые/яровые угодья',
                'verbose_name_plural': 'Озимые/яровые угодий',
                'db_table': 'agro_farmland_crop_season',
                'ordering': ['-year'],
            },
        ),
        migrations.AddIndex(
            model_name='farmlandcropseason',
            index=models.Index(fields=['farmland', 'year'], name='crop_season_fl_year_idx'),
        ),
        migrations.AddIndex(
            model_name='farmlandcropseason',
            index=models.Index(fields=['year', 'source', 'season_class'], name='crop_season_yr_src_cls_idx'),
        ),
        migrations.AlterUniqueTogether(
            name='farmlandcropseason',
            unique_together={('farmland', 'year', 'source')},
        ),
    ]
