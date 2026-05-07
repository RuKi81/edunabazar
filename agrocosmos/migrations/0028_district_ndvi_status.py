from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0027_index_renames_and_ndvibaseline_choices'),
    ]

    operations = [
        migrations.CreateModel(
            name='DistrictNdviStatus',
            fields=[
                ('district', models.OneToOneField(
                    on_delete=models.deletion.CASCADE,
                    primary_key=True,
                    related_name='ndvi_status',
                    serialize=False,
                    to='agrocosmos.district',
                )),
                ('latest_date', models.DateField(verbose_name='Последняя дата MODIS')),
                ('current_ndvi', models.FloatField(verbose_name='Текущее NDVI (area-weighted)')),
                ('baseline_ndvi', models.FloatField(
                    blank=True, null=True, verbose_name='Baseline NDVI на ту же DOY',
                )),
                ('pct_of_baseline', models.FloatField(
                    blank=True, null=True,
                    verbose_name='% от нормы (current / baseline * 100)',
                )),
                ('computed_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'agro_district_ndvi_status',
                'verbose_name': 'Статус NDVI района',
                'verbose_name_plural': 'Статусы NDVI районов',
            },
        ),
    ]
