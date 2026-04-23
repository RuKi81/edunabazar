from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0020_rename_is_anomaly_to_is_outlier'),
    ]

    operations = [
        migrations.CreateModel(
            name='GeeApiMetric',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('day', models.DateField(unique=True, verbose_name='Дата')),
                ('calls', models.BigIntegerField(default=0, verbose_name='Успешных вызовов')),
                ('errors', models.IntegerField(default=0, verbose_name='Ошибок')),
                ('throttled', models.IntegerField(default=0, verbose_name='Переповторов из-за лимита')),
                ('bytes_downloaded', models.BigIntegerField(default=0, verbose_name='Байт скачано')),
                ('last_error', models.TextField(blank=True, default='', verbose_name='Последняя ошибка')),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'Метрика GEE API',
                'verbose_name_plural': 'Метрики GEE API',
                'db_table': 'agro_gee_api_metric',
                'ordering': ['-day'],
            },
        ),
    ]
