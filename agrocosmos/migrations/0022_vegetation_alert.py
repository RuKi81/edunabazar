from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0021_gee_api_metric'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='VegetationAlert',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('alert_type', models.CharField(
                    choices=[('baseline_deviation', 'Отклонение от нормы'),
                             ('rapid_drop', 'Резкое падение NDVI')],
                    max_length=30, verbose_name='Тип алерта')),
                ('severity', models.CharField(
                    choices=[('warning', 'Предупреждение'), ('critical', 'Критично')],
                    default='warning', max_length=10, verbose_name='Критичность')),
                ('status', models.CharField(
                    choices=[('active', 'Активный'), ('acknowledged', 'Принят'),
                             ('resolved', 'Разрешён')],
                    default='active', max_length=15, verbose_name='Статус')),
                ('detected_on', models.DateField(verbose_name='Дата наблюдения, спровоцировавшего алерт')),
                ('triggered_at', models.DateTimeField(auto_now_add=True, verbose_name='Создан')),
                ('acknowledged_at', models.DateTimeField(blank=True, null=True, verbose_name='Принят')),
                ('resolved_at', models.DateTimeField(blank=True, null=True, verbose_name='Разрешён')),
                ('context', models.JSONField(blank=True, null=True,
                    verbose_name='Контекст (z-score, NDVI, baseline и т.п.)')),
                ('message', models.CharField(blank=True, default='', max_length=500,
                    verbose_name='Человеко-читаемое описание')),
                ('acknowledged_by', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=models.deletion.SET_NULL,
                    related_name='+',
                    to=settings.AUTH_USER_MODEL,
                    verbose_name='Кем принят')),
                ('farmland', models.ForeignKey(
                    on_delete=models.deletion.CASCADE,
                    related_name='alerts',
                    to='agrocosmos.farmland',
                    verbose_name='Угодье')),
            ],
            options={
                'verbose_name': 'Алерт вегетации',
                'verbose_name_plural': 'Алерты вегетации',
                'db_table': 'agro_vegetation_alert',
                'ordering': ['-triggered_at'],
            },
        ),
        migrations.AddIndex(
            model_name='vegetationalert',
            index=models.Index(fields=['status', '-triggered_at'], name='veg_alert_active_idx'),
        ),
        migrations.AddIndex(
            model_name='vegetationalert',
            index=models.Index(fields=['farmland', 'alert_type', 'status'],
                               name='agro_vegeta_farmlan_e65097_idx'),
        ),
    ]
