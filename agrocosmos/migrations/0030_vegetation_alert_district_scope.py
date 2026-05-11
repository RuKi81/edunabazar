"""Allow district-level VegetationAlerts.

- ``farmland`` becomes nullable (was required FK).
- Adds ``district`` FK, ``crop_type`` choice, ``source`` choice.
- CheckConstraint: either ``farmland`` or ``district`` must be set.
- New index on (district, crop_type, alert_type, status) for the
  reconcile hot-path used by ``detect_district_ndvi_alerts``.

Per-farmland alerts created before this migration keep working
(``farmland`` is still populated for them, ``district`` stays NULL).
The new district-level detector populates the opposite shape.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0029_district_ndvi_series'),
    ]

    operations = [
        migrations.AlterField(
            model_name='vegetationalert',
            name='farmland',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=models.deletion.CASCADE,
                related_name='alerts',
                to='agrocosmos.farmland',
                verbose_name='Угодье',
                help_text='NULL для district-level алертов (MODIS).',
            ),
        ),
        migrations.AddField(
            model_name='vegetationalert',
            name='district',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=models.deletion.CASCADE,
                related_name='district_alerts',
                to='agrocosmos.district',
                verbose_name='Район',
                help_text='Заполняется для district-level алертов.',
            ),
        ),
        migrations.AddField(
            model_name='vegetationalert',
            name='crop_type',
            field=models.CharField(
                blank=True, default='', max_length=20,
                choices=[
                    ('arable', 'Пашня'),
                    ('hayfield', 'Сенокос'),
                    ('pasture', 'Пастбище'),
                    ('perennial', 'Многолетние насаждения'),
                    ('other', 'Прочее'),
                ],
                verbose_name='Тип угодья',
                help_text='Для district-level алертов: культура, по которой сработал детектор.',
            ),
        ),
        migrations.AddField(
            model_name='vegetationalert',
            name='source',
            field=models.CharField(
                default='modis', max_length=10,
                choices=[
                    ('modis', 'MODIS'),
                    ('raster', 'Sentinel-2 / Landsat'),
                    ('fused', 'HLS Fused'),
                ],
                verbose_name='Источник данных',
            ),
        ),
        migrations.AddIndex(
            model_name='vegetationalert',
            index=models.Index(
                fields=['district', 'crop_type', 'alert_type', 'status'],
                name='veg_alert_district_idx',
            ),
        ),
        migrations.AddConstraint(
            model_name='vegetationalert',
            constraint=models.CheckConstraint(
                condition=models.Q(farmland__isnull=False) | models.Q(district__isnull=False),
                name='veg_alert_scope_required',
            ),
        ),
    ]
