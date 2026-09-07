from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0038_farmland_crop_season'),
    ]

    operations = [
        migrations.AddField(
            model_name='farmlandcropseason',
            name='peak_doy_threshold',
            field=models.FloatField(
                blank=True, null=True,
                help_text='Основной признак: пик раньше порога ⇒ озимые.',
                verbose_name='Порог дня пика на прогоне',
            ),
        ),
    ]
