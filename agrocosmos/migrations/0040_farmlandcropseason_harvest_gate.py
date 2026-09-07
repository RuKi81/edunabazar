from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0039_farmlandcropseason_peak_doy_threshold'),
    ]

    operations = [
        migrations.AddField(
            model_name='farmlandcropseason',
            name='harvest_doy',
            field=models.IntegerField(
                blank=True, null=True,
                help_text='День уборочного спада NDVI после пика.',
                verbose_name='Уборка (день года)',
            ),
        ),
        migrations.AddField(
            model_name='farmlandcropseason',
            name='harvest_drop',
            field=models.FloatField(
                blank=True, null=True,
                help_text='Доля спада NDVI после пика к амплитуде сезона (0..1+).',
                verbose_name='Глубина уборочного спада',
            ),
        ),
        migrations.AlterField(
            model_name='farmlandcropseason',
            name='season_class',
            field=models.CharField(
                choices=[
                    ('winter', 'Озимые'),
                    ('spring', 'Яровые'),
                    ('unused', 'Не обрабатывается'),
                    ('unknown', 'Не определено'),
                ],
                default='unknown', max_length=10, verbose_name='Класс сезона',
            ),
        ),
    ]
