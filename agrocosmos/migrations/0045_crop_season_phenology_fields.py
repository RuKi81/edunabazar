"""Фенологическая диагностика сезона + хранилище обученной модели.

``FarmlandCropSeason``: число укосов (циклы спад→отрастание — признак
сенокоса), покрытие сезона наблюдениями, доля зелёных, доля голых
пикселей весной (гетерогенность поля из ``VegetationIndex.histogram``)
и флаг ``crop_type_mismatch`` — расхождение фенологического класса с
кадастровым ``crop_type`` для аудита.

``SeasonClassifierModel``: сохранённые ступени обучаемой модели
(``train_season_model --save``) — состояние логистической регрессии
JSON и снапшот CV-метрик. Применяется прогоном
``classify_winter_spring --use-model`` с fallback на правила.
"""
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0044_training_label_unused_subclasses'),
    ]

    operations = [
        migrations.AddField(
            model_name='farmlandcropseason',
            name='n_mowings',
            field=models.PositiveSmallIntegerField(
                default=0, verbose_name='Число укосов',
                help_text='Циклы спад→отрастание NDVI за сезон (≥2 ⇒ сенокос).',
            ),
        ),
        migrations.AddField(
            model_name='farmlandcropseason',
            name='season_coverage',
            field=models.FloatField(
                null=True, blank=True,
                verbose_name='Покрытие сезона (0..1)',
            ),
        ),
        migrations.AddField(
            model_name='farmlandcropseason',
            name='green_fraction',
            field=models.FloatField(
                null=True, blank=True,
                verbose_name='Доля зелёных наблюдений',
            ),
        ),
        migrations.AddField(
            model_name='farmlandcropseason',
            name='bare_spring_fraction',
            field=models.FloatField(
                null=True, blank=True,
                verbose_name='Доля голых пикселей весной',
            ),
        ),
        migrations.AddField(
            model_name='farmlandcropseason',
            name='crop_type_mismatch',
            field=models.BooleanField(
                default=False,
                verbose_name='Расхождение вида угодья',
                help_text='Фенологический класс конфликтует с crop_type кадастра.',
            ),
        ),
        migrations.CreateModel(
            name='SeasonClassifierModel',
            fields=[
                ('id', models.BigAutoField(
                    auto_created=True, primary_key=True, serialize=False,
                    verbose_name='ID')),
                ('year', models.IntegerField(
                    verbose_name='Год обучающей выборки')),
                ('source', models.CharField(
                    choices=[('raster', 'S2/L8'), ('fused', 'HLS Fused')],
                    default='raster', max_length=10,
                    verbose_name='Источник рядов')),
                ('stage', models.CharField(
                    choices=[
                        ('cover', 'Культура vs не обрабатывается'),
                        ('season', 'Озимые vs яровые'),
                    ],
                    max_length=10, verbose_name='Ступень')),
                ('state', models.JSONField(
                    verbose_name='Состояние модели (JSON)')),
                ('metrics', models.JSONField(
                    default=dict,
                    verbose_name='Метрики CV на момент сохранения')),
                ('n_train', models.PositiveIntegerField(
                    default=0, verbose_name='Размер обучающей выборки')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('region', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='season_models', to='agrocosmos.region',
                    verbose_name='Регион (None — все регионы)')),
            ],
            options={
                'verbose_name': 'Модель классификации сезона',
                'verbose_name_plural': 'Модели классификации сезона',
                'db_table': 'agro_season_classifier_model',
                'ordering': ['-year'],
                'unique_together': {('region', 'year', 'source', 'stage')},
            },
        ),
    ]
