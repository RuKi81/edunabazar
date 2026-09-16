"""Два подкласса «не обрабатывается»: ДКР и сорная растительность.

Только расширение choices + max_length 10 → 20: значения 'unused_woody'
и 'unused_weeds' в 10 символов не влезали. УЖЕ НАБРАННЫЕ метки 'unused'
остаются валидными и НЕ переписываются — общий класс нужен там, где тип
зарастания по снимку не разобрать. Для калибровки все три сворачиваются
в 'unused' (``FarmlandTrainingLabel.family``), поэтому обратная
совместимость прогонов полная.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0043_pipelinerun_classify_season'),
    ]

    operations = [
        migrations.AlterField(
            model_name='farmlandtraininglabel',
            name='true_class',
            field=models.CharField(
                choices=[
                    ('winter', 'Озимые'),
                    ('spring', 'Яровые'),
                    ('hayfield', 'Сенокос'),
                    ('unused', 'Не обрабатывается'),
                    ('unused_woody', 'Не обраб.: ДКР'),
                    ('unused_weeds', 'Не обраб.: сорная'),
                    ('ignore', 'Пропустить (сад/прочее)'),
                ],
                max_length=20, verbose_name='Истинный класс',
            ),
        ),
    ]
