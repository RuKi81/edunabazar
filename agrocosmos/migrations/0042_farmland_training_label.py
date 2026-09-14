import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0041_farmlandcropseason_is_harvested_hayfield'),
    ]

    operations = [
        migrations.CreateModel(
            name='FarmlandTrainingLabel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.IntegerField(verbose_name='Год')),
                ('true_class', models.CharField(choices=[('winter', 'Озимые'), ('spring', 'Яровые'), ('hayfield', 'Сенокос'), ('unused', 'Не обрабатывается'), ('ignore', 'Пропустить (сад/прочее)')], max_length=10, verbose_name='Истинный класс')),
                ('note', models.CharField(blank=True, default='', max_length=255, verbose_name='Заметка')),
                ('labeled_by', models.CharField(blank=True, default='', max_length=150, verbose_name='Разметил (логин)')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('farmland', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='training_labels', to='agrocosmos.farmland')),
            ],
            options={
                'verbose_name': 'Метка обучающей выборки',
                'verbose_name_plural': 'Метки обучающей выборки',
                'db_table': 'agro_farmland_training_label',
                'ordering': ['-updated_at'],
            },
        ),
        migrations.AddIndex(
            model_name='farmlandtraininglabel',
            index=models.Index(fields=['year', 'true_class'], name='train_label_yr_cls_idx'),
        ),
        migrations.AlterUniqueTogether(
            name='farmlandtraininglabel',
            unique_together={('farmland', 'year')},
        ),
    ]
