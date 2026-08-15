from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('my_fields', '0005_gislayer'),
    ]

    operations = [
        migrations.AddField(
            model_name='gislayer',
            name='sort_order',
            field=models.IntegerField(
                default=0, verbose_name='Порядок',
                help_text='Меньше — выше в списке слоёв и на карте.',
            ),
        ),
        migrations.AlterModelOptions(
            name='gislayer',
            options={
                'ordering': ['sort_order', '-created_at'],
                'verbose_name': 'ГИС-слой (SHP)',
                'verbose_name_plural': 'ГИС-слои (SHP)',
            },
        ),
    ]
