from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('my_fields', '0006_gislayer_sort_order'),
    ]

    operations = [
        migrations.AddField(
            model_name='gislayer',
            name='style',
            field=models.JSONField(
                blank=True, default=dict, verbose_name='Стиль (раскраска)'),
        ),
    ]
