from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('access', '0002_alter_resourcegrant_resource_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='resourcegrant',
            name='resource_type',
            field=models.CharField(
                choices=[
                    ('gis_layer', 'ГИС-слой (SHP)'),
                    ('raster_layer', 'Растровый слой (GeoTIFF/COG)'),
                    ('farmland', 'Границы угодий (ЗСН, Агрокосмос)'),
                ],
                default='gis_layer', max_length=20, verbose_name='Тип ресурса',
            ),
        ),
    ]
