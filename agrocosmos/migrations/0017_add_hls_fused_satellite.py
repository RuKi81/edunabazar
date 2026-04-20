"""Add ``hls_fused`` to ``SatelliteScene.Satellite`` choices.

Schema-level this is a no-op (``CharField(max_length=20)`` is unchanged);
it only refreshes Django's internal migration state for the new choice
used by the ``compute_fused_ndvi`` pipeline.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0016_alter_pipelinerun_task_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='satellitescene',
            name='satellite',
            field=models.CharField(
                choices=[
                    ('sentinel2', 'Sentinel-2'),
                    ('landsat8', 'Landsat 8'),
                    ('landsat9', 'Landsat 9'),
                    ('modis_terra', 'MODIS Terra'),
                    ('modis_aqua', 'MODIS Aqua'),
                    ('hls_fused', 'HLS Fused (S2+L)'),
                ],
                max_length=20,
                verbose_name='Спутник',
            ),
        ),
    ]
