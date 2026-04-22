from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0018_pipelinerun_subprocess_tracking'),
    ]

    operations = [
        migrations.AddField(
            model_name='monitoringtask',
            name='task_type',
            field=models.CharField(
                max_length=20,
                choices=[
                    ('modis', 'MODIS (16-дн. архив)'),
                    ('raster', 'Sentinel-2 + Landsat (оперативно)'),
                ],
                default='modis',
                verbose_name='Тип мониторинга',
            ),
        ),
        migrations.AddField(
            model_name='monitoringtask',
            name='district',
            field=models.ForeignKey(
                to='agrocosmos.district',
                on_delete=models.deletion.CASCADE,
                related_name='monitoring_tasks',
                null=True, blank=True,
                verbose_name='Район (опц.)',
            ),
        ),
        migrations.AlterUniqueTogether(
            name='monitoringtask',
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name='monitoringtask',
            constraint=models.UniqueConstraint(
                fields=('task_type', 'region', 'district', 'year'),
                name='uniq_monitoring_task_scope',
            ),
        ),
    ]
