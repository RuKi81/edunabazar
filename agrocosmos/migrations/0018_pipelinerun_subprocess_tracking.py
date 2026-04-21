"""Add subprocess-tracking fields to PipelineRun.

Enables launching long-running pipelines as detached subprocesses instead
of in-process threads (which die when the gunicorn worker is recycled).
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0017_add_hls_fused_satellite'),
    ]

    operations = [
        migrations.AddField(
            model_name='pipelinerun',
            name='pid',
            field=models.IntegerField(blank=True, null=True, verbose_name='PID'),
        ),
        migrations.AddField(
            model_name='pipelinerun',
            name='log_file',
            field=models.CharField(
                blank=True, default='', max_length=255,
                verbose_name='Путь к файлу лога',
            ),
        ),
        migrations.AddField(
            model_name='pipelinerun',
            name='heartbeat_at',
            field=models.DateTimeField(
                blank=True, null=True, verbose_name='Последний heartbeat',
            ),
        ),
    ]
