"""Add 'queued' status and launch_args JSON field to PipelineRun.

Enables a separate worker container (see ``run_ndvi_worker`` management
command) to pick up queued pipeline runs created by the admin panel,
decoupling long-running jobs from the web container's lifecycle.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0023_agro_subscription'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pipelinerun',
            name='status',
            field=models.CharField(
                choices=[
                    ('queued', 'В очереди'),
                    ('running', 'Выполняется'),
                    ('completed', 'Завершён'),
                    ('failed', 'Ошибка'),
                ],
                default='running',
                max_length=20,
                verbose_name='Статус',
            ),
        ),
        migrations.AddField(
            model_name='pipelinerun',
            name='launch_args',
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text='CLI-аргументы для команды воркера (используется при status=queued).',
                verbose_name='Параметры запуска',
            ),
        ),
    ]
