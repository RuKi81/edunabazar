from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0005_rename_indexes'),
    ]

    operations = [
        migrations.CreateModel(
            name='PipelineRun',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('task_type', models.CharField(choices=[('upload_region', 'Загрузка региона'), ('upload_farmlands', 'Загрузка угодий'), ('archive_ndvi', 'Архивные данные NDVI'), ('monitoring', 'Мониторинг NDVI')], max_length=30, verbose_name='Тип процесса')),
                ('status', models.CharField(choices=[('running', 'Выполняется'), ('completed', 'Завершён'), ('failed', 'Ошибка')], default='running', max_length=20, verbose_name='Статус')),
                ('region', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='agrocosmos.region', verbose_name='Регион')),
                ('year', models.IntegerField(blank=True, null=True, verbose_name='Год')),
                ('description', models.CharField(blank=True, default='', max_length=500, verbose_name='Описание')),
                ('log', models.TextField(blank=True, default='', verbose_name='Лог')),
                ('records_count', models.IntegerField(default=0, verbose_name='Записей')),
                ('started_at', models.DateTimeField(auto_now_add=True, verbose_name='Начало')),
                ('finished_at', models.DateTimeField(blank=True, null=True, verbose_name='Окончание')),
            ],
            options={
                'verbose_name': 'Запуск пайплайна',
                'verbose_name_plural': 'Запуски пайплайна',
                'db_table': 'agro_pipeline_run',
                'ordering': ['-started_at'],
            },
        ),
    ]
