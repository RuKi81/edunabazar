from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0003_district_code_longer'),
    ]

    operations = [
        migrations.CreateModel(
            name='MonitoringTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.IntegerField(verbose_name='Год')),
                ('status', models.CharField(choices=[('active', 'Активный'), ('paused', 'Приостановлен'), ('completed', 'Завершён')], default='active', max_length=20, verbose_name='Статус')),
                ('last_check', models.DateTimeField(blank=True, null=True, verbose_name='Последняя проверка')),
                ('last_date_to', models.DateField(blank=True, null=True, verbose_name='Последний обработанный период (конец)')),
                ('records_total', models.IntegerField(default=0, verbose_name='Записей всего')),
                ('log', models.TextField(blank=True, default='', verbose_name='Лог')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('region', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='monitoring_tasks', to='agrocosmos.region')),
            ],
            options={
                'verbose_name': 'Задача мониторинга',
                'verbose_name_plural': 'Задачи мониторинга',
                'db_table': 'agro_monitoring_task',
                'ordering': ['-created_at'],
                'unique_together': {('region', 'year')},
            },
        ),
    ]
