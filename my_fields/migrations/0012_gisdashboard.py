from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('my_fields', '0011_rasterlayer_is_public'),
    ]

    operations = [
        migrations.CreateModel(
            name='GisDashboard',
            fields=[
                ('id', models.BigAutoField(
                    auto_created=True, primary_key=True, serialize=False,
                    verbose_name='ID')),
                ('name', models.CharField(
                    max_length=200, verbose_name='Название отчёта')),
                ('params', models.JSONField(
                    blank=True, default=dict, verbose_name='Параметры')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('layer', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='dashboards', to='my_fields.gislayer',
                    verbose_name='Слой')),
                ('owner', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='gis_dashboards', to=settings.AUTH_USER_MODEL,
                    verbose_name='Владелец')),
            ],
            options={
                'verbose_name': 'Сохранённый дашборд',
                'verbose_name_plural': 'Сохранённые дашборды',
                'db_table': 'myf_gis_dashboard',
                'ordering': ['name', 'id'],
            },
        ),
    ]
