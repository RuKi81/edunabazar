from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('my_fields', '0007_gislayer_style'),
    ]

    operations = [
        migrations.CreateModel(
            name='GisFolder',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(default='Новая папка', max_length=200, verbose_name='Название папки')),
                ('sort_order', models.IntegerField(default=0, help_text='Меньше — выше в списке слоёв.', verbose_name='Порядок')),
                ('collapsed', models.BooleanField(default=False, verbose_name='Свёрнута')),
                ('visible', models.BooleanField(default=True, verbose_name='Видима')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('owner', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='gis_folders', to=settings.AUTH_USER_MODEL, verbose_name='Создал')),
            ],
            options={
                'verbose_name': 'ГИС-папка',
                'verbose_name_plural': 'ГИС-папки',
                'db_table': 'myf_gis_folder',
                'ordering': ['sort_order', 'id'],
            },
        ),
        migrations.AddField(
            model_name='gislayer',
            name='folder',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='layers', to='my_fields.gisfolder', verbose_name='Папка'),
        ),
    ]
