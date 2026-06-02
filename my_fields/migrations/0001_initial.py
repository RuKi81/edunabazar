"""Initial migration for my_fields.

Создаётся вручную (без локального Docker на Windows-машине разработчика).
Структура полностью эквивалентна тому, что сгенерирует
``makemigrations`` для моделей ``my_fields.models``. Когда добавится
локальный docker-compose у разработчика — последующие миграции пойдут
через стандартный ``makemigrations`` и эта будет «зафиксирована» как
есть.

Зависимости:
* ``settings.AUTH_USER_MODEL`` — для ``UserField.owner``, ``UserPlan.user``,
  ``FieldEvent.created_by``, ``FieldPhoto.uploaded_by``.
* ``agrocosmos.0001_initial`` — для FK на ``Region`` и ``District``
  из ``UserField``. Более поздние миграции agrocosmos подтянутся
  транзитивно (Django выстраивает граф зависимостей сам).
"""
from __future__ import annotations

import django.contrib.gis.db.models.fields
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('agrocosmos', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        # ───────────────────── Plan ─────────────────────
        migrations.CreateModel(
            name='Plan',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('code', models.CharField(max_length=20, unique=True, verbose_name='Код')),
                ('name', models.CharField(max_length=80, verbose_name='Название')),
                ('monthly_price_rub', models.IntegerField(default=0, verbose_name='Цена/мес, ₽')),
                ('max_fields', models.IntegerField(blank=True, null=True, verbose_name='Макс. полей')),
                ('max_total_area_ha', models.FloatField(blank=True, null=True, verbose_name='Макс. площадь, га')),
                ('ndvi_history_years', models.IntegerField(default=1, verbose_name='Глубина NDVI-истории, лет')),
                ('weather_forecast_enabled', models.BooleanField(default=False, verbose_name='Прогноз погоды')),
                ('alerts_enabled', models.BooleanField(default=True, verbose_name='Алерты по полю')),
                ('is_active', models.BooleanField(default=True)),
                ('sort_order', models.IntegerField(default=0)),
            ],
            options={
                'verbose_name': 'Тариф',
                'verbose_name_plural': 'Тарифы',
                'db_table': 'myf_plan',
                'ordering': ['sort_order', 'monthly_price_rub'],
            },
        ),

        # ─────────────────── UserPlan ───────────────────
        migrations.CreateModel(
            name='UserPlan',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('activated_at', models.DateTimeField(auto_now_add=True)),
                ('expires_at', models.DateTimeField(blank=True, null=True)),
                ('last_payment_provider', models.CharField(blank=True, default='', max_length=40)),
                ('last_payment_id', models.CharField(blank=True, default='', max_length=120)),
                ('last_payment_at', models.DateTimeField(blank=True, null=True)),
                ('plan', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='my_fields.plan')),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='myf_plan', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'Тариф пользователя',
                'verbose_name_plural': 'Тарифы пользователей',
                'db_table': 'myf_user_plan',
            },
        ),

        # ─────────────────── UserField ───────────────────
        migrations.CreateModel(
            name='UserField',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=120, verbose_name='Название')),
                ('geom', django.contrib.gis.db.models.fields.MultiPolygonField(srid=4326, verbose_name='Границы')),
                ('area_ha', models.FloatField(default=0, verbose_name='Площадь, га')),
                ('crop_type', models.CharField(choices=[('arable', 'Пашня'), ('fallow', 'Залежь'), ('hayfield', 'Сенокос'), ('pasture', 'Пастбище'), ('perennial', 'Многолетние насаждения'), ('garden', 'Сад / огород'), ('other', 'Прочее')], default='arable', max_length=20, verbose_name='Тип угодья')),
                ('cadastral_number', models.CharField(blank=True, db_index=True, default='', max_length=50, verbose_name='Кадастровый номер')),
                ('notes', models.TextField(blank=True, default='', verbose_name='Заметки')),
                ('is_archived', models.BooleanField(default=False, help_text='Архивные поля не показываются на основной карте и не учитываются в квотах.', verbose_name='Архив')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('district', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to='agrocosmos.district', verbose_name='Район')),
                ('owner', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='myf_fields', to=settings.AUTH_USER_MODEL, verbose_name='Владелец')),
                ('region', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to='agrocosmos.region', verbose_name='Субъект')),
            ],
            options={
                'verbose_name': 'Поле пользователя',
                'verbose_name_plural': 'Поля пользователей',
                'db_table': 'myf_field',
                'ordering': ['-updated_at'],
            },
        ),
        migrations.AddIndex(
            model_name='userfield',
            index=models.Index(fields=['owner', 'is_archived'], name='myf_field_owner_i_idx'),
        ),
        migrations.AddIndex(
            model_name='userfield',
            index=models.Index(fields=['region', 'crop_type'], name='myf_field_region_c_idx'),
        ),

        # ─────────────────── FieldSeason ───────────────────
        migrations.CreateModel(
            name='FieldSeason',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.IntegerField(verbose_name='Год')),
                ('crop', models.CharField(choices=[('wheat', 'Пшеница'), ('barley', 'Ячмень'), ('rye', 'Рожь'), ('oats', 'Овёс'), ('corn', 'Кукуруза'), ('sunflower', 'Подсолнечник'), ('soybean', 'Соя'), ('rapeseed', 'Рапс'), ('sugar_beet', 'Сахарная свёкла'), ('potato', 'Картофель'), ('vegetables', 'Овощи'), ('fruits', 'Плодовые'), ('berries', 'Ягоды'), ('grass', 'Травы (кормовые)'), ('fallow', 'Пар'), ('other', 'Прочее')], max_length=20, verbose_name='Культура')),
                ('variety', models.CharField(blank=True, default='', max_length=120, verbose_name='Сорт / гибрид')),
                ('sowing_date', models.DateField(blank=True, null=True, verbose_name='Дата сева')),
                ('planned_harvest_date', models.DateField(blank=True, null=True, verbose_name='План. уборка')),
                ('actual_harvest_date', models.DateField(blank=True, null=True, verbose_name='Факт. уборка')),
                ('planned_yield_t_per_ha', models.FloatField(blank=True, null=True, verbose_name='План, т/га')),
                ('actual_yield_t_per_ha', models.FloatField(blank=True, null=True, verbose_name='Факт, т/га')),
                ('gross_t', models.FloatField(blank=True, null=True, verbose_name='Валовой сбор, т')),
                ('notes', models.TextField(blank=True, default='')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('field', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='seasons', to='my_fields.userfield', verbose_name='Поле')),
            ],
            options={
                'verbose_name': 'Сезон',
                'verbose_name_plural': 'Сезоны',
                'db_table': 'myf_field_season',
                'ordering': ['-year', '-created_at'],
            },
        ),
        migrations.AddConstraint(
            model_name='fieldseason',
            constraint=models.UniqueConstraint(
                fields=('field', 'year', 'crop'),
                name='myf_season_unique_field_year_crop',
            ),
        ),

        # ─────────────────── FieldEvent ───────────────────
        migrations.CreateModel(
            name='FieldEvent',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('event_type', models.CharField(choices=[('sowing', 'Сев'), ('fertilize', 'Внесение удобрений'), ('protect', 'Обработка СЗР'), ('tillage', 'Обработка почвы'), ('irrigate', 'Полив'), ('scout', 'Осмотр / разведка'), ('issue', 'Проблема'), ('harvest', 'Уборка'), ('soil_test', 'Анализ почвы'), ('other', 'Прочее')], max_length=20, verbose_name='Тип события')),
                ('event_date', models.DateField(verbose_name='Дата')),
                ('title', models.CharField(blank=True, default='', max_length=180, verbose_name='Заголовок')),
                ('description', models.TextField(blank=True, default='', verbose_name='Описание')),
                ('quantity', models.FloatField(blank=True, null=True, verbose_name='Количество')),
                ('quantity_unit', models.CharField(blank=True, default='', help_text='кг/га, л/га, т/га, шт., м³', max_length=20, verbose_name='Ед. изм.')),
                ('product_name', models.CharField(blank=True, default='', max_length=180, verbose_name='Препарат / удобрение')),
                ('cost_rub', models.FloatField(blank=True, null=True, verbose_name='Затраты, ₽')),
                ('weather_snapshot', models.JSONField(blank=True, help_text='Заполняется автоматически при создании из Open-Meteo.', null=True, verbose_name='Снимок погоды')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('field', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='events', to='my_fields.userfield', verbose_name='Поле')),
                ('season', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='events', to='my_fields.fieldseason', verbose_name='Сезон')),
            ],
            options={
                'verbose_name': 'Событие журнала',
                'verbose_name_plural': 'Журнал событий',
                'db_table': 'myf_field_event',
                'ordering': ['-event_date', '-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='fieldevent',
            index=models.Index(fields=['field', '-event_date'], name='myf_event_field_d_idx'),
        ),
        migrations.AddIndex(
            model_name='fieldevent',
            index=models.Index(fields=['season'], name='myf_event_season_idx'),
        ),
        migrations.AddIndex(
            model_name='fieldevent',
            index=models.Index(fields=['event_type', '-event_date'], name='myf_event_type_d_idx'),
        ),

        # ─────────────────── FieldPhoto ───────────────────
        migrations.CreateModel(
            name='FieldPhoto',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('image', models.ImageField(upload_to='my_fields/photos/%Y/%m/', verbose_name='Файл')),
                ('taken_at', models.DateTimeField(blank=True, null=True, verbose_name='Снято')),
                ('geo_lat', models.FloatField(blank=True, null=True)),
                ('geo_lon', models.FloatField(blank=True, null=True)),
                ('caption', models.CharField(blank=True, default='', max_length=300, verbose_name='Подпись')),
                ('uploaded_at', models.DateTimeField(auto_now_add=True)),
                ('event', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='photos', to='my_fields.fieldevent', verbose_name='Событие')),
                ('field', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='photos', to='my_fields.userfield', verbose_name='Поле')),
                ('uploaded_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'Фото поля',
                'verbose_name_plural': 'Фото полей',
                'db_table': 'myf_field_photo',
                'ordering': ['-taken_at', '-uploaded_at'],
            },
        ),
        migrations.AddIndex(
            model_name='fieldphoto',
            index=models.Index(fields=['field', '-taken_at'], name='myf_photo_field_t_idx'),
        ),
        migrations.AddIndex(
            model_name='fieldphoto',
            index=models.Index(fields=['event'], name='myf_photo_event_idx'),
        ),
    ]
