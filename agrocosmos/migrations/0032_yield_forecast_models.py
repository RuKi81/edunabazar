"""Yield-forecasting schema (V1).

Adds four models that together support the «прогноз урожайности»
feature described in the chat plan of 28 May 2026:

* ``CropYieldStat``     — фактическая урожайность из Росстат/региональных МСХ/партнёров.
* ``YieldFeatures``      — снимок фичей (NDVI/фенология) для (scope × год × культура).
* ``YieldForecastModel`` — обученная модель (ridge regression) с коэффициентами в JSON.
* ``YieldForecast``      — конкретный прогноз с CI80, привязанный к модели.

All four support BOTH ``region``-scoped and ``district``-scoped rows
(exactly one of the two FKs must be set, enforced by CheckConstraint).
This is what enables the hybrid scheme: train on Rosstat regional
yields, then apply the same coefficients to per-district NDVI features
to get district-level forecasts.
"""
from django.db import migrations, models
import django.db.models.deletion


YIELD_CROP_CHOICES = [
    ('grains_total', 'Зерновые и зернобобовые (всего)'),
    ('wheat', 'Пшеница (все)'),
    ('wheat_winter', 'Пшеница озимая'),
    ('wheat_spring', 'Пшеница яровая'),
    ('barley', 'Ячмень'),
    ('corn_grain', 'Кукуруза на зерно'),
    ('sunflower', 'Подсолнечник'),
    ('soy', 'Соя'),
    ('rapeseed', 'Рапс'),
    ('sugar_beet', 'Сахарная свёкла'),
]


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0031_districtndvistatus_coverage'),
    ]

    operations = [
        # ── CropYieldStat ────────────────────────────────────────────
        migrations.CreateModel(
            name='CropYieldStat',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.PositiveSmallIntegerField(verbose_name='Год')),
                ('crop', models.CharField(choices=YIELD_CROP_CHOICES, max_length=20, verbose_name='Культура')),
                ('yield_t_per_ha', models.FloatField(
                    help_text='Хранится в т/га. ЕМИСС публикует в ц/га — конвертируется ÷10 при загрузке.',
                    verbose_name='Урожайность, т/га',
                )),
                ('area_ha', models.FloatField(blank=True, null=True, verbose_name='Убранная площадь, га')),
                ('gross_t', models.FloatField(blank=True, null=True, verbose_name='Валовой сбор, т')),
                ('source', models.CharField(
                    choices=[
                        ('emiss', 'ЕМИСС (Росстат)'),
                        ('regional_msx', 'Региональный Минсельхоз'),
                        ('manual', 'Ручная загрузка'),
                        ('partner', 'Партнёр (агрохолдинг/страховщик)'),
                    ],
                    max_length=30,
                    verbose_name='Источник',
                )),
                ('source_note', models.TextField(
                    blank=True, default='',
                    help_text='Напр. имя XLS-файла, ссылка на отчёт, дата выгрузки.',
                    verbose_name='Комментарий к источнику',
                )),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('district', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_stats',
                    to='agrocosmos.district',
                    verbose_name='Район (если данные районного уровня)',
                )),
                ('region', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_stats',
                    to='agrocosmos.region',
                    verbose_name='Субъект (если данные регионального уровня)',
                )),
            ],
            options={
                'db_table': 'agro_crop_yield_stat',
                'verbose_name': 'Урожайность (факт)',
                'verbose_name_plural': 'Урожайности (факт)',
                'ordering': ['-year', 'crop'],
            },
        ),
        migrations.AddConstraint(
            model_name='cropyieldstat',
            constraint=models.CheckConstraint(
                condition=(
                    models.Q(region__isnull=False, district__isnull=True)
                    | models.Q(region__isnull=True, district__isnull=False)
                ),
                name='cys_exactly_one_scope',
            ),
        ),
        migrations.AddConstraint(
            model_name='cropyieldstat',
            constraint=models.UniqueConstraint(
                condition=models.Q(district__isnull=True),
                fields=('region', 'year', 'crop', 'source'),
                name='cys_unique_region',
            ),
        ),
        migrations.AddConstraint(
            model_name='cropyieldstat',
            constraint=models.UniqueConstraint(
                condition=models.Q(region__isnull=True),
                fields=('district', 'year', 'crop', 'source'),
                name='cys_unique_district',
            ),
        ),
        migrations.AddIndex(
            model_name='cropyieldstat',
            index=models.Index(fields=['region', 'crop', 'year'], name='cys_region_crop_year_idx'),
        ),
        migrations.AddIndex(
            model_name='cropyieldstat',
            index=models.Index(fields=['district', 'crop', 'year'], name='cys_district_crop_year_idx'),
        ),

        # ── YieldFeatures ────────────────────────────────────────────
        migrations.CreateModel(
            name='YieldFeatures',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.PositiveSmallIntegerField()),
                ('crop', models.CharField(choices=YIELD_CROP_CHOICES, max_length=20)),
                ('features', models.JSONField(
                    help_text='{ "indvi_total": 12.3, "peak_ndvi": 0.78, ... }',
                    verbose_name='Фичи',
                )),
                ('feature_set_version', models.CharField(
                    default='v1',
                    help_text='При смене состава фичей увеличиваем — старые записи не теряем.',
                    max_length=20,
                    verbose_name='Версия набора фичей',
                )),
                ('season_complete', models.BooleanField(
                    default=False,
                    help_text='True после EOS — годится для обучения; False — только для прогноза.',
                    verbose_name='Сезон завершён',
                )),
                ('computed_at', models.DateTimeField(auto_now=True)),
                ('district', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_features',
                    to='agrocosmos.district',
                )),
                ('region', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_features',
                    to='agrocosmos.region',
                )),
            ],
            options={
                'db_table': 'agro_yield_features',
                'verbose_name': 'Фичи для прогноза урожайности',
                'verbose_name_plural': 'Фичи для прогноза урожайности',
            },
        ),
        migrations.AddConstraint(
            model_name='yieldfeatures',
            constraint=models.CheckConstraint(
                condition=(
                    models.Q(region__isnull=False, district__isnull=True)
                    | models.Q(region__isnull=True, district__isnull=False)
                ),
                name='yf_exactly_one_scope',
            ),
        ),
        migrations.AddConstraint(
            model_name='yieldfeatures',
            constraint=models.UniqueConstraint(
                condition=models.Q(district__isnull=True),
                fields=('region', 'year', 'crop', 'feature_set_version'),
                name='yf_unique_region',
            ),
        ),
        migrations.AddConstraint(
            model_name='yieldfeatures',
            constraint=models.UniqueConstraint(
                condition=models.Q(region__isnull=True),
                fields=('district', 'year', 'crop', 'feature_set_version'),
                name='yf_unique_district',
            ),
        ),
        migrations.AddIndex(
            model_name='yieldfeatures',
            index=models.Index(fields=['region', 'crop', 'year'], name='agro_yield__region__be041d_idx'),
        ),
        migrations.AddIndex(
            model_name='yieldfeatures',
            index=models.Index(fields=['district', 'crop', 'year'], name='agro_yield__distric_2c9d68_idx'),
        ),

        # ── YieldForecastModel ───────────────────────────────────────
        migrations.CreateModel(
            name='YieldForecastModel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('scope', models.CharField(
                    choices=[
                        ('national', 'Вся РФ'),
                        ('fed_okrug', 'Федеральный округ'),
                        ('region', 'Субъект РФ'),
                    ],
                    default='national', max_length=10,
                    verbose_name='Область применения',
                )),
                ('crop', models.CharField(choices=YIELD_CROP_CHOICES, max_length=20)),
                ('model_version', models.CharField(
                    default='ridge_v1',
                    help_text='ridge_v1 / rf_v1 / lgbm_v1 …',
                    max_length=30,
                )),
                ('coefficients', models.JSONField(
                    help_text='{"indvi_total": 0.412, "peak_ndvi": 1.23, ...}',
                    verbose_name='Коэффициенты β',
                )),
                ('intercept', models.FloatField(verbose_name='Свободный член α')),
                ('feature_names', models.JSONField(
                    help_text='Список имён фичей в том порядке, в котором подаются в модель.',
                    verbose_name='Порядок фичей',
                )),
                ('feature_scaler', models.JSONField(
                    help_text='{"means": {feat: μ}, "stds": {feat: σ}}',
                    verbose_name='Параметры StandardScaler',
                )),
                ('r2_train', models.FloatField(verbose_name='R² на обучении')),
                ('r2_cv', models.FloatField(verbose_name='R² leave-one-year-out CV')),
                ('rmse_cv', models.FloatField(verbose_name='RMSE CV, т/га')),
                ('rmse_pct', models.FloatField(
                    help_text='RMSE / mean(y_train) — для сравнения качества по культурам.',
                    verbose_name='RMSE % от средней урожайности',
                )),
                ('n_samples', models.PositiveIntegerField(verbose_name='Точек обучения')),
                ('train_years', models.JSONField(verbose_name='Годы обучающей выборки')),
                ('residuals_cv', models.JSONField(
                    help_text='Список (y_actual - y_pred) на leave-one-year-out — '
                              'используется для эмпирических квантилей при расчёте CI.',
                    verbose_name='Остатки CV',
                )),
                ('is_production', models.BooleanField(
                    default=False,
                    help_text='Только одна модель может быть production для пары (scope, region, crop).',
                    verbose_name='В продакшене',
                )),
                ('diagnostics', models.JSONField(
                    blank=True, null=True,
                    help_text='Полная диагностика: per-year errors, важности фичей, и т.д.',
                    verbose_name='Диагностика',
                )),
                ('trained_at', models.DateTimeField(auto_now_add=True)),
                ('region', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_models',
                    to='agrocosmos.region',
                    verbose_name='Регион (если scope=region)',
                )),
            ],
            options={
                'db_table': 'agro_yield_forecast_model',
                'verbose_name': 'Модель прогноза урожайности',
                'verbose_name_plural': 'Модели прогноза урожайности',
                'ordering': ['-trained_at'],
            },
        ),
        migrations.AddConstraint(
            model_name='yieldforecastmodel',
            constraint=models.CheckConstraint(
                condition=(
                    models.Q(scope='region', region__isnull=False)
                    | (~models.Q(scope='region') & models.Q(region__isnull=True))
                ),
                name='yfm_scope_region_consistent',
            ),
        ),
        migrations.AddIndex(
            model_name='yieldforecastmodel',
            index=models.Index(fields=['scope', 'crop', 'is_production'], name='yfm_lookup_idx'),
        ),
        migrations.AddIndex(
            model_name='yieldforecastmodel',
            index=models.Index(fields=['region', 'crop', 'is_production'], name='yfm_region_lookup_idx'),
        ),

        # ── YieldForecast ────────────────────────────────────────────
        migrations.CreateModel(
            name='YieldForecast',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.PositiveSmallIntegerField()),
                ('crop', models.CharField(choices=YIELD_CROP_CHOICES, max_length=20)),
                ('forecasted_at', models.DateField(verbose_name='Дата расчёта прогноза')),
                ('season_progress', models.FloatField(
                    default=0.0,
                    help_text='0 = до сева, 1 = после уборки. Эвристика по DOY.',
                    verbose_name='Прогресс сезона (0..1)',
                )),
                ('forecast_t_per_ha', models.FloatField(verbose_name='Прогноз, т/га')),
                ('ci_lower', models.FloatField(
                    help_text='10-й перцентиль из bootstrap-остатков модели.',
                    verbose_name='Нижняя граница CI80',
                )),
                ('ci_upper', models.FloatField(verbose_name='Верхняя граница CI80')),
                ('features_used', models.JSONField(
                    help_text='Точная копия фичей, на которых построен прогноз — для аудита.',
                    verbose_name='Снимок фичей',
                )),
                ('features_completeness', models.FloatField(
                    default=1.0,
                    help_text='Доля фичей, заполненных реальными данными (а не fallback-средним).',
                    verbose_name='Полнота фичей (0..1)',
                )),
                ('is_latest', models.BooleanField(
                    default=True,
                    help_text='Последний прогноз для (scope, year, crop). См. сервис forecast_yield.',
                )),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('district', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_forecasts',
                    to='agrocosmos.district',
                )),
                ('region', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='yield_forecasts',
                    to='agrocosmos.region',
                )),
                ('model', models.ForeignKey(
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name='forecasts',
                    to='agrocosmos.yieldforecastmodel',
                )),
            ],
            options={
                'db_table': 'agro_yield_forecast',
                'verbose_name': 'Прогноз урожайности',
                'verbose_name_plural': 'Прогнозы урожайности',
                'ordering': ['-forecasted_at'],
            },
        ),
        migrations.AddConstraint(
            model_name='yieldforecast',
            constraint=models.CheckConstraint(
                condition=(
                    models.Q(region__isnull=False, district__isnull=True)
                    | models.Q(region__isnull=True, district__isnull=False)
                ),
                name='yfc_exactly_one_scope',
            ),
        ),
        migrations.AddIndex(
            model_name='yieldforecast',
            index=models.Index(fields=['region', 'year', 'crop', 'is_latest'], name='yfc_region_latest_idx'),
        ),
        migrations.AddIndex(
            model_name='yieldforecast',
            index=models.Index(fields=['district', 'year', 'crop', 'is_latest'], name='yfc_district_latest_idx'),
        ),
        migrations.AddIndex(
            model_name='yieldforecast',
            index=models.Index(fields=['year', 'is_latest'], name='yfc_year_latest_idx'),
        ),
    ]
