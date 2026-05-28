"""Обучение модели прогноза урожайности.

Идёт по всем (region × year) парам, где есть и YieldFeatures, и
yield-факт в CropYieldStat. Обучает Ridge-регрессию на аномалиях,
делает leave-one-year-out CV, сохраняет в YieldForecastModel.

По умолчанию SAVE-режим (запись в БД + активация). Используйте
``--dry-run`` чтобы посмотреть метрики без сохранения.

Примеры:
    # Обучить модель grains_total с автоподбором α
    python manage.py train_yield_model

    # Конкретная α
    python manage.py train_yield_model --alpha 1.0

    # Только показать метрики, ничего не писать
    python manage.py train_yield_model --dry-run

    # Обучить и СРАЗУ активировать (deactivate предыдущую production)
    python manage.py train_yield_model --activate

    # Кастомный alpha-grid
    python manage.py train_yield_model --alpha-grid 0.1 1.0 10.0
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from agrocosmos.models import YieldCrop, YieldForecastModel
from agrocosmos.services.yield_model import (
    compute_regional_baselines, prepare_training_data,
    train_full_model, train_trivial_model,
)


MODEL_VERSION_RIDGE = 'ridge_v1'
MODEL_VERSION_TRIVIAL = 'trivial_v1'


class Command(BaseCommand):
    help = 'Обучить Ridge-модель прогноза урожайности по NDVI-фичам'

    def add_arguments(self, parser):
        parser.add_argument(
            '--crop', default=YieldCrop.GRAINS_TOTAL,
            choices=[c.value for c in YieldCrop],
        )
        parser.add_argument(
            '--feature-set-version', default='v1',
            help='Версия фичей в YieldFeatures (default: v1)',
        )
        parser.add_argument(
            '--alpha', type=float, default=None,
            help='Конкретное значение α; если не задано — grid search',
        )
        parser.add_argument(
            '--alpha-grid', nargs='+', type=float, default=None,
            help='Список α для перебора (default: 0.01 0.1 1 3 10 30 100)',
        )
        parser.add_argument(
            '--trivial', action='store_true',
            help='Trivial baseline без NDVI-фичей (forecast = regional trend). '
                 'Используется как honest baseline, когда NDVI-сигнал слаб.',
        )
        parser.add_argument(
            '--activate', action='store_true',
            help='Сделать новую модель PRODUCTION (предыдущую снять с прода)',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Только посчитать и вывести метрики; в БД ничего не писать.',
        )

    def handle(self, *args, **opts):
        crop = opts['crop']
        version = opts['feature_set_version']

        model_version = MODEL_VERSION_TRIVIAL if opts['trivial'] else MODEL_VERSION_RIDGE
        self.stdout.write(self.style.MIGRATE_HEADING(
            f'═══ Обучение {model_version} / {crop} (features={version}) ═══'
        ))

        # ── Подготовка данных ───────────────────────────────────────
        baselines = compute_regional_baselines(crop)
        self.stdout.write(
            f'Регионов с надёжным baseline (≥5 лет факт-данных): {len(baselines)}'
        )

        data = prepare_training_data(
            crop=crop, feature_set_version=version, baselines=baselines,
        )
        n = len(data['y'])
        if n < 50:
            self.stdout.write(self.style.ERROR(
                f'Слишком мало точек обучения: {n}. Нужно ≥ 50.'
            ))
            return

        years = sorted(set(data['years'].tolist()))
        self.stdout.write(
            f'Точек обучения: {n}\n'
            f'Регионов в выборке: {len(set(data["region_ids"].tolist()))}\n'
            f'Годы: {years}\n'
        )

        # Описательная статистика таргета (аномалии).
        y = data['y']
        self.stdout.write(
            f'Аномалия y = yield − baseline:\n'
            f'  mean: {y.mean():+.3f} т/га   (≈0 если baseline корректен)\n'
            f'  std:  {y.std():.3f} т/га\n'
            f'  диапазон: [{y.min():+.2f}, {y.max():+.2f}]\n'
        )

        # ── Обучение ────────────────────────────────────────────────
        if opts['trivial']:
            result = train_trivial_model(data)
        else:
            result = train_full_model(
                data, alpha=opts['alpha'], alpha_grid=opts['alpha_grid'],
            )

        self.stdout.write(self.style.MIGRATE_HEADING('═══ Результат ═══'))
        if result.get('alpha_grid_scores'):
            self.stdout.write('  α grid search (LOYO RMSE на аномалиях):')
            for a, rmse in sorted(result['alpha_grid_scores'].items()):
                marker = ' ← best' if a == result['alpha'] else ''
                self.stdout.write(f'    α={a:>7.3f}  rmse={rmse:.4f}{marker}')
        if result.get('alpha') is not None:
            self.stdout.write(f'  Лучший α: {result["alpha"]}')
        self.stdout.write('')
        self.stdout.write('  Метрики на обучении:')
        self.stdout.write(f'    R²_train  = {result["r2_train"]:.3f}')
        self.stdout.write(f'    RMSE_train= {result["rmse_train"]:.3f} т/га (на аномалии)')
        self.stdout.write('')
        self.stdout.write('  Метрики LOYO CV:')
        self.stdout.write(f'    R²_cv     = {result["r2_cv"]:.3f}')
        self.stdout.write(f'    RMSE_cv   = {result["rmse_cv"]:.3f} т/га (на аномалии)')
        self.stdout.write(f'    MAE_cv    = {result["mae_cv"]:.3f} т/га')
        self.stdout.write(f'    RMSE %    = {result["rmse_pct"]:.1f} % от средн. урожайности')
        self.stdout.write('')
        self.stdout.write('  Per-year CV:')
        for yr, m in sorted(result['per_year_cv'].items()):
            self.stdout.write(
                f'    {yr}: n={m["n"]:3d}  rmse={m["rmse"]:.3f}  mae={m["mae"]:.3f}'
            )
        self.stdout.write('')
        if result['feature_names']:
            self.stdout.write('  Коэффициенты (на стандартизованных фичах):')
            for name in result['feature_names']:
                self.stdout.write(
                    f'    {name:>20s} : {result["coefficients"][name]:+.4f}'
                )
            self.stdout.write(f'    {"intercept":>20s} : {result["intercept"]:+.4f}')
        else:
            self.stdout.write('  Фичи не используются (trivial baseline).')

        # ── Сохранение ──────────────────────────────────────────────
        if opts['dry_run']:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING('Dry-run: модель не сохранена.'))
            return

        with transaction.atomic():
            if opts['activate']:
                # Снимаем PRODUCTION с предыдущих моделей этой пары.
                YieldForecastModel.objects.filter(
                    scope=YieldForecastModel.Scope.NATIONAL,
                    region__isnull=True,
                    crop=crop,
                    is_production=True,
                ).update(is_production=False)

            model = YieldForecastModel.objects.create(
                scope=YieldForecastModel.Scope.NATIONAL,
                region=None,
                crop=crop,
                model_version=model_version,
                coefficients=result['coefficients'],
                intercept=result['intercept'],
                feature_names=result['feature_names'],
                feature_scaler=result['feature_scaler'],
                r2_train=result['r2_train'],
                r2_cv=result['r2_cv'],
                rmse_cv=result['rmse_cv'],
                rmse_pct=result['rmse_pct'],
                n_samples=result['n_samples'],
                train_years=result['train_years'],
                residuals_cv=result['residuals_cv'],
                is_production=opts['activate'],
                diagnostics={
                    'alpha': result['alpha'],
                    'alpha_grid_scores': result['alpha_grid_scores'],
                    'mae_cv': result['mae_cv'],
                    'rmse_train': result['rmse_train'],
                    'per_year_cv': result['per_year_cv'],
                    'feature_set_version': version,
                    'regional_baselines': result['regional_baselines'],
                },
            )

        self.stdout.write('')
        flag = ' [PRODUCTION]' if opts['activate'] else ''
        self.stdout.write(self.style.SUCCESS(
            f'✓ Модель сохранена: id={model.id}, R²_cv={result["r2_cv"]:.3f}{flag}'
        ))
        if not opts['activate']:
            self.stdout.write(
                f'  Чтобы активировать: --activate при следующем запуске, '
                f'либо вручную через admin.'
            )
