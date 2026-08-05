"""Тесты команды ``train_yield_model`` — страховочная сетка перед
рефакторингом ``handle`` (C=12).

Сервисные функции (``prepare_training_data``, ``train_full_model``,
``train_trivial_model``) мокаются — ML-математика покрыта тестами
сервиса; здесь проверяем оркестрацию: guard на размер выборки,
dry-run, сохранение и активацию модели.
"""
import io
from unittest.mock import patch

import numpy as np
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import YieldCrop, YieldForecastModel

CMD_MODULE = 'agrocosmos.management.commands.train_yield_model'


def _data(n=60):
    return {
        'y': np.linspace(-1, 1, n),
        'years': np.array([2019 + (i % 5) for i in range(n)]),
        'region_ids': np.array([i % 10 for i in range(n)]),
    }


def _result(**overrides):
    res = {
        'alpha': 1.0,
        'alpha_grid_scores': {0.1: 0.5, 1.0: 0.4},
        'r2_train': 0.6, 'rmse_train': 0.35,
        'r2_cv': 0.4, 'rmse_cv': 0.45, 'mae_cv': 0.35, 'rmse_pct': 14.2,
        'per_year_cv': {2020: {'n': 12, 'rmse': 0.4, 'mae': 0.3}},
        'feature_names': ['peak_ndvi', 'indvi_total'],
        'coefficients': {'peak_ndvi': 0.2, 'indvi_total': 0.1},
        'intercept': 0.01,
        'feature_scaler': {'mean': [0.5, 30.0], 'std': [0.1, 5.0]},
        'n_samples': 60,
        'train_years': [2019, 2020, 2021, 2022, 2023],
        'residuals_cv': [0.1, -0.2],
        'regional_baselines': {'1': 3.2},
    }
    res.update(overrides)
    return res


class TrainYieldModelTests(TestCase):

    def _run(self, *args, data=None, result=None):
        out = io.StringIO()
        with patch(f'{CMD_MODULE}.compute_regional_baselines',
                   return_value={1: 3.2}), \
             patch(f'{CMD_MODULE}.prepare_training_data',
                   return_value=data if data is not None else _data()), \
             patch(f'{CMD_MODULE}.train_full_model',
                   return_value=result or _result()) as mock_full, \
             patch(f'{CMD_MODULE}.train_trivial_model',
                   return_value=result or _result(
                       feature_names=[], coefficients={},
                       alpha=None, alpha_grid_scores={},
                   )) as mock_trivial:
            call_command('train_yield_model', *args, stdout=out)
        return out.getvalue(), mock_full, mock_trivial

    def test_too_few_samples_aborts(self):
        out, mock_full, _ = self._run(data=_data(n=10))
        self.assertIn('Слишком мало точек обучения', out)
        mock_full.assert_not_called()
        self.assertFalse(YieldForecastModel.objects.exists())

    def test_dry_run_prints_metrics_without_saving(self):
        out, mock_full, _ = self._run('--dry-run')
        mock_full.assert_called_once()
        self.assertIn('R²_cv', out)
        self.assertIn('Dry-run', out)
        self.assertFalse(YieldForecastModel.objects.exists())

    def test_save_default_not_production(self):
        out, _, _ = self._run()
        model = YieldForecastModel.objects.get()
        self.assertFalse(model.is_production)
        self.assertEqual(model.model_version, 'ridge_v1')
        self.assertEqual(model.crop, YieldCrop.GRAINS_TOTAL)
        self.assertAlmostEqual(model.r2_cv, 0.4)
        self.assertEqual(model.diagnostics['feature_set_version'], 'v1')
        self.assertIn('Модель сохранена', out)
        self.assertIn('Чтобы активировать', out)

    def test_activate_deactivates_previous(self):
        self._run('--activate')
        first = YieldForecastModel.objects.get()
        self.assertTrue(first.is_production)

        out, _, _ = self._run('--activate')
        first.refresh_from_db()
        self.assertFalse(first.is_production)
        current = YieldForecastModel.objects.get(is_production=True)
        self.assertNotEqual(current.pk, first.pk)
        self.assertIn('[PRODUCTION]', out)

    def test_trivial_uses_trivial_trainer(self):
        out, mock_full, mock_trivial = self._run('--trivial')
        mock_trivial.assert_called_once()
        mock_full.assert_not_called()
        model = YieldForecastModel.objects.get()
        self.assertEqual(model.model_version, 'trivial_v1')
        self.assertIn('Фичи не используются', out)

    def test_alpha_grid_report(self):
        out, _, _ = self._run('--dry-run')
        self.assertIn('grid search', out)
        self.assertIn('← best', out)
