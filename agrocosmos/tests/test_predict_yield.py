"""Тесты команды ``predict_yield`` — страховочная сетка перед
рефакторингом ``handle`` (C=17).

``model_predict`` мокается — математика прогноза покрыта тестами
сервиса; здесь проверяем оркестрацию: выбор модели, фильтры,
trivial-fallback, dry-run, ротацию ``is_latest``.
"""
import io
from unittest.mock import patch

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import (
    Region, YieldCrop, YieldFeatures, YieldForecast, YieldForecastModel,
)

CMD_MODULE = 'agrocosmos.management.commands.predict_yield'


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


def _pred(**overrides):
    pred = {
        'forecast_t_per_ha': 3.7, 'anomaly': 0.2, 'baseline': 3.5,
        'ci_lower': 3.2, 'ci_upper': 4.2,
    }
    pred.update(overrides)
    return pred


class PredictYieldTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(name='Регион А', code='ra', geom=_mpoly())
        cls.other = Region.objects.create(name='Регион Б', code='rb', geom=_mpoly())
        cls.model = cls._make_model(is_production=True)
        for r in (cls.region, cls.other):
            YieldFeatures.objects.create(
                region=r, district=None, year=2024,
                crop=YieldCrop.GRAINS_TOTAL,
                features={'peak_ndvi': 0.7}, feature_set_version='v1',
                season_complete=True,
            )

    @classmethod
    def _make_model(cls, **overrides):
        kwargs = dict(
            scope=YieldForecastModel.Scope.NATIONAL, region=None,
            crop=YieldCrop.GRAINS_TOTAL, model_version='ridge_v1',
            coefficients={'peak_ndvi': 0.2}, intercept=0.01,
            feature_names=['peak_ndvi'],
            feature_scaler={'means': {'peak_ndvi': 0.5},
                            'stds': {'peak_ndvi': 0.1}},
            r2_train=0.6, r2_cv=0.4, rmse_cv=0.45, rmse_pct=14.0,
            n_samples=60, train_years=[2019, 2020, 2021],
            residuals_cv=[0.1, -0.2], is_production=False,
            diagnostics={'regional_baselines': {}},
        )
        kwargs.update(overrides)
        return YieldForecastModel.objects.create(**kwargs)

    def _run(self, *args, pred=_pred()):
        out = io.StringIO()
        with patch(f'{CMD_MODULE}.model_predict',
                   return_value=pred) as mock_mp:
            call_command('predict_yield', '--year', '2024', *args, stdout=out)
        return out.getvalue(), mock_mp

    def test_predicts_and_persists_all_regions(self):
        out, mock_mp = self._run()
        self.assertEqual(mock_mp.call_count, 2)
        forecasts = YieldForecast.objects.filter(is_latest=True)
        self.assertEqual(forecasts.count(), 2)
        fc = forecasts.get(region=self.region)
        self.assertAlmostEqual(fc.forecast_t_per_ha, 3.7)
        self.assertAlmostEqual(fc.ci_lower, 3.2)
        self.assertEqual(fc.model_id, self.model.pk)
        self.assertEqual(fc.features_used, {'peak_ndvi': 0.7})
        self.assertIn('Сохранено прогнозов: 2', out)

    def test_rerun_rotates_is_latest(self):
        self._run()
        out, _ = self._run(pred=_pred(forecast_t_per_ha=4.0))
        self.assertEqual(YieldForecast.objects.count(), 4)
        latest = YieldForecast.objects.filter(
            region=self.region, is_latest=True,
        ).get()
        self.assertAlmostEqual(latest.forecast_t_per_ha, 4.0)
        self.assertIn('заменено предыдущих is_latest: 2', out)

    def test_dry_run_writes_nothing(self):
        out, _ = self._run('--dry-run', '--verbose')
        self.assertFalse(YieldForecast.objects.exists())
        self.assertIn('Dry-run', out)
        self.assertIn('Прогнозов сделано: 2', out)

    def test_region_filter(self):
        out, mock_mp = self._run('--region', 'Регион А')
        self.assertEqual(mock_mp.call_count, 1)
        self.assertEqual(YieldForecast.objects.get().region_id, self.region.pk)

    def test_unknown_region(self):
        out, mock_mp = self._run('--region', 'Нет такого')
        self.assertIn('не найден', out)
        mock_mp.assert_not_called()

    def test_no_production_model(self):
        self.model.is_production = False
        self.model.save(update_fields=['is_production'])
        out, mock_mp = self._run()
        self.assertIn('PRODUCTION-модель', out)
        mock_mp.assert_not_called()

    def test_explicit_model_id(self):
        other_model = self._make_model()
        out, _ = self._run('--model-id', str(other_model.pk))
        self.assertEqual(
            set(YieldForecast.objects.values_list('model_id', flat=True)),
            {other_model.pk},
        )

    def test_unknown_model_id(self):
        out, mock_mp = self._run('--model-id', '999999')
        self.assertIn('не найдена', out)
        mock_mp.assert_not_called()

    def test_skips_none_predictions(self):
        out, _ = self._run('--verbose', pred=None)
        self.assertFalse(YieldForecast.objects.exists())
        self.assertIn('нет baseline', out)
        self.assertIn('Прогнозов сделано: 0', out)

    def test_trivial_fallback_without_features(self):
        YieldFeatures.objects.all().delete()
        self.model.feature_names = []
        self.model.diagnostics = {
            'regional_baselines': {str(self.region.pk): 3.5},
        }
        self.model.save(update_fields=['feature_names', 'diagnostics'])
        out, mock_mp = self._run()
        self.assertIn('trivial-fallback', out)
        self.assertEqual(mock_mp.call_count, 1)
        fc = YieldForecast.objects.get()
        self.assertEqual(fc.region_id, self.region.pk)
        self.assertEqual(fc.features_used, {})

    def test_no_features_no_fallback_for_ridge(self):
        YieldFeatures.objects.all().delete()
        out, mock_mp = self._run()
        mock_mp.assert_not_called()
        self.assertFalse(YieldForecast.objects.exists())
