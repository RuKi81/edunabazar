"""Тесты обучаемой модели класса угодья (``services/season_model.py``).

Проверяется то, из-за чего пороговый классификатор и был признан
тупиковым:

* вектор признаков имеет ПОСТОЯННУЮ длину при любых пропусках в ряду
  (облачность не должна ломать размерность);
* логрегрессия обучается и разделяет то, что разделимо;
* оценка идёт по ОТЛОЖЕННЫМ ГРУППАМ и не завышается, когда внутри
  группы контуры почти дублируют друг друга — это главная ловушка
  валидации на полевых данных.

Тесты без БД (``SimpleTestCase``): матчасть чистая.
"""
import numpy as np
from django.test import SimpleTestCase

from agrocosmos.services.season_model import (
    FEATURE_SET_COVER, FEATURE_SET_SEASON, L2_GRID, MIN_GROUPS_FOR_CV,
    PROFILE_BINS, SEASON_FEATURE_NAMES, binary_metrics, class_weights,
    cover_feature_names, feature_importance, features_cover, features_season,
    fit_impute, fit_logistic, group_cv, predict_row, profile_bins,
    profile_coverage, search_best_l2, train_model, vectorize,
)


def _series(peak_doy, step=8, amplitude=0.6, floor=0.15):
    """Синтетический годовой ряд с пиком в ``peak_doy`` (треугольник)."""
    doys = list(range(1, 366, step))
    vals = [floor + amplitude * max(0.0, 1.0 - abs(d - peak_doy) / 90.0)
            for d in doys]
    return doys, vals


class ProfileFeatureTests(SimpleTestCase):
    """Вектор признаков: длина, пропуски, состав."""

    def test_profile_bins_has_fixed_length(self):
        doys, vals = _series(180)
        bins = profile_bins(doys, vals)
        self.assertEqual(bins.shape, (PROFILE_BINS,))

    def test_profile_bins_interpolates_gaps(self):
        """Пропуск в середине года заполняется интерполяцией, а не нулём."""
        doys = [10, 20, 30, 40, 300, 310]
        vals = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
        bins = profile_bins(doys, vals)
        self.assertEqual(bins.shape, (PROFILE_BINS,))
        self.assertTrue(np.all(bins > 0.4))

    def test_profile_bins_none_for_short_series(self):
        self.assertIsNone(profile_bins([1, 2], [0.4, 0.5]))

    def test_profile_coverage_marks_only_observed_bins(self):
        mask = profile_coverage([10, 12])
        self.assertEqual(mask[0], 1.0)
        self.assertEqual(mask.sum(), 1.0)
        self.assertEqual(profile_coverage([10, 40]).sum(), 2.0)

    def test_features_cover_matches_declared_names(self):
        doys, vals = _series(180)
        feats = features_cover(doys, vals)
        self.assertEqual(sorted(feats), sorted(cover_feature_names()))

    def test_features_cover_none_for_short_series(self):
        self.assertIsNone(features_cover([1, 2], [0.4, 0.5]))

    def test_features_season_flags_missing_previous_autumn(self):
        doys, vals = _series(150)
        feats = features_season(doys, vals)
        self.assertEqual(sorted(feats), sorted(SEASON_FEATURE_NAMES))
        self.assertEqual(feats['autumn_prev_missing'], 1.0)
        self.assertIsNone(feats['autumn_prev'])

    def test_features_season_uses_previous_autumn_when_given(self):
        doys, vals = _series(150)
        prev_doys, prev_vals = _series(280)
        feats = features_season(doys, vals, prev_doys, prev_vals)
        self.assertEqual(feats['autumn_prev_missing'], 0.0)
        self.assertGreater(feats['autumn_prev'], 0.3)

    def test_features_season_peak_doy_is_early_for_winter_profile(self):
        doys, vals = _series(150)
        self.assertLess(features_season(doys, vals)['peak_doy'], 185)


class MatrixTests(SimpleTestCase):
    """Подстановка пропусков и сборка матрицы."""

    def test_fit_impute_uses_median_of_known_values(self):
        rows = [{'a': 1.0}, {'a': 3.0}, {'a': None}]
        self.assertEqual(fit_impute(rows, ['a']), {'a': 2.0})

    def test_fit_impute_zero_when_feature_never_observed(self):
        self.assertEqual(fit_impute([{'a': None}], ['a']), {'a': 0.0})

    def test_vectorize_fills_none_with_impute(self):
        X = vectorize([{'a': None}, {'a': 5.0}], ['a'], {'a': 2.0})
        self.assertEqual(X.tolist(), [[2.0], [5.0]])


class LogisticTests(SimpleTestCase):
    """Обучение логрегрессии."""

    def test_class_weights_equalize_contribution(self):
        y = np.array([1.0, 0.0, 0.0, 0.0])
        w = class_weights(y)
        self.assertAlmostEqual(float(w[y == 1].sum()), float(w[y == 0].sum()))

    def test_fit_logistic_separates_separable_data(self):
        X = np.array([[-2.0], [-1.0], [1.0], [2.0]])
        y = np.array([0.0, 0.0, 1.0, 1.0])
        beta, intercept = fit_logistic(X, y, l2=1.0)
        self.assertGreater(beta[0], 0.0)
        self.assertLess(abs(intercept), 1.0)

    def test_fit_logistic_survives_perfectly_separable_data(self):
        """Разделимая выборка не должна взрывать веса (L2 их держит)."""
        X = np.array([[-5.0], [-5.0], [5.0], [5.0]])
        y = np.array([0.0, 0.0, 1.0, 1.0])
        beta, _ = fit_logistic(X, y, l2=1.0)
        self.assertTrue(np.isfinite(beta).all())

    def test_binary_metrics_balanced_ignores_class_imbalance(self):
        y = np.array([1.0] + [0.0] * 9)
        always_negative = np.zeros(10)
        metrics = binary_metrics(y, always_negative)
        self.assertEqual(metrics['accuracy'], 0.9)
        self.assertEqual(metrics['balanced'], 0.5)


def _grouped_dataset(n_groups=5, per_group=8, separable=True, seed=0):
    """Датасет с группами: ``(rows, y, groups)``.

    Внутри группы объекты почти идентичны — имитация контуров одного
    массива, из-за которых случайный k-fold и врёт.
    """
    rng = np.random.default_rng(seed)
    rows, y, groups = [], [], []
    for g in range(n_groups):
        for i in range(per_group):
            label = float(i % 2)
            if separable:
                value = 1.0 + 0.05 * rng.normal() if label else 0.05 * rng.normal()
            else:
                value = float(rng.normal())
            rows.append({'x': value})
            y.append(label)
            groups.append(f'd{g}')
    return rows, y, groups


class GroupCvTests(SimpleTestCase):
    """Оценка по отложенным группам."""

    def test_none_when_too_few_groups(self):
        rows, y, groups = _grouped_dataset(n_groups=MIN_GROUPS_FOR_CV - 1)
        self.assertIsNone(group_cv(rows, y, groups, ['x'], l2=1.0))

    def test_separable_feature_scores_high(self):
        rows, y, groups = _grouped_dataset(separable=True)
        cv = group_cv(rows, y, groups, ['x'], l2=1.0)
        self.assertEqual(cv['scheme'], 'leave-one-group-out')
        self.assertEqual(cv['n_groups'], 5)
        self.assertGreater(cv['balanced'], 0.9)

    def test_noise_feature_collapses_to_coin_flip(self):
        """Главное свойство: на шуме оценка НЕ завышается."""
        rows, y, groups = _grouped_dataset(separable=False, seed=7)
        cv = group_cv(rows, y, groups, ['x'], l2=1.0)
        self.assertLess(cv['balanced'], 0.75)

    def test_per_group_report_covers_every_scored_group(self):
        rows, y, groups = _grouped_dataset()
        cv = group_cv(rows, y, groups, ['x'], l2=1.0)
        self.assertEqual(sorted(cv['per_group']),
                         [f'd{i}' for i in range(5)])

    def test_search_best_l2_returns_grid_member(self):
        rows, y, groups = _grouped_dataset()
        best, scores = search_best_l2(rows, y, groups, ['x'])
        self.assertIn(best, L2_GRID)
        self.assertEqual(sorted(scores), sorted(float(v) for v in L2_GRID))

    def test_search_best_l2_none_without_cv(self):
        rows, y, groups = _grouped_dataset(n_groups=1)
        self.assertEqual(search_best_l2(rows, y, groups, ['x']), (None, {}))


class TrainModelTests(SimpleTestCase):
    """Сквозное обучение ступени и применение модели."""

    def test_train_model_returns_state_and_cv(self):
        rows, y, groups = _grouped_dataset()
        state, cv, scores = train_model(
            rows, y, groups, ['x'], FEATURE_SET_COVER, 'crop', 'unused')
        self.assertEqual(state['feature_set'], FEATURE_SET_COVER)
        self.assertEqual(state['names'], ['x'])
        self.assertEqual(state['n_train'], len(rows))
        self.assertGreater(cv['balanced'], 0.9)
        self.assertTrue(scores)

    def test_train_model_requires_both_classes(self):
        rows = [{'x': 1.0}, {'x': 2.0}]
        with self.assertRaises(ValueError):
            train_model(rows, [1.0, 1.0], ['a', 'b'], ['x'],
                        FEATURE_SET_SEASON, 'winter', 'spring')

    def test_train_model_without_cv_still_trains(self):
        """Групп меньше минимума: модель обучается, но оценки нет."""
        rows, y, groups = _grouped_dataset(n_groups=1, per_group=10)
        state, cv, scores = train_model(
            rows, y, groups, ['x'], FEATURE_SET_COVER, 'crop', 'unused')
        self.assertIsNone(cv)
        self.assertEqual(scores, {})
        self.assertTrue(np.isfinite(state['beta']).all())

    def test_predict_row_labels_by_threshold(self):
        rows, y, groups = _grouped_dataset()
        state, _cv, _scores = train_model(
            rows, y, groups, ['x'], FEATURE_SET_COVER, 'crop', 'unused')
        prob_hi, label_hi = predict_row({'x': 1.0}, state)
        prob_lo, label_lo = predict_row({'x': 0.0}, state)
        self.assertGreater(prob_hi, prob_lo)
        self.assertEqual(label_hi, 'crop')
        self.assertEqual(label_lo, 'unused')

    def test_predict_row_handles_missing_feature(self):
        """Пропуск в признаке не должен ломать применение модели."""
        rows, y, groups = _grouped_dataset()
        state, _cv, _scores = train_model(
            rows, y, groups, ['x'], FEATURE_SET_COVER, 'crop', 'unused')
        prob, label = predict_row({'x': None}, state)
        self.assertTrue(0.0 <= prob <= 1.0)
        self.assertIn(label, ('crop', 'unused'))

    def test_feature_importance_sorted_by_absolute_weight(self):
        state = {'names': ['a', 'b', 'c'], 'beta': [0.1, -2.0, 1.0]}
        self.assertEqual([n for n, _w in feature_importance(state)],
                         ['b', 'c', 'a'])
