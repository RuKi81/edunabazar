"""Тесты подбора порогов классификатора сезона.

1. Чистая функция ``sweep_threshold`` — профиль качества по сетке порогов.
2. Команда ``tune_season_thresholds`` — развёртки по ручным меткам и
   гистограмма дня пика по всем угодьям (диагностика бимодальности).

Синтетические профили NDVI переиспользуются из ``test_winter_spring``:
озимые дают ранний пик, яровые — поздний, залежь — низкую долю зелени.
"""
import re
from datetime import date, timedelta
from io import StringIO

import numpy as np

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase

from agrocosmos.models import (
    District, Farmland, FarmlandCropSeason, FarmlandTrainingLabel, Region,
    SatelliteScene, VegetationIndex,
)
from agrocosmos.services.winter_spring import (
    best_split, cv_balanced, feature_grid, sweep_threshold,
)

from .test_winter_spring import (
    YEAR, _profile, _spring_ndvi, _square, _unused_cover_ndvi, _winter_ndvi,
)


class SweepThresholdTests(SimpleTestCase):
    """Профиль качества порога: обе стороны, крайние случаи."""

    def test_below_side_perfect_separation(self):
        # Положительный класс НИЖЕ порога (день пика у озимых).
        points = sweep_threshold([150, 155], [200, 205], [175], 'below')
        self.assertEqual(len(points), 1)
        p = points[0]
        self.assertEqual((p.pos_recall, p.neg_recall), (1.0, 1.0))
        self.assertEqual(p.balanced, 1.0)
        self.assertEqual(p.accuracy, 1.0)
        self.assertEqual((p.n_pos, p.n_neg), (2, 2))

    def test_above_side_and_grid_order(self):
        # Положительный класс ВЫШЕ порога (доля зелени у культур).
        points = sweep_threshold([0.5, 0.6], [0.1, 0.2], [0.05, 0.35, 0.9],
                                 'above')
        self.assertEqual([p.threshold for p in points], [0.05, 0.35, 0.9])
        # 0.05: все выше порога → культуры верно, залежь нет.
        self.assertEqual((points[0].pos_recall, points[0].neg_recall),
                         (1.0, 0.0))
        self.assertEqual(points[1].balanced, 1.0)      # 0.35 разделяет
        self.assertEqual((points[2].pos_recall, points[2].neg_recall),
                         (0.0, 1.0))

    def test_accuracy_weighted_by_class_size(self):
        """Точность взвешена размерами классов, баланс — нет."""
        points = sweep_threshold([150] * 9, [100], [175], 'below')
        p = points[0]
        self.assertEqual(p.pos_recall, 1.0)
        self.assertEqual(p.neg_recall, 0.0)     # 100 < 175 → ошибка
        self.assertEqual(p.balanced, 0.5)
        self.assertAlmostEqual(p.accuracy, 0.9)

    def test_none_values_dropped_and_empty_class(self):
        points = sweep_threshold([150, None], [], [175], 'below')
        self.assertEqual(points[0].n_pos, 1)
        self.assertEqual(points[0].n_neg, 0)
        self.assertEqual(points[0].neg_recall, 0.0)

    def test_invalid_side_rejected(self):
        with self.assertRaises(ValueError):
            sweep_threshold([1], [2], [1.5], 'sideways')


class FeatureGridAndBestSplitTests(SimpleTestCase):
    """Сетка кандидатов по выборке и лучший разрез с автовыбором стороны."""

    def test_grid_covers_range_and_dedups(self):
        grid = feature_grid([0.1, 0.1, 0.9])
        self.assertGreater(len(grid), 1)
        self.assertGreaterEqual(min(grid), 0.1)
        self.assertLessEqual(max(grid), 0.9)
        self.assertEqual(len(grid), len(set(grid)))

    def test_grid_edge_cases(self):
        self.assertEqual(feature_grid([]), [])
        self.assertEqual(feature_grid([None]), [])
        self.assertEqual(feature_grid([0.42]), [0.42])

    def test_best_split_detects_side_below(self):
        point, side = best_split([150, 152], [200, 202])
        self.assertEqual(side, 'below')
        self.assertEqual(point.balanced, 1.0)

    def test_best_split_detects_side_above(self):
        point, side = best_split([0.8, 0.9], [0.1, 0.2])
        self.assertEqual(side, 'above')
        self.assertEqual(point.balanced, 1.0)

    def test_best_split_reports_coin_flip_for_identical_classes(self):
        """Неразделимые классы → баланс около 0.5, а не ложный оптимум."""
        point, _side = best_split([1, 2, 3, 4], [1, 2, 3, 4])
        self.assertLess(point.balanced, 0.7)

    def test_best_split_requires_both_classes(self):
        self.assertEqual(best_split([1, 2], []), (None, None))
        self.assertEqual(best_split([], [1, 2]), (None, None))


class CvBalancedTests(SimpleTestCase):
    """Кросс-валидация отделяет реальный признак от подгонки под шум."""

    def test_separable_feature_survives_cv(self):
        pos = [0.1 + 0.01 * i for i in range(20)]
        neg = [0.9 + 0.01 * i for i in range(20)]
        self.assertEqual(cv_balanced(pos, neg), 1.0)

    def test_noise_feature_collapses_to_coin_flip(self):
        """Главное свойство: in-sample оптимум на шуме высок, CV — нет.

        Две выборки из ОДНОГО распределения — разделять нечего, но
        порог, подобранный по ним же, даёт заметно больше 0.5.
        """
        rng = np.random.default_rng(42)
        pos = list(rng.normal(size=12))
        neg = list(rng.normal(size=12))
        in_sample, _side = best_split(pos, neg)
        cv = cv_balanced(pos, neg)
        self.assertGreater(in_sample.balanced, 0.60)
        self.assertLess(cv, in_sample.balanced)
        self.assertLess(cv, 0.60)

    def test_quantized_ties_do_not_fake_separation(self):
        """Квантованный признак (тип зимнего baseline под снегом)."""
        pos = [0.06, 0.061, 0.062] * 12
        neg = [0.06, 0.061, 0.062] * 14
        self.assertLess(cv_balanced(pos, neg), 0.60)

    def test_none_when_sample_smaller_than_folds(self):
        self.assertIsNone(cv_balanced([1, 2], [3, 4], folds=5))

    def test_nones_dropped_before_split(self):
        pos = [0.1] * 5 + [None] * 3
        neg = [0.9] * 5
        self.assertEqual(cv_balanced(pos, neg, folds=5), 1.0)

    def test_folds_below_two_rejected(self):
        with self.assertRaises(ValueError):
            cv_balanced([1] * 5, [2] * 5, folds=1)

    def test_deterministic_for_same_seed(self):
        rng = np.random.default_rng(7)
        pos, neg = list(rng.normal(size=30)), list(rng.normal(size=30))
        self.assertEqual(cv_balanced(pos, neg, seed=3),
                         cv_balanced(pos, neg, seed=3))


class TuneSeasonThresholdsCommandTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50, 5),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50, 5),
        )
        cls.winter = [cls._farmland(30.1 + i * 0.1) for i in range(2)]
        cls.spring = [cls._farmland(31.1 + i * 0.1) for i in range(2)]
        cls.unused = [cls._farmland(32.1 + i * 0.1) for i in range(2)]

        for fl in cls.winter:
            cls._series(fl, _winter_ndvi)
            cls._label(fl, 'winter')
        for fl in cls.spring:
            cls._series(fl, _spring_ndvi)
            cls._label(fl, 'spring')
        for fl in cls.unused:
            cls._series(fl, _unused_cover_ndvi)
            cls._label(fl, 'unused')

    # -- helpers --
    @classmethod
    def _farmland(cls, x):
        return Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(x, 50.2),
        )

    @classmethod
    def _series(cls, fl, fn):
        for _doy, val, d in zip(*_profile(fn)):
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=f'sentinel2_{d}',
                defaults={'satellite': 'sentinel2', 'acquired_date': d},
            )
            VegetationIndex.objects.create(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=d, mean=val,
            )

    @classmethod
    def _prev_series(cls, fl, autumn_value):
        """Короткий MODIS-ряд ЗА ПРОШЛЫЙ год в осеннем окне (DOY 258-305)."""
        for day in (20, 30, 40, 50):   # середина сентября — начало ноября
            d = date(YEAR - 1, 9, 1) + timedelta(days=day)
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=f'modis_terra_{d}',
                defaults={'satellite': 'modis_terra', 'acquired_date': d},
            )
            VegetationIndex.objects.create(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=d, mean=autumn_value,
            )

    @classmethod
    def _label(cls, fl, true_class):
        FarmlandTrainingLabel.objects.create(
            farmland=fl, year=YEAR, true_class=true_class,
        )

    def _run(self, **kwargs):
        out = StringIO()
        kwargs.setdefault('region_id', self.region.pk)
        kwargs.setdefault('year', YEAR)
        call_command('tune_season_thresholds', stdout=out, stderr=out, **kwargs)
        return out.getvalue()

    # -- tests --
    def test_peak_sweep_finds_separating_threshold(self):
        """Синтетика разделима → у оптимума баланс 1.0."""
        out = self._run(skip_area=True)
        self.assertIn('ПОРОГ ДНЯ ПИКА', out)
        self.assertIn('Эталоны (метка → с рядом NDVI)', out)
        # Секция дня пика: оптимум найден с идеальным разделением.
        peak_block = out.split('ПОРОГ ДНЯ ПИКА')[1].split('ПОРОГ ДОЛИ')[0]
        self.assertIn('Оптимум', peak_block)
        self.assertIn('баланс 1.000', peak_block)
        self.assertIn('← текущий дефолт', peak_block)

    def test_cover_and_harvest_sweeps_use_unused_labels(self):
        out = self._run(skip_area=True)
        self.assertIn('ПОРОГ ДОЛИ ЗЕЛЁНЫХ', out)
        self.assertIn('ПОРОГ УБОРОЧНОГО СПАДА', out)
        cover = out.split('ПОРОГ ДОЛИ ЗЕЛЁНЫХ')[1].split('ПОРОГ УБОР')[0]
        # 4 культуры (2 озимых + 2 яровых) против 2 «не обрабатывается».
        self.assertIn('культуры=4', cover)
        self.assertIn('не обраб.=2', cover)
        # Сетка не выходит за верхнюю границу диапазона (float-накопление).
        harvest = out.split('ПОРОГ УБОРОЧНОГО СПАДА')[1]
        self.assertIn('0.80', harvest)
        self.assertNotIn('0.85', harvest)

    def test_reports_labels_without_series(self):
        """Метка без ряда NDVI не должна молча исчезать из отчёта."""
        orphan = self._farmland(33.5)
        self._label(orphan, 'winter')
        out = self._run(skip_area=True)
        # 3 метки озимых, но ряд есть только у 2.
        self.assertRegex(out, r'winter\s+3\s+→\s+2')
        self.assertIn('мало для достоверной оценки', out)

    def test_peak_histogram_detects_trough(self):
        """Гистограмма по сохранённым фичам находит провал между модами."""
        for fl, peak in [(self.winter[0], 150), (self.winter[1], 155),
                         (self.spring[0], 200), (self.spring[1], 205)]:
            FarmlandCropSeason.objects.create(
                farmland=fl, year=YEAR, source='raster',
                season_class='unknown', peak_doy=peak,
            )
        # Одинокое угодье в провале между модами 150 и 200.
        FarmlandCropSeason.objects.create(
            farmland=self.unused[0], year=YEAR, source='raster',
            season_class='unknown', peak_doy=175,
        )
        out = self._run()
        self.assertIn('РАСПРЕДЕЛЕНИЕ ДНЯ ПИКА', out)
        self.assertIn('Всего угодий с пиком: 5', out)
        self.assertIn('Провал между модами: бин 170', out)

    @staticmethod
    def _ranking_scores(out, title):
        """Значения колонки «баланс» из блока ранжирования, в порядке строк."""
        block = out.split(title)[1].split('\n\n')[0]
        return [float(m) for m in
                re.findall(r'\s(\d\.\d{3})\s+\S+\s+\d+/\d+\s+#', block)]

    def test_feature_ranking_sections_printed_and_sorted(self):
        out = self._run(skip_area=True, skip_prev_autumn=True)
        self.assertIn('РАЗДЕЛЯЮЩАЯ СИЛА ПРИЗНАКОВ: ОЗИМЫЕ vs ЯРОВЫЕ', out)
        self.assertIn('РАЗДЕЛЯЮЩАЯ СИЛА ПРИЗНАКОВ: КУЛЬТУРЫ vs', out)
        scores = self._ranking_scores(out, 'ОЗИМЫЕ vs ЯРОВЫЕ')
        self.assertTrue(scores)
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_cv_column_dash_when_labels_too_few(self):
        """На двух эталонах класса CV невозможна — честное «—»."""
        out = self._run(skip_area=True, skip_prev_autumn=True)
        block = out.split('ОЗИМЫЕ vs ЯРОВЫЕ')[1].split('\n\n')[0]
        self.assertIn('CV', block)
        self.assertRegex(block, r'\d\.\d{3}\s+—\s+\d+/\d+\s+#')
        self.assertIn('CV невозможна', block)

    def test_cv_can_be_disabled(self):
        """--cv-folds 0: ни CV, ни жалоб на её невозможность."""
        out = self._run(skip_area=True, skip_prev_autumn=True, cv_folds=0)
        block = out.split('ОЗИМЫЕ vs ЯРОВЫЕ')[1].split('\n\n')[0]
        self.assertRegex(block, r'\d\.\d{3}\s+—\s+\d+/\d+\s+#')
        self.assertNotIn('CV невозможна', block)

    def test_prev_autumn_skipped_without_previous_year_data(self):
        """Без рядов прошлого года признак осени не молчит, а сообщает."""
        out = self._run(skip_area=True)
        self.assertIn(f'Ряды NDVI за {YEAR - 1}', out)
        self.assertIn('признак осени недоступен', out)
        self.assertNotIn('NDVI сен-ноя ПРОШЛ. года', out)

    def test_prev_autumn_ranked_when_modis_archive_present(self):
        """Осень прошлого года берётся из MODIS, если S2/L8 за него нет."""
        for fl in self.winter:
            self._prev_series(fl, 0.62)   # всходы озимых осенью
        for fl in self.spring + self.unused:
            self._prev_series(fl, 0.15)   # стерня / вспашка
        out = self._run(skip_area=True)
        self.assertIn('(MODIS)', out)
        self.assertIn('NDVI сен-ноя ПРОШЛ. года', out)
        block = out.split('ОЗИМЫЕ vs ЯРОВЫЕ')[1].split('РАЗДЕЛЯЮЩАЯ')[0]
        autumn_line = [ln for ln in block.splitlines()
                       if 'ПРОШЛ. года' in ln][0]
        # Синтетика разделима идеально и разрез — сверху (≥).
        self.assertIn('1.000', autumn_line)
        self.assertIn('≥', autumn_line)

    def test_unused_subclasses_reported_under_unused(self):
        """ДКР/сорная — часть ``unused`` в развёртках, но видны в сводке."""
        FarmlandTrainingLabel.objects.filter(
            farmland=self.unused[0], year=YEAR,
        ).update(true_class='unused_woody')
        FarmlandTrainingLabel.objects.filter(
            farmland=self.unused[1], year=YEAR,
        ).update(true_class='unused_weeds')

        out = self._run(skip_area=True, skip_prev_autumn=True)
        # Оба подкласса сложились в общий класс: развёртки видят 2 залежи.
        self.assertRegex(out, r'unused\s+2 →\s+2')
        self.assertIn('не обраб.=2', out)      # развёртка гейта покрова
        self.assertRegex(out, r'из них ДКР\s+1 →\s+1')
        self.assertRegex(out, r'из них сорная\s+1 →\s+1')

    def test_histogram_reports_missing_crop_season_rows(self):
        out = self._run()
        self.assertIn('Нет строк FarmlandCropSeason', out)

    def test_no_labels_is_explicit_error(self):
        FarmlandTrainingLabel.objects.all().delete()
        with self.assertRaisesMessage(CommandError, 'Нет меток'):
            self._run(skip_area=True)

    def test_requires_scope(self):
        with self.assertRaisesMessage(CommandError, '--region-id'):
            call_command('tune_season_thresholds', year=YEAR, stdout=StringIO())


class RankingWithoutCvTests(TestCase):
    """Признак, посчитанный по единицам эталонов, не должен быть в топе.

    Прод-случай Тулы-2026: детальных снимков за январь-февраль почти нет,
    поэтому «зимний baseline» есть у горстки эталонов. In-sample оптимум на
    такой выборке высок (0.833), а CV невозможна — и признак-артефакт
    возглавлял рейтинг. Теперь такие строки уезжают вниз.
    """

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион CV', code='rcv', geom=_square(30, 50, 5),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район CV', geom=_square(30, 50, 5),
        )
        # По 6 эталонов класса: хватает на 5 фолдов у полных признаков.
        # Зимнее окно (DOY < 60) есть только у ОДНОГО эталона класса.
        for i in range(6):
            cls._labelled(30.1 + i * 0.1, _winter_ndvi, 'winter',
                          with_winter=(i == 0))
            cls._labelled(32.1 + i * 0.1, _spring_ndvi, 'spring',
                          with_winter=(i == 0))

    @classmethod
    def _labelled(cls, x, fn, true_class, with_winter):
        fl = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(x, 50.2),
        )
        for doy, val, d in zip(*_profile(fn)):
            if doy < 60 and not with_winter:
                continue          # нет январь-февральских снимков
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=f'sentinel2_{d}',
                defaults={'satellite': 'sentinel2', 'acquired_date': d},
            )
            VegetationIndex.objects.create(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=d, mean=val,
            )
        FarmlandTrainingLabel.objects.create(
            farmland=fl, year=YEAR, true_class=true_class,
        )

    def test_feature_without_cv_is_ranked_last(self):
        out = StringIO()
        call_command('tune_season_thresholds', region_id=self.region.pk,
                     year=YEAR, skip_area=True, skip_prev_autumn=True,
                     stdout=out, stderr=out)
        block = out.getvalue().split('ОЗИМЫЕ vs ЯРОВЫЕ')[1].split('\n\n')[0]
        rows = [ln for ln in block.splitlines() if '#' in ln]
        baseline = [ln for ln in rows if 'зимний baseline' in ln]
        self.assertEqual(len(baseline), 1)
        self.assertIn('CV невозможна', baseline[0])
        self.assertIn('1/1', baseline[0])          # колонка n
        self.assertIs(rows[-1], baseline[0])       # в самом конце рейтинга
        # У остальных признаков CV посчитана — они выше.
        self.assertRegex(rows[0], r'\d\.\d{3}\s+\d\.\d{3}')
