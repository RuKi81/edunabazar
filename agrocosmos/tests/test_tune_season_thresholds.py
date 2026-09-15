"""Тесты подбора порогов классификатора сезона.

1. Чистая функция ``sweep_threshold`` — профиль качества по сетке порогов.
2. Команда ``tune_season_thresholds`` — развёртки по ручным меткам и
   гистограмма дня пика по всем угодьям (диагностика бимодальности).

Синтетические профили NDVI переиспользуются из ``test_winter_spring``:
озимые дают ранний пик, яровые — поздний, залежь — низкую долю зелени.
"""
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase

from agrocosmos.models import (
    District, Farmland, FarmlandCropSeason, FarmlandTrainingLabel, Region,
    SatelliteScene, VegetationIndex,
)
from agrocosmos.services.winter_spring import sweep_threshold

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
