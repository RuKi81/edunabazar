"""Тесты сборки обучающей выборки и команды ``train_season_model``.

Проверяется то, что легко сломать незаметно:

* подклассы зарастания сворачиваются в ``unused`` (иначе ступень
  «культуры vs не обрабатывается» потеряет часть разметки);
* группы валидации берутся по районам, а при разметке в одном районе —
  по ячейкам сетки, иначе кросс-валидация просто отключилась бы;
* в обучение идут только угодья с рядом NDVI, а отброшенные считаются;
* команда печатает качество модели РЯДОМ с качеством порогового
  классификатора — без этой пары цифр решение принимать нельзя.

Синтетические профили переиспользуются из ``test_winter_spring``.
"""
from datetime import date, timedelta
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from agrocosmos.models import (
    District, Farmland, FarmlandTrainingLabel, Region, SatelliteScene,
    VegetationIndex,
)
from agrocosmos.services.season_dataset import (
    STAGE_COVER, STAGE_SEASON, build_dataset, build_stage, load_groups,
    load_labels, load_ndvi_series, rule_based_baseline,
)
from agrocosmos.services.season_model import (
    SEASON_FEATURE_NAMES, cover_feature_names,
)

from .test_winter_spring import (
    YEAR, _profile, _spring_ndvi, _square, _unused_cover_ndvi, _winter_ndvi,
)

# По столько эталонов каждого класса: выше порога MIN_PER_CLASS команды,
# и хватает на leave-one-group-out по четырём районам.
PER_CLASS = 24
N_DISTRICTS = 4


class SeasonDatasetTests(TestCase):
    """Сборка выборки из разметки и рядов NDVI."""

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='sd1', geom=_square(30, 50, 5),
        )
        cls.districts = [
            District.objects.create(
                region=cls.region, name=f'Район {i}',
                geom=_square(30 + i, 50, 1),
            )
            for i in range(N_DISTRICTS)
        ]
        cls.scenes = {}
        cls.by_class = {'winter': [], 'spring': [], 'unused': []}
        profiles = {
            'winter': _winter_ndvi,
            'spring': _spring_ndvi,
            'unused': _unused_cover_ndvi,
        }
        # Подклассы зарастания ставим части «залежи»: проверяем, что
        # ``family`` сворачивает их в ``unused``.
        unused_labels = (['unused'] * (PER_CLASS - 4)
                         + ['unused_woody'] * 2 + ['unused_weeds'] * 2)

        indices = []
        for cls_name, fn in profiles.items():
            for i in range(PER_CLASS):
                district = cls.districts[i % N_DISTRICTS]
                fl = Farmland.objects.create(
                    region=cls.region, district=district,
                    crop_type=Farmland.CropType.ARABLE, area_ha=100,
                    geom=_square(30.1 + i * 0.05 + district.pk * 0.001,
                                 50.2 + district.pk * 0.2),
                )
                cls.by_class[cls_name].append(fl)
                label = (unused_labels[i] if cls_name == 'unused'
                         else cls_name)
                FarmlandTrainingLabel.objects.create(
                    farmland=fl, year=YEAR, true_class=label,
                )
                indices.extend(cls._series_rows(fl, fn))
        VegetationIndex.objects.bulk_create(indices)

    @classmethod
    def _series_rows(cls, fl, fn):
        rows = []
        for _doy, val, d in zip(*_profile(fn)):
            scene = cls.scenes.get(d)
            if scene is None:
                scene, _ = SatelliteScene.objects.get_or_create(
                    scene_id=f'sentinel2_{d}',
                    defaults={'satellite': 'sentinel2', 'acquired_date': d},
                )
                cls.scenes[d] = scene
            rows.append(VegetationIndex(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=d, mean=val,
            ))
        return rows

    # -- метки и ряды --

    def test_load_labels_folds_unused_subclasses(self):
        labels = load_labels(YEAR, region_id=self.region.pk)
        self.assertEqual(len(labels), PER_CLASS * 3)
        self.assertEqual(sum(1 for c in labels.values() if c == 'unused'),
                         PER_CLASS)
        self.assertNotIn('unused_woody', set(labels.values()))

    def test_load_labels_scoped_by_district(self):
        labels = load_labels(YEAR, district_id=self.districts[0].pk)
        self.assertEqual(len(labels), PER_CLASS * 3 / N_DISTRICTS)

    def test_load_labels_other_year_empty(self):
        self.assertEqual(load_labels(YEAR + 1, region_id=self.region.pk), {})

    def test_load_ndvi_series_returns_doys_and_values(self):
        fl = self.by_class['winter'][0]
        series = load_ndvi_series([fl.pk], YEAR, ('sentinel2',))
        doys, vals = series[fl.pk]
        self.assertEqual(len(doys), len(vals))
        self.assertGreater(len(doys), 20)
        self.assertEqual(doys, sorted(doys))

    def test_load_ndvi_series_empty_ids(self):
        self.assertEqual(load_ndvi_series([], YEAR, ('sentinel2',)), {})

    # -- группы --

    def test_groups_use_districts_when_enough(self):
        ids = [fl.pk for fls in self.by_class.values() for fl in fls]
        groups, scheme = load_groups(ids)
        self.assertEqual(scheme, 'district')
        self.assertEqual(len(set(groups.values())), N_DISTRICTS)

    def test_groups_fall_back_to_grid_in_single_district(self):
        """Вся разметка в одном районе → группируем по ячейкам сетки."""
        ids = [fl.pk for fl in self.by_class['winter']
               if fl.district_id == self.districts[0].pk]
        groups, scheme = load_groups(ids)
        self.assertEqual(scheme, 'grid')
        self.assertGreater(len(set(groups.values())), 1)

    # -- ступени --

    def test_cover_stage_merges_crops_into_positive_class(self):
        data = build_dataset(YEAR, region_id=self.region.pk,
                             skip_prev_autumn=True)
        stage = data[STAGE_COVER]
        self.assertEqual(stage['n_pos'], PER_CLASS * 2)
        self.assertEqual(stage['n_neg'], PER_CLASS)
        self.assertEqual(stage['names'], list(cover_feature_names()))
        self.assertEqual(stage['dropped'], 0)

    def test_season_stage_uses_arable_classes_only(self):
        data = build_dataset(YEAR, region_id=self.region.pk,
                             skip_prev_autumn=True)
        stage = data[STAGE_SEASON]
        self.assertEqual((stage['n_pos'], stage['n_neg']),
                         (PER_CLASS, PER_CLASS))
        self.assertEqual(stage['names'], list(SEASON_FEATURE_NAMES))
        self.assertEqual(stage['positive_label'], 'winter')

    def test_stage_counts_labels_without_series(self):
        """Угодье без ряда NDVI не попадает в обучение, но считается."""
        orphan = Farmland.objects.create(
            region=self.region, district=self.districts[0],
            crop_type=Farmland.CropType.ARABLE, area_ha=10,
            geom=_square(35, 55),
        )
        FarmlandTrainingLabel.objects.create(
            farmland=orphan, year=YEAR, true_class='winter',
        )
        data = build_dataset(YEAR, region_id=self.region.pk,
                             skip_prev_autumn=True)
        self.assertEqual(data[STAGE_SEASON]['dropped'], 1)
        self.assertEqual(data[STAGE_SEASON]['n_pos'], PER_CLASS)

    def test_unknown_stage_rejected(self):
        with self.assertRaises(ValueError):
            build_stage('bogus', {}, {}, {})

    def test_rule_based_baseline_separates_synthetic_profiles(self):
        data = build_dataset(YEAR, region_id=self.region.pk,
                             skip_prev_autumn=True)
        stage = data[STAGE_SEASON]
        preds = rule_based_baseline(STAGE_SEASON, data['series'],
                                    stage['farmland_ids'])
        self.assertEqual(len(preds), len(stage['y']))
        correct = sum(1 for p, t in zip(preds, stage['y']) if p == t)
        self.assertGreater(correct / len(preds), 0.9)

    def test_dataset_reports_previous_autumn_source(self):
        data = build_dataset(YEAR, region_id=self.region.pk,
                             skip_prev_autumn=True)
        self.assertEqual(data['prev_source'], 'выключен')
        self.assertEqual(data['n_prev_series'], 0)

    def test_previous_autumn_ignores_modis(self):
        """Прошлогодняя осень берётся тем же источником, что текущий год.

        MODIS (250 м) на контуре в десятки га — смесь с соседями, то есть
        другая физическая величина; подмена источника дала бы скрытый
        сдвиг признака ``autumn_prev`` между обучением и применением.
        """
        fl = self.by_class['winter'][0]
        self._prev_autumn(fl, satellite='modis_terra', value=0.7)
        data = build_dataset(YEAR, region_id=self.region.pk)
        self.assertEqual(data['prev_source'], 'нет')
        self.assertEqual(data['n_prev_series'], 0)
        rows = data[STAGE_SEASON]['rows']
        self.assertTrue(all(r['autumn_prev_missing'] == 1.0 for r in rows))

    def test_previous_autumn_used_from_detailed_source(self):
        fl = self.by_class['winter'][0]
        self._prev_autumn(fl, satellite='sentinel2', value=0.7)
        data = build_dataset(YEAR, region_id=self.region.pk)
        self.assertEqual(data['prev_source'], 'детальный')
        self.assertEqual(data['n_prev_series'], 1)
        stage = data[STAGE_SEASON]
        row = stage['rows'][stage['farmland_ids'].index(fl.pk)]
        self.assertEqual(row['autumn_prev_missing'], 0.0)
        self.assertGreater(row['autumn_prev'], 0.5)

    @classmethod
    def _prev_autumn(cls, fl, satellite, value):
        """Короткий ряд ПРОШЛОГО года в осеннем окне (сентябрь-ноябрь)."""
        for day in (20, 30, 40, 50):
            d = date(YEAR - 1, 9, 1) + timedelta(days=day)
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=f'{satellite}_{d}',
                defaults={'satellite': satellite, 'acquired_date': d},
            )
            VegetationIndex.objects.create(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=d, mean=value,
            )


class TrainSeasonModelCommandTests(SeasonDatasetTests):
    """Команда обучения: отчёт по обеим ступеням и сравнение с порогами."""

    def _run(self, **kwargs):
        out = StringIO()
        kwargs.setdefault('region_id', self.region.pk)
        kwargs.setdefault('year', YEAR)
        kwargs.setdefault('skip_prev_autumn', True)
        call_command('train_season_model', stdout=out, stderr=out, **kwargs)
        return out.getvalue()

    def test_reports_both_stages_with_cv_and_baseline(self):
        out = self._run()
        self.assertIn('КУЛЬТУРЫ vs НЕ ОБРАБАТЫВАЕТСЯ', out)
        self.assertIn('ОЗИМЫЕ vs ЯРОВЫЕ', out)
        self.assertIn('пороговый классификатор', out)
        self.assertIn('модель (отложенные группы)', out)
        self.assertIn('Группы валидации: районы', out)

    def test_single_stage_option(self):
        out = self._run(stage=STAGE_SEASON)
        self.assertIn('ОЗИМЫЕ vs ЯРОВЫЕ', out)
        self.assertNotIn('КУЛЬТУРЫ vs НЕ ОБРАБАТЫВАЕТСЯ', out)

    def test_model_learns_separable_synthetic_data(self):
        """На разделимой синтетике CV-качество обеих ступеней высокое."""
        out = self._run()
        for block in out.split('═══')[1:]:
            if 'модель (отложенные группы)' not in block:
                continue
            line = [ln for ln in block.splitlines()
                    if 'отложенные группы' in ln][0]
            balanced = float(line.split()[-4])
            self.assertGreater(balanced, 0.9, line)

    def test_weights_printed_for_interpretation(self):
        out = self._run(stage=STAGE_SEASON, top_features=3)
        self.assertIn('Веса (на стандартизованных признаках', out)
        self.assertIn('intercept', out)

    def test_requires_scope(self):
        with self.assertRaisesMessage(CommandError, '--region-id'):
            call_command('train_season_model', year=YEAR, stdout=StringIO())

    def test_missing_labels_reported(self):
        with self.assertRaisesMessage(CommandError, 'Нет меток'):
            self._run(year=YEAR + 5)


class SmallSampleCommandTests(TestCase):
    """Мало разметки — команда обязана отказаться, а не выдать цифру."""

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион мало', code='sd2', geom=_square(40, 50, 5),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(40, 50, 1),
        )
        rows = []
        for i, (cls_name, fn) in enumerate(
                (('winter', _winter_ndvi), ('spring', _spring_ndvi))):
            for j in range(2):
                fl = Farmland.objects.create(
                    region=cls.region, district=cls.district,
                    crop_type=Farmland.CropType.ARABLE, area_ha=50,
                    geom=_square(40.1 + i + j * 0.1, 50.2),
                )
                FarmlandTrainingLabel.objects.create(
                    farmland=fl, year=YEAR, true_class=cls_name,
                )
                for _doy, val, d in zip(*_profile(fn)):
                    scene, _ = SatelliteScene.objects.get_or_create(
                        scene_id=f'sentinel2_{d}',
                        defaults={'satellite': 'sentinel2',
                                  'acquired_date': d},
                    )
                    rows.append(VegetationIndex(
                        farmland=fl, scene=scene, index_type='ndvi',
                        acquired_date=d, mean=val,
                    ))
        VegetationIndex.objects.bulk_create(rows)

    def test_stage_skipped_with_warning(self):
        out = StringIO()
        call_command('train_season_model', region_id=self.region.pk,
                     year=YEAR, skip_prev_autumn=True, stdout=out, stderr=out)
        text = out.getvalue()
        self.assertIn('Пропуск: нужно', text)
        self.assertNotIn('модель (отложенные группы)', text)
