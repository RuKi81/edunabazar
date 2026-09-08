"""Тесты классификатора озимые/яровые.

1. Чистый сервис ``winter_spring`` на синтетических профилях NDVI +
   раскладка crop→класс и двусторонняя калибровка.
2. Команда ``classify_winter_spring`` (загрузка рядов, опорные точки,
   калибровка, матрица ошибок, запись ``FarmlandCropSeason``).
3. Проброс сводки озимые/яровые в отчёты (district-detailed + паспорт поля).

Профили тюнингованы под среднюю полосу РФ (Тула):
* озимые — ранний пик NDVI (конец мая–июнь), зелёное поле уже в апреле;
* яровые — голая почва весной, поздний пик (июль–август).
Основной дискриминатор — день пика; ранневесенний NDVI вторичный.
"""
from datetime import date, timedelta
from io import StringIO

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.db import connection
from django.test import SimpleTestCase, TestCase, override_settings

from agrocosmos.models import (
    District, Farmland, FarmlandCropSeason, Region, SatelliteScene,
    VegetationIndex,
)
from agrocosmos.services.winter_spring import (
    calibrate_cover_threshold, calibrate_peak_doy_threshold,
    calibrate_threshold, calibrate_threshold_separating, classify_crop_value,
    classify_profile, detect_harvest, evaluate_predictions, profile_features,
)

YEAR = 2026


# ── Синтетические годовые профили (шаг 8 дней) ───────────────────────

def _winter_ndvi(doy):
    """Озимые: зимний покров, ранний весенний рост, летняя уборка."""
    if doy < 60:
        return 0.32
    if doy < 150:
        return 0.32 + (doy - 60) / 90 * 0.53   # рост к 0.85 к DOY150
    if doy < 200:
        return 0.85 - (doy - 150) / 50 * 0.55  # спад после пика
    return 0.20


def _spring_ndvi(doy):
    """Яровые: голая почва до конца мая, поздний единственный пик."""
    if doy < 140:
        return 0.16
    if doy < 220:
        return 0.16 + (doy - 140) / 80 * 0.69  # рост к 0.85 к DOY220
    if doy < 270:
        return 0.85 - (doy - 220) / 50 * 0.60
    return 0.20


def _grassland_ndvi(doy):
    """Многолетние травы / залежь: вег. цикл есть, но БЕЗ уборочного спада.

    NDVI плавно нарастает к лету и так же плавно снижается — обвала к
    голой почве (стерне) нет, поэтому гейт уборки относит контур к unused.
    """
    if doy < 60:
        return 0.35
    if doy < 170:
        return 0.35 + (doy - 60) / 110 * 0.45   # рост к 0.80 к DOY170
    if doy < 300:
        return 0.80 - (doy - 170) / 130 * 0.18  # медленный спад до ~0.62
    return 0.62


def _unused_cover_ndvi(doy):
    """Залежь/неудобья: почти весь год NDVI < 0.4, короткий слабый всплеск.

    Имеет уборко-подобный спад (проходит гейт уборки), но НИЗКУЮ долю
    зелёных наблюдений → ловится гейтом покрова.
    """
    if doy < 170:
        return 0.22
    if doy < 210:
        return 0.22 + (doy - 170) / 40 * 0.30   # к 0.52
    if doy < 250:
        return 0.52 - (doy - 210) / 40 * 0.30   # спад к 0.22
    return 0.22


def _winter_regrowth_ndvi(doy):
    """Озимые: ранний пик, уборочный обвал, затем повторный рост сорняков."""
    if doy < 60:
        return 0.32
    if doy < 150:
        return 0.32 + (doy - 60) / 90 * 0.53    # рост к 0.85 к DOY150
    if doy < 190:
        return 0.85 - (doy - 150) / 40 * 0.65   # уборочный обвал к 0.20 (~190)
    if doy < 250:
        return 0.20 + (doy - 190) / 60 * 0.35   # повторный рост сорняков к 0.55
    return 0.45


def _profile(fn, step=8):
    doys, vals, dates = [], [], []
    d = date(YEAR, 1, 4)
    while d.year == YEAR:
        doy = d.timetuple().tm_yday
        doys.append(doy)
        vals.append(fn(doy))
        dates.append(d)
        d += timedelta(days=step)
    return doys, vals, dates


class WinterSpringServiceTests(SimpleTestCase):

    def test_winter_profile_classified_winter(self):
        doys, vals, _ = _profile(_winter_ndvi)
        prof = classify_profile(doys, vals)
        self.assertEqual(prof.season_class, 'winter')
        self.assertLess(prof.peak_doy, 185)          # ранний пик
        self.assertGreater(prof.early_spring_ndvi, 0.35)
        self.assertGreater(prof.confidence, 0.5)

    def test_spring_profile_classified_spring(self):
        doys, vals, _ = _profile(_spring_ndvi)
        prof = classify_profile(doys, vals)
        self.assertEqual(prof.season_class, 'spring')
        self.assertGreaterEqual(prof.peak_doy, 185)  # поздний пик
        self.assertLess(prof.early_spring_ndvi, 0.35)

    def test_peak_threshold_moves_boundary(self):
        # Тот же ранний пик озимых, но порог сдвинут раньше пика → spring.
        doys, vals, _ = _profile(_winter_ndvi)
        prof = classify_profile(doys, vals, peak_doy_threshold=140)
        self.assertEqual(prof.season_class, 'spring')

    def test_flat_signal_unknown(self):
        doys, _, _ = _profile(_winter_ndvi)
        flat = [0.2] * len(doys)
        self.assertEqual(classify_profile(doys, flat).season_class, 'unknown')

    def test_too_few_points_unknown(self):
        prof = classify_profile([100, 110, 120], [0.5, 0.6, 0.7])
        self.assertEqual(prof.season_class, 'unknown')
        self.assertEqual(prof.confidence, 0.0)

    def test_no_early_spring_obs_still_classifies_by_peak(self):
        # Нет ранневесенних наблюдений, но есть поздний пик → яровые
        # (ранневесенний NDVI теперь вторичный, не обязателен).
        doys = list(range(150, 270, 8))
        vals = [_spring_ndvi(d) for d in doys]
        prof = classify_profile(doys, vals)
        self.assertEqual(prof.season_class, 'spring')
        self.assertIsNone(prof.early_spring_ndvi)

    # -- гейт уборки --
    def test_grassland_gated_to_unused(self):
        # Есть вег. цикл, но нет уборочного обвала → не обрабатывается.
        doys, vals, _ = _profile(_grassland_ndvi)
        prof = classify_profile(doys, vals, require_harvest=True)
        self.assertEqual(prof.season_class, 'unused')
        self.assertIsNotNone(prof.peak_doy)      # цикл есть
        self.assertIsNone(prof.harvest_doy)      # уборки нет
        self.assertLess(prof.harvest_drop, 0.5)  # мелкий спад

    def test_grassland_without_gate_falls_through(self):
        # С отключённым гейтом трава классифицируется как озимая/яровая.
        doys, vals, _ = _profile(_grassland_ndvi)
        prof = classify_profile(doys, vals, require_harvest=False)
        self.assertIn(prof.season_class, ('winter', 'spring'))

    def test_gate_off_by_default_grassland_falls_through(self):
        # Гейт по умолчанию ВЫКЛ (сезон может быть незавершён).
        doys, vals, _ = _profile(_grassland_ndvi)
        prof = classify_profile(doys, vals)
        self.assertIn(prof.season_class, ('winter', 'spring'))

    def test_winter_passes_gate(self):
        doys, vals, _ = _profile(_winter_ndvi)
        prof = classify_profile(doys, vals, require_harvest=True)
        self.assertEqual(prof.season_class, 'winter')
        self.assertIsNotNone(prof.harvest_doy)
        self.assertGreaterEqual(prof.harvest_drop, 0.5)

    def test_harvest_survives_weed_regrowth(self):
        # Обвал уборки + повторный рост сорняков → уборка всё равно найдена.
        doys, vals, _ = _profile(_winter_regrowth_ndvi)
        prof = classify_profile(doys, vals, require_harvest=True)
        self.assertEqual(prof.season_class, 'winter')
        self.assertIsNotNone(prof.harvest_doy)
        self.assertLess(prof.harvest_doy, 220)   # обвал ~190, не поздний рост

    def test_detect_harvest_direct(self):
        import numpy as np
        doys = np.array([100, 140, 170, 200, 230], dtype=np.int32)
        vals = np.array([0.4, 0.85, 0.8, 0.25, 0.2], dtype=np.float64)
        sig = detect_harvest(doys, vals, peak_doy=140, peak_ndvi=0.85,
                             baseline=0.3)
        self.assertTrue(sig.has_harvest)
        self.assertEqual(sig.harvest_doy, 230)

    def test_detect_harvest_no_post_obs(self):
        import numpy as np
        doys = np.array([100, 140], dtype=np.int32)
        vals = np.array([0.4, 0.85], dtype=np.float64)
        sig = detect_harvest(doys, vals, peak_doy=140, peak_ndvi=0.85,
                             baseline=0.3)
        self.assertFalse(sig.has_harvest)
        self.assertIsNone(sig.drop_ratio)

    # -- признак «убрано» (is_harvested) + сенокос (hayfield) --
    def test_is_harvested_true_for_winter(self):
        # Пашня с уборочным обвалом NDVI → «убрано».
        doys, vals, _ = _profile(_winter_ndvi)
        prof = classify_profile(doys, vals)
        self.assertTrue(prof.is_harvested)
        self.assertIsNotNone(prof.harvest_doy)

    def test_is_harvested_false_for_grassland(self):
        # Трава без уборки → не убрано.
        doys, vals, _ = _profile(_grassland_ndvi)
        prof = classify_profile(doys, vals)
        self.assertFalse(prof.is_harvested)

    def test_hayfield_classified_hayfield_with_harvest(self):
        # Сенокос с укосом (обвал NDVI) → класс hayfield, убрано=True.
        doys, vals, _ = _profile(_winter_ndvi)
        prof = classify_profile(doys, vals, hayfield=True)
        self.assertEqual(prof.season_class, 'hayfield')
        self.assertTrue(prof.is_harvested)

    def test_hayfield_skips_cover_gate(self):
        # Сенокос НЕ уходит в unused даже при низкой доле зелёных.
        doys, vals, _ = _profile(_unused_cover_ndvi)
        prof = classify_profile(doys, vals, hayfield=True, require_cover=True)
        self.assertEqual(prof.season_class, 'hayfield')
        self.assertLess(prof.green_fraction, 0.33)

    # -- диагностические признаки (profile_features) --
    def test_profile_features_grass_vs_spring(self):
        # Залежь/трава: высокий «пол» и малая амплитуда; яровые наоборот.
        g = profile_features(*_profile(_grassland_ndvi)[:2])
        s = profile_features(*_profile(_spring_ndvi)[:2])
        self.assertGreater(g['season_min'], s['season_min'])   # трава не голая
        self.assertLess(g['amplitude'], s['amplitude'])        # трава пологая
        self.assertGreater(g['green_fraction'], s['green_fraction'])

    def test_profile_features_empty(self):
        feats = profile_features([], [])
        self.assertEqual(feats['n_obs'], 0)
        self.assertIsNone(feats['season_min'])

    # -- гейт покрова (доля зелёных) --
    def test_cover_gate_low_green_to_unused(self):
        # Разреженный покров (мало зелени за год) → unused при гейте покрова.
        doys, vals, _ = _profile(_unused_cover_ndvi)
        prof = classify_profile(doys, vals, require_cover=True)
        self.assertEqual(prof.season_class, 'unused')
        self.assertLess(prof.green_fraction, 0.33)

    def test_cover_gate_off_by_default(self):
        doys, vals, _ = _profile(_unused_cover_ndvi)
        prof = classify_profile(doys, vals)
        self.assertIn(prof.season_class, ('winter', 'spring'))

    def test_cover_gate_keeps_high_cover(self):
        # Высоко-покровный профиль (много зелени весь сезон) НЕ отсеивается.
        doys, vals, _ = _profile(_grassland_ndvi)
        prof = classify_profile(doys, vals, require_cover=True)
        self.assertNotEqual(prof.season_class, 'unused')
        self.assertGreaterEqual(prof.green_fraction, 0.33)

    def test_calibrate_cover_threshold_separates(self):
        crop = [0.50, 0.58, 0.62, 0.47]
        unused = [0.10, 0.19, 0.22, 0.15]
        thr = calibrate_cover_threshold(crop, unused)
        self.assertTrue(0.20 <= thr <= 0.50)
        self.assertTrue(all(c >= thr for c in crop))
        self.assertTrue(all(u < thr for u in unused))

    def test_calibrate_cover_threshold_degenerate(self):
        self.assertEqual(calibrate_cover_threshold([], []), 0.33)
        # Только культуры → порог ниже почти всех.
        thr = calibrate_cover_threshold([0.5, 0.6], [])
        self.assertLessEqual(thr, 0.5)

    # -- раскладка crop → класс (реальные значения kultury_2026) --
    def test_classify_crop_value_real_labels(self):
        cases = {
            'Пшеница озимая': 'winter',
            'Соя': 'spring',
            'Вика + Овес': 'spring',
            'Ячмень': 'spring',
            'Кукуруза на зерно': 'spring',
            'Люцерна 2 гп': 'spring',
            'не используется/ неудобья': 'unused',
            'Сидеральный пар': 'unused',
            'Залежь': 'unused',
            'рекультивация': 'unused',
            'Молодой сад': 'ignore',
            'Молодой сад (пар)': 'ignore',
            'Плодоносящий сад (Яблоко)': 'ignore',
        }
        for value, expected in cases.items():
            self.assertEqual(classify_crop_value(value), expected, value)

    def test_classify_crop_value_empty(self):
        self.assertIsNone(classify_crop_value(None))
        self.assertIsNone(classify_crop_value('  '))

    # -- калибровка --
    def test_calibrate_threshold_clamped(self):
        self.assertEqual(calibrate_threshold([0.6, 0.65, 0.7, 0.62]), 0.5)

    def test_calibrate_separating_between_classes(self):
        thr = calibrate_threshold_separating([0.55, 0.6, 0.65], [0.1, 0.15, 0.2])
        self.assertGreater(thr, 0.2)
        self.assertLess(thr, 0.55)

    def test_calibrate_separating_winter_only(self):
        self.assertEqual(
            calibrate_threshold_separating([0.6, 0.62, 0.65], []),
            calibrate_threshold([0.6, 0.62, 0.65]),
        )

    def test_calibrate_separating_empty_default(self):
        self.assertEqual(calibrate_threshold_separating([], []), 0.35)

    def test_calibrate_peak_doy_between_classes(self):
        thr = calibrate_peak_doy_threshold([150, 155, 148], [215, 220, 225])
        self.assertGreaterEqual(thr, 160)
        self.assertLessEqual(thr, 215)

    def test_calibrate_peak_doy_empty_default(self):
        self.assertEqual(calibrate_peak_doy_threshold([], []), 185)

    def test_evaluate_predictions(self):
        pairs = [('winter', 'winter'), ('winter', 'spring'),
                 ('spring', 'spring'), ('spring', 'spring')]
        ev = evaluate_predictions(pairs)
        self.assertEqual(ev['winter'], {'n': 2, 'correct': 1})
        self.assertEqual(ev['spring'], {'n': 2, 'correct': 2})
        self.assertEqual(ev['total'], 4)
        self.assertAlmostEqual(ev['accuracy'], 0.75)


def _square(x, y, size=0.05):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class ClassifyWinterSpringCommandTests(TestCase):
    POINTS_TABLE = 'test_kultury_points'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50, 5),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50, 5),
        )

        cls.winter = [cls._farmland(30.1 + i * 0.1) for i in range(3)]
        cls.spring = [cls._farmland(31.1 + i * 0.1) for i in range(3)]
        cls.unused = cls._farmland(32.1)  # опорная точка «не используется»

        for fl in cls.winter:
            cls._series(fl, _winter_ndvi)
        for fl in cls.spring:
            cls._series(fl, _spring_ndvi)
        # «Не используется» — профиль травы без уборки: гейт → unused.
        cls._series(cls.unused, _grassland_ndvi)

        # Точечный ГИС-слой: озимые/яровые/не обрабатываемые эталоны.
        cls._make_points_layer(
            [(fl, 'Пшеница озимая') for fl in cls.winter]
            + [(fl, 'Соя') for fl in cls.spring]
            + [(cls.unused, 'не используется/ неудобья')]
        )

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
        for doy, val, d in zip(*_profile(fn)):
            scene, _ = SatelliteScene.objects.get_or_create(
                scene_id=f'sentinel2_{d}',
                defaults={'satellite': 'sentinel2', 'acquired_date': d},
            )
            VegetationIndex.objects.create(
                farmland=fl, scene=scene, index_type='ndvi',
                acquired_date=d, mean=val,
            )

    @classmethod
    def _make_points_layer(cls, farmland_values):
        from my_fields.models import GisLayer
        with connection.cursor() as cur:
            cur.execute(
                f'CREATE TABLE {cls.POINTS_TABLE} '
                '(id serial PRIMARY KEY, crop text, '
                'geom geometry(Point, 4326))'
            )
            for fl, crop in farmland_values:
                c = fl.geom.centroid
                cur.execute(
                    f'INSERT INTO {cls.POINTS_TABLE} (crop, geom) '
                    'VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))',
                    [crop, c.x, c.y],
                )
        cls.layer = GisLayer.objects.create(
            title='kultury_2026', table_name=cls.POINTS_TABLE,
            original_filename='kultury.shp', geom_kind='point',
            feature_count=len(farmland_values),
            attributes=[{'name': 'crop', 'db': 'crop', 'type': 'text'}],
        )

    def _run(self, **kwargs):
        out = StringIO()
        call_command('classify_winter_spring', stdout=out, stderr=out, **kwargs)
        return out.getvalue()

    # -- tests --
    def test_classifies_winter_and_spring(self):
        # cover_gate=False: изолируем гейт уборки; синтетический spring
        # намеренно «острый» (доля зелёных < 0.33), на реале spring ~0.58.
        self._run(region_id=self.region.pk, year=YEAR, harvest_gate=True,
                  cover_gate=False)
        winter = FarmlandCropSeason.objects.filter(
            year=YEAR, season_class='winter')
        self.assertEqual(winter.count(), 3)
        self.assertSetEqual(
            set(winter.values_list('farmland_id', flat=True)),
            {fl.pk for fl in self.winter},
        )
        self.assertEqual(
            FarmlandCropSeason.objects.filter(
                year=YEAR, season_class='spring').count(),
            3,
        )
        # Профиль травы без уборки отсеян гейтом уборки в unused.
        unused = FarmlandCropSeason.objects.filter(
            year=YEAR, season_class='unused')
        self.assertEqual(unused.count(), 1)
        self.assertEqual(unused.first().farmland_id, self.unused.pk)

    def test_reference_calibration_and_confusion(self):
        out = self._run(
            district_id=self.district.pk, year=YEAR, harvest_gate=True,
            cover_gate=False,
            reference_layer='kultury_2026', reference_attr='crop',
        )
        self.assertIn('Опорных угодий', out)
        self.assertIn('Валидация по эталонам', out)
        self.assertIn('озимые:  3/3', out)
        self.assertIn('яровые:  3/3', out)
        self.assertIn('общая точность: 100.0%', out)
        self.assertIn('не обрабатываемые', out)
        # Эталон «не используется» отсеян гейтом уборки.
        self.assertIn('отсеяно в unused=1', out)
        self.assertIn('Гейт уборки ВКЛ', out)

        refs = FarmlandCropSeason.objects.filter(is_reference=True, year=YEAR)
        self.assertEqual(refs.count(), 7)  # 3+3+1
        self.assertSetEqual(
            set(refs.values_list('reference_crop', flat=True)),
            {'Пшеница озимая', 'Соя', 'не используется/ неудобья'},
        )

    def test_reference_features_diagnostic(self):
        out = self._run(
            district_id=self.district.pk, year=YEAR, reference_features=True,
            reference_layer='kultury_2026', reference_attr='crop',
        )
        self.assertIn('Признаки по эталонным классам', out)
        self.assertIn('мин NDVI (пол)', out)
        self.assertIn('доля зелёных', out)
        # Диагностика ничего не пишет в БД.
        self.assertEqual(FarmlandCropSeason.objects.count(), 0)

    def test_cover_gate_status_and_calibration(self):
        # Гейт покрова ВКЛ без явного порога → калибровка по эталонам.
        out = self._run(
            district_id=self.district.pk, year=YEAR, cover_gate=True,
            reference_layer='kultury_2026', reference_attr='crop',
        )
        self.assertIn('Гейт покрова ВКЛ', out)
        self.assertIn('Калибровка покрова', out)

    def test_cover_gate_explicit_threshold_no_false_unused(self):
        # Явный низкий порог: высоко-покровные синтетики НЕ уходят в unused.
        self._run(region_id=self.region.pk, year=YEAR, cover_gate=True,
                  cover_min=0.05)
        self.assertEqual(
            FarmlandCropSeason.objects.filter(
                year=YEAR, season_class='unused').count(), 0)
        self.assertEqual(
            FarmlandCropSeason.objects.filter(
                year=YEAR, season_class__in=('winter', 'spring')).count(), 7)

    def test_cover_gate_on_by_default(self):
        # Гейт покрова включён по умолчанию (без явных cover-аргументов).
        out = self._run(region_id=self.region.pk, year=YEAR)
        self.assertIn('Гейт покрова ВКЛ', out)

    def test_cover_gate_can_be_disabled(self):
        # --no-cover-gate (cover_gate=False) выключает гейт покрова.
        out = self._run(region_id=self.region.pk, year=YEAR, cover_gate=False)
        self.assertIn('Гейт покрова ВЫКЛ', out)

    def test_missing_shp_fails_fast(self):
        from django.core.management.base import CommandError
        with self.assertRaises(CommandError):
            self._run(
                region_id=self.region.pk, year=YEAR,
                reference_shp='/no/such/kultury_2026.shp',
            )

    def test_dry_run_writes_nothing(self):
        self._run(region_id=self.region.pk, year=YEAR, dry_run=True)
        self.assertEqual(FarmlandCropSeason.objects.count(), 0)

    def test_manual_peak_threshold_overrides(self):
        # Порог дня пика = 140 (раньше пика озимых ~150) → 0 озимых.
        self._run(region_id=self.region.pk, year=YEAR, peak_threshold=140,
                  cover_gate=False)
        self.assertEqual(
            FarmlandCropSeason.objects.filter(season_class='winter').count(), 0,
        )

    def test_idempotent_upsert(self):
        self._run(region_id=self.region.pk, year=YEAR)
        self._run(region_id=self.region.pk, year=YEAR)
        self.assertEqual(FarmlandCropSeason.objects.filter(year=YEAR).count(), 7)

    def test_report_district_detailed_includes_crop_season(self):
        self._run(region_id=self.region.pk, year=YEAR, harvest_gate=True,
                  cover_gate=False)
        resp = self.client.get(
            '/agrocosmos/api/report/district-detailed/',
            {'district': self.district.pk, 'year': YEAR},
        ).json()
        self.assertTrue(resp['ok'])
        cs = resp['crop_season']
        self.assertIsNotNone(cs)
        self.assertEqual(cs['source'], 'raster')
        self.assertEqual(cs['classes']['winter']['count'], 3)
        self.assertEqual(cs['classes']['spring']['count'], 3)
        self.assertEqual(cs['classes']['unused']['count'], 1)

    def test_report_farmland_includes_crop_season(self):
        self._run(region_id=self.region.pk, year=YEAR, cover_gate=False)
        resp = self.client.get(
            '/agrocosmos/api/report/farmland/',
            {'farmland': self.winter[0].pk, 'year': YEAR},
        ).json()
        self.assertTrue(resp['ok'])
        self.assertIsNotNone(resp['crop_season'])
        self.assertEqual(resp['crop_season']['season_class'], 'winter')
        self.assertTrue(resp['crop_season']['is_harvested'])

    def test_hayfield_classified_and_harvested(self):
        # Сенокосное угодье → класс hayfield + признак «убрано».
        hay = Farmland.objects.create(
            region=self.region, district=self.district,
            crop_type=Farmland.CropType.HAYFIELD, area_ha=50,
            geom=_square(33.1, 50.2),
        )
        self._series(hay, _winter_ndvi)  # укос = уборочный спад
        self._run(region_id=self.region.pk, year=YEAR, cover_gate=False)
        rec = FarmlandCropSeason.objects.get(farmland=hay, year=YEAR)
        self.assertEqual(rec.season_class, 'hayfield')
        self.assertTrue(rec.is_harvested)

        resp = self.client.get(
            '/agrocosmos/api/report/district-detailed/',
            {'district': self.district.pk, 'year': YEAR},
        ).json()
        cs = resp['crop_season']
        self.assertEqual(cs['classes']['hayfield']['count'], 1)
        self.assertGreaterEqual(cs['harvested']['count'], 1)
