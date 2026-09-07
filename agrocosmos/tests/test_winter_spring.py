"""Тесты классификатора озимые/яровые.

1. Чистый сервис ``winter_spring`` на синтетических профилях NDVI +
   раскладка crop→класс и двусторонняя калибровка.
2. Команда ``classify_winter_spring`` (загрузка рядов, опорные точки,
   калибровка, матрица ошибок, запись ``FarmlandCropSeason``).
3. Проброс сводки озимые/яровые в отчёты (district-detailed + паспорт поля).

Профили тюнингованы под среднюю полосу РФ (Тула):
* озимые — зелёное поле уже в апреле (высокий ранневесенний NDVI), ранний SOS;
* яровые — голая почва весной, поздний единственный пик.
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
    calibrate_threshold, calibrate_threshold_separating, classify_crop_value,
    classify_profile, evaluate_predictions,
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
        self.assertGreater(prof.early_spring_ndvi, 0.35)
        self.assertGreater(prof.confidence, 0.5)

    def test_spring_profile_classified_spring(self):
        doys, vals, _ = _profile(_spring_ndvi)
        prof = classify_profile(doys, vals)
        self.assertEqual(prof.season_class, 'spring')
        self.assertLess(prof.early_spring_ndvi, 0.35)

    def test_threshold_moves_boundary(self):
        doys, vals, _ = _profile(_winter_ndvi)
        prof = classify_profile(doys, vals, threshold=0.95)
        self.assertEqual(prof.season_class, 'spring')

    def test_flat_signal_unknown(self):
        doys, _, _ = _profile(_winter_ndvi)
        flat = [0.2] * len(doys)
        self.assertEqual(classify_profile(doys, flat).season_class, 'unknown')

    def test_too_few_points_unknown(self):
        prof = classify_profile([100, 110, 120], [0.5, 0.6, 0.7])
        self.assertEqual(prof.season_class, 'unknown')
        self.assertEqual(prof.confidence, 0.0)

    def test_no_early_spring_obs_unknown(self):
        doys = list(range(160, 260, 8))
        vals = [0.8] * len(doys)
        self.assertEqual(classify_profile(doys, vals).season_class, 'unknown')

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
        for fl in cls.spring + [cls.unused]:
            cls._series(fl, _spring_ndvi)

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
        self._run(region_id=self.region.pk, year=YEAR)
        winter = FarmlandCropSeason.objects.filter(
            year=YEAR, season_class='winter')
        self.assertEqual(winter.count(), 3)
        self.assertSetEqual(
            set(winter.values_list('farmland_id', flat=True)),
            {fl.pk for fl in self.winter},
        )
        # 3 яровых + 1 «не используется» (профиль яровой) = 4 spring.
        self.assertEqual(
            FarmlandCropSeason.objects.filter(
                year=YEAR, season_class='spring').count(),
            4,
        )

    def test_reference_calibration_and_confusion(self):
        out = self._run(
            district_id=self.district.pk, year=YEAR,
            reference_layer='kultury_2026', reference_attr='crop',
        )
        self.assertIn('Опорных угодий', out)
        self.assertIn('Валидация по эталонам', out)
        self.assertIn('озимые:  3/3', out)
        self.assertIn('яровые:  3/3', out)
        self.assertIn('общая точность: 100.0%', out)
        self.assertIn('не обрабатываемые', out)

        refs = FarmlandCropSeason.objects.filter(is_reference=True, year=YEAR)
        self.assertEqual(refs.count(), 7)  # 3+3+1
        self.assertSetEqual(
            set(refs.values_list('reference_crop', flat=True)),
            {'Пшеница озимая', 'Соя', 'не используется/ неудобья'},
        )

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

    def test_manual_threshold_overrides(self):
        self._run(region_id=self.region.pk, year=YEAR, threshold=0.99)
        self.assertEqual(
            FarmlandCropSeason.objects.filter(season_class='winter').count(), 0,
        )

    def test_idempotent_upsert(self):
        self._run(region_id=self.region.pk, year=YEAR)
        self._run(region_id=self.region.pk, year=YEAR)
        self.assertEqual(FarmlandCropSeason.objects.filter(year=YEAR).count(), 7)

    def test_report_district_detailed_includes_crop_season(self):
        self._run(region_id=self.region.pk, year=YEAR)
        resp = self.client.get(
            '/agrocosmos/api/report/district-detailed/',
            {'district': self.district.pk, 'year': YEAR},
        ).json()
        self.assertTrue(resp['ok'])
        cs = resp['crop_season']
        self.assertIsNotNone(cs)
        self.assertEqual(cs['source'], 'raster')
        self.assertEqual(cs['classes']['winter']['count'], 3)
        self.assertEqual(cs['classes']['spring']['count'], 4)

    def test_report_farmland_includes_crop_season(self):
        self._run(region_id=self.region.pk, year=YEAR)
        resp = self.client.get(
            '/agrocosmos/api/report/farmland/',
            {'farmland': self.winter[0].pk, 'year': YEAR},
        ).json()
        self.assertTrue(resp['ok'])
        self.assertIsNotNone(resp['crop_season'])
        self.assertEqual(resp['crop_season']['season_class'], 'winter')
