"""Тесты команды ``compute_yield_features`` — страховочная сетка перед
рефакторингом ``handle`` (C=12).

``compute_region_features`` мокается — сама математика фичей покрыта
тестами сервиса; здесь проверяем оркестрацию: выбор пар, skip/force,
dry-run, upsert в ``YieldFeatures``.
"""
import io
from datetime import date
from unittest.mock import patch

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import (
    CropYieldStat, District, DistrictNdviSeries, Region, YieldCrop,
    YieldFeatures,
)
from agrocosmos.services.yield_features import YieldFeatureVector

CMD_MODULE = 'agrocosmos.management.commands.compute_yield_features'


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


def _vector(**overrides):
    kwargs = dict(
        peak_ndvi=0.78, peak_ndvi_doy=185, sos_doy=120,
        length_of_season=140, indvi_total=55.0, indvi_repro=20.0,
        n_observations=30, eos_doy=260, season_mean_ndvi=0.5,
    )
    kwargs.update(overrides)
    return YieldFeatureVector(**kwargs)


class ComputeYieldFeaturesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(name='Регион А', code='ra', geom=_mpoly())
        cls.other = Region.objects.create(name='Регион Б', code='rb', geom=_mpoly())
        cls.district = District.objects.create(
            region=cls.region, name='Район', code='d1', geom=_mpoly(),
        )
        # NDVI-данные только за 2022 → _select_years вернёт [2022]
        DistrictNdviSeries.objects.create(
            district=cls.district, acquired_date=date(2022, 6, 15),
            crop_type='arable', sum_ndvi_area=100.0, sum_area=200.0,
            obs_count=10,
        )
        # Yield-факт есть только у региона А
        CropYieldStat.objects.create(
            region=cls.region, year=2022, crop=YieldCrop.GRAINS_TOTAL,
            yield_t_per_ha=3.5, source=CropYieldStat.Source.EMISS,
        )

    def _run(self, *args, feature_result=_vector()):
        out = io.StringIO()
        with patch(f'{CMD_MODULE}.compute_region_features',
                   return_value=feature_result) as mock_crf:
            call_command('compute_yield_features', *args, stdout=out)
        return out.getvalue(), mock_crf

    def test_default_computes_only_pairs_with_yield(self):
        out, mock_crf = self._run()
        # Регион Б без yield-факта не считается
        self.assertEqual(mock_crf.call_count, 1)
        yf = YieldFeatures.objects.get()
        self.assertEqual(yf.region_id, self.region.pk)
        self.assertIsNone(yf.district_id)
        self.assertEqual(yf.year, 2022)
        self.assertEqual(yf.crop, YieldCrop.GRAINS_TOTAL)
        self.assertAlmostEqual(yf.features['peak_ndvi'], 0.78)
        self.assertIn('n_observations', yf.features)
        self.assertTrue(yf.season_complete)
        self.assertIn('Создано: 1', out)

    def test_include_no_yield_expands_pairs(self):
        out, mock_crf = self._run('--include-no-yield')
        self.assertEqual(mock_crf.call_count, 2)
        self.assertEqual(YieldFeatures.objects.count(), 2)

    def test_skips_existing_without_force(self):
        self._run()
        out, mock_crf = self._run()
        self.assertEqual(mock_crf.call_count, 0)
        self.assertIn('Пропущено уже посчитанных: 1', out)

    def test_force_recomputes(self):
        self._run()
        out, mock_crf = self._run('--force',
                                  feature_result=_vector(peak_ndvi=0.9))
        self.assertEqual(mock_crf.call_count, 1)
        yf = YieldFeatures.objects.get()
        self.assertAlmostEqual(yf.features['peak_ndvi'], 0.9)
        self.assertIn('обновлено: 1', out)

    def test_dry_run_writes_nothing(self):
        out, _ = self._run('--dry-run')
        self.assertFalse(YieldFeatures.objects.exists())
        self.assertIn('Dry-run', out)

    def test_none_features_counted_as_skipped(self):
        out, _ = self._run('--verbose', feature_result=None)
        self.assertFalse(YieldFeatures.objects.exists())
        self.assertIn('Пропущено (нет данных / низкая амплитуда): 1', out)

    def test_region_filter_by_name_and_code(self):
        out, mock_crf = self._run('--region', 'Регион А', '--include-no-yield')
        self.assertEqual(mock_crf.call_count, 1)
        YieldFeatures.objects.all().delete()
        out, mock_crf = self._run('--region', 'ra', '--include-no-yield',
                                  '--force')
        self.assertEqual(mock_crf.call_count, 1)

    def test_unknown_region(self):
        out, mock_crf = self._run('--region', 'Нет такого')
        self.assertIn('не найден', out)
        self.assertEqual(mock_crf.call_count, 0)

    def test_no_ndvi_years(self):
        DistrictNdviSeries.objects.all().delete()
        out, mock_crf = self._run()
        self.assertIn('Не найдено ни одного года', out)
        self.assertEqual(mock_crf.call_count, 0)

    def test_explicit_year_without_data_and_no_yield_fact(self):
        out, mock_crf = self._run('--year', '2030')
        # Нет yield-факта за 2030 → пар нет, ничего не считается
        self.assertEqual(mock_crf.call_count, 0)
        self.assertFalse(YieldFeatures.objects.exists())
