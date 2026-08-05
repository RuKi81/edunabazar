"""Тесты команды ``import_emiss_yield`` — страховочная сетка перед
рефакторингом ``handle`` (C=24).

``pandas.read_excel`` мокается DataFrame'ом с реальной структурой
ЕМИСС-выгрузки; проверяем парсинг заголовка, фильтры строк, матчинг
регионов, конвертацию ц/га → т/га, dry-run и upsert.
"""
import io
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from agrocosmos.management.commands.import_emiss_yield import (
    _find_header_row, _normalize_region_name, _to_float,
)
from agrocosmos.models import CropYieldStat, Region, YieldCrop


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


def _emiss_df(extra_rows=()):
    rows = [
        ['Урожайность сельскохозяйственных культур, ц/га', None, None, None, None],
        [None, None, None, None, None],
        ['Хозяйства всех категорий', None, None, None, None],
        ['Зерновые и зернобобовые культуры', None, None, None, None],
        [None, 2021, 2022, 2023, 2024],
        ['Российская Федерация', 26.0, 32.0, 30.0, 28.0],
        ['Центральный федеральный округ', 40.0, 45.0, 43.0, 41.0],
        ['Регион А', 30.0, '31,5', '—', 34.0],
        ['Республика Адыгея (Адыгея)', 45.0, 46.0, 47.0, 48.0],
        ['Кемеровская область - Кузбасс', 20.0, 21.0, 22.0, 23.0],
        ['Архангельская область (кроме Ненецкого автономного округа)',
         15.0, 16.0, 17.0, 18.0],
        ['Неизвестный край', 10.0, 11.0, 12.0, 13.0],
    ]
    rows.extend(extra_rows)
    return pd.DataFrame(rows)


class HelperTests(TestCase):
    def test_to_float(self):
        self.assertEqual(_to_float('31,5'), 31.5)
        self.assertEqual(_to_float(30), 30.0)
        self.assertIsNone(_to_float('—'))
        self.assertIsNone(_to_float('н/д'))
        self.assertIsNone(_to_float(None))
        self.assertIsNone(_to_float(float('nan')))
        self.assertIsNone(_to_float('abc'))

    def test_normalize_region_name(self):
        self.assertEqual(
            _normalize_region_name('Республика Адыгея (Адыгея)'),
            'Республика Адыгея',
        )
        self.assertEqual(
            _normalize_region_name('  Регион   А  '), 'Регион А',
        )

    def test_find_header_row(self):
        self.assertEqual(_find_header_row(_emiss_df()), 4)
        no_years = pd.DataFrame([['a', 'b'], ['c', 'd']])
        self.assertIsNone(_find_header_row(no_years))


class ImportEmissYieldTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region_a = Region.objects.create(
            name='Регион А', code='ra', geom=_mpoly())
        cls.adygeya = Region.objects.create(
            name='Республика Адыгея', code='ad', geom=_mpoly())
        cls.kemerovo = Region.objects.create(
            name='Кемеровская область', code='ke', geom=_mpoly())

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.xls', delete=False)
        self.tmp.close()
        self.path = Path(self.tmp.name)
        self.addCleanup(self.path.unlink)

    def _run(self, *args, df=None):
        out = io.StringIO()
        with patch('pandas.read_excel',
                   return_value=df if df is not None else _emiss_df()):
            call_command('import_emiss_yield', str(self.path), *args,
                         stdout=out)
        return out.getvalue()

    def test_missing_file_raises(self):
        with self.assertRaises(CommandError):
            call_command('import_emiss_yield', 'no/such/file.xls')

    def test_no_header_row_raises(self):
        with self.assertRaises(CommandError):
            self._run(df=pd.DataFrame([['a', 'b'], ['c', 'd']]))

    def test_imports_matched_regions_with_conversion(self):
        out = self._run()
        # 3 сматченных региона; у «Регион А» одна пустая ячейка → 11 записей
        self.assertEqual(CropYieldStat.objects.count(), 11)
        rec = CropYieldStat.objects.get(region=self.region_a, year=2021)
        self.assertAlmostEqual(rec.yield_t_per_ha, 3.0)  # 30 ц/га → 3 т/га
        self.assertEqual(rec.crop, YieldCrop.GRAINS_TOTAL)
        self.assertEqual(rec.source, CropYieldStat.Source.EMISS)
        self.assertEqual(rec.source_note, self.path.name)
        # '31,5' → 3.15
        rec22 = CropYieldStat.objects.get(region=self.region_a, year=2022)
        self.assertAlmostEqual(rec22.yield_t_per_ha, 3.15)
        self.assertIn('Создано/обновлено в БД: 11', out)

    def test_matches_parens_and_dash_aliases(self):
        self._run()
        self.assertEqual(
            CropYieldStat.objects.filter(region=self.adygeya).count(), 4)
        self.assertEqual(
            CropYieldStat.objects.filter(region=self.kemerovo).count(), 4)

    def test_skips_aggregates_duplicates_and_unmatched(self):
        out = self._run()
        self.assertIn('Агрегатов пропущено (РФ/ФО): 2', out)
        self.assertIn('Дубликатов пропущено:        1', out)
        self.assertIn('Не сматчилось субъектов: 1', out)
        self.assertIn('Неизвестный край', out)

    def test_dedupes_normalized_names(self):
        out = self._run(df=_emiss_df(
            extra_rows=[['Регион А (повтор)', 99.0, 99.0, 99.0, 99.0]],
        ))
        self.assertIn('Дубликатов пропущено:        2', out)
        rec = CropYieldStat.objects.get(region=self.region_a, year=2021)
        self.assertAlmostEqual(rec.yield_t_per_ha, 3.0)

    def test_dry_run_writes_nothing(self):
        out = self._run('--dry-run')
        self.assertFalse(CropYieldStat.objects.exists())
        self.assertIn('Dry-run', out)
        self.assertIn('К сохранению (region, year): 11', out)
        self.assertIn('Примеры (первые 5):', out)

    def test_rerun_upserts_without_duplicates(self):
        self._run()
        df2 = _emiss_df()
        df2.iat[7, 1] = 50.0  # Регион А / 2021: 30 → 50 ц/га
        out = self._run(df=df2)
        self.assertEqual(CropYieldStat.objects.count(), 11)
        rec = CropYieldStat.objects.get(region=self.region_a, year=2021)
        self.assertAlmostEqual(rec.yield_t_per_ha, 5.0)
        self.assertIn('новых: 0', out)

    def test_source_note_override(self):
        self._run('--source-note', 'ЕМИСС 06.04.2026')
        rec = CropYieldStat.objects.filter(region=self.region_a).first()
        self.assertEqual(rec.source_note, 'ЕМИСС 06.04.2026')
