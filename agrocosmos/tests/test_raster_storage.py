"""Тесты ``agrocosmos/services/raster_storage.py`` — страховочная сетка
перед рефакторингом ``list_folders`` (C=16) и ``delete_paths`` (C=14).

Работает поверх временных директорий: env-переменные сенсоров
(S2_RASTER_DIR и т.д.) указываются в tmp, что перекрывает settings.
"""
import os
import tempfile
from datetime import date
from pathlib import Path
from unittest import mock

from django.test import SimpleTestCase

from agrocosmos.services import raster_storage as rs


def _touch(path: Path, size: int = 3) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'x' * size)
    return path


class RasterStorageTestCase(SimpleTestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = Path(self.tmp.name)
        self.roots = {
            's2': base / 's2',
            'l8': base / 'landsat',
            'modis': base / 'modis',
        }
        env = {
            'S2_RASTER_DIR': str(self.roots['s2']),
            'LANDSAT_RASTER_DIR': str(self.roots['l8']),
            'MODIS_RASTER_DIR': str(self.roots['modis']),
        }
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _s2(self, scope, year, d_from, d_to, size=3):
        name = f's2_ndvi_{scope}_{d_from}_{d_to}.tif'
        return _touch(self.roots['s2'] / scope / year / name, size)


class ListFoldersTests(RasterStorageTestCase):

    def test_empty_roots(self):
        self.assertEqual(rs.list_folders(), [])

    def test_aggregates_per_folder(self):
        self._s2('37', '2026', '2026-05-01', '2026-05-10', size=5)
        self._s2('37', '2026', '2026-06-01', '2026-06-10', size=7)
        self._s2('d9', '2025', '2025-04-01', '2025-04-08')

        folders = rs.list_folders(['s2'])
        self.assertEqual(len(folders), 2)
        by_scope = {f.scope: f for f in folders}
        f37 = by_scope['37']
        self.assertEqual(f37.count, 2)
        self.assertEqual(f37.size_bytes, 12)
        self.assertEqual(f37.oldest, date(2026, 5, 1))
        self.assertEqual(f37.newest, date(2026, 6, 10))
        self.assertEqual(by_scope['d9'].year, '2025')

    def test_ignores_foreign_prefix_and_stray_files(self):
        self._s2('37', '2026', '2026-05-01', '2026-05-10')
        _touch(self.roots['s2'] / '37' / '2026' / 'landsat_ndvi_37_2026-05-01_2026-05-10.tif')
        _touch(self.roots['s2'] / '37' / 'readme.txt')  # файл на уровне года

        folders = rs.list_folders(['s2'])
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0].count, 1)

    def test_bad_dates_do_not_break_aggregation(self):
        _touch(self.roots['s2'] / '37' / '2026' / 's2_ndvi_37_2026-99-99_2026-88-88.tif')
        folders = rs.list_folders(['s2'])
        self.assertEqual(folders[0].count, 1)
        self.assertIsNone(folders[0].oldest)
        self.assertIsNone(folders[0].newest)

    def test_multiple_sensors(self):
        self._s2('37', '2026', '2026-05-01', '2026-05-10')
        _touch(self.roots['modis'] / '37' / '2026' / 'modis_ndvi_37_2026-05-01_2026-05-16.tif')
        sensors = {f.sensor for f in rs.list_folders()}
        self.assertEqual(sensors, {'s2', 'modis'})


class ListFilesTests(RasterStorageTestCase):

    def test_unknown_sensor_or_missing_folder(self):
        self.assertEqual(rs.list_files('nope', '37', '2026'), [])
        self.assertEqual(rs.list_files('s2', '37', '2026'), [])

    def test_lists_with_dates_and_age(self):
        self._s2('37', '2026', '2026-05-01', '2026-05-10', size=5)
        files = rs.list_files('s2', '37', '2026')
        self.assertEqual(len(files), 1)
        f = files[0]
        self.assertEqual(f.date_from, date(2026, 5, 1))
        self.assertEqual(f.size_bytes, 5)
        self.assertEqual(f.age_days, (date.today() - date(2026, 5, 10)).days)


class DeletePathsTests(RasterStorageTestCase):

    def test_deletes_and_reports_freed(self):
        f1 = self._s2('37', '2026', '2026-05-01', '2026-05-10', size=5)
        f2 = self._s2('37', '2026', '2026-06-01', '2026-06-10', size=7)
        removed, freed = rs.delete_paths([str(f1), str(f2)])
        self.assertEqual((removed, freed), (2, 12))
        self.assertFalse(f1.exists())
        self.assertFalse(f2.exists())

    def test_prunes_empty_year_dir(self):
        f1 = self._s2('37', '2026', '2026-05-01', '2026-05-10')
        year_dir = f1.parent
        rs.delete_paths([str(f1)])
        self.assertFalse(year_dir.exists())

    def test_keeps_nonempty_year_dir(self):
        f1 = self._s2('37', '2026', '2026-05-01', '2026-05-10')
        self._s2('37', '2026', '2026-06-01', '2026-06-10')
        rs.delete_paths([str(f1)])
        self.assertTrue(f1.parent.exists())

    def test_rejects_paths_outside_roots(self):
        outside = _touch(Path(self.tmp.name) / 'other' / 'victim.tif', size=9)
        removed, freed = rs.delete_paths([str(outside)])
        self.assertEqual((removed, freed), (0, 0))
        self.assertTrue(outside.exists())

    def test_zero_byte_file_counts_as_removed(self):
        f1 = self._s2('37', '2026', '2026-05-01', '2026-05-10', size=0)
        removed, freed = rs.delete_paths([str(f1)])
        self.assertEqual((removed, freed), (1, 0))
        self.assertFalse(f1.exists())

    def test_missing_and_dir_paths_ignored(self):
        removed, freed = rs.delete_paths([
            str(self.roots['s2'] / 'no' / 'such.tif'),
            str(self.roots['s2']),
        ])
        self.assertEqual((removed, freed), (0, 0))


class TotalsBySensorTests(RasterStorageTestCase):

    def test_totals(self):
        self._s2('37', '2026', '2026-05-01', '2026-05-10', size=5)
        self._s2('d9', '2025', '2025-04-01', '2025-04-08', size=7)
        out = rs.totals_by_sensor()
        self.assertEqual(out['s2']['count'], 2)
        self.assertEqual(out['s2']['size_bytes'], 12)
        self.assertEqual(out['modis']['count'], 0)
        self.assertEqual(out['l8']['label'], 'Landsat 8/9')
