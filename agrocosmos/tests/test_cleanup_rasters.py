"""Тесты команды ``cleanup_rasters`` — страховочная сетка перед
рефакторингом ``Command._clean_root`` (C=14).

Работает поверх tmp-директорий через env-переменные сенсоров.
"""
import io
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from django.core.management import call_command
from django.test import SimpleTestCase


def _tif(root: Path, scope: str, year: str, name: str, size: int = 3) -> Path:
    p = root / scope / year / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b'x' * size)
    return p


def _old_date():
    return (date.today() - timedelta(days=1000)).isoformat()


def _fresh_date():
    return (date.today() - timedelta(days=10)).isoformat()


class CleanupRastersTests(SimpleTestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = Path(self.tmp.name)
        self.modis = base / 'modis'
        self.s2 = base / 's2'
        env = {
            'MODIS_RASTER_DIR': str(self.modis),
            'S2_RASTER_DIR': str(self.s2),
            'LANDSAT_RASTER_DIR': str(base / 'landsat'),
        }
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, *args):
        out = io.StringIO()
        call_command('cleanup_rasters', *args, stdout=out, stderr=out)
        return out.getvalue()

    def test_removes_old_keeps_fresh(self):
        old = _tif(self.modis, '37', '2023',
                   f'modis_ndvi_37_2023-01-01_{_old_date()}.tif')
        fresh = _tif(self.modis, '37', '2026',
                     f'modis_ndvi_37_2026-01-01_{_fresh_date()}.tif')
        out = self._run()
        self.assertFalse(old.exists())
        self.assertTrue(fresh.exists())
        self.assertIn('removed 1 files', out)

    def test_dry_run_keeps_files(self):
        old = _tif(self.modis, '37', '2023',
                   f'modis_ndvi_37_2023-01-01_{_old_date()}.tif')
        out = self._run('--dry-run')
        self.assertTrue(old.exists())
        self.assertIn('would remove 1 files', out)

    def test_unparsable_filename_left_alone(self):
        weird = _tif(self.modis, '37', '2023', 'modis_ndvi_37_backup.tif')
        self._run()
        self.assertTrue(weird.exists())

    def test_prunes_empty_year_dirs(self):
        old = _tif(self.modis, '37', '2023',
                   f'modis_ndvi_37_2023-01-01_{_old_date()}.tif')
        self._run()
        self.assertFalse(old.parent.exists())

    def test_default_sensor_is_modis_only(self):
        s2_old = _tif(self.s2, '37', '2023',
                      f's2_ndvi_37_2023-01-01_{_old_date()}.tif')
        self._run()
        self.assertTrue(s2_old.exists())

    def test_sensor_all_cleans_s2(self):
        s2_old = _tif(self.s2, '37', '2023',
                      f's2_ndvi_37_2023-01-01_{_old_date()}.tif')
        self._run('--sensor', 'all')
        self.assertFalse(s2_old.exists())

    def test_keep_days_override(self):
        f = _tif(self.modis, '37', '2026',
                 f'modis_ndvi_37_2026-01-01_{_fresh_date()}.tif')
        self._run('--keep-days', '5')
        self.assertFalse(f.exists())
