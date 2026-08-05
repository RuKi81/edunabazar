"""Тесты команды ``import_farmlands_rosreestr`` — страховочная сетка перед
рефакторингом ``_process_region`` (C=13). ogr2ogr, схемы и SQL мокаются;
проверяем план (skip-причины), dry-run, промоушен staging → agro_farmland
и итоговую сводку.
"""
import io
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from agrocosmos.management.commands.import_farmlands_rosreestr import Command
from agrocosmos.models import Farmland, Region

MOD = 'agrocosmos.management.commands.import_farmlands_rosreestr'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _schema(usable=True):
    # MagicMock: атрибуты, которые нужны build_insert_sql (area_field и
    # пр.), разрешаются автоматически; SQL всё равно не исполняется.
    schema = mock.MagicMock()
    schema.is_usable = usable
    schema.all_fields = ['f1']
    schema.schema_id = 'schema_v1'
    schema.usage_field = 'usage'
    schema.fact_isp_field = None
    schema.cadastral_field = 'cad'
    return schema


class ResolveRegionTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kemerovo = Region.objects.create(
            name='Кемеровская область', code='kem', geom=_square(30, 50))
        cls.crimea = Region.objects.create(
            name='Республика Крым', code='crimea', geom=_square(40, 50))

    def test_iexact(self):
        cmd = Command()
        self.assertEqual(
            cmd._resolve_region('Республика Крым'), self.crimea)

    def test_underscores_normalised(self):
        cmd = Command()
        self.assertEqual(
            cmd._resolve_region('Кемеровская_область_'), self.kemerovo)

    def test_alias_after_dash(self):
        cmd = Command()
        self.assertEqual(
            cmd._resolve_region('Кемеровская область - Кузбасс'),
            self.kemerovo)

    def test_unknown_returns_none(self):
        self.assertIsNone(Command()._resolve_region('Атлантида'))

    def test_slug(self):
        self.assertEqual(Command._slug('Kem-77 Region'), 'kem_77_region')
        self.assertEqual(Command._slug('---'), 'unknown')


class ImportFarmlandsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))

    def setUp(self):
        self.base = Path(tempfile.mkdtemp())

    def _mk_region_dir(self, name='Регион', shps=('a.shp',)):
        d = self.base / name
        d.mkdir()
        for s in shps:
            (d / s).touch()
        return d

    def _run(self, *, rc=0, staged=5, inserted=3, schema=None, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        cur = mock.MagicMock()
        cur.fetchone.return_value = (staged,)
        cur.rowcount = inserted
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        with mock.patch(f'{MOD}.detect_schema',
                        return_value=schema or _schema()), \
             mock.patch(f'{MOD}.run_ogr2ogr',
                        return_value=SimpleNamespace(returncode=rc)) as ogr, \
             mock.patch(f'{MOD}.connection', conn), \
             mock.patch(f'{MOD}.shutil.which', return_value='/usr/bin/ogr2ogr'):
            call_command('import_farmlands_rosreestr', base=str(self.base),
                         stdout=out, stderr=err, **kwargs)
        return out.getvalue(), err.getvalue(), ogr, cur

    def test_base_not_found(self):
        with self.assertRaises(CommandError):
            call_command('import_farmlands_rosreestr',
                         base=str(self.base / 'nope'))

    def test_missing_ogr2ogr_binary(self):
        self._mk_region_dir()
        with mock.patch(f'{MOD}.shutil.which', return_value=None):
            with self.assertRaises(CommandError):
                call_command('import_farmlands_rosreestr',
                             base=str(self.base))

    def test_dry_run_prints_plan(self):
        self._mk_region_dir()
        out, _, ogr, _ = self._run(dry_run=True)
        self.assertIn('DRY: would ogr2ogr a.shp', out)
        self.assertIn('ok=1', out)
        ogr.assert_not_called()

    def test_no_shp_skipped(self):
        self._mk_region_dir(shps=())
        out, _, _, _ = self._run()
        self.assertIn('skipped: no .shp file', out)
        self.assertIn('skipped=1', out)

    def test_unusable_schema_skipped(self):
        self._mk_region_dir()
        out, _, _, _ = self._run(schema=_schema(usable=False))
        self.assertIn('unrecognised schema', out)

    def test_unknown_region_skipped(self):
        self._mk_region_dir(name='Атлантида')
        out, _, _, _ = self._run()
        self.assertIn("no matching Region for 'Атлантида'", out)

    def test_skip_existing(self):
        self._mk_region_dir()
        Farmland.objects.create(region=self.region, geom=_square(30.1, 50.1))
        out, _, ogr, _ = self._run(skip_existing=True)
        self.assertIn('already has farmlands', out)
        ogr.assert_not_called()

    def test_import_two_halves(self):
        self._mk_region_dir(shps=('b.shp', 'a.shp'))
        out, _, ogr, cur = self._run()
        self.assertEqual(ogr.call_count, 2)
        # Отсортировано по имени; staging-таблицы пронумерованы
        self.assertEqual(ogr.call_args_list[0].args[0].name, 'a.shp')
        self.assertEqual(ogr.call_args_list[0].args[1],
                         'staging_farmland_r1_1')
        self.assertIn('staged=10 inserted=6', out)
        self.assertIn('ok=1', out)

    def test_ogr_nonzero_rc_continues(self):
        self._mk_region_dir()
        out, err, _, _ = self._run(rc=1)
        self.assertIn('ogr2ogr rc=1', err)
        self.assertIn('staged=5 inserted=3', out)

    def test_empty_staging(self):
        self._mk_region_dir()
        out, _, _, _ = self._run(staged=0)
        self.assertIn('staging is empty', out)
        self.assertIn('staged=0 inserted=0', out)
