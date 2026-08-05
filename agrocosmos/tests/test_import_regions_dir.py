"""Тесты команды ``import_regions_dir`` — страховочная сетка перед
рефакторингом ``handle`` (C=13).
"""
import io
import json
import tempfile
from pathlib import Path

from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import Region

GEOMETRY = {
    'type': 'Polygon',
    'coordinates': [[
        [36.9, 54.9], [37.2, 54.9], [37.2, 55.2], [36.9, 55.2], [36.9, 54.9],
    ]],
}


def _feature_collection(name='Регион А', geometry=GEOMETRY, props=None):
    return {
        'type': 'FeatureCollection',
        'features': [{
            'type': 'Feature',
            'properties': props if props is not None else {'NAME': name},
            'geometry': geometry,
        }],
    }


class ImportRegionsDirTests(TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)

    def _write(self, fname, payload):
        (self.dir / fname).write_text(
            json.dumps(payload, ensure_ascii=False), encoding='utf-8')

    def _run(self, *args):
        out, err = io.StringIO(), io.StringIO()
        call_command('import_regions_dir', str(self.dir), *args,
                     stdout=out, stderr=err)
        return out.getvalue(), err.getvalue()

    def test_creates_regions_from_files(self):
        self._write('region_a.geojson', _feature_collection('Регион А'))
        self._write('region_b.json', _feature_collection('Регион Б'))
        out, err = self._run()
        self.assertEqual(Region.objects.count(), 2)
        r = Region.objects.get(code='region_a')
        self.assertEqual(r.name, 'Регион А')
        self.assertEqual(r.geom.geom_type, 'MultiPolygon')
        self.assertIn('2 created, 0 updated, 0 errors', out)

    def test_updates_existing_by_code(self):
        self._write('region_a.geojson', _feature_collection('Регион А'))
        self._run()
        self._write('region_a.geojson', _feature_collection('Новое имя'))
        out, _ = self._run()
        self.assertEqual(Region.objects.count(), 1)
        self.assertEqual(Region.objects.get().name, 'Новое имя')
        self.assertIn('0 created, 1 updated, 0 errors', out)

    def test_clear_deletes_existing(self):
        Region.objects.create(name='Старый', code='old',
                              geom='MULTIPOLYGON(((0 0,1 0,1 1,0 0)))')
        self._write('region_a.geojson', _feature_collection())
        out, _ = self._run('--clear')
        self.assertIn('Deleted', out)
        self.assertEqual(set(Region.objects.values_list('code', flat=True)),
                         {'region_a'})

    def test_error_cases_counted(self):
        self._write('broken.geojson', {})  # no features
        (self.dir / 'notjson.geojson').write_text('{oops', encoding='utf-8')
        self._write('noname.geojson', _feature_collection(props={}))
        self._write('badgeom.geojson', _feature_collection(
            geometry={'type': 'Polygon', 'coordinates': 'bad'}))
        out, err = self._run()
        self.assertFalse(Region.objects.exists())
        self.assertIn('0 created, 0 updated, 4 errors', out)
        self.assertIn('no features', err)
        self.assertIn('no NAME property', err)

    def test_missing_directory(self):
        out, err = io.StringIO(), io.StringIO()
        call_command('import_regions_dir', str(self.dir / 'nope'),
                     stdout=out, stderr=err)
        self.assertIn('Directory not found', err.getvalue())

    def test_empty_directory(self):
        out, err = self._run()
        self.assertIn('No .geojson files found', err)

    def test_custom_name_field(self):
        self._write('region_a.geojson', _feature_collection(
            props={'TITLE': 'Регион X'}))
        self._run('--name-field', 'TITLE')
        self.assertEqual(Region.objects.get().name, 'Регион X')
