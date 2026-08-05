"""Тесты команды ``import_farmlands`` — страховочная сетка перед
рефакторингом ``_import_shapefile`` (C=26) и ``_import_geojson`` (C=12)
с переводом на хелперы ``services/import_vector.py``.
"""
import io
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import District, Farmland, Region

SQUARE = {
    'type': 'Polygon',
    'coordinates': [[[37.0, 55.0], [37.1, 55.0], [37.1, 55.1],
                     [37.0, 55.1], [37.0, 55.0]]],
}


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


def _feature(props=None, geometry=SQUARE):
    return {'type': 'Feature', 'properties': props or {}, 'geometry': geometry}


class ImportFarmlandsBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(name='Регион', code='91', geom=_mpoly())
        cls.district = District.objects.create(
            region=cls.region, name='Первый район', code='d1', geom=_mpoly(),
        )

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _geojson_path(self, features):
        p = Path(self.tmp.name) / 'lands.geojson'
        p.write_text(
            json.dumps({'type': 'FeatureCollection', 'features': features}),
            encoding='utf-8',
        )
        return str(p)

    def _run(self, source, *args):
        out, err = io.StringIO(), io.StringIO()
        call_command('import_farmlands', source, '--region-code', '91',
                     *args, stdout=out, stderr=err)
        return out.getvalue(), err.getvalue()


class GeojsonCommandTests(ImportFarmlandsBase):

    def test_creates_farmland(self):
        path = self._geojson_path([_feature({
            'DISTRICT': 'Первый район', 'LAND_TYPE': 'Пашня',
            'AREA_HA': '2.5', 'CAD_NUM': '77:1', 'NOTE': 'x',
        })])
        out, _ = self._run(path)
        self.assertIn('Done: 1 created, 0 skipped', out)
        fl = Farmland.objects.get()
        self.assertEqual(fl.district, self.district)
        self.assertEqual(fl.crop_type, 'arable')
        self.assertEqual(float(fl.area_ha), 2.5)
        self.assertEqual(fl.properties['NOTE'], 'x')

    def test_default_crop_type_option(self):
        path = self._geojson_path([_feature({'DISTRICT': 'Первый район'})])
        self._run(path, '--default-crop-type', 'pasture')
        self.assertEqual(Farmland.objects.get().crop_type, 'pasture')

    def test_area_from_geometry(self):
        path = self._geojson_path([_feature({'DISTRICT': 'Первый район'})])
        self._run(path)
        self.assertGreater(float(Farmland.objects.get().area_ha), 0)

    def test_unknown_region(self):
        path = self._geojson_path([])
        out, err = io.StringIO(), io.StringIO()
        call_command('import_farmlands', path, '--region-code', 'nope',
                     stdout=out, stderr=err)
        self.assertIn('not found', err.getvalue())

    def test_requires_districts_or_autocreate(self):
        District.objects.all().delete()
        path = self._geojson_path([_feature({'DISTRICT': 'Новый'})])
        out, err = self._run(path)
        self.assertIn('No districts found', err)
        self.assertEqual(Farmland.objects.count(), 0)

    def test_auto_create_districts(self):
        path = self._geojson_path([_feature({'DISTRICT': 'Новый район'})])
        out, _ = self._run(path, '--auto-create-districts')
        self.assertIn('Done: 1 created', out)
        self.assertTrue(District.objects.filter(name='Новый район').exists())

    def test_clear_removes_existing(self):
        Farmland.objects.create(
            district=self.district, crop_type='arable', area_ha=1, geom=_mpoly(),
        )
        path = self._geojson_path([_feature({'DISTRICT': 'Первый район'})])
        out, _ = self._run(path, '--clear')
        self.assertIn('Deleted 1 existing', out)
        self.assertEqual(Farmland.objects.count(), 1)

    def test_bad_geometry_skipped(self):
        path = self._geojson_path([
            _feature({'DISTRICT': 'Первый район'},
                     geometry={'type': 'Polygon', 'coordinates': 'мусор'}),
        ])
        out, err = self._run(path)
        self.assertIn('Done: 0 created, 1 skipped', out)
        self.assertIn('Bad geometry', err)

    def test_batch_progress(self):
        path = self._geojson_path([
            _feature({'DISTRICT': 'Первый район'}),
            _feature({'DISTRICT': 'Первый район'}),
        ])
        out, _ = self._run(path, '--batch-size', '1')
        self.assertIn('... 1 created', out)
        self.assertIn('Done: 2 created', out)


class FakeShpFeature:
    def __init__(self, values, wkt, srid=4326):
        self._values = values
        self.geom = SimpleNamespace(wkt=wkt, srid=srid)

    def get(self, field):
        if field in self._values:
            return self._values[field]
        raise KeyError(field)


class FakeLayer(list):
    fields = ['DISTRICT', 'LAND_TYPE', 'AREA_HA', 'CAD_NUM', 'NOTE']


class ShapefileCommandTests(ImportFarmlandsBase):

    WKT = ('POLYGON ((37.0 55.0, 37.1 55.0, 37.1 55.1, '
           '37.0 55.1, 37.0 55.0))')

    def test_shapefile_import(self):
        layer = FakeLayer([FakeShpFeature({
            'DISTRICT': 'Первый район', 'LAND_TYPE': 'пастбище',
            'AREA_HA': 4.0, 'CAD_NUM': '77:2', 'NOTE': 'з',
        }, self.WKT)])
        ds = mock.MagicMock()
        ds.__getitem__.return_value = layer
        with mock.patch('django.contrib.gis.gdal.DataSource', return_value=ds):
            out, _ = self._run('lands.shp')
        self.assertIn('Done: 1 created, 0 skipped', out)
        fl = Farmland.objects.get()
        self.assertEqual(fl.crop_type, 'pasture')
        self.assertEqual(float(fl.area_ha), 4.0)
        self.assertEqual(fl.properties, {'NOTE': 'з'})
