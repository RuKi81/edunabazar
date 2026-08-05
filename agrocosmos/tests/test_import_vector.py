"""Тесты ``agrocosmos/services/import_vector.py`` — страховочная сетка
перед рефакторингом ``_import_farmland_geojson`` (C=12) и
``_import_farmland_shp`` (C=24).

GeoJSON-путь проверяется сквозным вызовом ``import_farmland_vector``
с настоящей геометрией; SHP-путь — через фейковый ``DataSource``.
"""
import json
from types import SimpleNamespace
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from agrocosmos.models import District, Farmland, Region
from agrocosmos.services import import_vector as iv

SQUARE = {
    'type': 'Polygon',
    'coordinates': [[[37.0, 55.0], [37.1, 55.0], [37.1, 55.1],
                     [37.0, 55.1], [37.0, 55.0]]],
}


def _geojson_upload(features):
    payload = json.dumps({'type': 'FeatureCollection', 'features': features})
    return SimpleUploadedFile('lands.geojson', payload.encode('utf-8'))


def _feature(props=None, geometry=SQUARE):
    return {'type': 'Feature', 'properties': props or {}, 'geometry': geometry}


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


class ImportVectorTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='reg', geom=_mpoly(),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Первый район', code='d1', geom=_mpoly(),
        )


class FarmlandGeojsonTests(ImportVectorTestCase):

    def _run(self, features, **kwargs):
        return iv.import_farmland_vector(
            _geojson_upload(features), self.region.pk, **kwargs,
        )

    def test_creates_farmland_with_fields(self):
        created, skipped, errors = self._run([_feature({
            'DISTRICT': 'Первый район', 'LAND_TYPE': 'Пашня',
            'AREA_HA': '12.5', 'CAD_NUM': '77:1:2:3', 'EXTRA': 7,
        })])
        self.assertEqual((created, skipped, errors), (1, 0, []))
        fl = Farmland.objects.get()
        self.assertEqual(fl.district, self.district)
        self.assertEqual(fl.crop_type, 'arable')
        self.assertEqual(float(fl.area_ha), 12.5)
        self.assertEqual(fl.cadastral_number, '77:1:2:3')
        self.assertEqual(fl.properties['EXTRA'], '7')

    def test_area_computed_from_geometry_when_missing(self):
        created, _, _ = self._run([_feature({'DISTRICT': 'Первый район'})])
        self.assertEqual(created, 1)
        fl = Farmland.objects.get()
        self.assertGreater(float(fl.area_ha), 0)

    def test_unknown_crop_type_defaults_to_arable(self):
        self._run([_feature({'DISTRICT': 'Первый район', 'LAND_TYPE': 'загадка'})])
        self.assertEqual(Farmland.objects.get().crop_type, 'arable')

    def test_single_district_fallback_when_no_name(self):
        created, skipped, _ = self._run([_feature({})])
        self.assertEqual((created, skipped), (1, 0))
        self.assertEqual(Farmland.objects.get().district, self.district)

    def test_auto_creates_missing_district(self):
        created, _, _ = self._run(
            [_feature({'DISTRICT': 'Новый район'})],
        )
        self.assertEqual(created, 1)
        self.assertTrue(
            District.objects.filter(region=self.region, name='Новый район').exists()
        )

    def test_skips_unknown_district_without_autocreate(self):
        District.objects.create(
            region=self.region, name='Второй район', code='d2', geom=_mpoly(),
        )  # два района — fallback выключается
        created, skipped, _ = self._run(
            [_feature({'DISTRICT': 'Неизвестный'})],
            auto_create_districts=False,
        )
        self.assertEqual((created, skipped), (0, 1))

    def test_prefix_match_district(self):
        created, _, _ = self._run([_feature({'DISTRICT': 'Первый'})])
        self.assertEqual(created, 1)
        self.assertEqual(Farmland.objects.get().district, self.district)

    def test_bad_geometry_recorded_as_error(self):
        created, skipped, errors = self._run([
            _feature({'DISTRICT': 'Первый район'},
                     geometry={'type': 'Polygon', 'coordinates': 'мусор'}),
        ])
        self.assertEqual((created, skipped), (0, 1))
        self.assertEqual(len(errors), 1)

    def test_clear_existing(self):
        Farmland.objects.create(
            district=self.district, crop_type='arable', area_ha=1,
            geom=_mpoly(),
        )
        created, _, _ = self._run(
            [_feature({'DISTRICT': 'Первый район'})], clear_existing=True,
        )
        self.assertEqual(created, 1)
        self.assertEqual(Farmland.objects.count(), 1)

    def test_unsupported_format(self):
        up = SimpleUploadedFile('data.csv', b'x')
        created, skipped, errors = iv.import_farmland_vector(up, self.region.pk)
        self.assertEqual((created, skipped), (0, 0))
        self.assertEqual(len(errors), 1)


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


class FarmlandShpTests(ImportVectorTestCase):

    WKT = ('POLYGON ((37.0 55.0, 37.1 55.0, 37.1 55.1, '
           '37.0 55.1, 37.0 55.0))')

    def _run_shp(self, feats, **kwargs):
        layer = FakeLayer(feats)
        ds = mock.MagicMock()
        ds.__getitem__.return_value = layer
        with mock.patch('django.contrib.gis.gdal.DataSource', return_value=ds):
            return iv._import_farmland_shp(
                'fake.shp', self.region,
                kwargs.get('crop_field', 'LAND_TYPE'),
                kwargs.get('area_field', 'AREA_HA'),
                kwargs.get('cad_field', 'CAD_NUM'),
                kwargs.get('dist_field', 'DISTRICT'),
                'utf-8',
                kwargs.get('auto_create_districts', True),
            )

    def test_creates_with_extra_properties(self):
        feats = [FakeShpFeature({
            'DISTRICT': 'Первый район', 'LAND_TYPE': 'сенокос',
            'AREA_HA': 3.5, 'CAD_NUM': '77:9', 'NOTE': 'заметка',
        }, self.WKT)]
        created, skipped, errors = self._run_shp(feats)
        self.assertEqual((created, skipped, errors), (1, 0, []))
        fl = Farmland.objects.get()
        self.assertEqual(fl.crop_type, 'hayfield')
        self.assertEqual(float(fl.area_ha), 3.5)
        self.assertEqual(fl.properties, {'NOTE': 'заметка'})

    def test_missing_attrs_use_defaults(self):
        feats = [FakeShpFeature({}, self.WKT)]  # все get() кидают KeyError
        created, skipped, _ = self._run_shp(feats)
        self.assertEqual((created, skipped), (1, 0))
        fl = Farmland.objects.get()
        self.assertEqual(fl.district, self.district)  # fallback
        self.assertEqual(fl.crop_type, 'arable')
        self.assertGreater(float(fl.area_ha), 0)  # площадь из геометрии

    def test_bad_geometry_is_error(self):
        feats = [FakeShpFeature({'DISTRICT': 'Первый район'}, 'НЕ WKT')]
        created, skipped, errors = self._run_shp(feats)
        self.assertEqual((created, skipped), (0, 1))
        self.assertEqual(len(errors), 1)
