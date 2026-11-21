"""Тесты ``import_gee_croplands``: векторизация маски блоками, обрезка
по районам и выбор регионов по стране. GEE и ogr2ogr не вызываются —
staging-таблица создаётся прямо в тестовой БД."""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management.base import CommandError
from django.db import connection
from django.test import SimpleTestCase, TestCase
from rasterio.transform import from_origin

# earthengine-api может быть не установлен локально — команда импортирует
# его на уровне модуля, а тесты GEE не трогают.
try:
    import ee  # noqa: F401
except ImportError:
    sys.modules['ee'] = mock.MagicMock(name='ee_stub')

from agrocosmos.management.commands.import_gee_croplands import Command  # noqa: E402
from agrocosmos.models import District, Farmland, Region  # noqa: E402

STAGING = 'staging_gee_croplands_test'


def _rect(x0, y0, x1, y1):
    return MultiPolygon(Polygon((
        (x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0),
    )))


class VectoriseTests(SimpleTestCase):
    """Маска 8×8 пикселей по 0.01°: полоса пашни в строках 2–3."""

    def _vectorise(self, block_px):
        arr = np.full((8, 8), np.nan, dtype='float32')
        arr[2:4, :] = 1.0
        with tempfile.TemporaryDirectory() as tmp:
            tif, out = Path(tmp) / 'mask.tif', Path(tmp) / 'out.geojson'
            with rasterio.open(
                tif, 'w', driver='GTiff', height=8, width=8, count=1,
                dtype='float32', nodata=float('nan'),
                transform=from_origin(30.0, 50.0, 0.01, 0.01),
            ) as dst:
                dst.write(arr, 1)
            count = Command._vectorise_to_geojson(tif, out, block_px=block_px)
            features = json.loads(out.read_text(encoding='utf-8'))['features']
        return count, features

    @staticmethod
    def _bbox(features):
        xs = [x for f in features for x, _ in f['geometry']['coordinates'][0]]
        ys = [y for f in features for _, y in f['geometry']['coordinates'][0]]
        return tuple(round(v, 4) for v in (min(xs), min(ys), max(xs), max(ys)))

    def test_single_block_gives_one_polygon(self):
        count, features = self._vectorise(block_px=512)
        self.assertEqual(count, 1)
        self.assertEqual(self._bbox(features), (30.0, 49.96, 30.08, 49.98))

    def test_blocks_split_polygon_but_keep_extent(self):
        """Полоса через границу блоков режется надвое, охват тот же."""
        count, features = self._vectorise(block_px=4)
        self.assertEqual(count, 2)
        self.assertEqual(len(features), 2)
        self.assertEqual(self._bbox(features), (30.0, 49.96, 30.08, 49.98))

    def test_empty_mask(self):
        with tempfile.TemporaryDirectory() as tmp:
            tif, out = Path(tmp) / 'mask.tif', Path(tmp) / 'out.geojson'
            with rasterio.open(
                tif, 'w', driver='GTiff', height=4, width=4, count=1,
                dtype='float32', nodata=float('nan'),
                transform=from_origin(30.0, 50.0, 0.01, 0.01),
            ) as dst:
                dst.write(np.full((4, 4), np.nan, dtype='float32'), 1)
            self.assertEqual(Command._vectorise_to_geojson(tif, out), 0)
            self.assertEqual(
                json.loads(out.read_text(encoding='utf-8'))['features'], [],
            )
            self.assertTrue(os.path.exists(out))


class InsertSqlTests(TestCase):
    """Регион 30..32 × 50..51 из двух районов (запад / восток)."""

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Область', code='KZ-1', country='KZ',
            geom=_rect(30, 50, 32, 51),
        )
        cls.west = District.objects.create(
            region=cls.region, name='Западный', geom=_rect(30, 50, 31, 51),
        )
        cls.east = District.objects.create(
            region=cls.region, name='Восточный', geom=_rect(31, 50, 32, 51),
        )

    def _stage(self, *polygons):
        with connection.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {STAGING}')
            cur.execute(
                f'CREATE TABLE {STAGING} (ogc_fid serial PRIMARY KEY, '
                f'wkb_geometry geometry(MultiPolygon, 4326))'
            )
            for poly in polygons:
                cur.execute(
                    f'INSERT INTO {STAGING} (wkb_geometry) '
                    f'VALUES (ST_GeomFromEWKT(%s))', [poly.ewkt],
                )

    def _insert(self, **kwargs):
        sql = Command._build_insert_sql(
            STAGING, self.region.pk, 'gee_test', 10000.0, **kwargs,
        )
        with connection.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount

    def test_clip_by_district_splits_polygon(self):
        # Полоса пашни 30.5..31.25: 2/3 в западном районе, 1/3 в восточном.
        self._stage(_rect(30.5, 50.2, 31.25, 50.4))
        self.assertEqual(self._insert(by_district=True), 2)

        by_district = {f.district_id: f for f in Farmland.objects.all()}
        self.assertEqual(set(by_district), {self.west.pk, self.east.pk})
        west, east = by_district[self.west.pk], by_district[self.east.pk]
        self.assertAlmostEqual(west.area_ha / east.area_ha, 2.0, places=2)
        self.assertEqual(
            tuple(round(v, 4) for v in west.geom.extent), (30.5, 50.2, 31.0, 50.4),
        )
        for f in (west, east):
            self.assertEqual(f.region_id, self.region.pk)
            self.assertEqual(f.crop_type, 'arable')
            self.assertEqual(f.source, 'gee_test')
            self.assertEqual(f.properties, {'source': 'gee_test', 'class': 40})

    def test_clip_by_district_drops_outside_and_small(self):
        self._stage(
            _rect(31.9, 50.2, 32.5, 50.4),      # выходит за регион → обрезка
            _rect(40.0, 50.0, 40.5, 50.5),      # целиком вне региона
            _rect(30.1, 50.1, 30.1005, 50.1005),  # < 1 га
        )
        self.assertEqual(self._insert(by_district=True), 1)
        f = Farmland.objects.get()
        self.assertEqual(f.district_id, self.east.pk)
        self.assertEqual(round(f.geom.extent[2], 4), 32.0)

    def test_clip_by_region_keeps_district_null(self):
        """Режим по умолчанию не изменился: один полигон, district NULL."""
        self._stage(_rect(30.5, 50.2, 31.25, 50.4))
        self.assertEqual(self._insert(), 1)
        f = Farmland.objects.get()
        self.assertIsNone(f.district_id)
        self.assertEqual(f.region_id, self.region.pk)
        self.assertEqual(
            tuple(round(v, 4) for v in f.geom.extent), (30.5, 50.2, 31.25, 50.4),
        )


class ResolveRegionIdsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.ru = Region.objects.create(
            name='РФ', code='ru-1', geom=_rect(30, 50, 31, 51))
        cls.kz = Region.objects.create(
            name='КЗ', code='KZ-1', country='KZ', geom=_rect(70, 50, 71, 51))

    def _resolve(self, regions='', country=''):
        return Command()._resolve_region_ids(
            {'regions': regions, 'country': country})

    def test_by_country(self):
        self.assertEqual(self._resolve(country='kz'), [self.kz.pk])

    def test_by_ids(self):
        self.assertEqual(self._resolve(regions='3, 5'), [3, 5])

    def test_errors(self):
        for kwargs in ({}, {'country': 'XX'}, {'country': 'BR'},
                       {'regions': '1', 'country': 'KZ'}):
            with self.assertRaises(CommandError, msg=kwargs):
                self._resolve(**kwargs)
