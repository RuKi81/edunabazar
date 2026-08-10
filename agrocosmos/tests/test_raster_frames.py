"""
Тесты «Снимков NDVI» и «Зон неоднородности» паспорта поля:

* ``/agrocosmos/api/farmland/raster-frames/`` — последние N композитов,
  покрывающих угодье (скоуп: район → регион, сенсор: S2 → L8);
* ``/agrocosmos/api/raster-preview/`` — PNG-превью растра по bbox поля;
* ``/agrocosmos/api/farmland/zones/`` — карта зон внутри поля vs медиана
  (+ динамика к предыдущему композиту и выбор композита через date/sensor).
"""
import os
import tempfile
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings

from agrocosmos.models import District, Farmland, Region

YEAR = '2025'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _touch_composites(root, scope, dates, prefix='s2_ndvi', year=YEAR):
    """Создать пустые файлы композитов ``{root}/{scope}/{year}/…tif``."""
    d = os.path.join(root, scope, year)
    os.makedirs(d, exist_ok=True)
    for date_from, date_to in dates:
        path = os.path.join(d, f'{prefix}_{scope}_{date_from}_{date_to}.tif')
        with open(path, 'wb') as f:
            f.write(b'\x00')


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class FarmlandRasterFramesApiTests(TestCase):
    URL = '/agrocosmos/api/farmland/raster-frames/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Тестовый регион', code='r-test', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тестовый район', geom=_square(30, 50),
        )
        cls.farmland = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(30.1, 50.1, 0.05),
        )

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_requires_farmland(self):
        self.assertEqual(self._get().status_code, 400)
        self.assertEqual(self._get(farmland='abc').status_code, 400)

    def test_unknown_farmland_404(self):
        self.assertEqual(self._get(farmland='999999').status_code, 404)

    def test_no_composites_empty_frames(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['frames'], [])

    def test_latest_five_district_scope_latest_first(self):
        dates = [(f'2025-0{m}-01', f'2025-0{m}-05') for m in range(1, 8)]
        with tempfile.TemporaryDirectory() as tmp:
            _touch_composites(tmp, f'd{self.district.pk}', dates)
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['sensor'], 's2')
        self.assertEqual(data['scope'], f'd{self.district.pk}')
        self.assertEqual(len(data['frames']), 5)
        # Последний композит первым.
        self.assertEqual(data['frames'][0]['date_from'], '2025-07-01')
        self.assertEqual(data['frames'][-1]['date_from'], '2025-03-01')
        self.assertEqual(data['frames'][0]['date'], '2025-07-01_2025-07-05')
        self.assertEqual(data['frames'][0]['sensor'], 's2')

    def test_falls_back_to_region_scope_and_l8(self):
        dates = [('2025-06-01', '2025-06-16')]
        with tempfile.TemporaryDirectory() as tmp:
            _touch_composites(tmp, str(self.region.pk), dates,
                              prefix='landsat_ndvi')
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['sensor'], 'l8')
        self.assertEqual(data['scope'], str(self.region.pk))
        self.assertEqual(len(data['frames']), 1)
        self.assertEqual(data['frames'][0]['sensor'], 'l8')

    def test_merges_s2_and_l8_preferring_s2(self):
        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            # S2: июнь; L8: тот же июнь (дубль) + июль (закрывает пропуск).
            _touch_composites(tmp, scope, [('2025-06-01', '2025-06-05')])
            _touch_composites(tmp, scope, [('2025-06-01', '2025-06-05'),
                                           ('2025-07-01', '2025-07-05')],
                              prefix='landsat_ndvi')
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['sensors'], ['l8', 's2'])
        self.assertEqual(len(data['frames']), 2)
        # Июльский период есть только у L8, июньский дубль — за S2.
        self.assertEqual(data['frames'][0]['sensor'], 'l8')
        self.assertEqual(data['frames'][0]['date_from'], '2025-07-01')
        self.assertEqual(data['frames'][1]['sensor'], 's2')
        self.assertEqual(data['frames'][1]['date_from'], '2025-06-01')

    def test_limit_param(self):
        dates = [(f'2025-0{m}-01', f'2025-0{m}-05') for m in range(1, 8)]
        with tempfile.TemporaryDirectory() as tmp:
            _touch_composites(tmp, f'd{self.district.pk}', dates)
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR, limit=2)
        self.assertEqual(len(resp.json()['frames']), 2)


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class RasterPreviewApiTests(TestCase):
    URL = '/agrocosmos/api/raster-preview/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r-prev', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50),
        )
        cls.farmland = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(30.1, 50.1, 0.05),
        )

    def test_missing_params_204(self):
        resp = self.client.get(self.URL)
        self.assertEqual(resp.status_code, 204)

    def test_unknown_farmland_204(self):
        resp = self.client.get(self.URL, {
            'farmland': '999999', 'scope': 'd1',
            'date': '2025-06-01_2025-06-05',
        })
        self.assertEqual(resp.status_code, 204)

    def test_missing_raster_204(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self.client.get(self.URL, {
                    'farmland': self.farmland.pk,
                    'scope': f'd{self.district.pk}',
                    'date': '2025-06-01_2025-06-05',
                })
        self.assertEqual(resp.status_code, 204)

    def test_renders_png_from_geotiff(self):
        try:
            import numpy as np
            import rasterio
            from rasterio.transform import from_bounds as tf_from_bounds
        except ImportError:  # pragma: no cover
            self.skipTest('rasterio не установлен')

        # Локальный PostGIS может подменять proj.db несовместимой версией —
        # используем базу PROJ из поставки rasterio.
        proj_env = {}
        proj_data = os.path.join(os.path.dirname(rasterio.__file__), 'proj_data')
        if os.path.isdir(proj_data):
            proj_env = {'PROJ_LIB': proj_data, 'PROJ_DATA': proj_data}

        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, scope, YEAR)
            os.makedirs(d)
            tif = os.path.join(
                d, f's2_ndvi_{scope}_2025-06-01_2025-06-05.tif',
            )
            # NDVI=0.6 на квадрате, накрывающем поле (30.1..30.15, 50.1..50.15).
            data = np.full((64, 64), 0.6, dtype='float32')
            transform = tf_from_bounds(30.0, 50.0, 30.3, 50.3, 64, 64)
            try:
                with mock.patch.dict(os.environ, proj_env), rasterio.Env():
                    with rasterio.open(
                        tif, 'w', driver='GTiff', height=64, width=64, count=1,
                        dtype='float32', crs='EPSG:4326', transform=transform,
                        nodata=-9999.0,
                    ) as ds:
                        ds.write(data, 1)
            except rasterio.errors.CRSError:  # pragma: no cover
                self.skipTest('несовместимый proj.db в окружении')

            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp}):
                resp = self.client.get(self.URL, {
                    'farmland': self.farmland.pk,
                    'sensor': 's2',
                    'scope': scope,
                    'date': '2025-06-01_2025-06-05',
                })
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'], 'image/png')
        self.assertTrue(resp.content.startswith(b'\x89PNG'))


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class FarmlandZonesApiTests(TestCase):
    URL = '/agrocosmos/api/farmland/zones/'

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r-zones', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50),
        )
        cls.farmland = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(30.1, 50.1, 0.05),
        )

    def _get(self, **params):
        return self.client.get(self.URL, params)

    def test_requires_farmland(self):
        self.assertEqual(self._get().status_code, 400)

    def test_unknown_farmland_404(self):
        self.assertEqual(self._get(farmland='999999').status_code, 404)

    def test_no_rasters_zones_null(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertIsNone(data['zones'])

    def _write_tif(self, path, fill=0.8, right_half=None):
        """GeoTIFF 64×64 на bbox (30..30.3, 50..50.3); NDVI=fill,
        правая половина — right_half (если задана). Skip без rasterio/proj."""
        try:
            import numpy as np
            import rasterio
            from rasterio.transform import from_bounds as tf_from_bounds
        except ImportError:  # pragma: no cover
            self.skipTest('rasterio не установлен')

        # Локальный PostGIS может подменять proj.db несовместимой версией —
        # используем базу PROJ из поставки rasterio.
        proj_env = {}
        proj_data = os.path.join(os.path.dirname(rasterio.__file__), 'proj_data')
        if os.path.isdir(proj_data):
            proj_env = {'PROJ_LIB': proj_data, 'PROJ_DATA': proj_data}

        data = np.full((64, 64), fill, dtype='float32')
        if right_half is not None:
            data[:, 27:] = right_half
        transform = tf_from_bounds(30.0, 50.0, 30.3, 50.3, 64, 64)
        try:
            with mock.patch.dict(os.environ, proj_env), rasterio.Env():
                with rasterio.open(
                    path, 'w', driver='GTiff', height=64, width=64, count=1,
                    dtype='float32', crs='EPSG:4326', transform=transform,
                    nodata=-9999.0,
                ) as ds:
                    ds.write(data, 1)
        except rasterio.errors.CRSError:  # pragma: no cover
            self.skipTest('несовместимый proj.db в окружении')

    def test_zone_map_and_stats(self):
        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, scope, YEAR)
            os.makedirs(d)
            # Левая половина растра NDVI=0.8, правая — 0.3: внутри поля
            # (30.1..30.15) появятся и «норма», и «проблемные» зоны.
            self._write_tif(
                os.path.join(d, f's2_ndvi_{scope}_2025-06-01_2025-06-05.tif'),
                fill=0.8, right_half=0.3,
            )

            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)

        self.assertEqual(resp.status_code, 200)
        zones = resp.json()['zones']
        self.assertIsNotNone(zones)
        self.assertEqual(zones['sensor'], 's2')
        self.assertEqual(zones['date_from'], '2025-06-01')
        self.assertTrue(zones['image'].startswith('data:image/png;base64,'))
        # Единственный композит — динамики нет.
        self.assertIsNone(zones['dynamics'])
        stats = zones['stats']
        self.assertGreater(stats['pixels'], 0)
        # Обе зоны присутствуют, доли в сумме ~100%.
        self.assertGreater(stats['problem_pct'], 5)
        self.assertGreater(stats['ok_pct'], 5)
        total = stats['problem_pct'] + stats['warn_pct'] + stats['ok_pct']
        self.assertAlmostEqual(total, 100.0, delta=1.0)

    def test_dynamics_between_two_composites(self):
        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, scope, YEAR)
            os.makedirs(d)
            # Июнь: поле однородное (0.8); июль: правая половина просела до 0.3
            # — ожидаем заметную долю «деградации» в динамике.
            self._write_tif(
                os.path.join(d, f's2_ndvi_{scope}_2025-06-01_2025-06-05.tif'),
                fill=0.8,
            )
            self._write_tif(
                os.path.join(d, f's2_ndvi_{scope}_2025-07-01_2025-07-05.tif'),
                fill=0.8, right_half=0.3,
            )

            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(farmland=self.farmland.pk, year=YEAR)

        zones = resp.json()['zones']
        self.assertIsNotNone(zones)
        self.assertEqual(zones['date_from'], '2025-07-01')
        dyn = zones['dynamics']
        self.assertIsNotNone(dyn)
        self.assertEqual(dyn['prev_date_from'], '2025-06-01')
        self.assertEqual(dyn['prev_sensor'], 's2')
        self.assertTrue(dyn['image'].startswith('data:image/png;base64,'))
        stats = dyn['stats']
        self.assertGreater(stats['degraded_pct'], 5)
        total = (stats['degraded_pct'] + stats['improved_pct']
                 + stats['stable_pct'])
        self.assertAlmostEqual(total, 100.0, delta=1.0)

    KML_URL = '/agrocosmos/api/farmland/zones/kml/'

    def test_kml_requires_farmland(self):
        self.assertEqual(self.client.get(self.KML_URL).status_code, 400)

    def test_kml_no_rasters_404(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self.client.get(self.KML_URL, {
                    'farmland': self.farmland.pk, 'year': YEAR,
                })
        self.assertEqual(resp.status_code, 404)

    def test_kml_export_zones(self):
        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, scope, YEAR)
            os.makedirs(d)
            # Правая половина 0.3 — гарантирует «проблемные» полигоны.
            self._write_tif(
                os.path.join(d, f's2_ndvi_{scope}_2025-06-01_2025-06-05.tif'),
                fill=0.8, right_half=0.3,
            )

            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self.client.get(self.KML_URL, {
                    'farmland': self.farmland.pk, 'year': YEAR,
                })

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'],
                         'application/vnd.google-earth.kml+xml')
        self.assertIn('attachment', resp['Content-Disposition'])
        kml = resp.content.decode()
        self.assertIn('<kml', kml)
        self.assertIn('Проблемная зона', kml)
        self.assertIn('Точка осмотра', kml)
        self.assertIn('<Polygon>', kml)
        # KML валиден как XML.
        import xml.etree.ElementTree as ET
        ET.fromstring(kml)

    SHP_URL = '/agrocosmos/api/farmland/zones/shp/'

    def test_shp_invalid_rate_400(self):
        resp = self.client.get(self.SHP_URL, {
            'farmland': self.farmland.pk, 'rate_problem': 'abc',
        })
        self.assertEqual(resp.status_code, 400)

    def test_shp_prescription_export(self):
        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, scope, YEAR)
            os.makedirs(d)
            self._write_tif(
                os.path.join(d, f's2_ndvi_{scope}_2025-06-01_2025-06-05.tif'),
                fill=0.8, right_half=0.3,
            )

            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self.client.get(self.SHP_URL, {
                    'farmland': self.farmland.pk, 'year': YEAR,
                    'rate_problem': '12.5', 'rate_warn': '10',
                    'rate_ok': '8',
                })

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'], 'application/zip')
        self.assertIn('attachment', resp['Content-Disposition'])

        import io
        import zipfile
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            exts = {n.rsplit('.', 1)[-1] for n in z.namelist()}
            self.assertEqual(exts, {'shp', 'shx', 'dbf', 'prj'})
            shp_name = [n for n in z.namelist() if n.endswith('.shp')][0]
            base = shp_name[:-4]
            # Читаем shapefile обратно и проверяем атрибуты и нормы.
            import shapefile as pyshp
            with pyshp.Reader(
                shp=io.BytesIO(z.read(base + '.shp')),
                shx=io.BytesIO(z.read(base + '.shx')),
                dbf=io.BytesIO(z.read(base + '.dbf')),
            ) as reader:
                fields = [f[0] for f in reader.fields[1:]]
                self.assertEqual(fields, ['ZONE', 'RATE', 'AREA_HA'])
                zone_rates = {}
                for rec in reader.records():
                    zone_rates[rec['ZONE']] = rec['RATE']
                self.assertEqual(zone_rates.get('problem'), 12.5)
                self.assertEqual(zone_rates.get('ok'), 8.0)
                # Геометрия в WGS84 внутри bbox поля с запасом.
                shape = reader.shapes()[0]
                xs = [p[0] for p in shape.points]
                self.assertTrue(all(29.9 < x < 30.4 for x in xs))

    def test_explicit_date_selects_composite(self):
        scope = f'd{self.district.pk}'
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, scope, YEAR)
            os.makedirs(d)
            for rng in ('2025-05-01_2025-05-05', '2025-06-01_2025-06-05',
                        '2025-07-01_2025-07-05'):
                self._write_tif(
                    os.path.join(d, f's2_ndvi_{scope}_{rng}.tif'), fill=0.8,
                )

            with mock.patch.dict(os.environ, {'S2_RASTER_DIR': tmp,
                                              'LANDSAT_RASTER_DIR': tmp}):
                resp = self._get(
                    farmland=self.farmland.pk, year=YEAR,
                    sensor='s2', date='2025-06-01_2025-06-05',
                )

        zones = resp.json()['zones']
        self.assertIsNotNone(zones)
        # Выбран именно июньский, а динамика — к майскому (не к июльскому).
        self.assertEqual(zones['date_from'], '2025-06-01')
        self.assertIsNotNone(zones['dynamics'])
        self.assertEqual(zones['dynamics']['prev_date_from'], '2025-05-01')
