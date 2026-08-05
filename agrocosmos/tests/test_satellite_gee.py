"""Тесты ``agrocosmos/services/satellite_gee.py`` — страховочная сетка
перед рефакторингом ``fetch_ndvi_batch`` (C=13) и
``fetch_modis_ndvi_batch`` (C=13).

Earth Engine мокается целиком: проверяется нормализация дат, средняя
дата композита, разбор ответа reduceRegions (фильтр по valid_ratio,
округление, группировка по fl_id) и обработка ошибок GEE.
"""
import sys
from datetime import date
from unittest import mock

from django.test import SimpleTestCase

try:
    import ee  # noqa: F401
except ImportError:  # локально earthengine-api может быть не установлен
    sys.modules['ee'] = mock.MagicMock(name='ee_stub')

from agrocosmos.services import satellite_gee as sg  # noqa: E402


def _props(fl_id=1, total=100, valid=98, mean=0.6512345,
           vmin=0.1, vmax=0.9, std=0.05):
    return {
        'fl_id': fl_id, 'total_count': total, 'NDVI_count': valid,
        'NDVI_mean': mean, 'NDVI_min': vmin, 'NDVI_max': vmax,
        'NDVI_stdDev': std,
    }


def _payload(*props_list):
    return {'features': [{'properties': p} for p in props_list]}


class GEEBatchTestCase(SimpleTestCase):
    """Общий мок цепочек ee.* для обеих batch-функций."""

    FARMLANDS = [
        {'id': 1, 'geometry': {'type': 'Polygon', 'coordinates': []}},
        {'id': 2, 'geometry': {'type': 'Polygon', 'coordinates': []}},
    ]

    def setUp(self):
        patcher_init = mock.patch.object(sg, 'initialize')
        patcher_init.start()
        self.addCleanup(patcher_init.stop)

        patcher_ee = mock.patch.object(sg, 'ee')
        self.ee = patcher_ee.start()
        self.addCleanup(patcher_ee.stop)

        # Коллекция изображений: s2/modis chain → self.col
        self.col = mock.MagicMock(name='image_collection')
        self.col.size.return_value.getInfo.return_value = 3
        (self.ee.ImageCollection.return_value
         .filterDate.return_value
         .filterBounds.return_value
         .filter.return_value) = self.col
        # MODIS: terra.filterBounds() → terra_col; terra_col.merge(aqua) → col
        (self.ee.ImageCollection.return_value
         .filterDate.return_value
         .filterBounds.return_value
         .merge.return_value) = self.col

        # composite/stacked chain → reduceRegions().getInfo() → payload
        stacked = mock.MagicMock(name='stacked')
        composite = self.col.map.return_value.median.return_value.rename.return_value
        composite.addBands.return_value = stacked
        self.get_info = stacked.reduceRegions.return_value.getInfo
        self.get_info.return_value = _payload()

    def _set_n_images(self, n):
        self.col.size.return_value.getInfo.return_value = n


class FetchNdviBatchTests(GEEBatchTestCase):

    def test_groups_by_farmland_and_uses_mid_date(self):
        self.get_info.return_value = _payload(
            _props(fl_id=1), _props(fl_id=2, mean=0.4),
        )
        out = sg.fetch_ndvi_batch(self.FARMLANDS, '2026-06-01', '2026-06-30')
        self.assertEqual(set(out), {1, 2})
        rec = out[1][0]
        self.assertEqual(rec['date'], '2026-06-15')  # середина периода
        self.assertEqual(rec['mean'], 0.6512)  # округление до 4 знаков
        self.assertEqual(rec['median'], rec['mean'])  # median = mean композита
        self.assertEqual(rec['pixel_count'], 100)
        self.assertEqual(rec['valid_ratio'], 0.98)

    def test_accepts_date_objects(self):
        self.get_info.return_value = _payload(_props())
        out = sg.fetch_ndvi_batch(
            self.FARMLANDS, date(2026, 6, 1), date(2026, 6, 30),
        )
        self.assertEqual(out[1][0]['date'], '2026-06-15')

    def test_filters_low_valid_ratio_and_broken_features(self):
        self.get_info.return_value = _payload(
            _props(fl_id=1, total=100, valid=50),   # ratio 0.5 < 0.95
            _props(fl_id=None),                      # нет fl_id
            _props(fl_id=3, mean=None),              # нет mean
            _props(fl_id=4, total=0),                # нет пикселей
            _props(fl_id=5),                         # проходит
        )
        out = sg.fetch_ndvi_batch(self.FARMLANDS, '2026-06-01', '2026-06-30')
        self.assertEqual(set(out), {5})

    def test_min_valid_ratio_override(self):
        self.get_info.return_value = _payload(_props(total=100, valid=50))
        out = sg.fetch_ndvi_batch(
            self.FARMLANDS, '2026-06-01', '2026-06-30', min_valid_ratio=0.4,
        )
        self.assertEqual(set(out), {1})

    def test_no_images_returns_empty(self):
        self._set_n_images(0)
        out = sg.fetch_ndvi_batch(self.FARMLANDS, '2026-06-01', '2026-06-30')
        self.assertEqual(out, {})

    def test_gee_error_wrapped(self):
        self.col.size.return_value.getInfo.side_effect = RuntimeError('quota')
        with self.assertRaises(sg.GEEError):
            sg.fetch_ndvi_batch(self.FARMLANDS, '2026-06-01', '2026-06-30')


class FetchModisNdviBatchTests(GEEBatchTestCase):

    def test_happy_path(self):
        self.get_info.return_value = _payload(
            _props(fl_id=1, total=10, valid=6, mean=0.55),
        )
        out = sg.fetch_modis_ndvi_batch(
            self.FARMLANDS, '2026-06-01', '2026-06-30',
        )
        rec = out[1][0]
        self.assertEqual(rec['date'], '2026-06-15')
        self.assertEqual(rec['mean'], 0.55)
        self.assertEqual(rec['valid_ratio'], 0.6)

    def test_default_ratio_is_lenient(self):
        # 0.5 проходит для MODIS (default 0.5), но не для S2 (0.95)
        self.get_info.return_value = _payload(_props(total=100, valid=50))
        out = sg.fetch_modis_ndvi_batch(
            self.FARMLANDS, '2026-06-01', '2026-06-30',
        )
        self.assertEqual(set(out), {1})

    def test_no_images_returns_empty(self):
        self._set_n_images(0)
        out = sg.fetch_modis_ndvi_batch(
            self.FARMLANDS, '2026-06-01', '2026-06-30',
        )
        self.assertEqual(out, {})

    def test_gee_error_wrapped(self):
        self.col.size.return_value.getInfo.side_effect = RuntimeError('boom')
        with self.assertRaises(sg.GEEError):
            sg.fetch_modis_ndvi_batch(
                self.FARMLANDS, '2026-06-01', '2026-06-30',
            )
