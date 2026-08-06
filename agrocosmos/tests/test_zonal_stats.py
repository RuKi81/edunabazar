"""
Тесты зональной статистики (``services/zonal_stats.py``).

Покрывается: гистограмма NDVI по 5 бинам (клип отрицательных в первый
бин, NDVI=1.0 в последний), интеграционный расчёт compute_zonal_stats
на синтетическом GeoTIFF (стата + гистограмма + фильтры nodata и
физического диапазона), min_valid_ratio.
"""
import os
import tempfile

import numpy as np
import rasterio
from django.test import SimpleTestCase
from rasterio.transform import from_origin

from agrocosmos.services.zonal_stats import (
    _ndvi_histogram, compute_zonal_stats,
)


class NdviHistogramTests(SimpleTestCase):

    def test_bins_and_clipping(self):
        vals = np.array([-0.1, 0.05, 0.25, 0.5, 0.85, 1.0])
        # -0.1 и 0.05 → бин 0; 0.25 → 1; 0.5 → 2; 0.85 и 1.0 → 4
        self.assertEqual(_ndvi_histogram(vals), [2, 1, 1, 0, 2])

    def test_bin_edges(self):
        vals = np.array([0.0, 0.2, 0.4, 0.6, 0.8])
        # левая граница включается в следующий бин (0.2 → бин 1 и т.д.)
        self.assertEqual(_ndvi_histogram(vals), [1, 1, 1, 1, 1])

    def test_sum_equals_pixel_count(self):
        vals = np.random.default_rng(42).uniform(-0.2, 1.0, size=1000)
        self.assertEqual(sum(_ndvi_histogram(vals)), 1000)


class ComputeZonalStatsTests(SimpleTestCase):
    """Синтетический GeoTIFF 4×4, пиксель = 1°, origin (0, 4)."""

    NODATA = -9999.0

    def _write_tif(self, data):
        fd, path = tempfile.mkstemp(suffix='.tif')
        os.close(fd)
        self.addCleanup(os.remove, path)
        with rasterio.open(
            path, 'w', driver='GTiff',
            height=data.shape[0], width=data.shape[1], count=1,
            dtype='float32', crs='EPSG:4326',
            transform=from_origin(0, 4, 1, 1), nodata=self.NODATA,
        ) as ds:
            ds.write(data.astype('float32'), 1)
        return path

    @staticmethod
    def _square(xmin, ymin, xmax, ymax):
        return {
            'type': 'Polygon',
            'coordinates': [[
                (xmin, ymin), (xmax, ymin), (xmax, ymax),
                (xmin, ymax), (xmin, ymin),
            ]],
        }

    def test_stats_and_histogram(self):
        data = np.full((4, 4), 0.5)
        data[0, 0] = 0.1          # бин 0
        data[0, 1] = 0.9          # бин 4
        data[1, 0] = self.NODATA  # невалидный
        data[1, 1] = 1.5          # вне физического диапазона → невалидный
        path = self._write_tif(data)

        results = compute_zonal_stats(
            path, [{'id': 7, 'geometry': self._square(0, 0, 4, 4)}],
        )
        self.assertIn(7, results)
        st = results[7]
        self.assertEqual(st['pixel_count'], 16)
        self.assertEqual(st['valid_pixel_count'], 14)
        # 0.1 → бин 0; 12×0.5 → бин 2; 0.9 → бин 4
        self.assertEqual(st['histogram'], [1, 0, 12, 0, 1])
        self.assertEqual(sum(st['histogram']), st['valid_pixel_count'])
        self.assertEqual(st['min'], 0.1)
        self.assertEqual(st['max'], 0.9)

    def test_bimodal_field(self):
        # Левая половина поля деградирует (0.15), правая здорова (0.75)
        data = np.hstack([np.full((4, 2), 0.15), np.full((4, 2), 0.75)])
        path = self._write_tif(data)

        results = compute_zonal_stats(
            path, [{'id': 1, 'geometry': self._square(0, 0, 4, 4)}],
        )
        self.assertEqual(results[1]['histogram'], [8, 0, 0, 8, 0])

    def test_min_valid_ratio_filters_polygon(self):
        data = np.full((4, 4), self.NODATA)
        data[0, 0] = 0.5  # 1/16 валидных
        path = self._write_tif(data)

        results = compute_zonal_stats(
            path, [{'id': 1, 'geometry': self._square(0, 0, 4, 4)}],
            min_valid_ratio=0.5,
        )
        self.assertEqual(results, {})

    def test_missing_file_returns_empty(self):
        results = compute_zonal_stats(
            'C:/nope/missing.tif',
            [{'id': 1, 'geometry': self._square(0, 0, 4, 4)}],
        )
        self.assertEqual(results, {})
