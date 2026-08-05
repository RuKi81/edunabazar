"""Тесты ``agrocosmos/services/gee_download.py`` — страховочная сетка
перед рефакторингом ``download_tiled_composite`` (C=13).

Сеть/GEE/rasterio мокаются: проверяется разбиение на тайлы, запись
одиночного тайла, параллельная выкачка с мержем, fail-fast и очистка
частичных файлов при ошибке.
"""
import sys
import tempfile
from pathlib import Path
from unittest import mock

from django.test import SimpleTestCase

for _mod in ('ee', 'rasterio', 'rasterio.merge'):
    if _mod not in sys.modules:
        try:
            __import__(_mod)
        except ImportError:  # локально пакеты GEE/rasterio не установлены
            sys.modules[_mod] = mock.MagicMock(name=f'{_mod}_stub')

from agrocosmos.services import gee_download as gd  # noqa: E402
from agrocosmos.services.satellite_gee import GEEError  # noqa: E402


class TileExtentsTests(SimpleTestCase):

    def test_single_tile_for_small_extent(self):
        tiles = gd.tile_extents(0, 0, 0.01, 0.01, scale_deg=0.0001)
        self.assertEqual(tiles, [(0, 0, 0.01, 0.01)])

    def test_splits_wide_extent(self):
        # 5000 px в ширину при max 2000 → 3 колонки
        tiles = gd.tile_extents(0, 0, 0.5, 0.01, scale_deg=0.0001)
        self.assertEqual(len(tiles), 3)
        self.assertAlmostEqual(tiles[0][2], 0.5 / 3)
        self.assertAlmostEqual(tiles[-1][2], 0.5)

    def test_grid_rows_and_cols(self):
        tiles = gd.tile_extents(0, 0, 0.5, 0.5, scale_deg=0.0001)
        self.assertEqual(len(tiles), 9)  # 3×3


class DownloadTileTests(SimpleTestCase):

    def test_returns_content(self):
        with mock.patch.object(gd, '_compute_pixels', return_value=b'tif'):
            out = gd.download_tile('composite', 0, 0, 0.01, 0.01, 0.0001)
        self.assertEqual(out, b'tif')

    def test_too_large_tile_raises(self):
        with self.assertRaises(GEEError):
            gd.download_tile('composite', 0, 0, 10, 10, 0.0001)

    def test_compute_error_wrapped(self):
        with mock.patch.object(gd, '_compute_pixels',
                               side_effect=RuntimeError('quota')):
            with self.assertRaises(GEEError):
                gd.download_tile('composite', 0, 0, 0.01, 0.01, 0.0001)


class DownloadTiledCompositeTests(SimpleTestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out_path = str(Path(self.tmp.name) / 'ndvi.tif')

        # финальный rasterio.open для логирования размеров
        patcher = mock.patch.dict(
            sys.modules, {'rasterio': mock.MagicMock(name='rasterio_stub')},
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_single_tile_written_directly(self):
        with mock.patch.object(gd, 'tile_extents',
                               return_value=[(0, 0, 1, 1)]), \
             mock.patch.object(gd, 'download_tile', return_value=b'DATA'), \
             mock.patch.object(gd, 'merge_tiles') as merge:
            result = gd.download_tiled_composite(
                'comp', (0, 0, 1, 1), 10, self.out_path,
            )
        self.assertEqual(result, self.out_path)
        self.assertEqual(Path(self.out_path).read_bytes(), b'DATA')
        merge.assert_not_called()

    def test_multi_tile_download_and_merge(self):
        tiles = [(0, 0, 1, 1), (1, 0, 2, 1), (0, 1, 1, 2)]

        def _fake_merge(paths, out):
            Path(out).write_bytes(b'MERGED')

        with mock.patch.object(gd, 'tile_extents', return_value=tiles), \
             mock.patch.object(gd, 'download_tile', return_value=b'T'), \
             mock.patch.object(gd, 'merge_tiles',
                               side_effect=_fake_merge) as merge:
            result = gd.download_tiled_composite(
                'comp', (0, 0, 2, 2), 10, self.out_path, sensor_label='S2',
            )
        self.assertEqual(result, self.out_path)
        (paths, out), _ = merge.call_args
        self.assertEqual(len(paths), 3)
        self.assertEqual(out, self.out_path)
        for p in paths:
            self.assertTrue(Path(p).exists())

    def test_tile_failure_raises_and_cleans_partials(self):
        tiles = [(0, 0, 1, 1), (1, 0, 2, 1)]

        def _dl(composite, tx0, ty0, tx1, ty1, scale_deg):
            if tx0 == 0:
                return b'OK'
            raise GEEError('boom')

        with mock.patch.object(gd, 'tile_extents', return_value=tiles), \
             mock.patch.object(gd, 'download_tile', side_effect=_dl), \
             mock.patch.object(gd, 'merge_tiles') as merge:
            with self.assertRaises(GEEError):
                gd.download_tiled_composite(
                    'comp', (0, 0, 2, 1), 10, self.out_path,
                )
        merge.assert_not_called()
        leftovers = list(Path(self.tmp.name).glob('*_tile*.tif'))
        self.assertEqual(leftovers, [])
