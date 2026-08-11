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
        payload = b'T' * gd.MIN_TILE_BYTES
        with mock.patch.object(gd, '_compute_pixels', return_value=payload):
            out = gd.download_tile('composite', 0, 0, 0.01, 0.01, 0.0001)
        self.assertEqual(out, payload)

    def test_too_large_tile_raises(self):
        with self.assertRaises(GEEError):
            gd.download_tile('composite', 0, 0, 10, 10, 0.0001)

    def test_compute_error_wrapped(self):
        with mock.patch.object(gd, '_compute_pixels',
                               side_effect=RuntimeError('quota')):
            with self.assertRaises(GEEError):
                gd.download_tile('composite', 0, 0, 0.01, 0.01, 0.0001)

    def test_empty_response_raises(self):
        # GEE изредка отдаёт пустое тело с HTTP 200 — не должно
        # превращаться в нулевой тайл на диске.
        with mock.patch.object(gd, '_compute_pixels', return_value=b''):
            with self.assertRaises(GEEError):
                gd.download_tile('composite', 0, 0, 0.01, 0.01, 0.0001)

    def test_truncated_response_raises(self):
        with mock.patch.object(gd, '_compute_pixels', return_value=b'x' * 10):
            with self.assertRaises(GEEError):
                gd.download_tile('composite', 0, 0, 0.01, 0.01, 0.0001)


class MergeTilesTests(SimpleTestCase):

    def test_merge_writes_with_bigtiff_if_safer(self):
        # Региональные мозаики (МО S2 = 56297×30088 px) превышают лимит
        # классического TIFF 4 ГБ → GDAL падал с "Write failed".
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tile_paths = []
        for i in range(2):
            p = Path(tmp.name) / f'ndvi_tile{i}.tif'
            p.write_bytes(b'TILE')
            tile_paths.append(str(p))
        out_path = str(Path(tmp.name) / 'ndvi.tif')

        mosaic = mock.MagicMock()
        mosaic.shape = (1, 2, 2)

        rio = mock.MagicMock(name='rasterio_stub')

        def _open(path, mode='r', **kwargs):
            ds = mock.MagicMock()
            ds.profile = {'driver': 'GTiff', 'count': 1, 'dtype': 'float32'}
            if mode == 'w':
                Path(path).write_bytes(b'MERGED')
            ds.__enter__ = mock.MagicMock(return_value=ds)
            ds.__exit__ = mock.MagicMock(return_value=False)
            return ds

        rio.open.side_effect = _open
        rio_merge = mock.MagicMock(name='rasterio.merge_stub')
        rio_merge.merge.return_value = (mosaic, 'transform')

        with mock.patch.dict(sys.modules, {'rasterio': rio,
                                           'rasterio.merge': rio_merge}):
            gd.merge_tiles(tile_paths, out_path)

        write_calls = [c for c in rio.open.call_args_list
                       if 'w' in c.args or c.kwargs.get('mode') == 'w']
        self.assertEqual(len(write_calls), 1)
        self.assertEqual(write_calls[0].kwargs.get('BIGTIFF'), 'IF_SAFER')
        self.assertTrue(Path(out_path).exists())
        # тайлы подчищены
        for p in tile_paths:
            self.assertFalse(Path(p).exists())


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

    def test_stale_tiles_removed_before_download(self):
        # Нулевые/битые тайлы от убитого прогона не должны пережить
        # новый запуск (иначе отравляют merge).
        base = self.out_path.replace('.tif', '')
        stale_tile = Path(f'{base}_tile7.tif')
        stale_part = Path(f'{base}.tif.part')
        stale_tile.write_bytes(b'')          # zero-size leftover
        stale_part.write_bytes(b'partial')

        with mock.patch.object(gd, 'tile_extents',
                               return_value=[(0, 0, 1, 1)]), \
             mock.patch.object(gd, 'download_tile', return_value=b'DATA'), \
             mock.patch.object(gd, 'merge_tiles'):
            gd.download_tiled_composite(
                'comp', (0, 0, 1, 1), 10, self.out_path,
            )
        self.assertFalse(stale_tile.exists())
        self.assertFalse(stale_part.exists())
        self.assertEqual(Path(self.out_path).read_bytes(), b'DATA')
