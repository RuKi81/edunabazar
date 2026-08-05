"""Тесты оркестратора ``run_ndvi_pipeline`` — страховочная сетка перед
рефакторингом ``handle`` (C=25). Стадии (внутренний ``call_command``)
мокаются; проверяем статусы PipelineRun, маршрутизацию стадий и парсинг
счётчика записей.
"""
import io
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.models import District, PipelineRun, Region

MOD = 'agrocosmos.management.commands.run_ndvi_pipeline'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


class RunNdviPipelineTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50))

    def setUp(self):
        self.run = PipelineRun.objects.create(
            task_type=PipelineRun.TaskType.RASTER_NDVI,
            status=PipelineRun.Status.QUEUED,
        )

    def _run(self, inner=None, **kwargs):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch(f'{MOD}.call_command', side_effect=inner) as mock_cc:
            call_command(
                'run_ndvi_pipeline',
                run_id=self.run.pk, year=2025,
                stdout=out, stderr=err,
                **kwargs,
            )
        return out.getvalue(), err.getvalue(), mock_cc

    def test_requires_scope(self):
        _, err, mock_cc = self._run()
        self.assertIn('--region-id or --district-id required', err)
        mock_cc.assert_not_called()
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.FAILED)

    def test_unknown_district_marks_failed(self):
        self._run(district_id=999999)
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.FAILED)
        self.assertIsNotNone(self.run.finished_at)

    def test_happy_path_s2_l8(self):
        def inner(name, **kw):
            kw['stdout'].write('  → 5 records saved\n')

        self._run(inner=inner, region_id=self.region.pk)
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.COMPLETED)
        self.assertIsNotNone(self.run.pid)
        self.assertIsNotNone(self.run.finished_at)
        # 5 записей × 2 сенсора
        self.assertEqual(self.run.records_count, 10)
        self.assertIn('Pipeline completed', self.run.log)

    def test_stage_routing_and_kwargs(self):
        _, _, mock_cc = self._run(region_id=self.region.pk)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names, ['fetch_raster_ndvi', 'fetch_raster_ndvi'])
        sensors = [c.kwargs['sensor'] for c in mock_cc.call_args_list]
        self.assertEqual(sensors, ['s2', 'l8'])
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['year'], 2025)
        self.assertEqual(kw['region_id'], self.region.pk)
        self.assertEqual(kw['min_valid_ratio'], 0.7)

    def test_skip_flags(self):
        _, _, mock_cc = self._run(region_id=self.region.pk, skip_s2=True)
        sensors = [c.kwargs['sensor'] for c in mock_cc.call_args_list]
        self.assertEqual(sensors, ['l8'])

    def test_fusion_stages(self):
        _, _, mock_cc = self._run(
            region_id=self.region.pk, skip_s2=True, skip_l8=True,
            fusion=True)
        names = [c.args[0] for c in mock_cc.call_args_list]
        self.assertEqual(names, ['compute_fused_ndvi', 'ndvi_postprocess'])
        self.assertEqual(mock_cc.call_args_list[0].kwargs['overwrite'], True)
        self.assertEqual(mock_cc.call_args_list[1].kwargs['source'], 'fused')

    def test_district_scope_fills_region(self):
        _, _, mock_cc = self._run(district_id=self.district.pk)
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['district_id'], self.district.pk)
        self.assertNotIn('region_id', kw)

    def test_date_window_overrides_year(self):
        _, _, mock_cc = self._run(
            region_id=self.region.pk,
            date_from='2025-06-01', date_to='2025-06-30')
        kw = mock_cc.call_args_list[0].kwargs
        self.assertEqual(kw['date_from'], '2025-06-01')
        self.assertEqual(kw['date_to'], '2025-06-30')
        self.assertNotIn('year', kw)

    def test_stage_failure_marks_failed_and_reraises(self):
        with self.assertRaises(RuntimeError):
            self._run(inner=RuntimeError('boom'), region_id=self.region.pk)
        self.run.refresh_from_db()
        self.assertEqual(self.run.status, PipelineRun.Status.FAILED)
        self.assertIsNotNone(self.run.finished_at)
        self.assertIn('boom', self.run.log)
