"""Сконвертировать растровый слой в COG в фоне (по queued ``PipelineRun``).

Запускается воркером ``run_ndvi_worker`` (task_type=``raster_ingest``) или
вручную через ``call_command('run_raster_ingest', run_id=<id>)``. Слой берётся
из ``PipelineRun.launch_args``::

    {"layer_id": <int>}

Команда управляет статусом ``PipelineRun`` (running → completed/failed) и
статусом самого :class:`RasterLayer` (``processing`` → ``ready``/``failed``);
тяжёлую конвертацию делает :func:`my_fields.services.raster_ingest.ingest_raster_layer`.
"""
from __future__ import annotations

import os
import traceback

from django.core.management.base import BaseCommand
from django.utils import timezone

from agrocosmos.models import PipelineRun


class Command(BaseCommand):
    help = 'Сконвертировать растровый слой в COG для queued PipelineRun.'

    def add_arguments(self, parser):
        parser.add_argument('--run-id', type=int, required=True,
                            help='id строки PipelineRun (task_type=raster_ingest).')
        parser.add_argument('--layer-id', type=int,
                            help='id RasterLayer (для ручного запуска).')

    def handle(self, *args, **options):
        run_id = options['run_id']
        run = PipelineRun.objects.filter(pk=run_id).first()
        if run is None:
            self.stderr.write(f'run #{run_id} not found')
            return

        args_src = dict(run.launch_args or {})
        layer_id = options.get('layer_id') or args_src.get('layer_id')

        self._bootstrap(run_id)

        try:
            layer = self._execute(run_id, layer_id)
        except Exception:
            tb = traceback.format_exc()
            self._log(run_id, f'!!! Raster ingest failed !!!\n{tb[-4000:]}')
            self._mark_layer_failed(layer_id, tb)
            PipelineRun.objects.filter(pk=run_id).exclude(
                status=PipelineRun.Status.COMPLETED,
            ).update(
                status=PipelineRun.Status.FAILED,
                finished_at=timezone.now(),
            )
            raise

        PipelineRun.objects.filter(pk=run_id).update(
            status=PipelineRun.Status.COMPLETED,
            finished_at=timezone.now(),
            heartbeat_at=timezone.now(),
            records_count=layer.band_count,
        )
        self._log(run_id,
                  f'Raster ingest completed → слой #{layer.pk} '
                  f'"{layer.title}" (COG {layer.cog_key})')

    # ------------------------------------------------------------------

    def _execute(self, run_id, layer_id):
        from my_fields.models import RasterLayer
        from my_fields.services.raster_ingest import (
            RasterIngestError, ingest_raster_layer,
        )

        if not layer_id:
            raise RasterIngestError('Не задан слой (layer_id).')
        layer = RasterLayer.objects.filter(pk=layer_id).first()
        if layer is None:
            raise RasterIngestError(f'Слой #{layer_id} не найден.')

        self.stdout.write(f'[raster_ingest] run: слой #{layer_id} "{layer.title}"')
        return ingest_raster_layer(
            layer, log=lambda m: self._log(run_id, m))

    @staticmethod
    def _mark_layer_failed(layer_id, tb: str) -> None:
        if not layer_id:
            return
        try:
            from my_fields.models import RasterLayer
            tail = tb.strip().splitlines()[-1] if tb else 'ошибка конвертации'
            RasterLayer.objects.filter(pk=layer_id).update(
                status=RasterLayer.Status.FAILED,
                error=tail[:500],
                updated_at=timezone.now(),
            )
        except Exception as exc:  # noqa: BLE001
            print(f'[raster_ingest fail-mark] warning: {exc}', flush=True)

    @staticmethod
    def _bootstrap(run_id: int) -> None:
        try:
            PipelineRun.objects.filter(pk=run_id).update(
                pid=os.getpid(),
                status=PipelineRun.Status.RUNNING,
                heartbeat_at=timezone.now(),
            )
        except Exception as exc:  # noqa: BLE001
            print(f'[raster_ingest bootstrap] warning: {exc}', flush=True)

    @staticmethod
    def _log(run_id: int, message: str) -> None:
        try:
            run = PipelineRun.objects.filter(pk=run_id).only('log').first()
            prev = (run.log or '') if run else ''
            PipelineRun.objects.filter(pk=run_id).update(
                log=(prev + ('\n' if prev else '') + message)[-8000:],
                heartbeat_at=timezone.now(),
            )
        except Exception as exc:  # noqa: BLE001
            print(f'[raster_ingest log] warning: {exc}', flush=True)
