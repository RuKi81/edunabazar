"""Выполнить оверлей двух ГИС-слоёв в фоне (по queued ``PipelineRun``).

Запускается воркером ``run_ndvi_worker`` (task_type=``gis_overlay``) или вручную
через ``call_command('run_gis_overlay', run_id=<id>)``. Параметры операции берутся
из ``PipelineRun.launch_args``:

    {
      "layer_a_id": <int>, "layer_b_id": <int>,
      "op": "intersection|difference|union|symmetric_difference",
      "title": "<название нового слоя>",
      "owner_id": <int|null>
    }

По завершении в ``launch_args["_result_layer_id"]`` кладётся id созданного слоя
(его читает эндпоинт статуса, чтобы фронт подхватил слой без перезагрузки).
"""
from __future__ import annotations

import os
import traceback

from django.core.management.base import BaseCommand
from django.utils import timezone

from agrocosmos.models import PipelineRun


class Command(BaseCommand):
    help = 'Выполнить оверлей двух ГИС-слоёв для queued PipelineRun.'

    def add_arguments(self, parser):
        parser.add_argument('--run-id', type=int, required=True,
                            help='id строки PipelineRun (task_type=gis_overlay).')
        # Прямые аргументы (для ручного запуска без launch_args).
        parser.add_argument('--layer-a-id', type=int)
        parser.add_argument('--layer-b-id', type=int)
        parser.add_argument('--op', type=str)
        parser.add_argument('--title', type=str)
        parser.add_argument('--owner-id', type=int)

    def handle(self, *args, **options):
        run_id = options['run_id']
        run = PipelineRun.objects.filter(pk=run_id).first()
        if run is None:
            self.stderr.write(f'run #{run_id} not found')
            return

        args_src = dict(run.launch_args or {})
        layer_a_id = options.get('layer_a_id') or args_src.get('layer_a_id')
        layer_b_id = options.get('layer_b_id') or args_src.get('layer_b_id')
        op = options.get('op') or args_src.get('op')
        title = options.get('title') or args_src.get('title')
        owner_id = options.get('owner_id') or args_src.get('owner_id')

        self._bootstrap(run_id)

        try:
            layer = self._execute(layer_a_id, layer_b_id, op, title, owner_id)
        except Exception:
            tb = traceback.format_exc()
            self._log(run_id, f'!!! Overlay failed !!!\n{tb[-4000:]}')
            PipelineRun.objects.filter(pk=run_id).exclude(
                status=PipelineRun.Status.COMPLETED,
            ).update(
                status=PipelineRun.Status.FAILED,
                finished_at=timezone.now(),
            )
            raise

        # Успех: сохраняем id результата в launch_args и помечаем completed.
        args_src['_result_layer_id'] = layer.pk
        PipelineRun.objects.filter(pk=run_id).update(
            status=PipelineRun.Status.COMPLETED,
            finished_at=timezone.now(),
            heartbeat_at=timezone.now(),
            records_count=layer.feature_count,
            launch_args=args_src,
        )
        self._log(run_id,
                  f'Overlay «{op}» completed → слой #{layer.pk} '
                  f'"{layer.title}" ({layer.feature_count} об.)')

    # ------------------------------------------------------------------

    def _execute(self, layer_a_id, layer_b_id, op, title, owner_id):
        from django.contrib.auth import get_user_model
        from my_fields.models import GisLayer
        from my_fields.services.overlay import OverlayError, op_label, run_overlay

        if not layer_a_id or not layer_b_id:
            raise OverlayError('Не заданы оба слоя (layer_a_id/layer_b_id).')
        layer_a = GisLayer.objects.filter(pk=layer_a_id).first()
        layer_b = GisLayer.objects.filter(pk=layer_b_id).first()
        if layer_a is None or layer_b is None:
            raise OverlayError('Один из слоёв не найден.')

        owner = None
        if owner_id:
            owner = get_user_model().objects.filter(pk=owner_id).first()

        self.stdout.write(
            f'[overlay] run: {op_label(op)}  A=#{layer_a_id} B=#{layer_b_id} '
            f'→ "{title}"'
        )
        return run_overlay(layer_a, layer_b, op, title, owner=owner)

    @staticmethod
    def _bootstrap(run_id: int) -> None:
        try:
            PipelineRun.objects.filter(pk=run_id).update(
                pid=os.getpid(),
                status=PipelineRun.Status.RUNNING,
                heartbeat_at=timezone.now(),
            )
        except Exception as exc:  # noqa: BLE001
            print(f'[overlay bootstrap] warning: {exc}', flush=True)

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
            print(f'[overlay log] warning: {exc}', flush=True)
