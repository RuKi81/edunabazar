"""Спутниковый мониторинг пользовательских полей.

Кнопка «Спутниковый мониторинг» в карточке поля ставит в очередь
``PipelineRun`` (task_type=raster_ndvi) c launch_args={'myf_field_id': …}.
Дальше работает существующая инфраструктура ``agrocosmos``:
``run_ndvi_worker`` подхватывает queued-run и выполняет
``run_ndvi_pipeline --myf-field-id`` → скачивание S2 (приоритет) и L8/L9
NDVI-композитов по bbox поля (scope ``f<id>`` в именах растров).

Окно выкачки: 1 января текущего года → сегодня-7 дней (свежие сцены GEE
публикует с задержкой; тот же лаг использует region/district-мониторинг).
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from django.conf import settings

# Лаг публикации сцен в GEE — не запрашиваем последние дни, там пусто.
FRESHNESS_LAG_DAYS = 7
DEFAULT_MIN_VALID = 0.7


def _pipeline_log_dir() -> Path:
    """Каталог per-run логов пайплайна (тот же, что у admin-панели agrocosmos)."""
    base = Path(getattr(settings, 'BASE_DIR', '.'))
    d = base / 'logs' / 'pipeline'
    d.mkdir(parents=True, exist_ok=True)
    return d


def _runs_for_field(field):
    """QuerySet всех PipelineRun этого поля (по launch_args.myf_field_id)."""
    from agrocosmos.models import PipelineRun
    return PipelineRun.objects.filter(
        task_type=PipelineRun.TaskType.RASTER_NDVI,
        launch_args__myf_field_id=field.pk,
    )


def active_run_for_field(field):
    """Текущий queued/running запуск для поля (или None)."""
    from agrocosmos.models import PipelineRun
    return (
        _runs_for_field(field)
        .filter(status__in=[PipelineRun.Status.QUEUED, PipelineRun.Status.RUNNING])
        .order_by('-started_at')
        .first()
    )


def latest_run_for_field(field):
    """Последний запуск для поля независимо от статуса (или None)."""
    return _runs_for_field(field).order_by('-started_at').first()


def enqueue_field_monitoring(field):
    """Поставить выкачку NDVI по полю в очередь воркера.

    Возвращает ``(run, created)``: если по полю уже есть активный
    (queued/running) запуск — возвращаем его с ``created=False``,
    дубликат не создаём.
    """
    from agrocosmos.models import PipelineRun

    existing = active_run_for_field(field)
    if existing is not None:
        return existing, False

    today = date.today()
    date_from = date(today.year, 1, 1)
    date_to = max(date_from, today - timedelta(days=FRESHNESS_LAG_DAYS))

    run = PipelineRun.objects.create(
        task_type=PipelineRun.TaskType.RASTER_NDVI,
        status=PipelineRun.Status.QUEUED,
        region=field.region,
        year=today.year,
        description=(
            f'[my_fields] поле #{field.pk} «{field.name}», {today.year} '
            f'({date_from}..{date_to})'
        ),
        launch_args={
            'myf_field_id': field.pk,
            'year': today.year,
            'date_from': date_from.isoformat(),
            'date_to': date_to.isoformat(),
            'min_valid': DEFAULT_MIN_VALID,
            'overwrite': False,
            'fusion': False,
            'skip_s2': False,
            'skip_l8': False,
        },
    )

    # Пред-создаём файл лога, чтобы heartbeat/tail работали сразу.
    log_path = _pipeline_log_dir() / f'run_{run.pk}.log'
    try:
        log_path.touch(exist_ok=True)
    except OSError:
        pass
    run.log_file = str(log_path)
    run.save(update_fields=['log_file'])
    return run, True


def run_to_dict(run) -> dict | None:
    """Сериализация PipelineRun для JSON-ответов my_fields API."""
    if run is None:
        return None
    return {
        'run_id': run.pk,
        'status': run.status,
        'status_display': run.get_status_display(),
        'started_at': run.started_at.isoformat() if run.started_at else None,
        'finished_at': run.finished_at.isoformat() if run.finished_at else None,
        'description': run.description,
    }
