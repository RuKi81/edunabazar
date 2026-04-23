from __future__ import annotations

import os
import subprocess
import sys
import threading
from datetime import date
from pathlib import Path

from django.conf import settings
from django.contrib import admin, messages
from django.shortcuts import redirect, render
from django.urls import path
from django.utils import timezone
from django.utils.html import format_html

from .models import (
    District, Farmland, GeeApiMetric, MonitoringTask, PipelineRun,
    Region, SatelliteScene, VegetationIndex,
)

import logging
logger = logging.getLogger('agrocosmos')


# ── Standard model admins ─────────────────────────────────────────

@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'farmland_count', 'created_at')
    search_fields = ('name', 'code')

    def farmland_count(self, obj):
        return Farmland.objects.filter(district__region=obj).count()
    farmland_count.short_description = 'Угодий'


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    list_display = ('name', 'region', 'code', 'created_at')
    list_filter = ('region',)
    search_fields = ('name', 'code')


@admin.register(Farmland)
class FarmlandAdmin(admin.ModelAdmin):
    list_display = ('id', 'district', 'crop_type', 'area_ha', 'cadastral_number')
    list_filter = ('crop_type', 'district__region')
    search_fields = ('cadastral_number',)


@admin.register(SatelliteScene)
class SatelliteSceneAdmin(admin.ModelAdmin):
    list_display = ('scene_id', 'satellite', 'acquired_date', 'cloud_cover', 'processed')
    list_filter = ('satellite', 'processed')
    search_fields = ('scene_id',)


@admin.register(VegetationIndex)
class VegetationIndexAdmin(admin.ModelAdmin):
    list_display = ('farmland', 'index_type', 'acquired_date', 'mean', 'median')
    list_filter = ('index_type', 'acquired_date')


@admin.register(GeeApiMetric)
class GeeApiMetricAdmin(admin.ModelAdmin):
    """Daily counters for Google Earth Engine ``computePixels`` calls."""
    list_display = ('day', 'calls', 'errors', 'throttled', 'mb_display', 'updated_at')
    list_filter = ('day',)
    ordering = ('-day',)
    readonly_fields = ('day', 'calls', 'errors', 'throttled',
                       'bytes_downloaded', 'last_error', 'updated_at')

    def mb_display(self, obj):
        return f'{(obj.bytes_downloaded or 0) / 1e6:.1f} МБ'
    mb_display.short_description = 'Скачано'

    def has_add_permission(self, request):
        return False  # rows are created by services.gee_client only


# ── PipelineRun admin ─────────────────────────────────────────────

@admin.register(PipelineRun)
class PipelineRunAdmin(admin.ModelAdmin):
    list_display = ('task_type', 'status_badge', 'region', 'year',
                    'description', 'records_count', 'started_at', 'finished_at', 'duration_col')
    list_filter = ('status', 'task_type')
    readonly_fields = ('task_type', 'status', 'region', 'year', 'description',
                       'log', 'records_count', 'started_at', 'finished_at')

    def status_badge(self, obj):
        colors = {'running': '#2196F3', 'completed': '#4CAF50', 'failed': '#F44336'}
        c = colors.get(obj.status, '#999')
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;'
            'border-radius:3px;font-size:11px;">{}</span>',
            c, obj.get_status_display(),
        )
    status_badge.short_description = 'Статус'

    def duration_col(self, obj):
        return obj.duration
    duration_col.short_description = 'Длительность'


# ── MonitoringTask admin ──────────────────────────────────────────

@admin.register(MonitoringTask)
class MonitoringTaskAdmin(admin.ModelAdmin):
    list_display = ('task_type', 'region', 'district', 'year', 'status_badge',
                    'last_check', 'last_date_to', 'records_total', 'created_at')
    list_filter = ('task_type', 'status', 'year')
    readonly_fields = ('last_check', 'last_date_to', 'records_total', 'log',
                       'created_at', 'updated_at')
    actions = ['pause_tasks', 'resume_tasks']

    def status_badge(self, obj):
        colors = {'active': '#4CAF50', 'paused': '#FF9800', 'completed': '#9E9E9E'}
        c = colors.get(obj.status, '#999')
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;'
            'border-radius:3px;font-size:11px;">{}</span>',
            c, obj.get_status_display(),
        )
    status_badge.short_description = 'Статус'

    @admin.action(description='Приостановить выбранные')
    def pause_tasks(self, request, queryset):
        queryset.update(status='paused')

    @admin.action(description='Возобновить выбранные')
    def resume_tasks(self, request, queryset):
        queryset.update(status='active')


# ── Agrocosmos control panel (custom admin views) ────────────────

class AgrocosmosAdminSite:
    """Registers custom admin views under /admin/agrocosmos/."""

    @staticmethod
    def get_urls():
        return [
            path('agrocosmos/panel/',
                 admin.site.admin_view(agro_panel_view),
                 name='agro_panel'),
            path('agrocosmos/upload-region/',
                 admin.site.admin_view(upload_region_view),
                 name='agro_upload_region'),
            path('agrocosmos/upload-districts/',
                 admin.site.admin_view(upload_districts_view),
                 name='agro_upload_districts'),
            path('agrocosmos/upload-farmlands/',
                 admin.site.admin_view(upload_farmlands_view),
                 name='agro_upload_farmlands'),
            path('agrocosmos/run-archive/',
                 admin.site.admin_view(run_archive_view),
                 name='agro_run_archive'),
            path('agrocosmos/run-raster/',
                 admin.site.admin_view(run_raster_view),
                 name='agro_run_raster'),
            path('agrocosmos/start-monitoring/',
                 admin.site.admin_view(start_monitoring_view),
                 name='agro_start_monitoring'),
            path('agrocosmos/start-raster-monitoring/',
                 admin.site.admin_view(start_raster_monitoring_view),
                 name='agro_start_raster_monitoring'),
            path('agrocosmos/force-check/',
                 admin.site.admin_view(force_check_monitoring_view),
                 name='agro_force_check'),
            path('agrocosmos/force-check-raster/',
                 admin.site.admin_view(force_check_raster_monitoring_view),
                 name='agro_force_check_raster'),
            path('agrocosmos/run-status/<int:run_id>/',
                 admin.site.admin_view(run_status_view),
                 name='agro_run_status'),
        ]


# Patch admin to include our URLs
_original_get_urls = admin.AdminSite.get_urls


def _patched_get_urls(self):
    custom = AgrocosmosAdminSite.get_urls()
    return custom + _original_get_urls(self)


admin.AdminSite.get_urls = _patched_get_urls


# ── Views ─────────────────────────────────────────────────────────

def agro_panel_view(request):
    """Main Agrocosmos control panel."""
    from datetime import timedelta
    from django.db.models import Count, Sum
    from .models import GeeApiMetric

    regions = Region.objects.annotate(
        farmland_count=Count('districts__farmlands'),
    )
    districts = District.objects.select_related('region').order_by('region__name', 'name')
    tasks = MonitoringTask.objects.select_related('region').all()[:20]
    pipeline_runs = PipelineRun.objects.select_related('region').all()[:30]
    current_year = date.today().year
    years = list(range(current_year, current_year - 6, -1))

    # ── GEE API metrics ────────────────────────────────────────────
    today = date.today()
    week_ago = today - timedelta(days=6)      # last 7 days inclusive
    month_ago = today - timedelta(days=29)    # last 30 days inclusive

    gee_rows = list(
        GeeApiMetric.objects.filter(day__gte=month_ago)
        .order_by('-day')
        .values('day', 'calls', 'errors', 'throttled', 'bytes_downloaded', 'last_error')
    )

    def _sum(rows, field, since):
        return sum((r[field] or 0) for r in rows if r['day'] >= since)

    gee_totals = {
        'today': {
            'calls': _sum(gee_rows, 'calls', today),
            'errors': _sum(gee_rows, 'errors', today),
            'throttled': _sum(gee_rows, 'throttled', today),
            'mb': round(_sum(gee_rows, 'bytes_downloaded', today) / 1e6, 1),
        },
        'week': {
            'calls': _sum(gee_rows, 'calls', week_ago),
            'errors': _sum(gee_rows, 'errors', week_ago),
            'throttled': _sum(gee_rows, 'throttled', week_ago),
            'mb': round(_sum(gee_rows, 'bytes_downloaded', week_ago) / 1e6, 1),
        },
        'month': {
            'calls': _sum(gee_rows, 'calls', month_ago),
            'errors': _sum(gee_rows, 'errors', month_ago),
            'throttled': _sum(gee_rows, 'throttled', month_ago),
            'mb': round(_sum(gee_rows, 'bytes_downloaded', month_ago) / 1e6, 1),
        },
    }
    # Precompute MB per row for the table
    for r in gee_rows:
        r['mb'] = round((r['bytes_downloaded'] or 0) / 1e6, 1)

    return render(request, 'admin/agrocosmos/panel.html', {
        **admin.site.each_context(request),
        'title': 'Агрокосмос — Управление',
        'regions': regions,
        'districts': districts,
        'tasks': tasks,
        'pipeline_runs': pipeline_runs,
        'years': years,
        'current_year': current_year,
        'gee_rows': gee_rows,
        'gee_totals': gee_totals,
        'gee_calls_per_minute': getattr(settings, 'GEE_CALLS_PER_MINUTE', 60),
    })


def upload_region_view(request):
    """Upload region boundaries (SHP/GeoJSON)."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    from .services.import_vector import import_region_vector

    file = request.FILES.get('file')
    if not file:
        messages.error(request, 'Файл не выбран')
        return redirect('admin:agro_panel')

    name_field = request.POST.get('name_field', 'NAME')
    code_field = request.POST.get('code_field', 'CODE')

    run = PipelineRun.objects.create(
        task_type='upload_region',
        description=f'Файл: {file.name}',
    )

    try:
        created, updated, errors = import_region_vector(
            file, name_field=name_field, code_field=code_field,
        )
        run.status = 'completed'
        run.records_count = created + updated
        run.log = f'Создано: {created}, обновлено: {updated}\n' + '\n'.join(errors[:10])
        run.finished_at = timezone.now()
        run.save()
        messages.success(request, f'Регионы: {created} создано, {updated} обновлено.')
        for e in errors[:5]:
            messages.warning(request, e)
    except Exception as e:
        run.status = 'failed'
        run.log = str(e)
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Ошибка импорта: {e}')

    return redirect('admin:agro_panel')


def upload_districts_view(request):
    """Upload district boundaries (SHP/GeoJSON)."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    from .services.import_vector import import_district_vector

    file = request.FILES.get('file')
    region_id = request.POST.get('region_id')
    if not file or not region_id:
        messages.error(request, 'Укажите файл и регион')
        return redirect('admin:agro_panel')

    region = Region.objects.get(pk=int(region_id))
    run = PipelineRun.objects.create(
        task_type='upload_districts',
        region=region,
        description=f'Файл: {file.name}',
    )

    try:
        name_field = request.POST.get('name_field', 'NAME')
        code_field = request.POST.get('code_field', 'CODE')
        created, updated, errors = import_district_vector(
            file, int(region_id),
            name_field=name_field, code_field=code_field,
        )
        run.status = 'completed'
        run.records_count = created + updated
        run.log = f'Создано: {created}, обновлено: {updated}\n' + '\n'.join(errors[:10])
        run.finished_at = timezone.now()
        run.save()
        messages.success(request, f'Районы: {created} создано, {updated} обновлено.')
        for e in errors[:5]:
            messages.warning(request, e)
    except Exception as e:
        run.status = 'failed'
        run.log = str(e)
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Ошибка импорта: {e}')

    return redirect('admin:agro_panel')


def upload_farmlands_view(request):
    """Upload farmland boundaries (SHP/GeoJSON)."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    from .services.import_vector import import_farmland_vector

    file = request.FILES.get('file')
    region_id = request.POST.get('region_id')
    if not file or not region_id:
        messages.error(request, 'Укажите файл и регион')
        return redirect('admin:agro_panel')

    region = Region.objects.get(pk=int(region_id))
    run = PipelineRun.objects.create(
        task_type='upload_farmlands',
        region=region,
        description=f'Файл: {file.name}',
    )

    try:
        created, skipped, errors = import_farmland_vector(
            file, int(region_id),
            crop_type_field=request.POST.get('crop_type_field', 'LAND_TYPE'),
            area_field=request.POST.get('area_field', 'AREA_HA'),
            cadastral_field=request.POST.get('cadastral_field', 'CAD_NUM'),
            district_field=request.POST.get('district_field', 'DISTRICT'),
            auto_create_districts=True,
            clear_existing=bool(request.POST.get('clear_existing')),
        )
        run.status = 'completed'
        run.records_count = created
        run.log = f'Создано: {created}, пропущено: {skipped}\n' + '\n'.join(errors[:10])
        run.finished_at = timezone.now()
        run.save()
        messages.success(request, f'Угодья: {created} создано, {skipped} пропущено.')
        for e in errors[:5]:
            messages.warning(request, e)
    except Exception as e:
        run.status = 'failed'
        run.log = str(e)
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Ошибка импорта: {e}')

    return redirect('admin:agro_panel')


def _run_modis_bg(region_id, year, run_id=None, min_valid=0.5):
    """Run modis_ndvi command in background thread."""
    try:
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        call_command(
            'modis_ndvi',
            region_id=region_id,
            year=year,
            min_valid_ratio=min_valid,
            stdout=out,
            stderr=out,
        )
        log_text = out.getvalue()
        logger.info('modis_ndvi done: region=%s year=%s', region_id, year)

        if run_id:
            try:
                run = PipelineRun.objects.get(pk=run_id)
                run.status = 'completed'
                run.log = log_text[-8000:]
                for line in log_text.splitlines():
                    if 'Records saved:' in line:
                        try:
                            run.records_count += int(line.split('Records saved:')[1].strip())
                        except (ValueError, IndexError):
                            pass
                run.finished_at = timezone.now()
                run.save()
            except PipelineRun.DoesNotExist:
                pass
    except Exception as e:
        logger.error('modis_ndvi error: %s', e)
        if run_id:
            try:
                run = PipelineRun.objects.get(pk=run_id)
                run.status = 'failed'
                run.log = str(e)
                run.finished_at = timezone.now()
                run.save()
            except PipelineRun.DoesNotExist:
                pass


def _run_check_monitoring_bg(run_id=None, force=False):
    """Run check_monitoring management command in background thread."""
    from django.core.management import call_command
    from io import StringIO
    try:
        out = StringIO()
        call_command('check_monitoring', force=force, stdout=out, stderr=out)
        log_text = out.getvalue()
        logger.info('check_monitoring done: %s', log_text[-2000:])

        if run_id:
            try:
                run = PipelineRun.objects.get(pk=run_id)
                run.status = 'completed'
                run.log = log_text[-8000:]
                run.finished_at = timezone.now()
                run.save()
            except PipelineRun.DoesNotExist:
                pass
    except Exception as e:
        logger.error('check_monitoring error: %s', e)
        if run_id:
            try:
                run = PipelineRun.objects.get(pk=run_id)
                run.status = 'failed'
                run.log = str(e)
                run.finished_at = timezone.now()
                run.save()
            except PipelineRun.DoesNotExist:
                pass


def run_archive_view(request):
    """Trigger archive NDVI download for a region + year."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    region_id = request.POST.get('region_id')
    year = request.POST.get('year')

    if not region_id or not year:
        messages.error(request, 'Укажите регион и год')
        return redirect('admin:agro_panel')

    region = Region.objects.get(pk=int(region_id))
    year = int(year)

    run = PipelineRun.objects.create(
        task_type='archive_ndvi',
        region=region,
        year=year,
        description=f'{region.name}, {year} год',
    )

    min_valid = float(request.POST.get('min_valid', 0.5))

    t = threading.Thread(
        target=_run_modis_bg,
        args=(int(region_id), year, run.pk, min_valid),
        daemon=True,
    )
    t.start()

    messages.success(
        request,
        f'Загрузка архивных данных NDVI запущена: {region.name}, {year} '
        f'(min_valid={min_valid:.0%}). Процесс выполняется в фоне (~20 мин).'
    )
    return redirect('admin:agro_panel')


def _pipeline_log_dir() -> Path:
    """Directory for per-run log files; created if missing."""
    base = Path(getattr(settings, 'BASE_DIR', '.'))
    d = base / 'logs' / 'pipeline'
    d.mkdir(parents=True, exist_ok=True)
    return d


def _launch_ndvi_pipeline_detached(
    *, run_id: int, region_id: int, district_id: int | None, year: int,
    min_valid: float, overwrite: bool, rebuild_fusion: bool,
    date_from: str | None = None, date_to: str | None = None,
) -> tuple[int, str]:
    """Spawn ``run_ndvi_pipeline`` as a fully detached child process.

    Returns ``(pid, log_file_path)``. The child survives gunicorn worker
    recycling because we start it in a new session (POSIX) / new process
    group (Windows) with stdio redirected to a file on disk.
    """
    log_dir = _pipeline_log_dir()
    log_path = log_dir / f'run_{run_id}.log'

    cmd = [
        sys.executable, 'manage.py', 'run_ndvi_pipeline',
        '--run-id', str(run_id),
        '--year', str(year),
        '--min-valid', f'{min_valid:.3f}',
    ]
    if district_id:
        cmd += ['--district-id', str(district_id)]
    else:
        cmd += ['--region-id', str(region_id)]
    if overwrite:
        cmd.append('--overwrite')
    if rebuild_fusion:
        cmd.append('--fusion')
    if date_from:
        cmd += ['--date-from', date_from]
    if date_to:
        cmd += ['--date-to', date_to]

    env = os.environ.copy()
    env.setdefault('PYTHONUNBUFFERED', '1')

    # Detach: new session/group so SIGHUP from gunicorn doesn't kill us.
    popen_kwargs: dict = {
        'cwd': str(getattr(settings, 'BASE_DIR', '.')),
        'env': env,
        'stdin': subprocess.DEVNULL,
        'close_fds': True,
    }
    if os.name == 'posix':
        popen_kwargs['start_new_session'] = True
    else:   # Windows
        popen_kwargs['creationflags'] = (
            getattr(subprocess, 'DETACHED_PROCESS', 0x00000008) |
            getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0x00000200)
        )

    log_f = open(log_path, 'ab', buffering=0)
    try:
        proc = subprocess.Popen(
            cmd, stdout=log_f, stderr=subprocess.STDOUT, **popen_kwargs,
        )
    finally:
        # The child inherits the fd; we can safely close ours.
        log_f.close()

    logger.info(
        'NDVI pipeline detached: run_id=%s pid=%s log=%s',
        run_id, proc.pid, log_path,
    )
    return proc.pid, str(log_path)


def run_raster_view(request):
    """Trigger S2+L8 NDVI download for a region/district + year."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    region_id = request.POST.get('region_id')
    district_id = request.POST.get('district_id')
    year = request.POST.get('year')
    min_valid = float(request.POST.get('min_valid', 0.7))
    overwrite = request.POST.get('overwrite') == '1'
    rebuild_fusion = request.POST.get('rebuild_fusion') == '1'

    if not region_id or not year:
        messages.error(request, 'Укажите регион и год')
        return redirect('admin:agro_panel')

    region = Region.objects.get(pk=int(region_id))
    year = int(year)

    district_name = ''
    did = None
    if district_id:
        try:
            did = int(district_id)
            d = District.objects.get(pk=did)
            district_name = f', {d.name}'
        except (TypeError, ValueError, District.DoesNotExist):
            did = None

    flags = []
    if overwrite:
        flags.append('overwrite')
    if rebuild_fusion:
        flags.append('+fusion')
    flags_s = f' [{", ".join(flags)}]' if flags else ''

    run = PipelineRun.objects.create(
        task_type='raster_ndvi',
        region=region,
        year=year,
        status='running',
        description=(
            f'{region.name}{district_name}, {year} год '
            f'(S2+L8, valid≥{min_valid:.0%}){flags_s}'
        ),
    )

    try:
        pid, log_path = _launch_ndvi_pipeline_detached(
            run_id=run.pk, region_id=int(region_id), district_id=did,
            year=year, min_valid=min_valid,
            overwrite=overwrite, rebuild_fusion=rebuild_fusion,
        )
        run.pid = pid
        run.log_file = log_path
        run.heartbeat_at = timezone.now()
        run.save(update_fields=['pid', 'log_file', 'heartbeat_at'])
    except Exception as exc:
        logger.exception('failed to launch NDVI pipeline subprocess')
        run.status = 'failed'
        run.log = f'Ошибка запуска подпроцесса: {exc}'
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Не удалось запустить пайплайн: {exc}')
        return redirect('admin:agro_panel')

    messages.success(
        request,
        f'Загрузка S2+L8 NDVI запущена (run #{run.pk}, pid={pid}): '
        f'{region.name}{district_name}, {year} (min_valid={min_valid:.0%})'
        f'{flags_s}. Процесс в фоне (~1-3 ч для S2). '
        f'Обновляйте страницу для прогресса.'
    )
    return redirect('admin:agro_panel')


def run_status_view(request, run_id: int):
    """JSON status + log tail for a single PipelineRun (polled by admin UI)."""
    from django.http import JsonResponse, Http404
    try:
        run = PipelineRun.objects.get(pk=run_id)
    except PipelineRun.DoesNotExist:
        raise Http404

    tail = ''
    if run.log_file:
        try:
            with open(run.log_file, 'rb') as f:
                try:
                    f.seek(-8192, os.SEEK_END)
                except OSError:
                    f.seek(0)
                tail = f.read().decode('utf-8', errors='replace')
        except OSError:
            tail = run.log or ''
    else:
        tail = run.log or ''

    alive = False
    if run.pid and run.status == 'running':
        try:
            from agrocosmos.management.commands.cleanup_stale_runs import _pid_alive
            alive = _pid_alive(run.pid)
        except Exception:
            alive = False

    return JsonResponse({
        'id': run.pk,
        'status': run.status,
        'pid': run.pid,
        'alive': alive,
        'heartbeat_at': run.heartbeat_at.isoformat() if run.heartbeat_at else None,
        'started_at': run.started_at.isoformat() if run.started_at else None,
        'finished_at': run.finished_at.isoformat() if run.finished_at else None,
        'records_count': run.records_count,
        'duration': run.duration,
        'tail': tail[-8000:],
    })


def force_check_monitoring_view(request):
    """Force-run check_monitoring for all active tasks."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    run = PipelineRun.objects.create(
        task_type='monitoring',
        description='Принудительная проверка мониторинга',
    )

    t = threading.Thread(
        target=_run_check_monitoring_bg,
        args=(run.pk,),
        kwargs={'force': True},
        daemon=True,
    )
    t.start()

    messages.success(
        request,
        'Принудительная проверка мониторинга запущена в фоне (--force).'
    )
    return redirect('admin:agro_panel')


def start_raster_monitoring_view(request):
    """Create a raster (S2+L8) monitoring task and run an initial catch-up."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    region_id = request.POST.get('region_id')
    district_id = request.POST.get('district_id')
    year = request.POST.get('year')
    min_valid = float(request.POST.get('min_valid', 0.7))

    if not region_id or not year:
        messages.error(request, 'Укажите регион и год')
        return redirect('admin:agro_panel')

    region = Region.objects.get(pk=int(region_id))
    year = int(year)

    district = None
    did = None
    if district_id:
        try:
            did = int(district_id)
            district = District.objects.get(pk=did)
        except (TypeError, ValueError, District.DoesNotExist):
            did = None
            district = None

    task, created = MonitoringTask.objects.get_or_create(
        task_type='raster', region=region, district=district, year=year,
        defaults={'status': 'active'},
    )
    if not created:
        if task.status != 'active':
            task.status = 'active'
            task.save(update_fields=['status'])
            messages.info(request, f'Мониторинг S2+L8 возобновлён: {task}')
        else:
            messages.info(request, f'Мониторинг S2+L8 уже активен: {task}')
    else:
        messages.success(request, f'Мониторинг S2+L8 создан: {task}')

    # Initial catch-up: fetch [year_start .. today-7] right now as detached
    # subprocess via run_ndvi_pipeline. Subsequent updates are handled by
    # the daily cron `check_raster_monitoring`.
    from datetime import timedelta
    today = date.today()
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    window_from = (task.last_date_to + timedelta(days=1)
                   if task.last_date_to else year_start)
    window_to = min(year_end, today - timedelta(days=7))

    if window_to < window_from:
        messages.info(
            request,
            f'Окно пока пустое ({window_from}..{window_to}) — '
            f'cron добавит данные, когда сцены опубликуют.'
        )
        return redirect('admin:agro_panel')

    scope_desc = f'{region.name}' + (f' / {district.name}' if district else '')
    run = PipelineRun.objects.create(
        task_type='raster_ndvi',
        region=region, year=year, status='running',
        description=(
            f'[monitor init] {scope_desc}, {year} '
            f'({window_from}..{window_to}, valid≥{min_valid:.0%})'
        ),
    )
    try:
        pid, log_path = _launch_ndvi_pipeline_detached(
            run_id=run.pk, region_id=region.pk, district_id=did,
            year=year, min_valid=min_valid,
            overwrite=False, rebuild_fusion=True,
            date_from=window_from.isoformat(),
            date_to=window_to.isoformat(),
        )
        run.pid = pid
        run.log_file = log_path
        run.heartbeat_at = timezone.now()
        run.save(update_fields=['pid', 'log_file', 'heartbeat_at'])
        messages.success(
            request,
            f'Начальная выкачка S2+L8 запущена (run #{run.pk}, pid={pid}) '
            f'для окна {window_from}..{window_to}. Дальше cron раз в сутки.'
        )
    except Exception as exc:
        logger.exception('failed to launch initial raster monitoring run')
        run.status = 'failed'
        run.log = f'Ошибка запуска подпроцесса: {exc}'
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Не удалось запустить начальную выкачку: {exc}')
    return redirect('admin:agro_panel')


def force_check_raster_monitoring_view(request):
    """Force-run check_raster_monitoring for all active raster tasks."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    run = PipelineRun.objects.create(
        task_type='monitoring',
        description='Принудительная проверка S2+L8 мониторинга',
    )

    def _bg(run_id: int):
        from io import StringIO
        from django.core.management import call_command
        out = StringIO()
        try:
            call_command(
                'check_raster_monitoring', force=True,
                stdout=out, stderr=out,
            )
            PipelineRun.objects.filter(pk=run_id).update(
                status='completed', log=out.getvalue()[-8000:],
                finished_at=timezone.now(),
            )
        except Exception as exc:
            PipelineRun.objects.filter(pk=run_id).update(
                status='failed',
                log=(out.getvalue() + f'\nERROR: {exc}')[-8000:],
                finished_at=timezone.now(),
            )

    threading.Thread(target=_bg, args=(run.pk,), daemon=True).start()
    messages.success(
        request,
        'Запущена принудительная проверка S2+L8 мониторинга (все активные задачи).'
    )
    return redirect('admin:agro_panel')


def start_monitoring_view(request):
    """Create a monitoring task for a region + year."""
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    region_id = request.POST.get('region_id')
    year = request.POST.get('year')

    if not region_id or not year:
        messages.error(request, 'Укажите регион и год')
        return redirect('admin:agro_panel')

    region = Region.objects.get(pk=int(region_id))
    year = int(year)

    task, created = MonitoringTask.objects.get_or_create(
        region=region, year=year,
        defaults={'status': 'active'},
    )
    if not created:
        if task.status == 'paused':
            task.status = 'active'
            task.save()
            messages.info(request, f'Мониторинг возобновлён: {region.name}, {year}')
        else:
            messages.info(request, f'Мониторинг уже активен: {region.name}, {year}')
    else:
        messages.success(
            request,
            f'Мониторинг создан: {region.name}, {year}. '
            f'Обработка всех доступных периодов запущена в фоне.'
        )

    # Run check_monitoring in background to process all available periods
    run = PipelineRun.objects.create(
        task_type='monitoring',
        region=region,
        year=year,
        description=f'Мониторинг: {region.name}, {year} год',
    )

    t = threading.Thread(
        target=_run_check_monitoring_bg,
        args=(run.pk,),
        daemon=True,
    )
    t.start()

    return redirect('admin:agro_panel')
