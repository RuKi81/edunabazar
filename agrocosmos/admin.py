from __future__ import annotations

import os
import threading
from datetime import date
from pathlib import Path

from django.conf import settings
from django.contrib import admin, messages
from django.core.paginator import Paginator
from django.shortcuts import redirect, render
from django.urls import path
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.html import format_html

from .models import (
    AgroSubscription, District, Farmland, GeeApiMetric, MonitoringTask, PipelineRun,
    Region, SatelliteScene, VegetationAlert, VegetationIndex,
)

import logging
logger = logging.getLogger('agrocosmos')


# ── Standard model admins ─────────────────────────────────────────

@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'farmland_count', 'created_at')
    search_fields = ('name', 'code')

    def get_queryset(self, request):
        # Annotate farmland count once via the direct ``Farmland.region`` FK
        # instead of issuing an N+1 ``COUNT()`` query per row that joins
        # through ``district`` (which also silently drops every parcel with
        # ``district_id=NULL``).
        from django.db.models import Count
        return super().get_queryset(request).annotate(
            _farmland_count=Count('farmlands'),
        )

    def farmland_count(self, obj):
        return obj._farmland_count
    farmland_count.short_description = 'Угодий'
    farmland_count.admin_order_field = '_farmland_count'


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    list_display = ('name', 'region', 'code', 'created_at')
    list_filter = ('region',)
    search_fields = ('name', 'code')


class _NoCountPaginator(Paginator):
    """Paginator that skips ``SELECT COUNT(*)`` on huge tables.

    Returns a fixed, deliberately-large ``count`` so the admin renders pagination
    controls without ever asking Postgres to count 19M+ rows. Trade-off: the
    "Last page" link points at a fictional offset, but Next / Previous /
    direct page numbers all work correctly.
    """

    @cached_property
    def count(self):
        return 10_000_000_000


@admin.register(Farmland)
class FarmlandAdmin(admin.ModelAdmin):
    """Tuned for a 19M+ row table.

    ``list_filter``, the default pagination COUNT, and the model's
    ``Meta.ordering`` all triggered full table scans / 19M-row sorts that
    blew past the 30 s gunicorn timeout, killing the worker and wedging
    the agrocosmos panel that shared the worker pool. Stripped back to:

    * ``ordering=('-id',)`` — primary-key descending, uses the PK index
      for an O(log n) ``LIMIT 100`` instead of sorting all 19M rows;
    * ``paginator=_NoCountPaginator`` — skip the unbounded ``COUNT(*)``;
    * ``show_full_result_count=False`` — and the "X results" line that
      would otherwise also count;
    * no ``list_filter`` — every DISTINCT on this table is a seq-scan;
    * ``raw_id_fields`` on FK columns — avoid pre-loading thousands of
      options into the change form;
    * ``list_select_related`` so per-row FK renders don't N+1.
    """
    list_display = ('id', 'region', 'district', 'crop_type', 'area_ha',
                    'cadastral_number')
    list_select_related = ('region', 'district')
    search_fields = ('cadastral_number',)
    raw_id_fields = ('region', 'district')
    ordering = ('-id',)
    paginator = _NoCountPaginator
    show_full_result_count = False


@admin.register(SatelliteScene)
class SatelliteSceneAdmin(admin.ModelAdmin):
    list_display = ('scene_id', 'satellite', 'acquired_date', 'cloud_cover', 'processed')
    list_filter = ('satellite', 'processed')
    search_fields = ('scene_id',)


@admin.register(VegetationIndex)
class VegetationIndexAdmin(admin.ModelAdmin):
    """Tuned for a multi-million row table.

    ``list_filter=('acquired_date',)`` builds a ``SELECT DISTINCT
    acquired_date`` hierarchical widget on every page load — on 3M+ rows
    that's seconds to tens of seconds and guaranteed to kill workers.
    Dropped for the same reasons as :class:`FarmlandAdmin`.
    """
    list_display = ('id', 'farmland', 'index_type', 'acquired_date',
                    'mean', 'median')
    list_select_related = ('farmland',)
    raw_id_fields = ('farmland', 'scene')
    search_fields = ('farmland__cadastral_number',)
    ordering = ('-id',)
    paginator = _NoCountPaginator
    show_full_result_count = False


@admin.register(VegetationAlert)
class VegetationAlertAdmin(admin.ModelAdmin):
    """Biological NDVI alerts (baseline deviation, rapid drops)."""
    list_display = ('id', 'farmland', 'alert_type', 'severity_badge',
                    'status', 'detected_on', 'triggered_at', 'acknowledged_by')
    list_filter = ('status', 'severity', 'alert_type', 'triggered_at')
    search_fields = ('farmland__cadastral_number', 'message')
    readonly_fields = ('farmland', 'alert_type', 'detected_on', 'triggered_at',
                       'context', 'message')

    def severity_badge(self, obj):
        color = '#b91c1c' if obj.severity == 'critical' else '#b45309'
        return format_html(
            '<span style="color:{}; font-weight:600;">{}</span>',
            color, obj.get_severity_display(),
        )
    severity_badge.short_description = 'Критичность'

    def has_add_permission(self, request):
        return False  # managed by detect_vegetation_alerts command


@admin.register(AgroSubscription)
class AgroSubscriptionAdmin(admin.ModelAdmin):
    """User subscriptions for Agrocosmos email notifications."""
    list_display = ('id', 'legacy_user_id', 'region', 'district',
                    'notify_anomalies', 'notify_updates', 'last_update_notified_at')
    list_filter = ('notify_anomalies', 'notify_updates', 'region')
    search_fields = ('legacy_user_id',)
    autocomplete_fields = ('region', 'district')


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
            path('agrocosmos/rasters/',
                 admin.site.admin_view(agro_rasters_view),
                 name='agro_rasters'),
            path('agrocosmos/rasters/files/',
                 admin.site.admin_view(agro_raster_files_view),
                 name='agro_raster_files'),
            path('agrocosmos/alerts/action/',
                 admin.site.admin_view(agro_alert_action_view),
                 name='agro_alert_action'),
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

    # Farmland counts per region are cached in Redis for 5 minutes.
    # The naive ``Region.objects.annotate(Count('farmlands'))`` groups a
    # LEFT JOIN over 19.6M rows and takes 30-40 s even with indexes — the
    # panel does not need real-time precision, so a short TTL is fine.
    # Invalidated automatically when large imports finish (see below).
    from django.core.cache import cache
    counts = cache.get('agro_panel_farmland_counts')
    if counts is None:
        counts = dict(
            Farmland.objects
            .values('region_id')
            .annotate(c=Count('id'))
            .values_list('region_id', 'c')
        )
        cache.set('agro_panel_farmland_counts', counts, 300)

    # ``.defer('geom')`` is critical here: Region/District both carry a
    # PostGIS MultiPolygon that runs to tens of MB per row for large
    # subjects (Yakutia, Krasnoyarsk Krai, …). Pulling all 84 regions or
    # all ~2.7K districts with their geometry serialises hundreds of MB of
    # WKB over the wire and turns this view into a 60-120 s request even
    # though the panel template never reads ``.geom``.
    regions = list(Region.objects.defer('geom').order_by('name'))
    for r in regions:
        r.farmland_count = counts.get(r.pk, 0)

    districts = (
        District.objects
        .select_related('region')
        .defer('geom', 'region__geom')
        .order_by('region__name', 'name')
    )
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

    # ── Vegetation alerts ──────────────────────────────────────────
    alerts_active = (
        VegetationAlert.objects
        .filter(status__in=['active', 'acknowledged'])
        .select_related('farmland', 'farmland__district', 'farmland__district__region')
        .order_by('-severity', '-triggered_at')[:30]
    )
    alert_counts = {
        'critical': VegetationAlert.objects.filter(
            status='active', severity='critical').count(),
        'warning': VegetationAlert.objects.filter(
            status='active', severity='warning').count(),
        'acknowledged': VegetationAlert.objects.filter(
            status='acknowledged').count(),
    }

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
        'alerts_active': alerts_active,
        'alert_counts': alert_counts,
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


# Note: ``_run_modis_bg`` was removed — archive MODIS now flows through the
# ``run_archive_pipeline`` worker command (see ``run_archive_view`` and
# ``run_ndvi_worker``) instead of an in-process daemon thread. Running
# rasterio inside gunicorn workers held the GIL and starved HTTP workers
# whenever an archive download was in flight.


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
    """Queue an archive MODIS NDVI pipeline for a region + year window.

    The actual work runs inside the ``worker`` container (see
    :mod:`run_archive_pipeline`), not inside gunicorn — this decouples a
    multi-hour MODIS download from web lifecycle (deploys, gunicorn
    worker recycling) and prevents a GIL-bound rasterio run from starving
    HTTP workers on the web container.
    """
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    region_id = request.POST.get('region_id')
    year = request.POST.get('year')
    year_from = request.POST.get('year_from')
    year_to = request.POST.get('year_to')

    if not region_id:
        messages.error(request, 'Укажите регион')
        return redirect('admin:agro_panel')

    # Accept either a single --year or a --year-from/--year-to window.
    try:
        if year_from and year_to:
            yf, yt = int(year_from), int(year_to)
        elif year:
            yf = yt = int(year)
        else:
            messages.error(request, 'Укажите год или диапазон лет')
            return redirect('admin:agro_panel')
    except ValueError:
        messages.error(request, 'Год должен быть числом')
        return redirect('admin:agro_panel')

    min_valid = float(request.POST.get('min_valid', 0.5))
    skip_baseline = request.POST.get('skip_baseline') == 'on'

    region = Region.objects.get(pk=int(region_id))

    run = PipelineRun.objects.create(
        task_type=PipelineRun.TaskType.ARCHIVE_NDVI,
        status=PipelineRun.Status.QUEUED,
        region=region,
        year=yt,
        description=f'{region.name}, {yf}..{yt}'
                    + (' (без baseline)' if skip_baseline else ''),
    )

    # Pre-create the on-disk log file so admin tail-f works immediately.
    log_dir = _pipeline_log_dir()
    log_path = log_dir / f'run_{run.pk}.log'
    try:
        log_path.touch(exist_ok=True)
    except OSError:
        pass

    launch_args = {
        'region_id': int(region_id),
        'year_from': yf,
        'year_to': yt,
        'min_valid': float(f'{min_valid:.3f}'),
        'overwrite': False,
        'skip_baseline': skip_baseline,
    }
    run.launch_args = launch_args
    run.log_file = str(log_path)
    run.heartbeat_at = timezone.now()
    run.save(update_fields=['launch_args', 'log_file', 'heartbeat_at'])

    messages.success(
        request,
        f'Архивный MODIS NDVI поставлен в очередь: {region.name}, '
        f'{yf}..{yt} (min_valid={min_valid:.0%}). '
        f'Worker подхватит задачу в течение 5 секунд.'
    )
    return redirect('admin:agro_panel')


def _pipeline_log_dir() -> Path:
    """Directory for per-run log files; created if missing."""
    base = Path(getattr(settings, 'BASE_DIR', '.'))
    d = base / 'logs' / 'pipeline'
    d.mkdir(parents=True, exist_ok=True)
    return d


def _enqueue_ndvi_pipeline(
    *, run_id: int, region_id: int, district_id: int | None, year: int,
    min_valid: float, overwrite: bool, rebuild_fusion: bool,
    date_from: str | None = None, date_to: str | None = None,
) -> str:
    """Build the ``launch_args`` dict for the NDVI worker and create the
    per-run log file.

    The actual pipeline is executed by the separate ``worker`` container
    (see ``run_ndvi_worker`` management command) — the web process only
    puts the request into the database and returns. This decouples long
    pipelines from the web container's lifecycle (deploys, healthcheck
    restarts, gunicorn recycling).
    """
    log_dir = _pipeline_log_dir()
    log_path = log_dir / f'run_{run_id}.log'
    # Pre-create the log file so admin polling / tail -f work immediately.
    try:
        log_path.touch(exist_ok=True)
    except OSError:
        pass

    launch_args: dict = {
        'year': year,
        'min_valid': float(f'{min_valid:.3f}'),
        'overwrite': bool(overwrite),
        'fusion': bool(rebuild_fusion),
        'skip_s2': False,
        'skip_l8': False,
    }
    if district_id:
        launch_args['district_id'] = int(district_id)
    else:
        launch_args['region_id'] = int(region_id)
    if date_from:
        launch_args['date_from'] = str(date_from)
    if date_to:
        launch_args['date_to'] = str(date_to)

    run = PipelineRun.objects.filter(pk=run_id).first()
    if run is not None:
        run.status = PipelineRun.Status.QUEUED
        run.launch_args = launch_args
        run.log_file = str(log_path)
        run.heartbeat_at = timezone.now()
        run.save(update_fields=['status', 'launch_args', 'log_file', 'heartbeat_at'])

    logger.info(
        'NDVI pipeline queued: run_id=%s log=%s args=%s',
        run_id, log_path, launch_args,
    )
    return str(log_path)


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
        status=PipelineRun.Status.QUEUED,
        description=(
            f'{region.name}{district_name}, {year} год '
            f'(S2+L8, valid≥{min_valid:.0%}){flags_s}'
        ),
    )

    try:
        log_path = _enqueue_ndvi_pipeline(
            run_id=run.pk, region_id=int(region_id), district_id=did,
            year=year, min_valid=min_valid,
            overwrite=overwrite, rebuild_fusion=rebuild_fusion,
        )
    except Exception as exc:
        logger.exception('failed to enqueue NDVI pipeline run')
        run.status = PipelineRun.Status.FAILED
        run.log = f'Ошибка постановки в очередь: {exc}'
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Не удалось поставить пайплайн в очередь: {exc}')
        return redirect('admin:agro_panel')

    messages.success(
        request,
        f'Загрузка S2+L8 NDVI поставлена в очередь (run #{run.pk}): '
        f'{region.name}{district_name}, {year} (min_valid={min_valid:.0%})'
        f'{flags_s}. Воркер подхватит в течение нескольких секунд. '
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


# ── Vegetation alerts ack / resolve ────────────────────────────────

def agro_alert_action_view(request):
    """POST handler: acknowledge or resolve a VegetationAlert.

    Form params: ``alert_id`` (int), ``action`` ("acknowledge" | "resolve").
    Redirects back to ``agro_panel`` with a flash message.
    """
    if request.method != 'POST':
        return redirect('admin:agro_panel')

    try:
        alert_id = int(request.POST.get('alert_id') or 0)
    except (TypeError, ValueError):
        alert_id = 0
    action = request.POST.get('action', '')

    try:
        alert = VegetationAlert.objects.get(pk=alert_id)
    except VegetationAlert.DoesNotExist:
        messages.error(request, 'Алерт не найден.')
        return redirect('admin:agro_panel')

    now = timezone.now()
    if action == 'acknowledge' and alert.status == VegetationAlert.Status.ACTIVE:
        alert.status = VegetationAlert.Status.ACKNOWLEDGED
        alert.acknowledged_at = now
        alert.acknowledged_by = request.user if request.user.is_authenticated else None
        alert.save(update_fields=['status', 'acknowledged_at', 'acknowledged_by'])
        messages.success(request, f'Алерт #{alert.pk} принят.')
    elif action == 'resolve':
        alert.status = VegetationAlert.Status.RESOLVED
        alert.resolved_at = now
        alert.save(update_fields=['status', 'resolved_at'])
        messages.success(request, f'Алерт #{alert.pk} разрешён.')
    else:
        messages.warning(request, f'Действие "{action}" неприменимо к алерту в статусе {alert.status}.')

    return redirect(request.POST.get('next') or 'admin:agro_panel')


# ── Raster storage management ──────────────────────────────────────

def agro_rasters_view(request):
    """Folder-level summary of on-disk rasters with bulk-delete by folder.

    POST handler: ``folders`` = list of ``sensor:scope:year`` tokens — deletes
    every file within those folders.  GET renders the summary table.
    """
    from .services import raster_storage

    if request.method == 'POST':
        tokens = request.POST.getlist('folders')
        paths: list[str] = []
        for tok in tokens:
            try:
                sensor, scope, year = tok.split(':')
            except ValueError:
                continue
            for f in raster_storage.list_files(sensor, scope, year):
                paths.append(f.path)
        removed, freed = raster_storage.delete_paths(paths)
        messages.success(
            request,
            f'Удалено файлов: {removed} ({freed/1e6:.1f} МБ) из {len(tokens)} папок.',
        )
        return redirect('admin:agro_rasters')

    folders = raster_storage.list_folders()
    totals = raster_storage.totals_by_sensor()
    grand_size = sum(t['size_bytes'] for t in totals.values())
    grand_count = sum(t['count'] for t in totals.values())

    return render(request, 'admin/agrocosmos/rasters.html', {
        **admin.site.each_context(request),
        'title': 'Растры — хранилище и очистка',
        'folders': folders,
        'totals': totals,
        'grand_size_gb': round(grand_size / 1e9, 2),
        'grand_count': grand_count,
    })


def agro_raster_files_view(request):
    """File-level list for a single folder with checkbox bulk-delete.

    GET params: ``sensor``, ``scope``, ``year``, ``page`` (1-based, 100 per page).
    POST: ``paths`` = list of absolute file paths to delete.
    """
    from django.core.paginator import Paginator
    from .services import raster_storage

    if request.method == 'POST':
        paths = request.POST.getlist('paths')
        removed, freed = raster_storage.delete_paths(paths)
        messages.success(
            request,
            f'Удалено файлов: {removed} ({freed/1e6:.1f} МБ).',
        )
        qs = request.POST.get('return_query', '')
        return redirect(f'{request.path}?{qs}' if qs else 'admin:agro_rasters')

    sensor = request.GET.get('sensor', '')
    scope = request.GET.get('scope', '')
    year = request.GET.get('year', '')
    if sensor not in raster_storage.SENSORS or not scope or not year:
        messages.error(request, 'Укажите sensor, scope, year в URL.')
        return redirect('admin:agro_rasters')

    files = raster_storage.list_files(sensor, scope, year)
    paginator = Paginator(files, 100)
    page_num = request.GET.get('page') or 1
    page = paginator.get_page(page_num)

    total_size = sum(f.size_bytes for f in files)
    return render(request, 'admin/agrocosmos/raster_files.html', {
        **admin.site.each_context(request),
        'title': f'Растры — {sensor.upper()} / {scope} / {year}',
        'sensor': sensor,
        'scope': scope,
        'year': year,
        'page': page,
        'total_count': len(files),
        'total_mb': round(total_size / 1e6, 1),
        'return_query': request.GET.urlencode(),
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
        region=region, year=year, status=PipelineRun.Status.QUEUED,
        description=(
            f'[monitor init] {scope_desc}, {year} '
            f'({window_from}..{window_to}, valid≥{min_valid:.0%})'
        ),
    )
    try:
        _enqueue_ndvi_pipeline(
            run_id=run.pk, region_id=region.pk, district_id=did,
            year=year, min_valid=min_valid,
            overwrite=False, rebuild_fusion=True,
            date_from=window_from.isoformat(),
            date_to=window_to.isoformat(),
        )
        messages.success(
            request,
            f'Начальная выкачка S2+L8 поставлена в очередь (run #{run.pk}) '
            f'для окна {window_from}..{window_to}. Воркер подхватит в течение '
            f'нескольких секунд. Дальше cron раз в сутки.'
        )
    except Exception as exc:
        logger.exception('failed to enqueue initial raster monitoring run')
        run.status = PipelineRun.Status.FAILED
        run.log = f'Ошибка постановки в очередь: {exc}'
        run.finished_at = timezone.now()
        run.save()
        messages.error(request, f'Не удалось поставить начальную выкачку в очередь: {exc}')
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
