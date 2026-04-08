import threading
from datetime import date

from django.contrib import admin, messages
from django.shortcuts import redirect, render
from django.urls import path
from django.utils import timezone
from django.utils.html import format_html

from .models import (
    Region, District, Farmland, SatelliteScene, VegetationIndex,
    MonitoringTask, PipelineRun,
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
    list_display = ('region', 'year', 'status_badge', 'last_check',
                    'last_date_to', 'records_total', 'created_at')
    list_filter = ('status', 'year')
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
            path('agrocosmos/upload-farmlands/',
                 admin.site.admin_view(upload_farmlands_view),
                 name='agro_upload_farmlands'),
            path('agrocosmos/run-archive/',
                 admin.site.admin_view(run_archive_view),
                 name='agro_run_archive'),
            path('agrocosmos/start-monitoring/',
                 admin.site.admin_view(start_monitoring_view),
                 name='agro_start_monitoring'),
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
    from django.db.models import Count
    regions = Region.objects.annotate(
        farmland_count=Count('districts__farmlands'),
    )
    tasks = MonitoringTask.objects.select_related('region').all()[:20]
    pipeline_runs = PipelineRun.objects.select_related('region').all()[:30]
    current_year = date.today().year
    years = list(range(current_year, current_year - 6, -1))

    return render(request, 'admin/agrocosmos/panel.html', {
        **admin.site.each_context(request),
        'title': 'Агрокосмос — Управление',
        'regions': regions,
        'tasks': tasks,
        'pipeline_runs': pipeline_runs,
        'years': years,
        'current_year': current_year,
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


def _run_modis_bg(region_id, year, run_id=None):
    """Run modis_ndvi command in background thread."""
    try:
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        call_command(
            'modis_ndvi',
            region_id=region_id,
            year=year,
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


def _run_check_monitoring_bg(run_id=None):
    """Run check_monitoring management command in background thread."""
    from django.core.management import call_command
    from io import StringIO
    try:
        out = StringIO()
        call_command('check_monitoring', stdout=out, stderr=out)
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

    t = threading.Thread(
        target=_run_modis_bg,
        args=(int(region_id), year, run.pk),
        daemon=True,
    )
    t.start()

    messages.success(
        request,
        f'Загрузка архивных данных NDVI запущена: {region.name}, {year}. '
        f'Процесс выполняется в фоне (~20 мин).'
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
