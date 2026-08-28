"""REST API для ``my_fields``.

GeoJSON in/out — без зависимости от ``djangorestframework-gis``: GEOS
поддерживает ``GEOSGeometry.geojson`` нативно, парсинг через
``GEOSGeometry(json_str)``. Этого достаточно для CRUD-кейсов; если в
фазе 2 понадобятся сложные операции (PointField для GPS-фото и т.п.) —
введём библиотеку.

Все view-функции:
* требуют ``request.user.is_authenticated``;
* ограничивают выборку через ``permissions.can_*_field``;
* возвращают JSON в формате GeoJSON Feature / FeatureCollection для
  объектов с геометрией, и обычный JSON для журнала / сезонов.

Подход «функциональные view + ручная сериализация» выбран сознательно
вместо ``ModelViewSet`` + ``GeoFeatureModelSerializer``: контроля над
форматом больше, миграция на любые spec'ы (например, OGC API Features)
проще, и нет зависимостей сверх уже установленного DRF.
"""
from __future__ import annotations

import json
import re
from typing import Any

from django.conf import settings
from django.contrib.gis.geos import GEOSGeometry
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.dateparse import parse_date
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import (
    FieldEvent, FieldSeason, GisFolder, GisLayer, RasterLayer, UserField,
)
from .permissions import can_edit_field, can_view_field
from .services.geometry import (
    compute_area_ha, ensure_multipolygon, resolve_region_district,
)


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _require_auth(request: HttpRequest):
    """Проверка ``is_authenticated`` с единым форматом 401-ответа."""
    if not request.user.is_authenticated:
        return JsonResponse(
            {'error': 'authentication_required', 'detail': 'Войдите в кабинет.'},
            status=401,
        )
    return None


def _parse_json(request: HttpRequest) -> tuple[Any, JsonResponse | None]:
    """Распарсить тело запроса как JSON. На ошибку — 400."""
    try:
        return json.loads(request.body or b'{}'), None
    except json.JSONDecodeError as exc:
        return None, JsonResponse(
            {'error': 'invalid_json', 'detail': str(exc)},
            status=400,
        )


def _field_to_feature(f: UserField) -> dict:
    """Сериализация ``UserField`` → GeoJSON Feature.

    ``current_season`` подмешиваем в properties, чтобы UI правого
    сайдбара мог сразу показать актуальную культуру без N+1 на
    отдельный эндпоинт. Берём «свежий» сезон по году DESC + created DESC.
    Безопасно: если сезонов нет — поле просто отсутствует.
    """
    season = (
        f.seasons.order_by('-year', '-created_at').first()
        if f.pk else None
    )
    return {
        'type': 'Feature',
        'id': f.id,
        'geometry': json.loads(f.geom.geojson) if f.geom else None,
        'properties': {
            'name': f.name,
            'area_ha': f.area_ha,
            'crop_type': f.crop_type,
            'crop_type_display': f.get_crop_type_display(),
            'cadastral_number': f.cadastral_number,
            'notes': f.notes,
            'is_archived': f.is_archived,
            'region_id': f.region_id,
            'region_name': f.region.name if f.region_id else None,
            'district_id': f.district_id,
            'district_name': f.district.name if f.district_id else None,
            'created_at': f.created_at.isoformat(),
            'updated_at': f.updated_at.isoformat(),
            'current_season': _season_to_dict(season) if season else None,
        },
    }


def _event_to_dict(e: FieldEvent) -> dict:
    return {
        'id': e.id,
        'field_id': e.field_id,
        'season_id': e.season_id,
        'event_type': e.event_type,
        'event_type_display': e.get_event_type_display(),
        'event_date': e.event_date.isoformat(),
        'title': e.title,
        'description': e.description,
        'quantity': e.quantity,
        'quantity_unit': e.quantity_unit,
        'product_name': e.product_name,
        'cost_rub': e.cost_rub,
        'created_at': e.created_at.isoformat(),
    }


def _season_to_dict(s: FieldSeason) -> dict:
    return {
        'id': s.id,
        'field_id': s.field_id,
        'year': s.year,
        'crop': s.crop,
        'crop_display': s.get_crop_display(),
        'variety': s.variety,
        'sowing_date': s.sowing_date.isoformat() if s.sowing_date else None,
        'planned_harvest_date': s.planned_harvest_date.isoformat() if s.planned_harvest_date else None,
        'actual_harvest_date': s.actual_harvest_date.isoformat() if s.actual_harvest_date else None,
        'planned_yield_t_per_ha': s.planned_yield_t_per_ha,
        'actual_yield_t_per_ha': s.actual_yield_t_per_ha,
        'gross_t': s.gross_t,
        'notes': s.notes,
    }


def _apply_geom(field: UserField, geometry: dict) -> JsonResponse | None:
    """Применить GeoJSON-geometry к полю с резолвом площади и региона.

    Возвращает 400-JsonResponse при невалидной геометрии, иначе ``None``.
    """
    try:
        geom = GEOSGeometry(json.dumps(geometry), srid=4326)
        geom = ensure_multipolygon(geom)
    except (ValueError, TypeError, Exception) as exc:  # GEOSException наследует Exception
        return JsonResponse(
            {'error': 'invalid_geometry', 'detail': str(exc)},
            status=400,
        )
    field.geom = geom
    field.area_ha = compute_area_ha(geom)
    region_id, district_id = resolve_region_district(geom)
    field.region_id = region_id
    field.district_id = district_id
    return None


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/    — list / create
# ─────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_http_methods(['GET', 'POST'])
def fields_collection(request: HttpRequest) -> JsonResponse:
    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    if request.method == 'GET':
        return _fields_list(request)
    return _field_create(request)


def _fields_list(request: HttpRequest) -> JsonResponse:
    # По умолчанию — только активные. ``?archived=1`` показывает все.
    qs = UserField.objects.filter(owner=request.user)
    if request.GET.get('archived') != '1':
        qs = qs.filter(is_archived=False)
    qs = qs.select_related('region', 'district').order_by('-updated_at')
    return JsonResponse({
        'type': 'FeatureCollection',
        'features': [_field_to_feature(f) for f in qs],
    })


def _field_create(request: HttpRequest) -> JsonResponse:
    payload, err = _parse_json(request)
    if err:
        return err

    geometry = payload.get('geometry')
    name = (payload.get('properties') or {}).get('name') or payload.get('name')
    if not geometry or not name:
        return JsonResponse(
            {'error': 'missing_fields',
             'detail': 'Требуются ``geometry`` (GeoJSON) и ``name``.'},
            status=400,
        )

    # Парсинг геометрии (валидация формата; площадь сохраним в модели позже).
    try:
        tmp = GEOSGeometry(json.dumps(geometry), srid=4326)
        tmp = ensure_multipolygon(tmp)
        compute_area_ha(tmp)  # ранний smoke-тест геометрии
    except Exception as exc:
        return JsonResponse(
            {'error': 'invalid_geometry', 'detail': str(exc)}, status=400,
        )

    # ГИС-модуль не лимитируется тарифами: пользователи свободно
    # оцифровывают поля. Если в будущем понадобится квота — вернуть
    # вызов can_create_field() здесь.

    props = payload.get('properties') or {}
    field = UserField(
        owner=request.user,
        name=name[:120],
        crop_type=props.get('crop_type', UserField.CropType.ARABLE),
        cadastral_number=(props.get('cadastral_number') or '')[:50],
        notes=props.get('notes', ''),
    )
    err = _apply_geom(field, geometry)
    if err:
        return err
    field.save()

    season_warning = _maybe_create_season(field, payload.get('season'))

    feature = _field_to_feature(field)
    if season_warning:
        feature['properties']['season_warning'] = season_warning
    return JsonResponse(feature, status=201)


def _maybe_create_season(field: UserField, season_payload) -> str | None:
    """Опциональный сезон при создании поля.

    Все поля сезона необязательны, кроме ``year`` и ``crop`` — без них
    запись бессмысленна. Любая ошибка парсинга дат/чисел в сезоне НЕ
    должна откатывать создание поля — возвращаем текст предупреждения
    для ``season_warning`` в ответе.
    """
    if not (season_payload and season_payload.get('year') and season_payload.get('crop')):
        return None
    try:
        FieldSeason.objects.create(
            field=field,
            year=int(season_payload['year']),
            crop=season_payload['crop'],
            variety=(season_payload.get('variety') or '')[:120],
            sowing_date=parse_date(season_payload.get('sowing_date') or ''),
            planned_harvest_date=parse_date(
                season_payload.get('planned_harvest_date') or '',
            ),
            planned_yield_t_per_ha=season_payload.get('planned_yield_t_per_ha') or None,
            notes=season_payload.get('notes', ''),
        )
        return None
    except (ValueError, TypeError) as exc:
        return f'Сезон не создан: {exc}'


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/<id>/  — get / update / delete
# ─────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_http_methods(['GET', 'PATCH', 'DELETE'])
def field_detail(request: HttpRequest, pk: int) -> JsonResponse:
    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    field = get_object_or_404(
        UserField.objects.select_related('region', 'district'), pk=pk,
    )
    if request.method == 'GET':
        if not can_view_field(request.user, field):
            return JsonResponse({'error': 'forbidden'}, status=403)
        return JsonResponse(_field_to_feature(field))

    if not can_edit_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    if request.method == 'DELETE':
        field.delete()
        return JsonResponse({'ok': True}, status=200)

    return _field_patch(request, field)


def _field_patch(request: HttpRequest, field: UserField) -> JsonResponse:
    payload, err = _parse_json(request)
    if err:
        return err

    props = payload.get('properties') or {}
    if 'name' in props:
        field.name = (props['name'] or '')[:120]
    if 'crop_type' in props:
        field.crop_type = props['crop_type']
    if 'cadastral_number' in props:
        field.cadastral_number = (props['cadastral_number'] or '')[:50]
    if 'notes' in props:
        field.notes = props['notes'] or ''
    if 'is_archived' in props:
        field.is_archived = bool(props['is_archived'])

    if payload.get('geometry'):
        err = _apply_geom(field, payload['geometry'])
        if err:
            return err

    field.save()
    return JsonResponse(_field_to_feature(field))


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/monitoring/   — мониторинг ВСЕХ полей пользователя
# ─────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_http_methods(['GET', 'POST'])
def monitoring_collection(request: HttpRequest) -> JsonResponse:
    """Спутниковый мониторинг всех полей пользователя (кнопка в сайдбаре).

    * ``POST`` — ставит в очередь выкачку NDVI S2+L8 по каждому полю
      владельца; активные запуски не дублируются.
    * ``GET`` — сводка: сколько полей сейчас в обработке (для поллинга).
    """
    from .services.monitoring import (
        active_runs_count, enqueue_all_fields_monitoring,
    )

    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    if request.method == 'GET':
        return JsonResponse({'active': active_runs_count(request.user)})

    summary = enqueue_all_fields_monitoring(request.user)
    return JsonResponse(summary, status=202 if summary['created'] else 200)


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/<id>/monitoring/   — спутниковый мониторинг (NDVI S2+L8)
# ─────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_http_methods(['GET', 'POST'])
def field_monitoring(request: HttpRequest, pk: int) -> JsonResponse:
    """Запуск и статус выкачки NDVI-композитов по полю.

    * ``POST`` — поставить в очередь воркера скачивание S2 (приоритет) и
      L8/L9 NDVI по bbox поля за текущий год. Если запуск уже активен —
      возвращаем его же (200), дубликата не будет.
    * ``GET`` — статус последнего запуска (для поллинга из UI).
    """
    from .services.monitoring import (
        enqueue_field_monitoring, latest_run_for_field, run_to_dict,
    )

    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    field = get_object_or_404(UserField, pk=pk)
    if not can_view_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    if request.method == 'GET':
        return JsonResponse({'run': run_to_dict(latest_run_for_field(field))})

    if not can_edit_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    run, created = enqueue_field_monitoring(field)
    return JsonResponse(
        {'run': run_to_dict(run), 'created': created},
        status=202 if created else 200,
    )


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/<id>/events/    — list / create
# /api/my/fields/<id>/events/<eid>/   — patch / delete
# ─────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_http_methods(['GET', 'POST'])
def events_collection(request: HttpRequest, pk: int) -> JsonResponse:
    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    field = get_object_or_404(UserField, pk=pk)
    if not can_view_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    if request.method == 'GET':
        events = field.events.all()
        return JsonResponse({
            'count': events.count(),
            'results': [_event_to_dict(e) for e in events],
        })

    if not can_edit_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    payload, err = _parse_json(request)
    if err:
        return err

    event_type = payload.get('event_type')
    event_date = payload.get('event_date')
    if not event_type or not event_date:
        return JsonResponse(
            {'error': 'missing_fields',
             'detail': 'Требуются ``event_type`` и ``event_date``.'},
            status=400,
        )
    parsed_date = parse_date(event_date)
    if parsed_date is None:
        return JsonResponse(
            {'error': 'invalid_date', 'detail': 'event_date должно быть YYYY-MM-DD.'},
            status=400,
        )

    season = None
    if payload.get('season_id'):
        season = FieldSeason.objects.filter(
            pk=payload['season_id'], field=field,
        ).first()

    event = FieldEvent.objects.create(
        field=field,
        season=season,
        event_type=event_type,
        event_date=parsed_date,
        title=(payload.get('title') or '')[:180],
        description=payload.get('description', ''),
        quantity=payload.get('quantity'),
        quantity_unit=(payload.get('quantity_unit') or '')[:20],
        product_name=(payload.get('product_name') or '')[:180],
        cost_rub=payload.get('cost_rub'),
        created_by=request.user,
    )
    return JsonResponse(_event_to_dict(event), status=201)


@csrf_exempt
@require_http_methods(['PATCH', 'DELETE'])
def event_detail(request: HttpRequest, pk: int, eid: int) -> JsonResponse:
    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    field = get_object_or_404(UserField, pk=pk)
    if not can_edit_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)
    event = get_object_or_404(FieldEvent, pk=eid, field=field)

    if request.method == 'DELETE':
        event.delete()
        return JsonResponse({'ok': True})

    return _event_patch(request, field, event)


def _event_patch(request: HttpRequest, field: UserField, event: FieldEvent) -> JsonResponse:
    payload, err = _parse_json(request)
    if err:
        return err

    if 'event_type' in payload:
        event.event_type = payload['event_type']
    if 'event_date' in payload:
        d = parse_date(payload['event_date'] or '')
        if d is None:
            return JsonResponse(
                {'error': 'invalid_date'}, status=400,
            )
        event.event_date = d
    for f in ('title', 'description', 'product_name', 'quantity_unit'):
        if f in payload:
            setattr(event, f, payload[f] or '')
    for f in ('quantity', 'cost_rub'):
        if f in payload:
            setattr(event, f, payload[f])
    if 'season_id' in payload:
        event.season = FieldSeason.objects.filter(
            pk=payload['season_id'], field=field,
        ).first()

    event.save()
    return JsonResponse(_event_to_dict(event))


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/<id>/seasons/    — list / create
# /api/my/fields/<id>/seasons/<sid>/   — patch / delete
# ─────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_http_methods(['GET', 'POST'])
def seasons_collection(request: HttpRequest, pk: int) -> JsonResponse:
    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    field = get_object_or_404(UserField, pk=pk)
    if not can_view_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    if request.method == 'GET':
        seasons = field.seasons.all()
        return JsonResponse({
            'count': seasons.count(),
            'results': [_season_to_dict(s) for s in seasons],
        })

    if not can_edit_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)

    payload, err = _parse_json(request)
    if err:
        return err
    if not payload.get('year') or not payload.get('crop'):
        return JsonResponse(
            {'error': 'missing_fields',
             'detail': 'Требуются ``year`` и ``crop``.'},
            status=400,
        )

    season, created = FieldSeason.objects.update_or_create(
        field=field, year=int(payload['year']), crop=payload['crop'],
        defaults={
            'variety': (payload.get('variety') or '')[:120],
            'sowing_date': parse_date(payload.get('sowing_date') or ''),
            'planned_harvest_date': parse_date(payload.get('planned_harvest_date') or ''),
            'actual_harvest_date': parse_date(payload.get('actual_harvest_date') or ''),
            'planned_yield_t_per_ha': payload.get('planned_yield_t_per_ha'),
            'actual_yield_t_per_ha': payload.get('actual_yield_t_per_ha'),
            'gross_t': payload.get('gross_t'),
            'notes': payload.get('notes', ''),
        },
    )
    return JsonResponse(_season_to_dict(season), status=201 if created else 200)


@csrf_exempt
@require_http_methods(['PATCH', 'DELETE'])
def season_detail(request: HttpRequest, pk: int, sid: int) -> JsonResponse:
    auth_err = _require_auth(request)
    if auth_err:
        return auth_err

    field = get_object_or_404(UserField, pk=pk)
    if not can_edit_field(request.user, field):
        return JsonResponse({'error': 'forbidden'}, status=403)
    season = get_object_or_404(FieldSeason, pk=sid, field=field)

    if request.method == 'DELETE':
        season.delete()
        return JsonResponse({'ok': True})

    return _season_patch(request, season)


def _season_patch(request: HttpRequest, season: FieldSeason) -> JsonResponse:
    payload, err = _parse_json(request)
    if err:
        return err
    if 'variety' in payload:
        season.variety = (payload['variety'] or '')[:120]
    if 'notes' in payload:
        season.notes = payload['notes'] or ''
    for f in ('sowing_date', 'planned_harvest_date', 'actual_harvest_date'):
        if f in payload:
            setattr(season, f, parse_date(payload[f] or ''))
    for f in ('planned_yield_t_per_ha', 'actual_yield_t_per_ha', 'gross_t'):
        if f in payload:
            setattr(season, f, payload[f])
    season.save()
    return JsonResponse(_season_to_dict(season))


# ─────────────────────────────────────────────────────────────────────
# /api/my/fields/<id>/passport/...   — паспорт поля (NDVI-снимки + зоны)
# ─────────────────────────────────────────────────────────────────────

def _viewable_field_or_error(request: HttpRequest, pk: int):
    """(field, None) или (None, JsonResponse) — auth + права на просмотр."""
    auth_err = _require_auth(request)
    if auth_err:
        return None, auth_err
    field = get_object_or_404(
        UserField.objects.select_related('region', 'district'), pk=pk,
    )
    if not can_view_field(request.user, field):
        return None, JsonResponse({'error': 'forbidden'}, status=403)
    return field, None


@require_http_methods(['GET'])
def field_passport_frames(request: HttpRequest, pk: int) -> JsonResponse:
    """Последние NDVI-композиты по полю (кадры-превью «Снимки NDVI»)."""
    from .services import passport

    field, err = _viewable_field_or_error(request, pk)
    if err:
        return err

    year = request.GET.get('year') or passport.default_year()
    try:
        limit = max(1, min(int(request.GET.get('limit', 6)), 12))
    except (TypeError, ValueError):
        limit = 6

    data = passport.raster_frames(field, year, limit=limit)
    return JsonResponse({'ok': True, **data})


@require_http_methods(['GET'])
def field_passport_preview(request: HttpRequest, pk: int) -> HttpResponse:
    """PNG-превью композита, обрезанное по bbox поля (для кадров NDVI)."""
    from .services import passport

    field, err = _viewable_field_or_error(request, pk)
    if err:
        # для <img> отдаём 204 вместо JSON, чтобы фрейм просто скрылся
        return HttpResponse(b'', content_type='image/png', status=204)

    sensor = request.GET.get('sensor', 's2')
    date_range = request.GET.get('date', '')
    if not date_range:
        return HttpResponse(b'', content_type='image/png', status=204)

    png_bytes = passport.preview_png(field, sensor, date_range)
    if not png_bytes:
        return HttpResponse(b'', content_type='image/png', status=204)

    resp = HttpResponse(png_bytes, content_type='image/png')
    resp['Cache-Control'] = 'public, max-age=3600'
    return resp


@require_http_methods(['GET'])
def field_passport_zones(request: HttpRequest, pk: int) -> JsonResponse:
    """Карта зон неоднородности + динамика к предыдущему композиту."""
    from .services import passport

    field, err = _viewable_field_or_error(request, pk)
    if err:
        return err

    year = request.GET.get('year') or passport.default_year()
    date_range = request.GET.get('date', '')
    sensor = request.GET.get('sensor', '')
    return JsonResponse({
        'ok': True,
        'zones': passport.zones(field, year, date_range, sensor),
    })


@require_http_methods(['GET'])
def field_passport_zones_kml(request: HttpRequest, pk: int) -> HttpResponse:
    """KML-экспорт зон неоднородности для ПО БПЛА DJI (Pilot 2 / Fly)."""
    from agrocosmos.services.raster_tiles import zones_to_kml_document

    from .services import passport

    field, err = _viewable_field_or_error(request, pk)
    if err:
        return err

    year = request.GET.get('year') or passport.default_year()
    feats, comp = passport.zone_features(
        field, year, request.GET.get('date', ''), request.GET.get('sensor', ''),
    )
    if not feats:
        return JsonResponse({'ok': False, 'error': 'no raster data'}, status=404)

    kml = zones_to_kml_document(feats, field.pk, comp['date_from'], comp['date_to'])
    resp = HttpResponse(kml, content_type='application/vnd.google-earth.kml+xml')
    fname = f"zones_f{field.pk}_{comp['date_from']}.kml"
    resp['Content-Disposition'] = f'attachment; filename="{fname}"'
    return resp


@require_http_methods(['GET'])
def field_passport_zones_shp(request: HttpRequest, pk: int) -> HttpResponse:
    """SHP-карта-предписание (VRA) для DJI Agras — zip с shapefile.

    Нормы внесения (л/га или кг/га) задаёт агроном через ``rate_problem`` /
    ``rate_warn`` / ``rate_ok``. Импорт в пульте Agras: SD-карта →
    Prescription Map → Map Source: Other, unit: ha.
    """
    from agrocosmos.services.raster_tiles import zones_to_agras_shp_zip

    from .services import passport

    field, err = _viewable_field_or_error(request, pk)
    if err:
        return err

    rates = {}
    for zone in ('problem', 'warn', 'ok'):
        raw = request.GET.get(f'rate_{zone}', '0')
        try:
            rates[zone] = max(float(raw), 0.0)
        except (TypeError, ValueError):
            return JsonResponse(
                {'ok': False, 'error': f'invalid rate_{zone}'}, status=400)

    year = request.GET.get('year') or passport.default_year()
    feats, comp = passport.zone_features(
        field, year, request.GET.get('date', ''), request.GET.get('sensor', ''),
    )
    if not feats:
        return JsonResponse({'ok': False, 'error': 'no raster data'}, status=404)

    name = f"prescription_f{field.pk}_{comp['date_from']}"
    zip_bytes = zones_to_agras_shp_zip(feats, rates, name)
    resp = HttpResponse(zip_bytes, content_type='application/zip')
    resp['Content-Disposition'] = f'attachment; filename="{name}.zip"'
    return resp


# ─────────────────────────────────────────────────────────────────────
# ГИС-слои: загрузка SHP (ZIP) → таблица PostGIS на каждый .shp
# /me/gis/api/layers/           — GET список / POST загрузка (multipart)
# /me/gis/api/layers/<id>/      — DELETE (дроп таблицы)
# /me/gis/api/layers/<id>/tiles/<z>/<x>/<y>.pbf — универсальный MVT
# Всё gated под admin (как и вся страница /me/gis).
# ─────────────────────────────────────────────────────────────────────

_GIS_RESOURCE = 'gis_layer'
_RASTER_RESOURCE = 'raster_layer'

_HEX_COLOR_RE = re.compile(r'^#[0-9a-fA-F]{6}$')


def _normalize_hex_color(value: Any) -> str | None:
    """Приводит цвет к ``#rrggbb`` (нижний регистр) или ``None`` если невалиден.

    Принимает как ``#RRGGBB``, так и короткую форму ``#RGB`` (расширяем).
    """
    if not isinstance(value, str):
        return None
    color = value.strip()
    if re.fullmatch(r'#[0-9a-fA-F]{3}', color):
        color = '#' + ''.join(ch * 2 for ch in color[1:])
    if _HEX_COLOR_RE.fullmatch(color):
        return color.lower()
    return None


# Режимы тематической раскраски и лимиты (защита от «тяжёлых» стилей).
_STYLE_MODES = frozenset({'single', 'categorical', 'graduated'})
_MAX_CATEGORIES = 60
_MAX_STOPS = 12


def _normalize_categorical(value: Any, field: str):
    """Ветка ``categorical`` для :func:`_normalize_style`."""
    raw = value.get('categories')
    if not isinstance(raw, list) or not raw:
        return None, 'categories пуст'
    if len(raw) > _MAX_CATEGORIES:
        return None, f'слишком много категорий (>{_MAX_CATEGORIES})'
    cats = []
    for item in raw:
        if not isinstance(item, dict):
            return None, 'категория должна быть объектом'
        color = _normalize_hex_color(item.get('color'))
        if color is None:
            return None, 'некорректный цвет категории'
        # value приводим к строке (сопоставление в MVT — по строке).
        cats.append({'value': str(item.get('value', '')), 'color': color})
    other = _normalize_hex_color(value.get('other_color')) or '#cccccc'
    return {'mode': 'categorical', 'field': field,
            'categories': cats, 'other_color': other}, None


def _normalize_graduated(value: Any, field: str):
    """Ветка ``graduated`` для :func:`_normalize_style`."""
    raw = value.get('stops')
    if not isinstance(raw, list) or len(raw) < 2:
        return None, 'нужно минимум 2 stops'
    if len(raw) > _MAX_STOPS:
        return None, f'слишком много stops (>{_MAX_STOPS})'
    stops = []
    for item in raw:
        if not isinstance(item, dict):
            return None, 'stop должен быть объектом'
        try:
            num = float(item.get('value'))
        except (TypeError, ValueError):
            return None, 'value stop должен быть числом'
        color = _normalize_hex_color(item.get('color'))
        if color is None:
            return None, 'некорректный цвет stop'
        stops.append({'value': num, 'color': color})
    stops.sort(key=lambda s: s['value'])
    return {'mode': 'graduated', 'field': field, 'stops': stops}, None


def _attach_display_opts(style: dict, value: dict) -> dict:
    """Добавить к нормализованному стилю сквозные опции отображения слоя.

    ``opacity`` — прозрачность заливки полигонов/точек (float, клипуется в
    [0, 1]); ``locked`` — запрет выбора объектов слоя по клику на карте (bool).
    Обе опции не зависят от режима раскраски. Некорректная ``opacity`` тихо
    игнорируется (не роняем сохранение стиля).
    """
    opacity = value.get('opacity')
    if opacity is not None:
        try:
            style['opacity'] = max(0.0, min(1.0, float(opacity)))
        except (TypeError, ValueError):
            pass
    if 'locked' in value:
        style['locked'] = bool(value.get('locked'))
    return style


def _normalize_style(value: Any, layer: GisLayer):
    """Валидировать и нормализовать style-конфиг раскраски слоя.

    Возвращает ``(style_dict, None)`` при успехе или ``(None, error_msg)``.
    ``single`` (или пустой) → ``{'mode': 'single'}``. Для categorical/graduated
    поле должно присутствовать в ``layer.attributes``. Сквозные опции
    отображения (``opacity``, ``locked``) сохраняются при любом режиме.
    """
    if value in (None, '', {}):
        return {'mode': 'single'}, None
    if not isinstance(value, dict):
        return None, 'style должен быть объектом'

    mode = value.get('mode', 'single')
    if mode not in _STYLE_MODES:
        return None, 'неизвестный mode'

    if mode == 'single':
        base, err = {'mode': 'single'}, None
    else:
        field = value.get('field')
        valid_fields = {a.get('db') for a in (layer.attributes or [])}
        if not isinstance(field, str) or field not in valid_fields:
            return None, 'field не найден среди атрибутов слоя'
        if mode == 'categorical':
            base, err = _normalize_categorical(value, field)
        else:
            base, err = _normalize_graduated(value, field)

    if err:
        return None, err
    return _attach_display_opts(base, value), None


def _require_gis_authenticated(request: HttpRequest):
    """None если есть Django-сессия, иначе 401 (как раньше)."""
    if not request.user.is_authenticated:
        return JsonResponse(
            {'error': 'authentication_required'}, status=401)
    return None


def _require_resource_access(request: HttpRequest, resource: str, *,
                             level: str = 'view', pk: int | None = None):
    """Гейт доступа к ресурсу ``resource`` на основе грантов (``access``).

    Возвращает ``None`` при доступе, иначе ``JsonResponse`` 401/403.

    * ``pk`` задан — проверяем доступ к конкретному ресурсу (или whole-class);
    * ``pk`` не задан — действие над «всем классом» (загрузка/список/reorder):
      для ``view`` достаточно доступа к странице (любой ГИС/растровый грант),
      для ``edit``/``manage`` нужен whole-class грант нужного уровня.
    """
    from access.services import (
        can_open_gis_page, has_resource_access, is_admin_legacy_user,
    )

    auth = _require_gis_authenticated(request)
    if auth:
        return auth
    user = getattr(request, 'legacy_user', None)
    if is_admin_legacy_user(user):
        return None

    if pk is not None:
        allowed = has_resource_access(user, resource, pk, level)
    elif level == 'view':
        allowed = can_open_gis_page(user)
    else:
        allowed = has_resource_access(user, resource, None, level)

    return None if allowed else JsonResponse({'error': 'forbidden'}, status=403)


def _require_gis_access(request: HttpRequest, *, level: str = 'view',
                        pk: int | None = None):
    """Гейт доступа к векторным ГИС-слоям (SHP). См. :func:`_require_resource_access`."""
    return _require_resource_access(request, _GIS_RESOURCE, level=level, pk=pk)


def _require_raster_access(request: HttpRequest, *, level: str = 'view',
                           pk: int | None = None):
    """Гейт доступа к растровым слоям. См. :func:`_require_resource_access`."""
    return _require_resource_access(request, _RASTER_RESOURCE, level=level, pk=pk)


def _gis_folder_to_dict(folder) -> dict:
    return {
        'id': folder.pk,
        'name': folder.name,
        'sort_order': folder.sort_order,
        'collapsed': folder.collapsed,
        'visible': folder.visible,
        'created_at': folder.created_at.isoformat(),
    }


def _gis_layer_to_dict(layer: GisLayer) -> dict:
    return {
        'id': layer.pk,
        'folder': layer.folder_id,
        'title': layer.title,
        'table_name': layer.table_name,
        'original_filename': layer.original_filename,
        'source_archive': layer.source_archive,
        'geom_kind': layer.geom_kind,
        'geom_type': layer.geom_type,
        'srid_original': layer.srid_original,
        'feature_count': layer.feature_count,
        'attributes': layer.attributes,
        'extent': layer.extent,
        'color': layer.color,
        'style': layer.style or {},
        'sort_order': layer.sort_order,
        'created_at': layer.created_at.isoformat(),
        'tiles_url': f'/me/gis/api/layers/{layer.pk}/tiles/{{z}}/{{x}}/{{y}}.pbf',
    }


def _gis_layers_list(request: HttpRequest) -> JsonResponse:
    """GET — список слоёв, отфильтрованный по грантам пользователя."""
    gate = _require_gis_access(request, level='view')
    if gate:
        return gate
    from access.services import accessible_gis_layer_ids
    layers = GisLayer.objects.all()
    ids = accessible_gis_layer_ids(getattr(request, 'legacy_user', None))
    if ids is not None:
        layers = layers.filter(pk__in=ids)
    return JsonResponse({
        'ok': True,
        'count': layers.count(),
        'results': [_gis_layer_to_dict(x) for x in layers],
        'folders': [_gis_folder_to_dict(f) for f in GisFolder.objects.all()],
    })


def _gis_layers_upload(request: HttpRequest) -> JsonResponse:
    """POST — создание слоёв из ZIP (multipart). Нужен whole-class 'manage'."""
    gate = _require_gis_access(request, level='manage')
    if gate:
        return gate

    # POST — приём одного/нескольких ZIP через multipart/form-data.
    from .services.shp_import import ShapefileImportError, import_zip

    files = request.FILES.getlist('files') or request.FILES.getlist('file')
    if not files:
        return JsonResponse(
            {'ok': False, 'error': 'no_files',
             'detail': 'Прикрепите ZIP-архив(ы) с шейп-файлами.'},
            status=400,
        )

    created, errors = [], []
    for f in files:
        if not f.name.lower().endswith('.zip'):
            errors.append({'file': f.name, 'error': 'Ожидается .zip архив.'})
            continue
        try:
            result = import_zip(f, owner=request.user, archive_name=f.name)
        except ShapefileImportError as e:
            errors.append({'file': f.name, 'error': str(e)})
            continue
        except Exception as e:  # noqa: BLE001
            errors.append({'file': f.name, 'error': f'Ошибка импорта: {e}'})
            continue
        created.extend(_gis_layer_to_dict(x) for x in result['created'])
        errors.extend(result['errors'])

    status = 200 if created else (400 if errors else 200)
    return JsonResponse(
        {'ok': bool(created), 'created': created, 'errors': errors},
        status=status,
    )


@csrf_exempt
@require_http_methods(['GET', 'POST'])
def gis_layers_collection(request: HttpRequest) -> JsonResponse:
    """Список ГИС-слоёв (GET) или загрузка ZIP с шейп-файлами (POST)."""
    if request.method == 'GET':
        return _gis_layers_list(request)
    return _gis_layers_upload(request)


@csrf_exempt
@require_http_methods(['POST'])
def gis_layer_create(request: HttpRequest) -> JsonResponse:
    """POST JSON — создать новый пустой слой с типом геометрии и атрибутами.

    Body: ``{title, geom_kind: point|line|polygon, attributes: [{name, type}]}``.
    Нужен whole-class ``manage`` (как загрузка SHP).
    """
    gate = _require_gis_access(request, level='manage')
    if gate:
        return gate

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    title = str(data.get('title', '')).strip()
    geom_kind = data.get('geom_kind')
    attributes = data.get('attributes') or []
    if not title:
        return JsonResponse(
            {'ok': False, 'error': 'empty_title',
             'detail': 'Укажите название слоя.'}, status=400)
    if geom_kind not in ('point', 'line', 'polygon'):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_geom_kind',
             'detail': 'Выберите тип геометрии.'}, status=400)
    if not isinstance(attributes, list):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_attributes',
             'detail': 'attributes должен быть списком.'}, status=400)

    from .services.shp_import import ShapefileImportError, create_empty_layer
    try:
        layer = create_empty_layer(
            title, geom_kind, attributes, owner=request.user)
    except ShapefileImportError as e:
        return JsonResponse(
            {'ok': False, 'error': 'create_failed', 'detail': str(e)},
            status=400)
    except Exception as e:  # noqa: BLE001
        return JsonResponse(
            {'ok': False, 'error': 'create_failed',
             'detail': f'Ошибка создания слоя: {e}'}, status=400)
    return JsonResponse(
        {'ok': True, 'layer': _gis_layer_to_dict(layer)}, status=201)


@csrf_exempt
@require_http_methods(['PATCH', 'DELETE'])
def gis_layer_detail(request: HttpRequest, pk: int) -> JsonResponse:
    """PATCH — переименовать слой; DELETE — дроп таблицы + записи реестра."""
    # DELETE — удаление (manage); PATCH — переименование (edit) конкретного слоя.
    level = 'manage' if request.method == 'DELETE' else 'edit'
    gate = _require_gis_access(request, level=level, pk=pk)
    if gate:
        return gate

    layer = get_object_or_404(GisLayer, pk=pk)

    if request.method == 'DELETE':
        from .services.shp_import import drop_layer
        drop_layer(layer)
        return JsonResponse({'ok': True})

    return _gis_layer_patch(request, layer)


def _gis_layer_patch(request: HttpRequest, layer: GisLayer) -> JsonResponse:
    """PATCH-часть :func:`gis_layer_detail`: title / color / style."""
    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_json'}, status=400)

    update_fields: list[str] = []

    if 'title' in data:
        title = str(data.get('title', '')).strip()
        if not title:
            return JsonResponse(
                {'ok': False, 'error': 'empty_title',
                 'detail': 'Название слоя не может быть пустым.'},
                status=400,
            )
        layer.title = title[:200]
        update_fields.append('title')

    if 'color' in data:
        color = _normalize_hex_color(data.get('color'))
        if color is None:
            return JsonResponse(
                {'ok': False, 'error': 'invalid_color',
                 'detail': 'Ожидается цвет в формате #RRGGBB.'},
                status=400,
            )
        layer.color = color
        update_fields.append('color')

    if 'style' in data:
        style, err = _normalize_style(data.get('style'), layer)
        if err:
            return JsonResponse(
                {'ok': False, 'error': 'invalid_style', 'detail': err},
                status=400,
            )
        layer.style = style
        update_fields.append('style')

    if not update_fields:
        return JsonResponse(
            {'ok': False, 'error': 'nothing_to_update',
             'detail': 'Не передано ни title, ни color, ни style.'},
            status=400,
        )

    layer.save(update_fields=update_fields)
    return JsonResponse({'ok': True, 'layer': _gis_layer_to_dict(layer)})


def _parse_bbox(raw):
    """Разобрать ``bbox=minx,miny,maxx,maxy`` (EPSG:4326) в кортеж float.

    Возвращает ``None``, если параметр отсутствует/некорректен либо вырожден
    (нулевая площадь) — тогда вызывающий код грузит без фильтра по экстенту.
    """
    if not raw:
        return None
    parts = str(raw).split(',')
    if len(parts) != 4:
        return None
    try:
        minx, miny, maxx, maxy = (float(p) for p in parts)
    except (TypeError, ValueError):
        return None
    if maxx <= minx or maxy <= miny:
        return None
    return (minx, miny, maxx, maxy)


def _gis_feature_rank(request: HttpRequest, layer, sort: str,
                      direction: str, query_text: str) -> JsonResponse:
    """Ответ на ``?rank_of=<fid>``: позиция объекта в текущем порядке/фильтре."""
    from .services.shp_import import feature_rank
    try:
        fid = int(request.GET.get('rank_of'))
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid rank_of'},
                            status=400)
    info = feature_rank(layer, fid, sort=sort, direction=direction,
                        query_text=query_text)
    return JsonResponse({'ok': True, 'rank': info['rank'],
                         'total': info['total']})


@csrf_exempt
@require_http_methods(['GET', 'POST', 'DELETE'])
def gis_layer_features(request: HttpRequest, pk: int) -> JsonResponse:
    """GET — список объектов слоя; POST — создать объект; DELETE — пакетно
    удалить объекты.

    * ``GET`` (уровень ``view``): постранично id + атрибуты для таблицы. С
      ``?geometry=1`` — GeoJSON FeatureCollection (id + точная геометрия) для
      загрузки в редактор draw.
    * ``POST`` (уровень ``edit``): создать новый объект с переданной
      геометрией (атрибуты — NULL). Тело: ``{"geometry": <GeoJSON>}``.
    * ``DELETE`` (уровень ``edit``): удалить объекты списком id одним запросом.
      Тело: ``{"ids": [...]}``.
    """
    if request.method == 'POST':
        return _gis_feature_create(request, pk)
    if request.method == 'DELETE':
        return _gis_features_bulk_delete(request, pk)

    gate = _require_gis_access(request, level='view', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    if request.GET.get('geometry') in ('1', 'true', 'yes'):
        from .services.shp_import import get_features_geojson
        bbox = _parse_bbox(request.GET.get('bbox'))
        fc = get_features_geojson(layer, bbox=bbox)
        return JsonResponse({
            'ok': True,
            'geom_kind': layer.geom_kind,
            'featurecollection': fc,
        })

    sort = request.GET.get('sort', 'id') or 'id'
    direction = request.GET.get('dir', 'asc')
    query_text = request.GET.get('q', '')

    # rank_of=<fid>: 0-based позиция объекта в текущем порядке/фильтре — для
    # перехода таблицы на страницу с этой строкой при клике по полигону на
    # карте (двусторонняя синхронизация на слоях с сотнями тысяч объектов).
    if request.GET.get('rank_of') is not None:
        return _gis_feature_rank(request, layer, sort, direction, query_text)

    # ids_only=1: все id объектов (с учётом поиска), без пагинации — для
    # «выделить все объекты таблицы».
    if request.GET.get('ids_only') in ('1', 'true', 'yes'):
        from .services.shp_import import list_feature_ids
        ids = list_feature_ids(layer, query_text=query_text)
        return JsonResponse({'ok': True, 'ids': ids, 'total': len(ids)})

    from .services.shp_import import list_features
    try:
        limit = int(request.GET.get('limit', 1000))
    except (TypeError, ValueError):
        limit = 1000
    limit = max(1, min(limit, 5000))
    try:
        offset = int(request.GET.get('offset', 0))
    except (TypeError, ValueError):
        offset = 0
    offset = max(0, offset)

    data = list_features(layer, limit=limit, offset=offset,
                         sort=sort, direction=direction, query_text=query_text)
    return JsonResponse({
        'ok': True,
        'total': data['total'],
        'results': data['results'],
        'attributes': layer.attributes,
        'limit': limit,
        'offset': offset,
        'sort': sort,
        'dir': direction,
        'q': query_text,
    })


@csrf_exempt
@require_http_methods(['GET'])
def gis_layer_field_stats(request: HttpRequest, pk: int) -> JsonResponse:
    """GET — статистика по колонке слоя для построения раскраски.

    ``?field=<db>`` — обязателен. Для числовых полей вернёт min/max, для
    остальных — уникальные значения (по частоте). Уровень доступа ``view``.
    """
    gate = _require_gis_access(request, level='view', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    field = request.GET.get('field', '')
    if not field:
        return JsonResponse(
            {'ok': False, 'error': 'no_field',
             'detail': 'Укажите параметр field.'},
            status=400,
        )

    from .services.shp_import import field_stats
    stats = field_stats(layer, field)
    if stats is None:
        return JsonResponse(
            {'ok': False, 'error': 'unknown_field',
             'detail': 'Поле не найдено среди атрибутов слоя.'},
            status=404,
        )
    return JsonResponse({'ok': True, 'stats': stats})


@csrf_exempt
@require_http_methods(['POST'])
def gis_layer_query(request: HttpRequest, pk: int) -> JsonResponse:
    """POST — SQL-выборка (визуальный конструктор) по таблице слоя.

    Тело JSON::

        {"filter": {...}, "q": "", "sort": "id", "dir": "asc",
         "limit": 1000, "offset": 0, "save_as": "<название>"?}

    * без ``save_as`` — постраничный список отфильтрованных объектов (как
      таблица атрибутов), уровень ``view``;
    * с ``save_as`` — материализовать результат выборки в НОВЫЙ слой (уровень
      whole-class ``manage``, как загрузка/создание слоя) и вернуть его.

    ``filter`` компилируется безопасно на сервере (см.
    :mod:`my_fields.services.layer_query`); сырой SQL от клиента не принимается.
    """
    gate = _require_gis_access(request, level='view', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)
    if not isinstance(data, dict):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    filter_spec = data.get('filter')
    query_text = str(data.get('q', '') or '')
    sort = data.get('sort', 'id') or 'id'
    direction = data.get('dir', 'asc')

    # rank_of=<fid>: 0-based позиция объекта в текущем фильтре/порядке — для
    # синхронизации карты и таблицы (клик по полигону → нужная страница) при
    # активном структурном фильтре (аналог GET-ветки features).
    if data.get('rank_of') is not None:
        return _gis_query_rank(layer, data, sort, direction, query_text, filter_spec)
    if data.get('save_as') is not None:
        return _gis_query_save_as(
            request, layer, data.get('save_as'), filter_spec, query_text)
    if data.get('ids_only'):
        return _gis_query_ids(layer, query_text, filter_spec)
    return _gis_query_list(layer, data, sort, direction, query_text, filter_spec)


def _gis_query_ids(layer, query_text, filter_spec):
    """Все id объектов под текущим фильтром/поиском (без пагинации) — для
    «выделить все объекты таблицы»."""
    from .services import shp_import
    from .services.layer_query import LayerQueryError

    try:
        ids = shp_import.list_feature_ids(
            layer, query_text=query_text, filter_spec=filter_spec)
    except LayerQueryError as e:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_filter', 'detail': str(e)}, status=400)
    return JsonResponse({'ok': True, 'ids': ids, 'total': len(ids)})


def _gis_query_rank(layer, data, sort, direction, query_text, filter_spec):
    """Позиция (rank) объекта в текущем фильтре/порядке (для клика по карте)."""
    from .services import shp_import
    from .services.layer_query import LayerQueryError

    try:
        fid = int(data.get('rank_of'))
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid rank_of'}, status=400)
    try:
        info = shp_import.feature_rank(
            layer, fid, sort=sort, direction=direction,
            query_text=query_text, filter_spec=filter_spec)
    except LayerQueryError as e:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_filter', 'detail': str(e)}, status=400)
    return JsonResponse({'ok': True, 'rank': info['rank'], 'total': info['total']})


def _gis_query_save_as(request, layer, save_as, filter_spec, query_text):
    """Материализовать результат выборки в новый слой (whole-class manage)."""
    from .services import shp_import
    from .services.layer_query import LayerQueryError

    mgate = _require_gis_access(request, level='manage')
    if mgate:
        return mgate
    title = str(save_as).strip()
    if not title:
        return JsonResponse(
            {'ok': False, 'error': 'empty_title',
             'detail': 'Укажите название слоя.'}, status=400)
    try:
        new_layer = shp_import.create_layer_from_query(
            layer, title, filter_spec=filter_spec,
            query_text=query_text, owner=request.user)
    except LayerQueryError as e:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_filter', 'detail': str(e)}, status=400)
    return JsonResponse({'ok': True, 'layer': _gis_layer_to_dict(new_layer)})


@csrf_exempt
@require_http_methods(['POST'])
def gis_layer_duplicate(request: HttpRequest, pk: int) -> JsonResponse:
    """POST — создать полную копию слоя (все объекты и атрибуты).

    Название нового слоя — с префиксом ``копия_`` (можно переопределить полем
    ``title`` в теле). Уровень доступа — как у создания слоя: whole-class
    ``manage``.
    """
    from .services import shp_import

    gate = _require_gis_access(request, level='manage')
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    title = ''
    if request.body:
        try:
            data = json.loads(request.body)
            if isinstance(data, dict):
                title = str(data.get('title', '') or '')
        except (ValueError, TypeError):
            pass
    try:
        new_layer = shp_import.duplicate_layer(
            layer, owner=request.user, title=title)
    except shp_import.ShapefileImportError as e:
        return JsonResponse(
            {'ok': False, 'error': 'duplicate_failed', 'detail': str(e)},
            status=400)
    return JsonResponse(
        {'ok': True, 'layer': _gis_layer_to_dict(new_layer)}, status=201)


def _gis_query_list(layer, data, sort, direction, query_text, filter_spec):
    """Постраничный список отфильтрованных объектов (как таблица атрибутов)."""
    from .services import shp_import
    from .services.layer_query import LayerQueryError

    try:
        limit = int(data.get('limit', 1000))
    except (TypeError, ValueError):
        limit = 1000
    limit = max(1, min(limit, 5000))
    try:
        offset = int(data.get('offset', 0))
    except (TypeError, ValueError):
        offset = 0
    offset = max(0, offset)

    try:
        result = shp_import.list_features(
            layer, limit=limit, offset=offset, sort=sort,
            direction=direction, query_text=query_text, filter_spec=filter_spec)
    except LayerQueryError as e:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_filter', 'detail': str(e)}, status=400)
    return JsonResponse({
        'ok': True,
        'total': result['total'],
        'results': result['results'],
        'attributes': layer.attributes,
        'limit': limit,
        'offset': offset,
        'sort': sort,
        'dir': direction,
        'filter': filter_spec,
        'q': query_text,
    })


@csrf_exempt
@require_http_methods(['POST'])
def gis_overlay_create(request: HttpRequest) -> JsonResponse:
    """POST — поставить в очередь оверлей двух слоёв (async через PipelineRun).

    Тело JSON::

        {"layer_a_id": <int>, "layer_b_id": <int>,
         "op": "intersection|difference|union|symmetric_difference",
         "title": "<название нового слоя>"}

    Создание нового слоя — whole-class ``manage`` (как загрузка SHP); плюс
    ``view`` на каждый исходный слой. Тяжёлая операция выполняется воркером
    (``run_gis_overlay``); эндпоинт лишь ставит задачу и возвращает ``run_id``
    для опроса статуса.
    """
    gate = _require_gis_access(request, level='manage')
    if gate:
        return gate

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)
    if not isinstance(data, dict):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    prepared = _prepare_overlay(request, data)
    if isinstance(prepared, JsonResponse):
        return prepared
    layer_a, layer_b, params = prepared

    from agrocosmos.models import PipelineRun
    from .services.overlay import op_label

    owner = request.user if getattr(request.user, 'is_authenticated', False) else None
    if layer_b is not None:
        desc = f'{op_label(params["op"])}: {layer_a.title} × {layer_b.title}'
    else:
        desc = f'{op_label(params["op"])}: {layer_a.title}'
    run = PipelineRun.objects.create(
        task_type=PipelineRun.TaskType.GIS_OVERLAY,
        status=PipelineRun.Status.QUEUED,
        description=desc[:500],
        launch_args={
            'layer_a_id': params['layer_a_id'],
            'layer_b_id': params['layer_b_id'],
            'op': params['op'],
            'title': params['title'],
            'params': params['params'],
            'owner_id': owner.pk if owner is not None else None,
        },
    )
    return JsonResponse(
        {'ok': True, 'run_id': run.pk, 'status': run.status}, status=202)


def _validate_overlay_request(data):
    """Проверить тело запроса геообработки → ``(params, None)`` | ``(None, JsonResponse)``.

    Поддерживает одно-слойные операции (только ``layer_a_id``), двух-слойные
    оверлеи и spatial join (``layer_a_id`` + ``layer_b_id``). Параметры операции
    передаются в ``params`` (буфер/упрощение/dissolve/spatial join).
    """
    from .services.overlay import ALL_OPS, SINGLE_OPS

    op = str(data.get('op', '') or '')
    title = str(data.get('title', '') or '').strip()
    op_params = data.get('params')
    if not isinstance(op_params, dict):
        op_params = {}
    if op not in ALL_OPS:
        return None, JsonResponse(
            {'ok': False, 'error': 'invalid_op',
             'detail': 'Неизвестная операция.'}, status=400)
    try:
        layer_a_id = int(data.get('layer_a_id'))
    except (TypeError, ValueError):
        return None, JsonResponse(
            {'ok': False, 'error': 'invalid_layers',
             'detail': 'Укажите слой.'}, status=400)

    single = op in SINGLE_OPS
    layer_b_id = None
    if not single:
        try:
            layer_b_id = int(data.get('layer_b_id'))
        except (TypeError, ValueError):
            return None, JsonResponse(
                {'ok': False, 'error': 'invalid_layers',
                 'detail': 'Укажите оба слоя.'}, status=400)
        if layer_a_id == layer_b_id:
            return None, JsonResponse(
                {'ok': False, 'error': 'same_layer',
                 'detail': 'Выберите два разных слоя.'}, status=400)
    if not title:
        return None, JsonResponse(
            {'ok': False, 'error': 'empty_title',
             'detail': 'Укажите название слоя.'}, status=400)
    return {'op': op, 'title': title, 'layer_a_id': layer_a_id,
            'layer_b_id': layer_b_id, 'params': op_params}, None


def _prepare_overlay(request, data):
    """Валидация + доступ + загрузка слоёв.

    Возвращает ``(layer_a, layer_b, params)`` при успехе (``layer_b`` может быть
    ``None`` для одно-слойных операций) либо ``JsonResponse`` с ошибкой.
    """
    from .services.overlay import OVERLAY_OPS

    params, err = _validate_overlay_request(data)
    if err:
        return err

    ids = [params['layer_a_id']]
    if params['layer_b_id'] is not None:
        ids.append(params['layer_b_id'])
    # Доступ на чтение каждого исходного слоя.
    for lid in ids:
        vgate = _require_gis_access(request, level='view', pk=lid)
        if vgate:
            return vgate

    layer_a = get_object_or_404(GisLayer, pk=params['layer_a_id'])
    layer_b = (get_object_or_404(GisLayer, pk=params['layer_b_id'])
               if params['layer_b_id'] is not None else None)
    # Полигональность требуется только для классических оверлеев; одно-слойные
    # операции и spatial join валидируют тип геометрии в сервисе.
    if params['op'] in OVERLAY_OPS:
        if layer_a.geom_kind != 'polygon' or (
                layer_b is not None and layer_b.geom_kind != 'polygon'):
            return JsonResponse(
                {'ok': False, 'error': 'not_polygon',
                 'detail': 'Оверлеи поддерживаются только для полигональных слоёв.'},
                status=400)
    return layer_a, layer_b, params


@require_http_methods(['GET'])
def gis_overlay_status(request: HttpRequest, run_id: int) -> JsonResponse:
    """GET — статус фоновой оверлейной задачи ``run_id``.

    Возвращает ``{status}`` и, когда задача завершена — созданный слой
    (``layer``), чтобы фронт подхватил его без перезагрузки. Уровень ``view``.
    """
    gate = _require_gis_access(request, level='view')
    if gate:
        return gate

    from agrocosmos.models import PipelineRun

    run = PipelineRun.objects.filter(
        pk=run_id, task_type=PipelineRun.TaskType.GIS_OVERLAY).first()
    if run is None:
        return JsonResponse({'ok': False, 'error': 'not_found'}, status=404)

    resp = {
        'ok': True,
        'run_id': run.pk,
        'status': run.status,
        'records_count': run.records_count,
    }
    if run.status == PipelineRun.Status.COMPLETED:
        result_id = (run.launch_args or {}).get('_result_layer_id')
        layer = GisLayer.objects.filter(pk=result_id).first() if result_id else None
        resp['layer'] = _gis_layer_to_dict(layer) if layer else None
    elif run.status == PipelineRun.Status.FAILED:
        tail = '\n'.join((run.log or '').splitlines()[-5:])
        resp['detail'] = tail or 'Оверлей завершился с ошибкой.'
    return JsonResponse(resp)


@require_http_methods(['GET'])
def gis_layer_export(request: HttpRequest, pk: int) -> HttpResponse:
    """GET — скачать данные слоя одним ZIP-архивом.

    ``?format=shp|geojson|xlsx`` (по умолчанию ``shp``). Уровень ``view``.
    Архив собирается на сервере из таблицы PostGIS слоя (геометрия 4326 +
    атрибуты) и отдаётся как ``attachment``.
    """
    gate = _require_gis_access(request, level='view', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    fmt = (request.GET.get('format') or 'shp').lower()
    from .services.shp_import import EXPORT_FORMATS, export_layer
    if fmt not in EXPORT_FORMATS:
        return JsonResponse(
            {'ok': False, 'error': 'unknown_format',
             'detail': 'format должен быть shp, geojson или xlsx.'},
            status=400,
        )

    try:
        zip_bytes, filename = export_layer(layer, fmt)
    except Exception:  # noqa: BLE001 — не роняем 500-стектрейсом
        import logging
        logging.getLogger('my_fields').exception(
            'gis layer %s export (%s) failed', pk, fmt)
        return JsonResponse(
            {'ok': False, 'error': 'export_failed',
             'detail': 'Не удалось сформировать архив.'},
            status=500,
        )

    resp = HttpResponse(zip_bytes, content_type='application/zip')
    # Имя = название слоя (может быть кириллицей) → RFC 5987: ASCII-фолбэк
    # filename + UTF-8 filename* для современных браузеров.
    from urllib.parse import quote
    ascii_name = filename.encode('ascii', 'ignore').decode('ascii') or 'layer.zip'
    resp['Content-Disposition'] = (
        f'attachment; filename="{ascii_name}"; '
        f"filename*=UTF-8''{quote(filename)}"
    )
    resp['Content-Length'] = str(len(zip_bytes))
    resp['Cache-Control'] = 'private, no-store'
    return resp


def _gis_feature_create(request: HttpRequest, pk: int) -> JsonResponse:
    """POST — создать объект слоя по геометрии (уровень ``edit``)."""
    gate = _require_gis_access(request, level='edit', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    geometry = data.get('geometry')
    if not isinstance(geometry, dict):
        return JsonResponse(
            {'ok': False, 'error': 'no_geometry',
             'detail': 'Ожидается объект geometry (GeoJSON).'},
            status=400,
        )

    from .services.shp_import import create_feature
    try:
        new_id = create_feature(layer, geometry)
    except ValueError as exc:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_geometry', 'detail': str(exc)},
            status=400,
        )
    return JsonResponse({'ok': True, 'id': new_id}, status=201)


def _gis_features_bulk_delete(request: HttpRequest, pk: int) -> JsonResponse:
    """DELETE — пакетно удалить объекты слоя по списку id (уровень ``edit``).

    Тело JSON: ``{"ids": [...]}``. Один SQL-запрос вместо пофайлового удаления —
    иначе при «выделить все объекты» тысячи параллельных запросов частично
    падали и объекты оставались в таблице.
    """
    gate = _require_gis_access(request, level='edit', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    ids = data.get('ids')
    if not isinstance(ids, list):
        return JsonResponse(
            {'ok': False, 'error': 'no_ids',
             'detail': 'Ожидается массив ids.'},
            status=400,
        )

    from .services.shp_import import delete_features
    deleted = delete_features(layer, ids)
    return JsonResponse({'ok': True, 'deleted': deleted})


@csrf_exempt
@require_http_methods(['GET', 'PATCH', 'DELETE'])
def gis_layer_feature_detail(request: HttpRequest, pk: int, fid: int) -> JsonResponse:
    """GET — охват объекта; PATCH — обновить атрибуты/геометрию; DELETE — удалить.

    * ``GET`` (уровень ``view``): ``{ok, extent: [minx,miny,maxx,maxy]}`` —
      для «перелёта» к объекту по клику 🔍 в таблице атрибутов.
    * ``PATCH``/``DELETE`` (уровень ``edit``): PATCH принимает ``props``
      (атрибуты) и/или ``geometry`` (GeoJSON) — хотя бы одно.
    """
    if request.method == 'GET':
        gate = _require_gis_access(request, level='view', pk=pk)
        if gate:
            return gate
        layer = get_object_or_404(GisLayer, pk=pk)
        from .services.shp_import import feature_extent
        extent = feature_extent(layer, fid)
        if extent is None:
            return JsonResponse(
                {'ok': False, 'error': 'not_found',
                 'detail': 'Объект не найден или без геометрии.'},
                status=404,
            )
        return JsonResponse({'ok': True, 'extent': extent})

    gate = _require_gis_access(request, level='edit', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    if request.method == 'DELETE':
        from .services.shp_import import delete_feature
        if not delete_feature(layer, fid):
            return JsonResponse(
                {'ok': False, 'error': 'not_found',
                 'detail': 'Объект не найден.'},
                status=404,
            )
        return JsonResponse({'ok': True, 'deleted': 1})

    return _gis_feature_patch(request, layer, fid)


def _gis_feature_patch(request: HttpRequest, layer: GisLayer,
                       fid: int) -> JsonResponse:
    """PATCH-часть :func:`gis_layer_feature_detail`: props и/или geometry."""
    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    props = data.get('props')
    geometry = data.get('geometry')
    has_props = isinstance(props, dict) and bool(props)
    if not has_props and not isinstance(geometry, dict):
        return JsonResponse(
            {'ok': False, 'error': 'nothing_to_update',
             'detail': 'Ожидается props и/или geometry.'},
            status=400,
        )

    from .services.shp_import import update_feature, update_feature_geom

    geom_updated = None
    if isinstance(geometry, dict):
        try:
            geom_updated = update_feature_geom(layer, fid, geometry)
        except ValueError as exc:
            return JsonResponse(
                {'ok': False, 'error': 'invalid_geometry', 'detail': str(exc)},
                status=400,
            )
        if not geom_updated:
            return JsonResponse(
                {'ok': False, 'error': 'not_found', 'detail': 'Объект не найден.'},
                status=404,
            )

    attr_updated = update_feature(layer, fid, props) if has_props else 0
    if has_props and not attr_updated and geom_updated is None:
        return JsonResponse(
            {'ok': False, 'error': 'not_found_or_noop',
             'detail': 'Объект не найден или нет допустимых полей для правки.'},
            status=404,
        )

    return JsonResponse({
        'ok': True,
        'updated': attr_updated or 0,
        'geometry_updated': bool(geom_updated),
    })


@csrf_exempt
@require_http_methods(['GET', 'POST'])
def gis_folders_collection(request: HttpRequest) -> JsonResponse:
    """GET — список папок; POST — создать папку (по умолчанию «Новая папка»)."""
    if request.method == 'GET':
        gate = _require_gis_access(request, level='view')
        if gate:
            return gate
        return JsonResponse({
            'ok': True,
            'results': [_gis_folder_to_dict(f) for f in GisFolder.objects.all()],
        })

    # POST — создание (изменяет набор → whole-class 'edit').
    gate = _require_gis_access(request, level='edit')
    if gate:
        return gate
    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        data = {}
    name = str(data.get('name', '') or '').strip() or 'Новая папка'
    owner = request.user if getattr(request, 'user', None) \
        and request.user.is_authenticated else None
    # Новую папку кладём наверх списка (sort_order меньше всех существующих).
    top = GisFolder.objects.order_by('sort_order').first()
    sort_order = (top.sort_order - 1) if top else 0
    folder = GisFolder.objects.create(
        name=name[:200], sort_order=sort_order, owner=owner)
    return JsonResponse(
        {'ok': True, 'folder': _gis_folder_to_dict(folder)}, status=201)


@csrf_exempt
@require_http_methods(['PATCH', 'DELETE'])
def gis_folder_detail(request: HttpRequest, pk: int) -> JsonResponse:
    """PATCH — имя/свёрнутость/видимость папки; DELETE — удалить папку
    (слои внутри переходят в корень через ``on_delete=SET_NULL``)."""
    level = 'manage' if request.method == 'DELETE' else 'edit'
    gate = _require_gis_access(request, level=level)
    if gate:
        return gate

    folder = get_object_or_404(GisFolder, pk=pk)

    if request.method == 'DELETE':
        folder.delete()
        return JsonResponse({'ok': True})

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse({'ok': False, 'error': 'invalid_json'}, status=400)

    update_fields: list[str] = []
    if 'name' in data:
        name = str(data.get('name', '') or '').strip()
        if not name:
            return JsonResponse(
                {'ok': False, 'error': 'empty_name',
                 'detail': 'Название папки не может быть пустым.'},
                status=400,
            )
        folder.name = name[:200]
        update_fields.append('name')
    if 'collapsed' in data:
        folder.collapsed = bool(data.get('collapsed'))
        update_fields.append('collapsed')
    if 'visible' in data:
        folder.visible = bool(data.get('visible'))
        update_fields.append('visible')

    if not update_fields:
        return JsonResponse(
            {'ok': False, 'error': 'nothing_to_update'}, status=400)

    folder.save(update_fields=update_fields)
    return JsonResponse({'ok': True, 'folder': _gis_folder_to_dict(folder)})


def _coerce_int(value):
    """Привести значение к int либо вернуть ``None`` (без исключений)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_folder_id(raw, valid_folder_ids: set):
    """ID папки для слоя: невалидная/несуществующая/пустая → ``None`` (корень)."""
    if raw in (None, '', 'null'):
        return None
    fid = _coerce_int(raw)
    return fid if fid in valid_folder_ids else None


def _gis_layout_save(request: HttpRequest, data: dict) -> JsonResponse:
    """Сохранить состав/порядок дерева: папки (порядок) + слои + растры
    (порядок + принадлежность папке). Вызывается из :func:`gis_layers_reorder`
    при наличии ключей ``folders``/``layers``/``rasters``."""
    from django.db import transaction

    folders = data.get('folders') or []
    layers = data.get('layers') or []
    rasters = data.get('rasters') or []
    valid_folder_ids = set(GisFolder.objects.values_list('pk', flat=True))

    with transaction.atomic():
        for idx, f in enumerate(folders):
            fid = _coerce_int(f.get('id') if isinstance(f, dict) else None)
            if fid is not None:
                GisFolder.objects.filter(pk=fid).update(sort_order=idx)
        for idx, item in enumerate(layers):
            if not isinstance(item, dict):
                continue
            lid = _coerce_int(item.get('id'))
            if lid is None:
                continue
            folder_id = _resolve_folder_id(item.get('folder'), valid_folder_ids)
            GisLayer.objects.filter(pk=lid).update(
                sort_order=idx, folder_id=folder_id)
        # Растровые слои — свой независимый порядок (sort_order), но общая
        # система папок (folder_id) с векторными слоями.
        for idx, item in enumerate(rasters):
            if not isinstance(item, dict):
                continue
            rid = _coerce_int(item.get('id'))
            if rid is None:
                continue
            folder_id = _resolve_folder_id(item.get('folder'), valid_folder_ids)
            RasterLayer.objects.filter(pk=rid).update(
                sort_order=idx, folder_id=folder_id)

    return JsonResponse({'ok': True})


@csrf_exempt
@require_http_methods(['POST'])
def gis_layers_reorder(request: HttpRequest) -> JsonResponse:
    """Сохранить порядок слоёв. Тело: ``{"order": [id, id, ...]}`` —
    сверху вниз (первый = верхний в списке и на карте).

    Расширенный формат (папки): ``{"folders": [{"id":..}, ...],
    "layers": [{"id":.., "folder": fid|null}, ...]}`` — сохраняет порядок
    папок и принадлежность/порядок слоёв."""
    # Изменение порядка затрагивает набор слоёв — нужен whole-class 'edit'.
    gate = _require_gis_access(request, level='edit')
    if gate:
        return gate

    from django.db import transaction

    try:
        data = json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_json'}, status=400)

    if 'layers' in data or 'folders' in data or 'rasters' in data:
        return _gis_layout_save(request, data)

    order = data.get('order')
    if not isinstance(order, list):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_order'}, status=400)

    ids = []
    for value in order:
        try:
            ids.append(int(value))
        except (TypeError, ValueError):
            continue

    with transaction.atomic():
        for idx, lid in enumerate(ids):
            GisLayer.objects.filter(pk=lid).update(sort_order=idx)

    return JsonResponse({'ok': True})


@require_http_methods(['GET'])
def gis_layer_tiles(request: HttpRequest, pk: int, z: int, x: int,
                    y: int) -> HttpResponse:
    """Универсальный MVT для загруженного слоя (source-layer = table_name)."""
    from django.db import connection

    from agrocosmos.views.tiles import _tile_bbox
    from psycopg import sql

    gate = _require_gis_access(request, level='view', pk=pk)
    if gate:
        # для тайлов отдаём пустой protobuf, а не JSON, чтобы MapLibre молчал
        resp = HttpResponse(b'', content_type='application/x-protobuf',
                            status=gate.status_code)
        resp['Cache-Control'] = 'private, no-store'
        return resp

    layer = get_object_or_404(GisLayer, pk=pk)
    xmin, ymin, xmax, ymax = _tile_bbox(z, x, y)

    # id + атрибуты (из реестра, идентификаторы уже sanitized) + MVT-геометрия.
    attr_idents = [sql.Identifier(a['db']) for a in (layer.attributes or [])]
    select_cols = [sql.Identifier('id')] + attr_idents
    query = sql.SQL(
        'WITH bounds AS (SELECT ST_MakeEnvelope(%s, %s, %s, %s, 3857) AS env), '
        'tile AS ('
        '  SELECT {cols}, ST_AsMVTGeom('
        '    ST_Transform(t.geom, 3857), b.env, 4096, 256, true) AS geom '
        '  FROM {table} t CROSS JOIN bounds b '
        '  WHERE t.geom && ST_Transform(b.env, 4326)'
        ') '
        'SELECT ST_AsMVT(tile, {srclayer}, 4096, \'geom\') '
        'FROM tile WHERE geom IS NOT NULL'
    ).format(
        cols=sql.SQL(', ').join(select_cols),
        table=sql.Identifier(layer.table_name),
        srclayer=sql.Literal(layer.table_name),
    )

    tile_bytes = b''
    try:
        with connection.cursor() as cur:
            cur.execute(query, [xmin, ymin, xmax, ymax])
            row = cur.fetchone()
            raw = row[0] if row and row[0] else b''
            tile_bytes = bytes(raw) if not isinstance(raw, bytes) else raw
    except Exception:
        tile_bytes = b''

    resp = HttpResponse(tile_bytes, content_type='application/x-protobuf')
    resp['Cache-Control'] = 'private, max-age=60'
    resp['Access-Control-Allow-Origin'] = '*'
    return resp


# ─────────────────────────────────────────────────────────────────────
# Растровые слои (GeoTIFF → COG в MinIO/S3)
# Загрузка идёт напрямую браузер→MinIO через presigned S3 Multipart Upload,
# минуя gunicorn (файлы до десятков ГБ). Поток:
#   1. POST rasters/upload/init/     → создаёт RasterLayer + multipart, отдаёт
#                                       part_size / part_count / upload_id
#   2. POST rasters/upload/sign/     → presigned URL для пачки частей (браузер
#                                       PUT'ит части прямо в MinIO, собирает ETag)
#   3. POST rasters/upload/complete/ → финализирует multipart, статус=queued
#      POST rasters/upload/abort/    → отменяет multipart, удаляет слой
# Конвейер COG (status queued→processing→ready) подключается в Фазе 3.
# ─────────────────────────────────────────────────────────────────────

# S3 multipart: часть ≥5 МБ (кроме последней), ≤10000 частей. 64 МБ —
# компромисс между числом запросов и памятью браузера; для очень крупных
# файлов размер части поднимается, чтобы уложиться в лимит частей.
_RASTER_PART_SIZE = 64 * 1024 * 1024
_RASTER_MAX_PARTS = 10000
_RASTER_MAX_SIZE = 200 * 1024 * 1024 * 1024  # 200 ГБ — потолок здравого смысла
_RASTER_SIGN_BATCH = 1000                    # макс. частей на один /sign/
_RASTER_EXTS = ('.tif', '.tiff')


def _raster_layer_to_dict(r: RasterLayer) -> dict:
    return {
        'id': r.pk,
        'folder': r.folder_id,
        'title': r.title,
        'status': r.status,
        'status_display': r.get_status_display(),
        'original_filename': r.original_filename,
        'size_bytes': r.size_bytes,
        'srid': r.srid,
        'bounds': r.bounds,
        'band_count': r.band_count,
        'nodata': r.nodata,
        'stats': r.stats or [],
        'style': r.style or {},
        'opacity': r.opacity,
        'error': r.error,
        'sort_order': r.sort_order,
        'has_original': bool(r.upload_key),
        'created_at': r.created_at.isoformat(),
    }


def _raster_part_plan(size: int) -> tuple[int, int]:
    """(part_size, part_count) для файла ``size`` байт под лимиты S3."""
    part_size = _RASTER_PART_SIZE
    # Если частей больше лимита — увеличиваем размер части (кратно МБ).
    if size > part_size * _RASTER_MAX_PARTS:
        mb = 1024 * 1024
        part_size = -(-size // (_RASTER_MAX_PARTS * mb)) * mb  # ceil до МБ
    part_count = max(1, -(-size // part_size))  # ceil
    return part_size, part_count


def _raster_storage_gate():
    """503, если объектное хранилище не сконфигурировано (модуль выключен)."""
    from .services import s3_storage
    if not s3_storage.is_configured():
        return JsonResponse(
            {'ok': False, 'error': 'storage_disabled',
             'detail': 'Объектное хранилище не настроено (S3_* не заданы).'},
            status=503,
        )
    return None


def _raster_list(request: HttpRequest) -> JsonResponse:
    """GET — список растровых слоёв, отфильтрованный по грантам."""
    gate = _require_raster_access(request, level='view')
    if gate:
        return gate
    from access.services import accessible_raster_layer_ids
    rasters = RasterLayer.objects.all()
    ids = accessible_raster_layer_ids(getattr(request, 'legacy_user', None))
    if ids is not None:
        rasters = rasters.filter(pk__in=ids)
    return JsonResponse({
        'ok': True,
        'count': rasters.count(),
        'results': [_raster_layer_to_dict(x) for x in rasters],
    })


@csrf_exempt
@require_http_methods(['GET'])
def raster_layers_collection(request: HttpRequest) -> JsonResponse:
    """GET — список растровых слоёв (загрузка идёт через отдельные /upload/*)."""
    return _raster_list(request)


@csrf_exempt
@require_http_methods(['POST'])
def raster_upload_init(request: HttpRequest) -> JsonResponse:
    """POST — инициировать multipart-загрузку растра.

    Body: ``{filename, size}``. Создаёт ``RasterLayer(status=uploading)`` и
    S3 multipart upload, возвращает план частей + ``upload_id`` для клиента.
    """
    gate = _require_raster_access(request, level='manage')
    if gate:
        return gate
    disabled = _raster_storage_gate()
    if disabled:
        return disabled

    data, err = _parse_json(request)
    if err:
        return err
    filename = str(data.get('filename', '')).strip()
    size = _coerce_int(data.get('size'))

    if not filename or not filename.lower().endswith(_RASTER_EXTS):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_filename',
             'detail': 'Ожидается файл .tif/.tiff.'}, status=400)
    if not size or size <= 0:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_size',
             'detail': 'Некорректный размер файла.'}, status=400)
    if size > _RASTER_MAX_SIZE:
        return JsonResponse(
            {'ok': False, 'error': 'too_large',
             'detail': 'Файл превышает допустимый размер.'}, status=400)

    from .services import s3_storage

    owner_id = getattr(request.user, 'id', None)
    title = filename.rsplit('.', 1)[0][:200] or 'Растровый слой'
    part_size, part_count = _raster_part_plan(size)

    key = s3_storage.build_upload_key(owner_id, filename)
    try:
        upload_id = s3_storage.create_multipart_upload(
            key, content_type='image/tiff')
    except Exception as e:  # noqa: BLE001
        return JsonResponse(
            {'ok': False, 'error': 'storage_error',
             'detail': f'Не удалось начать загрузку: {e}'}, status=502)

    layer = RasterLayer.objects.create(
        title=title, status=RasterLayer.Status.UPLOADING,
        original_filename=filename[:255], upload_key=key,
        upload_id=upload_id, size_bytes=size, owner=request.user,
    )
    return JsonResponse({
        'ok': True, 'layer_id': layer.pk, 'upload_id': upload_id, 'key': key,
        'part_size': part_size, 'part_count': part_count,
    }, status=201)


def _get_uploading_layer(request, data):
    """Загрузить RasterLayer в статусе uploading по layer_id из тела.

    Возвращает ``(layer, None)`` или ``(None, JsonResponse-ошибка)``.
    """
    layer_id = _coerce_int(data.get('layer_id'))
    if not layer_id:
        return None, JsonResponse(
            {'ok': False, 'error': 'invalid_layer_id'}, status=400)
    layer = RasterLayer.objects.filter(pk=layer_id).first()
    if not layer:
        return None, JsonResponse(
            {'ok': False, 'error': 'not_found'}, status=404)
    if layer.status != RasterLayer.Status.UPLOADING or not layer.upload_id:
        return None, JsonResponse(
            {'ok': False, 'error': 'not_uploading',
             'detail': 'Загрузка уже завершена или отменена.'}, status=409)
    return layer, None


@csrf_exempt
@require_http_methods(['POST'])
def raster_upload_sign(request: HttpRequest) -> JsonResponse:
    """POST — presigned URL'ы для пачки частей.

    Body: ``{layer_id, part_numbers: [int, ...]}`` → ``{urls: {n: url}}``.
    """
    gate = _require_raster_access(request, level='manage')
    if gate:
        return gate
    disabled = _raster_storage_gate()
    if disabled:
        return disabled

    data, err = _parse_json(request)
    if err:
        return err
    layer, err = _get_uploading_layer(request, data)
    if err:
        return err

    raw = data.get('part_numbers')
    if not isinstance(raw, list) or not raw:
        return JsonResponse(
            {'ok': False, 'error': 'invalid_part_numbers'}, status=400)
    if len(raw) > _RASTER_SIGN_BATCH:
        return JsonResponse(
            {'ok': False, 'error': 'batch_too_large',
             'detail': f'Не более {_RASTER_SIGN_BATCH} частей за запрос.'},
            status=400)

    from .services import s3_storage

    urls = {}
    for value in raw:
        n = _coerce_int(value)
        if not n or n < 1 or n > _RASTER_MAX_PARTS:
            return JsonResponse(
                {'ok': False, 'error': 'invalid_part_number',
                 'detail': f'Некорректный номер части: {value}.'}, status=400)
        urls[n] = s3_storage.presign_part_url(
            layer.upload_key, layer.upload_id, n)
    return JsonResponse({'ok': True, 'urls': urls})


def _clean_upload_parts(parts):
    """Провалидировать ``parts`` из тела complete-запроса.

    Возвращает ``(cleaned, None)`` со списком ``{'PartNumber', 'ETag'}`` либо
    ``(None, JsonResponse-ошибка)`` (``invalid_parts``, 400).
    """
    err = JsonResponse({'ok': False, 'error': 'invalid_parts'}, status=400)
    if not isinstance(parts, list) or not parts:
        return None, err
    cleaned = []
    for p in parts:
        n = _coerce_int(p.get('PartNumber')) if isinstance(p, dict) else None
        etag = p.get('ETag') if isinstance(p, dict) else None
        if not n or not etag:
            return None, err
        cleaned.append({'PartNumber': n, 'ETag': etag})
    return cleaned, None


@csrf_exempt
@require_http_methods(['POST'])
def raster_upload_complete(request: HttpRequest) -> JsonResponse:
    """POST — финализировать multipart-загрузку.

    Body: ``{layer_id, parts: [{PartNumber, ETag}, ...]}``. По успеху слой
    переходит в ``queued`` (ждёт конвейер COG, Фаза 3).
    """
    gate = _require_raster_access(request, level='manage')
    if gate:
        return gate
    disabled = _raster_storage_gate()
    if disabled:
        return disabled

    data, err = _parse_json(request)
    if err:
        return err
    layer, err = _get_uploading_layer(request, data)
    if err:
        return err

    cleaned, err = _clean_upload_parts(data.get('parts'))
    if err:
        return err

    from .services import s3_storage

    try:
        s3_storage.complete_multipart_upload(
            layer.upload_key, layer.upload_id, cleaned)
    except Exception as e:  # noqa: BLE001
        return JsonResponse(
            {'ok': False, 'error': 'storage_error',
             'detail': f'Не удалось завершить загрузку: {e}'}, status=502)

    # Уточняем реальный размер объекта (клиент мог соврать в init).
    real = s3_storage.object_size(
        layer.upload_key, bucket=None)
    if real:
        layer.size_bytes = real
    layer.status = RasterLayer.Status.QUEUED
    layer.upload_id = ''
    # sort_order: наверх списка (как у SHP — новые сверху не требуется, но
    # держим детерминированно по created_at через дефолтный ordering).
    layer.save(update_fields=['size_bytes', 'status', 'upload_id', 'updated_at'])

    # Ставим конвертацию в COG в очередь (воркер run_ndvi_worker подхватит по
    # task_type='raster_ingest' и вызовет run_raster_ingest).
    from agrocosmos.models import PipelineRun

    PipelineRun.objects.create(
        task_type=PipelineRun.TaskType.RASTER_INGEST,
        status=PipelineRun.Status.QUEUED,
        description=f'Конвертация растра в COG: {layer.title}'[:500],
        launch_args={'layer_id': layer.pk},
    )
    return JsonResponse({'ok': True, 'layer': _raster_layer_to_dict(layer)})


@csrf_exempt
@require_http_methods(['POST'])
def raster_upload_abort(request: HttpRequest) -> JsonResponse:
    """POST — отменить незавершённую загрузку и удалить слой.

    Body: ``{layer_id}``. Освобождает залитые части в S3 и удаляет запись.
    """
    gate = _require_raster_access(request, level='manage')
    if gate:
        return gate
    disabled = _raster_storage_gate()
    if disabled:
        return disabled

    data, err = _parse_json(request)
    if err:
        return err
    layer, err = _get_uploading_layer(request, data)
    if err:
        return err

    from .services import s3_storage

    try:
        s3_storage.abort_multipart_upload(layer.upload_key, layer.upload_id)
    except Exception:  # noqa: BLE001
        pass  # отмена best-effort — запись всё равно удаляем
    layer.delete()
    return JsonResponse({'ok': True})


@csrf_exempt
@require_http_methods(['PATCH', 'DELETE'])
def raster_layer_detail(request: HttpRequest, pk: int) -> JsonResponse:
    """PATCH — title/style/opacity; DELETE — удалить слой (+ S3-объекты)."""
    level = 'manage' if request.method == 'DELETE' else 'edit'
    gate = _require_raster_access(request, level=level, pk=pk)
    if gate:
        return gate

    layer = get_object_or_404(RasterLayer, pk=pk)

    if request.method == 'DELETE':
        from .services import s3_storage
        # Незавершённая загрузка — отменить multipart; иначе почистить объекты.
        if layer.upload_id:
            try:
                s3_storage.abort_multipart_upload(
                    layer.upload_key, layer.upload_id)
            except Exception:  # noqa: BLE001
                pass
        for key, bucket in (
            (layer.upload_key, settings.S3_BUCKET_UPLOADS),
            (layer.cog_key, settings.S3_BUCKET_COG),
        ):
            if key and s3_storage.is_configured():
                try:
                    s3_storage.delete_object(key, bucket=bucket)
                except Exception:  # noqa: BLE001
                    pass
        layer.delete()
        return JsonResponse({'ok': True})

    return _raster_layer_patch(request, layer)


@csrf_exempt
@require_http_methods(['POST'])
def raster_reprocess(request: HttpRequest, pk: int) -> JsonResponse:
    """POST — повторно запустить конвейер ingest для слоя (напр. после failed).

    Требует уровень ``manage`` и наличие исходного файла (``upload_key``).
    Нельзя перезапускать слой, который сейчас грузится/в очереди/обрабатывается.
    """
    gate = _require_raster_access(request, level='manage', pk=pk)
    if gate:
        return gate
    disabled = _raster_storage_gate()
    if disabled:
        return disabled

    layer = get_object_or_404(RasterLayer, pk=pk)
    if not layer.upload_key:
        return JsonResponse(
            {'ok': False, 'error': 'no_original',
             'detail': 'Нет исходного файла для повторной обработки.'}, status=409)
    if layer.status in (RasterLayer.Status.UPLOADING, RasterLayer.Status.QUEUED,
                        RasterLayer.Status.PROCESSING):
        return JsonResponse(
            {'ok': False, 'error': 'busy',
             'detail': 'Слой уже загружается или обрабатывается.'}, status=409)

    layer.status = RasterLayer.Status.QUEUED
    layer.error = ''
    layer.save(update_fields=['status', 'error', 'updated_at'])

    from agrocosmos.models import PipelineRun

    PipelineRun.objects.create(
        task_type=PipelineRun.TaskType.RASTER_INGEST,
        status=PipelineRun.Status.QUEUED,
        description=f'Повторная конвертация растра в COG: {layer.title}'[:500],
        launch_args={'layer_id': layer.pk},
    )
    return JsonResponse({'ok': True, 'layer': _raster_layer_to_dict(layer)})


@require_http_methods(['GET'])
def raster_tile(request: HttpRequest, pk: int, z: int, x: int,
                y: int) -> HttpResponse:
    """GET — PNG-тайл ``z/x/y`` растрового слоя (рендер из COG, кэш).

    Уровень доступа ``view``. Тайл рендерится из COG в объектном хранилище
    (перепроекция в Web Mercator + палитра/RGB по ``style``) и кэшируется.
    Вне охвата / нет данных / слой не готов — ``204`` (MapLibre это ок).
    """
    gate = _require_raster_access(request, level='view', pk=pk)
    if gate:
        return gate

    layer = get_object_or_404(RasterLayer, pk=pk)
    if layer.status != RasterLayer.Status.READY or not layer.cog_key:
        return HttpResponse(b'', content_type='image/png', status=204)
    disabled = _raster_storage_gate()
    if disabled:
        return disabled

    from django.core.cache import cache

    from .services import raster_render

    # Версия в ключе кэша = updated_at: смена стиля/повторный ingest сбрасывают
    # старые тайлы автоматически.
    ver = int(layer.updated_at.timestamp())
    cache_key = f'rastile:{pk}:{ver}:{z}:{x}:{y}'
    png = cache.get(cache_key)
    if png is None:
        png = raster_render.render_layer_tile(layer, z, x, y) or b''
        cache.set(cache_key, png, 86400)
    if not png:
        return HttpResponse(b'', content_type='image/png', status=204)

    resp = HttpResponse(png, content_type='image/png')
    resp['Cache-Control'] = 'private, max-age=3600'
    return resp


def _patch_raster_title(data, layer, update_fields):
    if 'title' not in data:
        return None
    title = str(data.get('title', '')).strip()
    if not title:
        return JsonResponse(
            {'ok': False, 'error': 'empty_title',
             'detail': 'Название слоя не может быть пустым.'}, status=400)
    layer.title = title[:200]
    update_fields.append('title')
    return None


def _patch_raster_style(data, layer, update_fields):
    if 'style' not in data:
        return None
    style = data.get('style')
    if style is None:
        style = {}
    if not isinstance(style, dict):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_style',
             'detail': 'style должен быть объектом.'}, status=400)
    layer.style = style
    update_fields.append('style')
    return None


def _patch_raster_opacity(data, layer, update_fields):
    if 'opacity' not in data:
        return None
    try:
        layer.opacity = max(0.0, min(1.0, float(data.get('opacity'))))
    except (TypeError, ValueError):
        return JsonResponse(
            {'ok': False, 'error': 'invalid_opacity'}, status=400)
    update_fields.append('opacity')
    return None


def _raster_layer_patch(request: HttpRequest, layer: RasterLayer) -> JsonResponse:
    """PATCH-часть :func:`raster_layer_detail`: title / style / opacity."""
    data, err = _parse_json(request)
    if err:
        return err

    update_fields: list[str] = []
    for handler in (_patch_raster_title, _patch_raster_style,
                    _patch_raster_opacity):
        err = handler(data, layer, update_fields)
        if err:
            return err

    if not update_fields:
        return JsonResponse(
            {'ok': False, 'error': 'nothing_to_update'}, status=400)

    update_fields.append('updated_at')
    layer.save(update_fields=update_fields)
    return JsonResponse({'ok': True, 'layer': _raster_layer_to_dict(layer)})
