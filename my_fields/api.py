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

from django.contrib.gis.geos import GEOSGeometry
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.dateparse import parse_date
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import FieldEvent, FieldSeason, GisFolder, GisLayer, UserField
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


def _normalize_style(value: Any, layer: GisLayer):
    """Валидировать и нормализовать style-конфиг раскраски слоя.

    Возвращает ``(style_dict, None)`` при успехе или ``(None, error_msg)``.
    ``single`` (или пустой) → ``{'mode': 'single'}``. Для categorical/graduated
    поле должно присутствовать в ``layer.attributes``.
    """
    if value in (None, '', {}):
        return {'mode': 'single'}, None
    if not isinstance(value, dict):
        return None, 'style должен быть объектом'

    mode = value.get('mode', 'single')
    if mode not in _STYLE_MODES:
        return None, 'неизвестный mode'
    if mode == 'single':
        return {'mode': 'single'}, None

    field = value.get('field')
    valid_fields = {a.get('db') for a in (layer.attributes or [])}
    if not isinstance(field, str) or field not in valid_fields:
        return None, 'field не найден среди атрибутов слоя'

    if mode == 'categorical':
        return _normalize_categorical(value, field)
    return _normalize_graduated(value, field)


def _require_gis_authenticated(request: HttpRequest):
    """None если есть Django-сессия, иначе 401 (как раньше)."""
    if not request.user.is_authenticated:
        return JsonResponse(
            {'error': 'authentication_required'}, status=401)
    return None


def _require_gis_access(request: HttpRequest, *, level: str = 'view',
                        pk: int | None = None):
    """Гейт доступа к ГИС-данным на основе грантов (``access.services``).

    Возвращает ``None`` при доступе, иначе ``JsonResponse`` 401/403.

    * ``pk`` задан — проверяем доступ к конкретному слою (или whole-class);
    * ``pk`` не задан — действие над «всем классом» (загрузка/список/reorder):
      для ``view`` достаточно доступа к странице (любой ГИС-грант),
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
        allowed = has_resource_access(user, _GIS_RESOURCE, pk, level)
    elif level == 'view':
        allowed = can_open_gis_page(user)
    else:
        allowed = has_resource_access(user, _GIS_RESOURCE, None, level)

    return None if allowed else JsonResponse({'error': 'forbidden'}, status=403)


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


@csrf_exempt
@require_http_methods(['GET', 'POST'])
def gis_layer_features(request: HttpRequest, pk: int) -> JsonResponse:
    """GET — список объектов слоя; POST — создать объект по геометрии.

    * ``GET`` (уровень ``view``): постранично id + атрибуты для таблицы. С
      ``?geometry=1`` — GeoJSON FeatureCollection (id + точная геометрия) для
      загрузки в редактор draw.
    * ``POST`` (уровень ``edit``): создать новый объект с переданной
      геометрией (атрибуты — NULL). Тело: ``{"geometry": <GeoJSON>}``.
    """
    if request.method == 'POST':
        return _gis_feature_create(request, pk)

    gate = _require_gis_access(request, level='view', pk=pk)
    if gate:
        return gate
    layer = get_object_or_404(GisLayer, pk=pk)

    if request.GET.get('geometry') in ('1', 'true', 'yes'):
        from .services.shp_import import get_features_geojson
        fc = get_features_geojson(layer)
        return JsonResponse({
            'ok': True,
            'geom_kind': layer.geom_kind,
            'featurecollection': fc,
        })

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

    data = list_features(layer, limit=limit, offset=offset)
    return JsonResponse({
        'ok': True,
        'total': data['total'],
        'results': data['results'],
        'attributes': layer.attributes,
        'limit': limit,
        'offset': offset,
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


@csrf_exempt
@require_http_methods(['PATCH', 'DELETE'])
def gis_layer_feature_detail(request: HttpRequest, pk: int, fid: int) -> JsonResponse:
    """PATCH — обновить атрибуты и/или геометрию объекта; DELETE — удалить.

    Оба метода требуют уровень ``edit``. PATCH принимает ``props`` (атрибуты)
    и/или ``geometry`` (GeoJSON) — хотя бы одно.
    """
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
    """Сохранить состав/порядок дерева: папки (порядок) + слои
    (порядок + принадлежность папке). Вызывается из :func:`gis_layers_reorder`
    при наличии ключей ``folders``/``layers``."""
    from django.db import transaction

    folders = data.get('folders') or []
    layers = data.get('layers') or []
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

    if 'layers' in data or 'folders' in data:
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
