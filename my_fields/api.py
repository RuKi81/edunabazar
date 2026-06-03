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
from typing import Any

from django.contrib.gis.geos import GEOSGeometry
from django.http import HttpRequest, JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.dateparse import parse_date
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import FieldEvent, FieldSeason, UserField
from .permissions import can_edit_field, can_view_field
from .services.geometry import (
    compute_area_ha, ensure_multipolygon, resolve_region_district,
)
from .services.quotas import can_create_field


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
        # По умолчанию — только активные. ``?archived=1`` показывает все.
        qs = UserField.objects.filter(owner=request.user)
        if request.GET.get('archived') != '1':
            qs = qs.filter(is_archived=False)
        qs = qs.select_related('region', 'district').order_by('-updated_at')
        return JsonResponse({
            'type': 'FeatureCollection',
            'features': [_field_to_feature(f) for f in qs],
        })

    # POST: создать поле
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

    # Проверка квоты ПОСЛЕ парсинга геометрии (нужна площадь).
    try:
        tmp = GEOSGeometry(json.dumps(geometry), srid=4326)
        tmp = ensure_multipolygon(tmp)
        new_area = compute_area_ha(tmp)
    except Exception as exc:
        return JsonResponse(
            {'error': 'invalid_geometry', 'detail': str(exc)}, status=400,
        )

    quota = can_create_field(request.user, new_area_ha=new_area)
    if not quota.ok:
        return JsonResponse(
            {'error': 'quota_exceeded',
             'detail': quota.reason, 'hint': quota.hint},
            status=403,
        )

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

    # Опционально — создаём сезон одной транзакцией. Все поля сезона
    # необязательны, кроме ``year`` и ``crop`` — без них запись
    # бессмысленна. Любая ошибка парсинга дат/чисел в сезоне НЕ должна
    # откатывать создание поля; собираем такие случаи в ``season_warnings``.
    season_payload = payload.get('season')
    season_warning: str | None = None
    if season_payload and season_payload.get('year') and season_payload.get('crop'):
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
        except (ValueError, TypeError) as exc:
            season_warning = f'Сезон не создан: {exc}'

    feature = _field_to_feature(field)
    if season_warning:
        feature['properties']['season_warning'] = season_warning
    return JsonResponse(feature, status=201)


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

    # PATCH
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
