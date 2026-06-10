"""Admin-only CRUD for editing ``agro_farmland`` polygons from the GIS page.

The ЗСН (Росреестр) layer is served read-only as MVT tiles for everyone,
but the experimental admin GIS page (``my_fields:gis_page``) needs to edit
the underlying ``Farmland`` rows directly: move vertices, split, delete and
create new polygons.

GeoJSON in/out, no DRF dependency — mirrors ``my_fields.api`` conventions
and reuses ``my_fields.services.geometry`` for area/region resolution.

Access is gated by the same admin check as the MVT endpoint
(``tiles._is_admin_legacy``). Writes mutate a shared reference dataset, so
they are intentionally restricted to admins.
"""
from __future__ import annotations

import json

from django.contrib.gis.geos import GEOSGeometry
from django.http import HttpRequest, JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from my_fields.services.geometry import (
    compute_area_ha, ensure_multipolygon, resolve_region_district,
)

from ..models import Farmland
from .tiles import _is_admin_legacy


def _parse_json(request: HttpRequest):
    try:
        return json.loads(request.body or b'{}'), None
    except (ValueError, TypeError) as exc:
        return None, JsonResponse(
            {'error': 'invalid_json', 'detail': str(exc)}, status=400,
        )


def _is_used_to_int(value) -> int:
    """Tri-state ``is_used`` → MVT-compatible int (1 / 0 / -1)."""
    if value is True:
        return 1
    if value is False:
        return 0
    return -1


def _int_to_is_used(value):
    """Client int (1 / 0 / -1) → model tri-state bool/None."""
    if value in (1, '1', True):
        return True
    if value in (0, '0', False):
        return False
    return None


def _farmland_to_feature(f: Farmland) -> dict:
    return {
        'type': 'Feature',
        'id': f.pk,
        'geometry': json.loads(f.geom.geojson) if f.geom else None,
        'properties': {
            'id': f.pk,
            'crop_type': f.crop_type,
            'crop_type_display': f.get_crop_type_display(),
            'area_ha': f.area_ha,
            'cadastral_number': f.cadastral_number,
            'is_used': _is_used_to_int(f.is_used),
            'district': f.district.name if f.district_id else '',
            'source': f.source,
        },
    }


def _apply_geom(f: Farmland, geometry: dict) -> JsonResponse | None:
    """Apply GeoJSON geometry to a farmland with area/region resolution.

    Returns a 400 JsonResponse on invalid geometry, otherwise ``None``.
    """
    try:
        geom = GEOSGeometry(json.dumps(geometry), srid=4326)
        geom = ensure_multipolygon(geom)
    except (ValueError, TypeError, Exception) as exc:  # GEOSException -> Exception
        return JsonResponse(
            {'error': 'invalid_geometry', 'detail': str(exc)}, status=400,
        )
    f.geom = geom
    f.area_ha = compute_area_ha(geom)
    region_id, district_id = resolve_region_district(geom)
    f.region_id = region_id
    f.district_id = district_id
    return None


@csrf_exempt
@require_http_methods(['POST'])
def api_farmland_collection(request: HttpRequest) -> JsonResponse:
    """POST — create a new ``Farmland`` from GeoJSON geometry (admin-only)."""
    if not _is_admin_legacy(request):
        return JsonResponse({'error': 'forbidden'}, status=403)

    payload, err = _parse_json(request)
    if err:
        return err
    geometry = payload.get('geometry')
    if not geometry:
        return JsonResponse({'error': 'geometry_required'}, status=400)

    props = payload.get('properties') or {}
    f = Farmland(
        crop_type=props.get('crop_type', Farmland.CropType.ARABLE),
        cadastral_number=(props.get('cadastral_number') or '')[:50],
        source=props.get('source') or 'gis_manual',
    )
    if 'is_used' in props:
        f.is_used = _int_to_is_used(props['is_used'])
    err = _apply_geom(f, geometry)
    if err:
        return err
    f.save()
    return JsonResponse(_farmland_to_feature(f), status=201)


@csrf_exempt
@require_http_methods(['GET', 'PATCH', 'DELETE'])
def api_farmland_detail(request: HttpRequest, pk: int) -> JsonResponse:
    """GET full geometry · PATCH geometry/attrs · DELETE a farmland (admin)."""
    if not _is_admin_legacy(request):
        return JsonResponse({'error': 'forbidden'}, status=403)

    f = get_object_or_404(Farmland, pk=pk)

    if request.method == 'GET':
        return JsonResponse(_farmland_to_feature(f))

    if request.method == 'DELETE':
        f.delete()
        return JsonResponse({'ok': True}, status=200)

    # PATCH
    payload, err = _parse_json(request)
    if err:
        return err
    props = payload.get('properties') or {}
    if 'crop_type' in props and props['crop_type']:
        f.crop_type = props['crop_type']
    if 'cadastral_number' in props:
        f.cadastral_number = (props['cadastral_number'] or '')[:50]
    if 'is_used' in props:
        f.is_used = _int_to_is_used(props['is_used'])
    if payload.get('geometry'):
        err = _apply_geom(f, payload['geometry'])
        if err:
            return err
    f.save()
    return JsonResponse(_farmland_to_feature(f))
