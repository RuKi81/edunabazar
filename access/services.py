"""Единая точка проверки доступа к данным портала.

Все гейты (ГИС-страница, ГИС-API, MVT-тайлы) должны ходить через эти
функции, а не дублировать проверки. Ключ доступа — ``LegacyUser`` (он
каноничный: под ним же строится админ-гейт и сессия портала).

Уровни упорядочены: view < edit < manage. Грант уровня N разрешает
любое действие уровня ≤ N.
"""
from __future__ import annotations

from django.conf import settings
from django.db.models import Q

from .models import ResourceGrant

_LEVEL_ORDER = {'view': 1, 'edit': 2, 'manage': 3}


def is_admin_legacy_user(user) -> bool:
    """superuser ИЛИ username ∈ ADMIN_USERNAMES (как остальные гейты)."""
    if user is None:
        return False
    if bool(getattr(user, 'is_superuser', False)):
        return True
    username = (getattr(user, 'username', '') or '').strip().lower()
    admin_usernames = getattr(settings, 'ADMIN_USERNAMES', {'admin'})
    return username in {u.lower() for u in admin_usernames}


def has_resource_access(user, resource_type: str, resource_id=None,
                        min_level: str = 'view') -> bool:
    """True, если у пользователя есть доступ уровня ≥ ``min_level``.

    Админ имеет доступ всегда. Грант с ``resource_id IS NULL`` (весь класс)
    покрывает любой конкретный ресурс этого типа.
    """
    if is_admin_legacy_user(user):
        return True
    uid = getattr(user, 'id', None)
    if not uid:
        return False
    need = _LEVEL_ORDER.get(min_level, 1)
    qs = ResourceGrant.objects.filter(
        legacy_user_id=uid, resource_type=resource_type,
    )
    if resource_id is None:
        # Действие над «всем классом» покрывается только whole-class грантом.
        qs = qs.filter(resource_id__isnull=True)
    else:
        qs = qs.filter(Q(resource_id=resource_id) | Q(resource_id__isnull=True))
    for level in qs.values_list('level', flat=True):
        if _LEVEL_ORDER.get(level, 0) >= need:
            return True
    return False


def _accessible_resource_ids(user, resource_type: str):
    """Множество id ресурсов ``resource_type``, доступных на просмотр.

    Возвращает ``None`` — если доступны ВСЕ ресурсы этого типа (админ или
    whole-class грант), иначе ``set`` конкретных id (может быть пустым).
    """
    if is_admin_legacy_user(user):
        return None
    uid = getattr(user, 'id', None)
    if not uid:
        return set()
    ids = set()
    grants = ResourceGrant.objects.filter(
        legacy_user_id=uid, resource_type=resource_type,
    ).values_list('resource_id', flat=True)
    for rid in grants:
        if rid is None:
            return None  # whole-class → все ресурсы типа
        ids.add(rid)
    return ids


def accessible_gis_layer_ids(user):
    """Множество id ГИС-слоёв (SHP), доступных на просмотр (см. helper)."""
    return _accessible_resource_ids(
        user, ResourceGrant.ResourceType.GIS_LAYER)


def accessible_raster_layer_ids(user):
    """Множество id растровых слоёв, доступных на просмотр (см. helper)."""
    return _accessible_resource_ids(
        user, ResourceGrant.ResourceType.RASTER_LAYER)


def can_open_gis_page(user) -> bool:
    """Пускать на /me/gis, если админ или есть хоть один ГИС/растровый грант.

    Страница /me/gis хостит и векторные (SHP), и растровые слои, поэтому
    достаточно любого гранта одного из этих типов.
    """
    if is_admin_legacy_user(user):
        return True
    uid = getattr(user, 'id', None)
    if not uid:
        return False
    return ResourceGrant.objects.filter(
        legacy_user_id=uid,
        resource_type__in=(
            ResourceGrant.ResourceType.GIS_LAYER,
            ResourceGrant.ResourceType.RASTER_LAYER,
        ),
    ).exists()
