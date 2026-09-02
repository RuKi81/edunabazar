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


def _public_raster_layer_ids():
    """id всех публичных растровых слоёв (``RasterLayer.is_public=True``).

    Импорт ленивый: ``access`` не должен зависеть от ``my_fields`` на уровне
    модуля (иначе циклический импорт).
    """
    from my_fields.models import RasterLayer
    return set(
        RasterLayer.objects.filter(is_public=True).values_list('id', flat=True)
    )


def accessible_raster_layer_ids(user):
    """Множество id растровых слоёв, доступных пользователю на просмотр.

    Помимо грантов, ВСЕГДА включает публичные слои (``is_public=True``) —
    они видны всем. Возвращает ``None``, если доступны ВСЕ слои (админ или
    whole-class грант).
    """
    base = _accessible_resource_ids(
        user, ResourceGrant.ResourceType.RASTER_LAYER)
    if base is None:
        return None
    return base | _public_raster_layer_ids()


def raster_view_allowed(user, resource_id) -> bool:
    """True, если пользователь может просматривать растровый слой ``resource_id``.

    Доступ даёт админ-статус, грант (view/edit/manage) ИЛИ публичность слоя.
    """
    if has_resource_access(
            user, ResourceGrant.ResourceType.RASTER_LAYER, resource_id, 'view'):
        return True
    from my_fields.models import RasterLayer
    return RasterLayer.objects.filter(
        pk=resource_id, is_public=True).exists()


def has_any_public_raster() -> bool:
    """True, если существует хотя бы один публичный растровый слой."""
    from my_fields.models import RasterLayer
    return RasterLayer.objects.filter(is_public=True).exists()


def can_open_gis_page(user) -> bool:
    """Пускать на /me/gis, если админ, есть ГИС/растровый грант ИЛИ есть
    публичные растры.

    Страница /me/gis хостит и векторные (SHP), и растровые слои. Публичные
    растры (``is_public=True``) видны всем, поэтому наличие хотя бы одного
    такого слоя открывает страницу любому авторизованному пользователю.
    """
    if is_admin_legacy_user(user):
        return True
    uid = getattr(user, 'id', None)
    if not uid:
        return False
    has_grant = ResourceGrant.objects.filter(
        legacy_user_id=uid,
        resource_type__in=(
            ResourceGrant.ResourceType.GIS_LAYER,
            ResourceGrant.ResourceType.RASTER_LAYER,
        ),
    ).exists()
    return has_grant or has_any_public_raster()
