"""Права доступа к пользовательским полям.

В MVP-1 строго: пользователь видит и редактирует только свои поля.
В фазе 4 (командная работа) добавится концепция Farm + роли (viewer /
agronom / manager); все проверки будут продолжать ходить через эти
функции, чтобы не плодить проверки прав в каждом view.
"""
from __future__ import annotations

from my_fields.models import UserField


def can_view_field(user, field: UserField) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_staff or user.is_superuser:
        return True
    return field.owner_id == user.id


def can_edit_field(user, field: UserField) -> bool:
    # MVP: read-permissions == write-permissions для владельца.
    return can_view_field(user, field)
