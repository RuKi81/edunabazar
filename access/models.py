"""Пер-ресурсные гранты доступа к данным портала.

Единая, расширяемая таблица выдачи прав: «кто (LegacyUser) → к какому
ресурсу (тип + опциональный id) → с каким уровнем (view/edit/manage)».

Почему так, а не флаги на самих ресурсах:
* доступ кросс-модельный (ГИС-слои сейчас, farmland/поля позже) — одна
  таблица вместо M2M на каждый тип;
* ``resource_id = NULL`` означает «весь класс ресурсов этого типа»
  (напр. все ГИС-слои) — удобно для роли-подобной выдачи;
* FK на ``legacy_user`` идёт с ``db_constraint=False``: таблица
  ``legacy_user`` — ``managed=False`` (ей владеет старый PHP), поэтому
  внешний ключ на уровне БД не навешиваем, связь только логическая.
"""
from __future__ import annotations

from django.db import models


class ResourceGrant(models.Model):
    class ResourceType(models.TextChoices):
        GIS_LAYER = 'gis_layer', 'ГИС-слой (SHP)'
        # Зарезервировано под фазу 2 (см. предложение по доступу):
        # FARMLAND = 'farmland', 'Слой ЗСН (farmland)'
        # FIELD = 'field', 'Поле пользователя'

    class Level(models.TextChoices):
        VIEW = 'view', 'Просмотр'
        EDIT = 'edit', 'Редактирование'
        MANAGE = 'manage', 'Управление (в т.ч. удаление)'

    legacy_user = models.ForeignKey(
        'legacy.LegacyUser', on_delete=models.DO_NOTHING, db_constraint=False,
        db_column='legacy_user_id', related_name='resource_grants',
        verbose_name='Пользователь',
    )
    resource_type = models.CharField(
        max_length=20, choices=ResourceType.choices,
        default=ResourceType.GIS_LAYER, verbose_name='Тип ресурса',
    )
    resource_id = models.IntegerField(
        null=True, blank=True, verbose_name='ID ресурса',
        help_text='Пусто = доступ ко ВСЕМ ресурсам этого типа.',
    )
    level = models.CharField(
        max_length=10, choices=Level.choices, default=Level.VIEW,
        verbose_name='Уровень доступа',
    )
    granted_by = models.ForeignKey(
        'legacy.LegacyUser', on_delete=models.SET_NULL, null=True, blank=True,
        db_constraint=False, db_column='granted_by_id', related_name='+',
        verbose_name='Кем выдан',
    )
    note = models.CharField(
        max_length=255, blank=True, default='', verbose_name='Примечание',
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'access_resource_grant'
        unique_together = [('legacy_user', 'resource_type', 'resource_id')]
        indexes = [
            models.Index(fields=['legacy_user', 'resource_type']),
        ]
        ordering = ['legacy_user_id', 'resource_type', 'resource_id']
        verbose_name = 'Грант доступа'
        verbose_name_plural = 'Гранты доступа'

    def __str__(self) -> str:
        scope = 'все' if self.resource_id is None else f'#{self.resource_id}'
        return f'{self.legacy_user_id} → {self.resource_type}:{scope} ({self.level})'
