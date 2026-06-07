"""Снимает лимит площади 100 га с бесплатного плана.

Изначальная миграция ``0002_default_plan`` выставила ``free`` →
``max_total_area_ha=100``. В MVP-1 этот лимит оказался слишком жёстким
(блокировал даже одно крупное поле в фермерском хозяйстве), а отдельной
бизнес-модели для платных планов пока нет — поэтому снимаем безусловно.

На случай отката (``migrate my_fields 0002``) ``reverse`` возвращает
прежний потолок 100 га. Это нужно, чтобы downgrade чейна не оставлял
систему с тихо изменённой схемой данных.
"""
from __future__ import annotations

from django.db import migrations


def drop_area_cap(apps, schema_editor):
    Plan = apps.get_model('my_fields', 'Plan')
    Plan.objects.filter(code='free').update(max_total_area_ha=None)


def restore_area_cap(apps, schema_editor):
    Plan = apps.get_model('my_fields', 'Plan')
    Plan.objects.filter(code='free').update(max_total_area_ha=100.0)


class Migration(migrations.Migration):

    dependencies = [
        ('my_fields', '0002_default_plan'),
    ]

    operations = [
        migrations.RunPython(drop_area_cap, restore_area_cap),
    ]
