"""Создаёт дефолтный тариф ``free`` и заготовки под платные.

``services/quotas.get_user_plan`` отдаёт ``DEFAULT_FREE_LIMITS`` если в
БД нет записи ``Plan(code='free')`` — но это аварийный fallback,
нормальный путь работает через эту строку. Поэтому миграция —
обязательная часть схемы, а не «опциональный seed».

Платные планы (``pro``, ``farm``, ``enterprise``) создаются как
``is_active=False`` заглушки — чтобы интеграция со страницей тарифов
имела на что ссылаться, но в UI «купить» сейчас не показывалось.
"""
from __future__ import annotations

from django.db import migrations


DEFAULT_PLANS = [
    # (code, name, price, max_fields, max_area_ha, ndvi_years, weather, alerts, sort, active)
    ('free',        'Бесплатный',         0,    5,    100.0,  1, False, True,  0,  True),
    ('pro',         'Pro',              990,   25,    500.0,  3, True,  True, 10, False),
    ('farm',        'Хозяйство',       2490,  100,   2500.0,  5, True,  True, 20, False),
    ('enterprise',  'Enterprise',         0, None,    None,  10, True,  True, 30, False),
]


def create_default_plans(apps, schema_editor):
    Plan = apps.get_model('my_fields', 'Plan')
    for code, name, price, max_f, max_a, ndvi_y, weather, alerts, sort, active in DEFAULT_PLANS:
        Plan.objects.update_or_create(
            code=code,
            defaults={
                'name': name,
                'monthly_price_rub': price,
                'max_fields': max_f,
                'max_total_area_ha': max_a,
                'ndvi_history_years': ndvi_y,
                'weather_forecast_enabled': weather,
                'alerts_enabled': alerts,
                'sort_order': sort,
                'is_active': active,
            },
        )


def remove_default_plans(apps, schema_editor):
    Plan = apps.get_model('my_fields', 'Plan')
    Plan.objects.filter(code__in=[p[0] for p in DEFAULT_PLANS]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('my_fields', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(create_default_plans, remove_default_plans),
    ]
