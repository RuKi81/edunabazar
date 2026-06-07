"""Лимиты и тарифы для ``my_fields``.

В MVP-1 фактически работает только дефолтный план ``free``. Но все
проверки лимитов делаются через ``can_create_field`` / ``can_*`` —
поэтому переключение на платные тарифы в фазе 4 не потребует менять
ничего во views.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from django.contrib.auth import get_user_model

User = get_user_model()


@dataclass(frozen=True)
class QuotaCheck:
    """Результат проверки лимита.

    ``ok=False`` обязательно сопровождается ``reason`` — пользовательским
    сообщением (RU). ``hint`` опционально — call-to-action на апгрейд.
    """
    ok: bool
    reason: str = ''
    hint: str = ''


# Дефолтные лимиты ``free``-плана. Используются как fallback, если в БД
# нет записи Plan(code='free') — это нештатная ситуация (миграция должна
# её создать), но валиться в 500 из-за этого глупо.
DEFAULT_FREE_LIMITS = {
    'max_fields': 5,
    # Лимит площади убран: в MVP-1 он мешал даже бесплатным пользователям с
    # большими хозяйствами. Регулирование объёма теперь только через
    # ``max_fields`` (число записей). ``None`` ⇒ безлимит.
    'max_total_area_ha': None,
    'ndvi_history_years': 1,
    'weather_forecast_enabled': False,
    'alerts_enabled': True,
}


def get_user_plan(user) -> dict:
    """Вернуть «эффективные» лимиты для пользователя.

    Никаких объектов ORM наружу не отдаём — только plain dict, чтобы
    точки потребления (views, templates) не зависели от схемы. Если у
    пользователя нет ``UserPlan`` — считаем его на ``free``.
    """
    if not user or not user.is_authenticated:
        return DEFAULT_FREE_LIMITS

    from my_fields.models import Plan, UserPlan

    up = (
        UserPlan.objects
        .filter(user=user)
        .select_related('plan')
        .first()
    )
    plan = up.plan if up else Plan.objects.filter(code='free', is_active=True).first()
    if plan is None:
        return DEFAULT_FREE_LIMITS

    return {
        'code': plan.code,
        'name': plan.name,
        'max_fields': plan.max_fields,
        'max_total_area_ha': plan.max_total_area_ha,
        'ndvi_history_years': plan.ndvi_history_years,
        'weather_forecast_enabled': plan.weather_forecast_enabled,
        'alerts_enabled': plan.alerts_enabled,
    }


def can_create_field(user, new_area_ha: float = 0) -> QuotaCheck:
    """Проверить, может ли пользователь создать ещё одно поле.

    Учитывает два лимита: количество полей и суммарную площадь. Поля в
    архиве (``is_archived=True``) не считаются.
    """
    from my_fields.models import UserField

    plan = get_user_plan(user)
    max_fields = plan.get('max_fields')
    max_area = plan.get('max_total_area_ha')

    qs = UserField.objects.filter(owner=user, is_archived=False)

    if max_fields is not None:
        current = qs.count()
        if current >= max_fields:
            return QuotaCheck(
                ok=False,
                reason=f'Достигнут лимит вашего тарифа: {max_fields} полей.',
                hint='Заархивируйте неиспользуемые поля или перейдите на расширенный тариф.',
            )

    if max_area is not None:
        total = sum(qs.values_list('area_ha', flat=True))
        if total + new_area_ha > max_area:
            return QuotaCheck(
                ok=False,
                reason=(
                    f'Превышение лимита площади: '
                    f'{total + new_area_ha:.1f} га > {max_area:.0f} га.'
                ),
                hint='Перейдите на расширенный тариф для увеличения лимита.',
            )

    return QuotaCheck(ok=True)
