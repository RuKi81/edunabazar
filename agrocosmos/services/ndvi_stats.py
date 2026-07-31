"""Общие примитивы area-weighted NDVI-агрегации.

Используются публичными read-эндпоинтами (``views.ndvi``,
``views.reports``): среднее из сумм ``Σ(ndvi×area)/Σ(area)``,
конвертация day-of-year в дату/метку, z-score против baseline,
конец последнего MODIS-композита и текстовая оценка вегетации.

Модуль намеренно без обращений к ORM — только чистые функции,
чтобы их можно было переиспользовать и тестировать изолированно.
"""
from datetime import date, timedelta

# MODIS 16-дневный композит: mid_date = start + 7, конец = start + 15 = mid + 8.
MODIS_COMPOSITE_TAIL_DAYS = 8


def weighted_mean(sum_ndvi_area, sum_area):
    """Area-weighted среднее NDVI из накопленных сумм (None при нулевой площади)."""
    return (sum_ndvi_area / sum_area) if sum_area else None


def doy_to_mmdd(doy):
    """День года → метка 'MM-DD'.

    ВНИМАНИЕ: конвертация идёт через фиксированный високосный 2024 год —
    для дат после 28 февраля невисокосного года метка съезжает на день
    назад (doy 177 → '06-25', хотя в 2025 это 26 июня). Поведение
    сохранено как есть ради стабильности контракта API (см. тест
    ``test_baseline_and_z_score``).
    """
    try:
        return (date(2024, 1, 1) + timedelta(days=doy - 1)).strftime('%m-%d')
    except Exception:
        return f'{doy:03d}'


def doy_to_date(doy, year):
    """День года → ``datetime.date`` внутри указанного года."""
    return date(year, 1, 1) + timedelta(days=doy - 1)


def compute_z_score(value, bl_mean, bl_std, precision=2):
    """z-score наблюдения против baseline (None, если baseline непригоден).

    Baseline непригоден при отсутствии среднего или при std <= 0.01 —
    деление на почти нулевой разброс даёт бессмысленные всплески.
    """
    if value is None or bl_mean is None:
        return None
    if not bl_std or bl_std <= 0.01:
        return None
    return round((value - bl_mean) / bl_std, precision)


def modis_last_period_end(series):
    """Конец последнего 16-дневного композита для пунктирного «хвоста» на графике.

    ``series`` — хронологический список точек ``{'date': 'YYYY-MM-DD', ...}``
    (mid-даты композитов); возвращает ISO-строку либо None.
    """
    if not series:
        return None
    try:
        last_mid = date.fromisoformat(series[-1]['date'])
        return str(last_mid + timedelta(days=MODIS_COMPOSITE_TAIL_DAYS))
    except Exception:
        return None


def ndvi_assessment(mean_ndvi, z_score=None):
    """Короткая текстовая оценка состояния вегетации."""
    if mean_ndvi is None:
        return 'Нет данных'
    if z_score is not None:
        if z_score < -2:
            return 'Критическое снижение вегетации'
        if z_score < -1:
            return 'Вегетация ниже нормы'
        if z_score > 2:
            return 'Вегетация значительно выше нормы'
        if z_score > 1:
            return 'Вегетация выше нормы'
    if mean_ndvi >= 0.6:
        return 'Активная вегетация'
    if mean_ndvi >= 0.4:
        return 'Умеренная вегетация'
    if mean_ndvi >= 0.2:
        return 'Слабая вегетация'
    return 'Вегетация практически отсутствует'
