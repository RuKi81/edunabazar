"""Вычисление NDVI-фичей для прогноза урожайности.

Цель: для пары (регион × год × культура) получить вектор фичей,
который потом подаётся в обученную модель ``YieldForecastModel``.

V1 — фичи формата «фенология + интегралы NDVI», 6 штук:

    peak_ndvi          — пик area-weighted NDVI за вегетационный сезон
    peak_ndvi_doy      — день года пика (1..365)
    sos_doy            — Start of Season (DOY): когда NDVI поднимается до
                          50 % амплитуды от зимнего минимума к пику
    length_of_season   — EOS - SOS, дней
    indvi_total        — ∫ NDVI dDOY с SOS до EOS (трапеции, baseline=0)
    indvi_repro        — ∫ NDVI dDOY в окне peak ± 20 дней
                          (репродуктивная фаза — критическое окно для зерновых)

Источник NDVI: ``DistrictNdviSeries`` (MODIS-агрегаты по районам).
Сборка региона = sum(sum_ndvi_area) / sum(sum_area) по всем районам
региона на каждую дату — это даёт корректное area-weighted среднее.

Фильтр угодий: только ``crop_type='arable'`` (пашня) — пастбища и
сенокосы биологически отличаются по динамике NDVI и шумят сигнал.

Фильтр сезона: DOY 60..330 (≈ март..ноябрь) — отбрасывает зимние
шумы (снег, низкое солнце).

Все расчёты numpy/scipy — без внешних зависимостей.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Optional

import numpy as np
from django.db.models import Sum

from agrocosmos.models import DistrictNdviSeries, Region


# ── Константы детектора фенологии ────────────────────────────────────
# DOY-окно вегетационного сезона: для всей РФ грубо март-ноябрь.
# Узкие специальные случаи (Кубань — раньше, Архангельск — позже)
# модель «съест» через peak_doy и sos_doy фичи; нам важно отрезать
# зимний шум (снег искажает MODIS NDVI до отрицательных значений).
SEASON_DOY_START = 60
SEASON_DOY_END = 330

# Доля от амплитуды (peak − winter_baseline), на которой ставим SOS/EOS.
# 0.5 — стандарт Jönsson & Eklundh 2002, устойчив к шуму.
PHENOLOGY_THRESHOLD_RATIO = 0.5

# Окно репродуктивной фазы (для indvi_repro).
REPRO_WINDOW_DAYS = 20

# Минимально необходимое количество наблюдений в сезоне для надёжной
# детекции — иначе фичи признаём недостоверными.
MIN_SEASON_OBSERVATIONS = 8


# ── Контейнер результата ─────────────────────────────────────────────
@dataclass
class YieldFeatureVector:
    """Результат ``compute_region_features`` — вектор фичей + диагностика."""
    peak_ndvi: float
    peak_ndvi_doy: int
    sos_doy: int
    length_of_season: int
    indvi_total: float
    indvi_repro: float

    # Метаданные, не идущие в модель, но полезные для аудита.
    n_observations: int
    eos_doy: int
    season_mean_ndvi: float

    def as_dict(self) -> dict:
        """Возвращает только модельные фичи (без метаданных)."""
        return {
            'peak_ndvi': round(self.peak_ndvi, 4),
            'peak_ndvi_doy': self.peak_ndvi_doy,
            'sos_doy': self.sos_doy,
            'length_of_season': self.length_of_season,
            'indvi_total': round(self.indvi_total, 4),
            'indvi_repro': round(self.indvi_repro, 4),
        }

    def diagnostics(self) -> dict:
        return {
            'n_observations': self.n_observations,
            'eos_doy': self.eos_doy,
            'season_mean_ndvi': round(self.season_mean_ndvi, 4),
        }


# ── Внутренние утилиты ───────────────────────────────────────────────
def _aggregate_region_ndvi_series(
    region: Region,
    year: int,
    crop_type: str = 'arable',
) -> tuple[np.ndarray, np.ndarray]:
    """Достать area-weighted NDVI ряд по региону за год.

    Возвращает (doy_array, ndvi_array), отсортированный по DOY.
    Если данных нет — оба массива пустые.
    """
    rows = (
        DistrictNdviSeries.objects
        .filter(
            district__region=region,
            crop_type=crop_type,
            acquired_date__year=year,
        )
        .values('acquired_date')
        .annotate(
            sum_n=Sum('sum_ndvi_area'),
            sum_a=Sum('sum_area'),
        )
        .order_by('acquired_date')
    )

    doys: list[int] = []
    ndvis: list[float] = []
    for row in rows:
        sum_a = row['sum_a'] or 0.0
        if sum_a <= 0:
            continue
        ndvi = (row['sum_n'] or 0.0) / sum_a
        doy = row['acquired_date'].timetuple().tm_yday
        doys.append(doy)
        ndvis.append(ndvi)

    return np.asarray(doys, dtype=np.int32), np.asarray(ndvis, dtype=np.float64)


def _smooth_series(ndvi: np.ndarray) -> np.ndarray:
    """Лёгкое сглаживание для устойчивости детектора пика к шуму.

    Используем savgol filter с window=5, polyorder=2 — стандарт для
    MODIS 8-day. Если точек < 5 — возвращаем как есть.
    """
    if len(ndvi) < 5:
        return ndvi
    try:
        from scipy.signal import savgol_filter
        return savgol_filter(ndvi, window_length=5, polyorder=2, mode='nearest')
    except ImportError:
        # Fallback: simple moving average (ширина 3).
        kernel = np.array([1, 2, 1]) / 4.0
        return np.convolve(ndvi, kernel, mode='same')


def _detect_phenology(
    doys: np.ndarray, ndvi: np.ndarray
) -> Optional[dict]:
    """Найти SOS / Peak / EOS из NDVI-ряда.

    Алгоритм (упрощённый Jönsson & Eklundh 2002):

    1. Считаем зимний baseline = min NDVI в DOY < 60 или > 330
       (если таких точек нет — берём min всего ряда).
    2. Pиск peak = argmax внутри сезона (DOY 60..330).
    3. Threshold = baseline + 0.5 * (peak − baseline).
    4. SOS = первый DOY ≥ начало_сезона, где сглаженный NDVI ≥ threshold,
       причём идёт ВВЕРХ (i.e. до peak_doy).
    5. EOS = последний DOY ≤ конец_сезона, где сглаженный NDVI ≥ threshold,
       причём идёт ВНИЗ (i.e. после peak_doy).

    Возвращает None, если сезон детектировать не удалось.
    """
    if len(doys) < MIN_SEASON_OBSERVATIONS:
        return None

    season_mask = (doys >= SEASON_DOY_START) & (doys <= SEASON_DOY_END)
    if season_mask.sum() < MIN_SEASON_OBSERVATIONS:
        return None

    smoothed = _smooth_series(ndvi)

    # Зимний baseline.
    winter_mask = ~season_mask
    if winter_mask.any():
        baseline = float(np.min(smoothed[winter_mask]))
    else:
        baseline = float(np.min(smoothed))

    # Пик внутри сезона.
    season_doys = doys[season_mask]
    season_ndvi = smoothed[season_mask]
    peak_idx = int(np.argmax(season_ndvi))
    peak_ndvi = float(season_ndvi[peak_idx])
    peak_doy = int(season_doys[peak_idx])

    # Если амплитуда слишком мала — это не пашня под зерновыми, а пустыня,
    # снег или сильно облачный регион. Признаём фичи недостоверными.
    amplitude = peak_ndvi - baseline
    if amplitude < 0.1:
        return None

    threshold = baseline + PHENOLOGY_THRESHOLD_RATIO * amplitude

    # SOS — первый «подъём через threshold» до пика.
    rise_mask = (season_doys <= peak_doy) & (season_ndvi >= threshold)
    if not rise_mask.any():
        return None
    sos_doy = int(season_doys[rise_mask][0])

    # EOS — последний «спад через threshold» после пика.
    fall_mask = (season_doys >= peak_doy) & (season_ndvi >= threshold)
    if not fall_mask.any():
        return None
    eos_doy = int(season_doys[fall_mask][-1])

    if eos_doy <= sos_doy:
        return None

    return {
        'peak_ndvi': peak_ndvi,
        'peak_ndvi_doy': peak_doy,
        'sos_doy': sos_doy,
        'eos_doy': eos_doy,
        'season_doys': season_doys,
        'season_ndvi': season_ndvi,
        'baseline': baseline,
    }


def _integrate_ndvi(
    doys: np.ndarray, ndvi: np.ndarray,
    start_doy: int, end_doy: int,
    baseline: float = 0.0,
) -> float:
    """Численный интеграл NDVI − baseline по [start_doy, end_doy].

    Использует трапеции на нерегулярной сетке.
    """
    mask = (doys >= start_doy) & (doys <= end_doy)
    if mask.sum() < 2:
        return 0.0
    xs = doys[mask].astype(np.float64)
    ys = ndvi[mask] - baseline
    ys = np.clip(ys, 0.0, None)  # отрицательные «вычеты» не считаем
    return float(np.trapezoid(ys, xs))


# ── Публичный API ────────────────────────────────────────────────────
def compute_region_features(
    region: Region,
    year: int,
    crop_type: str = 'arable',
) -> Optional[YieldFeatureVector]:
    """Полный пайплайн: NDVI ряд → фенология → 6 фичей.

    Возвращает ``None`` если данных недостаточно (нет точек, низкая
    амплитуда, не детектируется пик и т.п.) — caller должен это
    обработать (пропустить запись или fallback).
    """
    doys, ndvi = _aggregate_region_ndvi_series(region, year, crop_type=crop_type)
    if len(doys) == 0:
        return None

    pheno = _detect_phenology(doys, ndvi)
    if pheno is None:
        return None

    # Используем сглаженный сезонный ряд для интегралов — устойчивее.
    s_doys = pheno['season_doys']
    s_ndvi = pheno['season_ndvi']
    baseline = pheno['baseline']

    indvi_total = _integrate_ndvi(
        s_doys, s_ndvi,
        start_doy=pheno['sos_doy'], end_doy=pheno['eos_doy'],
        baseline=baseline,
    )

    indvi_repro = _integrate_ndvi(
        s_doys, s_ndvi,
        start_doy=pheno['peak_ndvi_doy'] - REPRO_WINDOW_DAYS,
        end_doy=pheno['peak_ndvi_doy'] + REPRO_WINDOW_DAYS,
        baseline=baseline,
    )

    season_mask = (s_doys >= pheno['sos_doy']) & (s_doys <= pheno['eos_doy'])
    season_mean = float(np.mean(s_ndvi[season_mask])) if season_mask.any() else 0.0

    return YieldFeatureVector(
        peak_ndvi=pheno['peak_ndvi'],
        peak_ndvi_doy=pheno['peak_ndvi_doy'],
        sos_doy=pheno['sos_doy'],
        length_of_season=pheno['eos_doy'] - pheno['sos_doy'],
        indvi_total=indvi_total,
        indvi_repro=indvi_repro,
        n_observations=int(len(s_doys)),
        eos_doy=pheno['eos_doy'],
        season_mean_ndvi=season_mean,
    )


def is_season_complete(year: int, today: Optional[_dt.date] = None) -> bool:
    """Считаем сезон завершённым, если EOS точно прошёл.

    Эвристика: если DOY текущего дня > 330, текущий год тоже complete.
    Прошлые годы — всегда complete.
    """
    if today is None:
        today = _dt.date.today()
    if year < today.year:
        return True
    if year == today.year and today.timetuple().tm_yday > SEASON_DOY_END:
        return True
    return False
