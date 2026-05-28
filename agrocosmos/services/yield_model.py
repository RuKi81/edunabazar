"""Обучение и применение модели прогноза урожайности.

V1.3: Ridge regression на 6 NDVI-фичах → предсказание АНОМАЛИИ
урожайности (отклонения от долгосрочного регионального среднего).

Почему аномалия, а не абсолютная урожайность:

1. **Тренд + смещение по регионам.** Кубань в любой год даёт ≈ 60 ц/га,
   Якутия ≈ 15 ц/га. Если предсказывать абсолют, модель в основном учит
   «куда какой регион», а не «какой год лучше». Аномалия (≈ ±5 ц/га
   вокруг 0) — то, что можно вытянуть из NDVI.

2. **Длинная история до NDVI.** ЕМИСС даёт 14 лет (2010-2023).
   NDVI — только 4 года (2020-2023). Усреднение за 2010-2019 даёт
   стабильный baseline без NDVI, и его не надо «выучивать».

3. **Технологический тренд.** За 10 лет урожайность РФ выросла на
   ~0.1 т/га/год за счёт техники и сортов. Если этот тренд не выделить
   из baseline, модель будет систематически завышать прошлое и
   занижать будущее. Здесь мы линейно детрендим baseline (см. ниже).

Ridge решается аналитически без sklearn — это закрытое решение
``β = (XᵀX + αI)⁻¹ Xᵀy``. Один np.linalg.solve. ~30 строк кода.
"""
from __future__ import annotations

from typing import Iterable, Optional

import numpy as np
from django.db.models import Q

from agrocosmos.models import CropYieldStat, Region, YieldFeatures


# Порядок фичей — фиксированный, должен совпадать в train и predict.
FEATURE_NAMES: list[str] = [
    'peak_ndvi',
    'peak_ndvi_doy',
    'sos_doy',
    'length_of_season',
    'indvi_total',
    'indvi_repro',
]

# Окно для регионального baseline. Берём годы ДО эры NDVI, чтобы не было
# leakage между baseline и фичами.
BASELINE_YEARS = list(range(2010, 2020))

# Минимум лет с фактом для надёжного baseline. Если у региона меньше —
# исключаем его из обучения и прогноза.
MIN_BASELINE_YEARS = 5


# ── Подготовка таргета: detrend через линейную регрессию по году ─────
def compute_regional_baselines(
    crop: str,
    baseline_years: Iterable[int] = BASELINE_YEARS,
) -> dict[int, dict[str, float]]:
    """Линейная регрессия yield ~ year для каждого региона.

    Возвращает {region_id: {'slope': k, 'intercept': b, 'mean_yield': m,
                            'n_years': n, 'last_year': year_max}}.

    Прогноз baseline на год Y: ``b + k * Y``. Это даёт детренд:
    в южных регионах с быстрым ростом тренд значительный (k ≈ 0.1-0.2
    т/га/год), в стабильных — слабый.

    Регионы с менее ``MIN_BASELINE_YEARS`` точек НЕ попадают в результат —
    их прогноз модель не сделает.
    """
    qs = (
        CropYieldStat.objects
        .filter(
            crop=crop,
            district__isnull=True,
            year__in=list(baseline_years),
        )
        .values('region_id', 'year', 'yield_t_per_ha')
    )

    by_region: dict[int, list[tuple[int, float]]] = {}
    for row in qs:
        rid = row['region_id']
        if rid is None:
            continue
        by_region.setdefault(rid, []).append((int(row['year']), float(row['yield_t_per_ha'])))

    out: dict[int, dict[str, float]] = {}
    for rid, points in by_region.items():
        if len(points) < MIN_BASELINE_YEARS:
            continue
        years = np.array([p[0] for p in points], dtype=np.float64)
        yields = np.array([p[1] for p in points], dtype=np.float64)
        # numpy.polyfit deg=1: y = k*x + b
        if np.std(years) > 0:
            k, b = np.polyfit(years, yields, deg=1)
        else:
            k, b = 0.0, float(np.mean(yields))
        out[rid] = {
            'slope': float(k),
            'intercept': float(b),
            'mean_yield': float(np.mean(yields)),
            'n_years': len(points),
            'last_year': int(np.max(years)),
        }
    return out


def baseline_for(
    region_id: int, year: int, baselines: dict[int, dict[str, float]],
) -> Optional[float]:
    """Прогноз baseline для (region, year). None если регион не покрыт."""
    bl = baselines.get(region_id)
    if bl is None:
        return None
    return bl['intercept'] + bl['slope'] * year


# ── Сборка обучающей матрицы ─────────────────────────────────────────
def prepare_training_data(
    crop: str,
    feature_set_version: str = 'v1',
    baselines: Optional[dict[int, dict[str, float]]] = None,
) -> dict:
    """Собрать (X, y, regions, years) для обучения.

    Берёт пересечение ``YieldFeatures(season_complete=True)`` с
    ``CropYieldStat`` (есть факт-урожайность). Регионы без baseline
    (мало истории) исключаются.

    Возвращает dict:
        X            : np.ndarray (n, n_features)
        y            : np.ndarray (n,)  — АНОМАЛИЯ (yield - baseline)
        y_actual     : np.ndarray (n,)  — фактический yield, для метрик
        y_baseline   : np.ndarray (n,)  — baseline-прогноз, для аудита
        region_ids   : np.ndarray (n,)
        years        : np.ndarray (n,)
        feature_names: list[str]
        baselines    : {region_id: {...}}
    """
    if baselines is None:
        baselines = compute_regional_baselines(crop)

    # YieldFeatures со завершённым сезоном.
    yf_qs = (
        YieldFeatures.objects
        .filter(
            crop=crop,
            feature_set_version=feature_set_version,
            district__isnull=True,
            season_complete=True,
        )
        .values('region_id', 'year', 'features')
    )

    # Yield-факты — только для региональных (district IS NULL).
    yield_qs = (
        CropYieldStat.objects
        .filter(crop=crop, district__isnull=True)
        .values('region_id', 'year', 'yield_t_per_ha')
    )
    yield_map: dict[tuple[int, int], float] = {
        (r['region_id'], int(r['year'])): float(r['yield_t_per_ha']) for r in yield_qs
    }

    X_rows: list[list[float]] = []
    y_anom: list[float] = []
    y_actual: list[float] = []
    y_baseline: list[float] = []
    region_ids: list[int] = []
    years_list: list[int] = []

    for f_row in yf_qs:
        rid = f_row['region_id']
        year = int(f_row['year'])
        if rid is None:
            continue
        # Нужен факт-yield этого года.
        actual = yield_map.get((rid, year))
        if actual is None:
            continue
        # Нужен baseline (>=5 лет истории).
        bl = baseline_for(rid, year, baselines)
        if bl is None:
            continue
        feats = f_row['features'] or {}
        try:
            x_row = [float(feats[name]) for name in FEATURE_NAMES]
        except (KeyError, TypeError, ValueError):
            continue
        X_rows.append(x_row)
        y_anom.append(actual - bl)
        y_actual.append(actual)
        y_baseline.append(bl)
        region_ids.append(int(rid))
        years_list.append(year)

    return {
        'X': np.array(X_rows, dtype=np.float64),
        'y': np.array(y_anom, dtype=np.float64),
        'y_actual': np.array(y_actual, dtype=np.float64),
        'y_baseline': np.array(y_baseline, dtype=np.float64),
        'region_ids': np.array(region_ids, dtype=np.int64),
        'years': np.array(years_list, dtype=np.int32),
        'feature_names': list(FEATURE_NAMES),
        'baselines': baselines,
    }


# ── Стандартизация и Ridge ───────────────────────────────────────────
def fit_scaler(X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Per-feature mean / std. std=1 если столбец константа."""
    mean = X.mean(axis=0)
    std = X.std(axis=0)
    std = np.where(std < 1e-12, 1.0, std)
    return mean, std


def apply_scaler(X: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    return (X - mean) / std


def fit_ridge(
    X_scaled: np.ndarray, y: np.ndarray, alpha: float,
) -> tuple[np.ndarray, float]:
    """Ridge с центрированным Y, без штрафа на интерсепт.

    β̂ = (XᵀX + αI)⁻¹ Xᵀ(y - ȳ),    α̂ = ȳ.
    """
    y_mean = float(y.mean())
    y_centered = y - y_mean
    n_features = X_scaled.shape[1]
    A = X_scaled.T @ X_scaled + alpha * np.eye(n_features)
    b = X_scaled.T @ y_centered
    beta = np.linalg.solve(A, b)
    return beta, y_mean


def predict_anomaly(
    X: np.ndarray, mean: np.ndarray, std: np.ndarray,
    beta: np.ndarray, intercept: float,
) -> np.ndarray:
    """Прогноз аномалии. Caller прибавляет baseline для абсолюта."""
    Xs = apply_scaler(X, mean, std)
    return Xs @ beta + intercept


# ── Кросс-валидация ──────────────────────────────────────────────────
def cross_validate_loyo(
    X: np.ndarray, y: np.ndarray, years: np.ndarray,
    alpha: float,
) -> dict:
    """Leave-one-year-out CV.

    Для каждого уникального года: тренируем на остальных, прогнозируем
    тестовый год. Возвращает residuals и метрики.
    """
    unique_years = sorted(set(years.tolist()))
    residuals = np.zeros_like(y)
    per_year: dict[int, dict[str, float]] = {}

    for test_year in unique_years:
        test_mask = (years == test_year)
        train_mask = ~test_mask
        if train_mask.sum() < 10:  # минимальная защита от пустого train
            continue

        X_tr = X[train_mask]
        y_tr = y[train_mask]
        mean, std = fit_scaler(X_tr)
        Xs_tr = apply_scaler(X_tr, mean, std)
        beta, intercept = fit_ridge(Xs_tr, y_tr, alpha=alpha)

        y_pred = predict_anomaly(X[test_mask], mean, std, beta, intercept)
        res = y[test_mask] - y_pred
        residuals[test_mask] = res

        per_year[int(test_year)] = {
            'n': int(test_mask.sum()),
            'rmse': float(np.sqrt(np.mean(res ** 2))),
            'mae': float(np.mean(np.abs(res))),
        }

    rmse_cv = float(np.sqrt(np.mean(residuals ** 2)))
    mae_cv = float(np.mean(np.abs(residuals)))
    ss_res = float(np.sum(residuals ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2_cv = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return {
        'residuals': residuals,
        'rmse': rmse_cv,
        'mae': mae_cv,
        'r2': r2_cv,
        'per_year': per_year,
    }


def search_best_alpha(
    X: np.ndarray, y: np.ndarray, years: np.ndarray,
    alpha_grid: Iterable[float] = (0.01, 0.1, 1.0, 3.0, 10.0, 30.0, 100.0),
) -> tuple[float, dict[float, float]]:
    """Подбор α по LOYO RMSE. Возвращает (best_alpha, {alpha: rmse})."""
    scores: dict[float, float] = {}
    for alpha in alpha_grid:
        cv = cross_validate_loyo(X, y, years, alpha=alpha)
        scores[float(alpha)] = cv['rmse']
    best_alpha = min(scores, key=scores.get)
    return best_alpha, scores


# ── Финальное обучение и сериализация ────────────────────────────────
def train_full_model(
    data: dict,
    alpha: Optional[float] = None,
    alpha_grid: Optional[Iterable[float]] = None,
) -> dict:
    """Полный пайплайн: подбор α (если нужен) → CV → финальная модель.

    Возвращает dict, готовый для сохранения в YieldForecastModel.
    """
    X = data['X']
    y = data['y']
    years = data['years']

    if alpha is None:
        grid = alpha_grid or (0.01, 0.1, 1.0, 3.0, 10.0, 30.0, 100.0)
        alpha, alpha_scores = search_best_alpha(X, y, years, alpha_grid=grid)
    else:
        alpha_scores = None

    # CV с лучшей α — даёт RMSE и residuals для CI.
    cv = cross_validate_loyo(X, y, years, alpha=alpha)

    # Финальная модель — на ВСЕХ данных.
    mean, std = fit_scaler(X)
    Xs = apply_scaler(X, mean, std)
    beta, intercept = fit_ridge(Xs, y, alpha=alpha)

    # Train metrics.
    y_pred_train = Xs @ beta + intercept
    res_train = y - y_pred_train
    rmse_train = float(np.sqrt(np.mean(res_train ** 2)))
    ss_res = float(np.sum(res_train ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2_train = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # RMSE % (через y_actual, не аномалию — даёт интерпретируемый %).
    y_actual = data['y_actual']
    rmse_pct = float(cv['rmse'] / y_actual.mean() * 100.0) if y_actual.mean() > 0 else 0.0

    return {
        'alpha': float(alpha),
        'alpha_grid_scores': alpha_scores,
        'feature_names': list(FEATURE_NAMES),
        'coefficients': {name: float(b) for name, b in zip(FEATURE_NAMES, beta)},
        'intercept': float(intercept),
        'feature_scaler': {
            'means': {name: float(m) for name, m in zip(FEATURE_NAMES, mean)},
            'stds': {name: float(s) for name, s in zip(FEATURE_NAMES, std)},
        },
        'r2_train': r2_train,
        'rmse_train': rmse_train,
        'r2_cv': cv['r2'],
        'rmse_cv': cv['rmse'],
        'mae_cv': cv['mae'],
        'rmse_pct': rmse_pct,
        'residuals_cv': cv['residuals'].tolist(),
        'per_year_cv': cv['per_year'],
        'n_samples': int(len(y)),
        'train_years': sorted(set(years.tolist())),
        # Сохраняем baselines в модели — нужны при прогнозе.
        'regional_baselines': {
            int(rid): {k: v for k, v in bl.items()}
            for rid, bl in data['baselines'].items()
        },
    }


# ── Trivial baseline (без NDVI-фичей) ────────────────────────────────
def train_trivial_model(data: dict) -> dict:
    """Trivial-модель: forecast = regional baseline (линейный тренд по году).

    Не использует NDVI-фичи. Эквивалент «всегда предсказывать аномалию = 0».
    RMSE_cv ≈ σ(аномалий), R²_cv = 0 по определению.

    Полезна как honest baseline когда NDVI-сигнал слабее baseline-тренда.
    Сохраняется как полноценная YieldForecastModel с пустыми coefficients,
    чтобы predict_yield мог её использовать единообразно.
    """
    y = data['y']
    y_actual = data['y_actual']
    years = data['years']

    # Per-year «predictions» (всегда 0 — это и есть trivial).
    # residuals = y - 0 = y (аномалия).
    residuals = y.copy()

    rmse = float(np.sqrt(np.mean(y ** 2)))
    mae = float(np.mean(np.abs(y)))
    ss_res = float(np.sum(y ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    # R²=0 если y центрирован вокруг 0; небольшой сдвиг даёт R²<0.
    r2_cv = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    per_year = {}
    for yr in sorted(set(years.tolist())):
        m = (years == yr)
        per_year[int(yr)] = {
            'n': int(m.sum()),
            'rmse': float(np.sqrt(np.mean(y[m] ** 2))),
            'mae': float(np.mean(np.abs(y[m]))),
        }

    rmse_pct = float(rmse / y_actual.mean() * 100.0) if y_actual.mean() > 0 else 0.0

    return {
        'alpha': None,
        'alpha_grid_scores': None,
        'feature_names': [],
        'coefficients': {},
        'intercept': 0.0,
        'feature_scaler': {'means': {}, 'stds': {}},
        'r2_train': r2_cv,
        'rmse_train': rmse,
        'r2_cv': r2_cv,
        'rmse_cv': rmse,
        'mae_cv': mae,
        'rmse_pct': rmse_pct,
        'residuals_cv': residuals.tolist(),
        'per_year_cv': per_year,
        'n_samples': int(len(y)),
        'train_years': sorted(set(years.tolist())),
        'regional_baselines': {
            int(rid): {k: v for k, v in bl.items()}
            for rid, bl in data['baselines'].items()
        },
    }


# ── Применение модели для прогноза ───────────────────────────────────
def model_predict(
    features: dict, model_state: dict, region_id: int, year: int,
) -> Optional[dict]:
    """Прогноз для одной (region × year) пары.

    ``features`` — JSON из YieldFeatures.features.
    ``model_state`` — что мы сохранили в train_full_model.

    Возвращает {forecast_t_per_ha, anomaly, baseline, ci_lower, ci_upper}
    или None, если не хватает данных.
    """
    feature_names = model_state['feature_names']
    try:
        x = np.array([float(features[name]) for name in feature_names], dtype=np.float64)
    except (KeyError, TypeError, ValueError):
        return None

    means = np.array([model_state['feature_scaler']['means'][n] for n in feature_names])
    stds = np.array([model_state['feature_scaler']['stds'][n] for n in feature_names])
    coef = np.array([model_state['coefficients'][n] for n in feature_names])
    intercept = float(model_state['intercept'])

    x_scaled = (x - means) / stds
    anomaly = float(x_scaled @ coef + intercept)

    # Baseline для (region, year).
    rb = model_state.get('regional_baselines', {})
    bl = rb.get(str(region_id)) or rb.get(region_id)
    if bl is None:
        return None
    baseline = float(bl['intercept']) + float(bl['slope']) * year

    forecast = baseline + anomaly

    # CI80 — эмпирические квантили остатков CV.
    residuals = np.array(model_state.get('residuals_cv', []), dtype=np.float64)
    if len(residuals) >= 10:
        q_lo = float(np.quantile(residuals, 0.10))
        q_hi = float(np.quantile(residuals, 0.90))
        ci_lower = forecast + q_lo
        ci_upper = forecast + q_hi
    else:
        ci_lower = forecast
        ci_upper = forecast

    return {
        'forecast_t_per_ha': forecast,
        'anomaly': anomaly,
        'baseline': baseline,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
    }
