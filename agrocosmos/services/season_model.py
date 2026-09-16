"""Обучаемая модель класса угодья по годовому профилю NDVI.

Замена ручных порогов (``services/winter_spring.py``) на модель, обученную
по ручной разметке (:class:`FarmlandTrainingLabel`). Пороги и гейты
приходилось задавать, упорядочивать и калибровать вручную, а качество
мерилось на тех же эталонах, на которых порог и подбирался. Здесь веса
признаков подбираются оптимизацией, а качество измеряется на отложенных
ГРУППАХ угодий.

ДВЕ БИНАРНЫЕ СТУПЕНИ, а не один многоклассовый софтмакс: разметка сильно
несбалансирована (не обрабатывается ≫ озимые ≈ яровые), и единая модель
выучила бы «всё не обрабатывается». Кроме того, у ступеней РАЗНЫЙ объём
данных, а значит и допустимая размерность вектора признаков:

* **cover** (``crop`` против ``unused``) — сотни эталонов, можно позволить
  весь профиль: средний NDVI по полумесяцам плюс полнота ряда;
* **season** (``winter`` против ``spring``) — около сотни эталонов, поэтому
  компактный вектор физически осмысленных признаков, где главный —
  прошлогодняя осень (озимые всходят в августе-сентябре).

ВАЛИДАЦИЯ ТОЛЬКО ПО ГРУППАМ (район/хозяйство). Контуры одного массива —
почти дубликаты друг друга: при случайном k-fold они попадают и в train,
и в test, и качество получается завышенным. Leave-one-group-out такой
утечки не даёт.

Логистическая регрессия считается здесь же на numpy (метод Ньютона / IRLS
с L2), без новых зависимостей — тем же способом, что ``Ridge`` в
``services/yield_model.py``. На выборке из сотен примеров интерпретируемые
веса важнее гибкости бустинга: по вектору весов сразу видно, какие декады
сезона модель считает решающими.
"""
from __future__ import annotations

from typing import Optional, Sequence

import numpy as np

from agrocosmos.services.winter_spring import (
    classify_profile, profile_features,
)
from agrocosmos.services.yield_model import apply_scaler, fit_scaler

# ── Наборы признаков ─────────────────────────────────────────────────
# Версия в имени набора пишется в модель: старые сохранённые модели
# нельзя применять к вектору другого состава.
FEATURE_SET_COVER = 'cover_v1'
FEATURE_SET_SEASON = 'season_v1'

# Профиль режется на полумесяцы: 24 бина — компромисс между разрешением
# (уборка озимых от уборки яровых отстоит на ~месяц) и числом признаков
# при нескольких сотнях эталонов. Декады (36) дали бы больше шума.
PROFILE_BINS = 24
DAYS_IN_YEAR = 366

# Ряд короче этого числа наблюдений не описывает сезон — такой эталон
# в обучение не берётся (то же правило, что в ``classify_profile``).
MIN_OBS = 6

# Сетка L2 для подбора по группам. Ноль исключён намеренно: на
# разделимой выборке нерегуляризованный Ньютон уходит в бесконечность.
L2_GRID = (0.3, 1.0, 3.0, 10.0, 30.0, 100.0)

# Меньше трёх групп — leave-one-group-out бессмысленен (в train остаётся
# одна-две группы), тогда честнее сказать, что оценки нет.
MIN_GROUPS_FOR_CV = 3

# Признаки ступени «озимые vs яровые». ``autumn_prev`` — NDVI сентября-
# ноября ПРЕДЫДУЩЕГО года: единственный прямой признак озимых, остальные
# косвенные. ``autumn_prev_missing`` — индикатор пропуска: угодья без
# прошлогоднего ряда нельзя выбрасывать (их большинство в новых
# регионах), но и подставлять медиану молча тоже нельзя.
SEASON_FEATURE_NAMES = (
    'autumn_prev', 'autumn_prev_missing', 'early_spring', 'summer',
    'autumn', 'peak_doy', 'peak_ndvi', 'amplitude', 'green_fraction',
    'season_min', 'winter_baseline',
)


def profile_bins(doys: Sequence[int], ndvi: Sequence[float],
                 n_bins: int = PROFILE_BINS) -> Optional[np.ndarray]:
    """Средний NDVI по ``n_bins`` равным интервалам года или ``None``.

    Пустые интервалы (облачность, зимние пропуски) заполняются линейной
    интерполяцией между заполненными, крайние — ближайшим значением:
    модели нужен вектор постоянной длины, а «нулевой NDVI» в пропуске
    выглядел бы как голая почва и искажал бы обучение.
    """
    if len(doys) < MIN_OBS:
        return None
    doys_arr = np.asarray(doys, dtype=np.float64)
    vals = np.asarray(ndvi, dtype=np.float64)
    idx = np.clip((doys_arr - 1) / DAYS_IN_YEAR * n_bins, 0,
                  n_bins - 1).astype(np.int64)

    sums = np.zeros(n_bins)
    counts = np.zeros(n_bins)
    np.add.at(sums, idx, vals)
    np.add.at(counts, idx, 1.0)
    filled = counts > 0
    if not filled.any():
        return None

    out = np.empty(n_bins)
    out[filled] = sums[filled] / counts[filled]
    if not filled.all():
        positions = np.arange(n_bins, dtype=np.float64)
        out = np.interp(positions, positions[filled], out[filled])
    return out


def features_cover(doys: Sequence[int],
                   ndvi: Sequence[float]) -> Optional[dict[str, float]]:
    """Признаки ступени «культуры vs не обрабатывается» или ``None``.

    Полный профиль плюс два признака полноты ряда: у необрабатываемых
    угодий наблюдений нередко меньше (мелкие контуры, склоны), и модель
    не должна принимать это за признак класса — но и скрывать от неё
    полноту нельзя, иначе интерполяция пропусков выглядит как данные.
    """
    bins = profile_bins(doys, ndvi)
    if bins is None:
        return None
    coverage = float(np.mean(profile_coverage(doys)))
    feats = {f'ndvi_b{i:02d}': float(v) for i, v in enumerate(bins)}
    feats['coverage'] = coverage
    feats['n_obs'] = float(len(doys))
    return feats


def profile_coverage(doys: Sequence[int],
                     n_bins: int = PROFILE_BINS) -> np.ndarray:
    """Маска «в интервале есть хотя бы одно наблюдение» (0/1)."""
    doys_arr = np.asarray(doys, dtype=np.float64)
    idx = np.clip((doys_arr - 1) / DAYS_IN_YEAR * n_bins, 0,
                  n_bins - 1).astype(np.int64)
    mask = np.zeros(n_bins)
    mask[np.unique(idx)] = 1.0
    return mask


def cover_feature_names(n_bins: int = PROFILE_BINS) -> tuple[str, ...]:
    """Имена признаков ступени cover в фиксированном порядке."""
    return tuple([f'ndvi_b{i:02d}' for i in range(n_bins)]
                 + ['coverage', 'n_obs'])


def features_season(
    doys: Sequence[int],
    ndvi: Sequence[float],
    prev_doys: Optional[Sequence[int]] = None,
    prev_ndvi: Optional[Sequence[float]] = None,
) -> Optional[dict[str, float]]:
    """Признаки ступени «озимые vs яровые» или ``None``.

    Значения берутся из :func:`profile_features` — те же величины, что
    использовались в пороговом классификаторе, но взвешивает их теперь
    модель. Отсутствующие значения остаются ``None`` и заполняются
    медианой обучающей выборки в :func:`vectorize`.
    """
    if len(doys) < MIN_OBS:
        return None
    feats = profile_features(doys, ndvi)
    # День пика берём из ``classify_profile``: он считается по СГЛАЖЕННОМУ
    # ряду, поэтому единичный облачный выброс не сдвигает пик на месяц.
    prof = classify_profile(doys, ndvi)

    prev_autumn = None
    if prev_doys is not None and prev_ndvi is not None and len(prev_doys):
        prev_autumn = profile_features(prev_doys, prev_ndvi).get('autumn')

    return {
        'autumn_prev': prev_autumn,
        'autumn_prev_missing': 0.0 if prev_autumn is not None else 1.0,
        'early_spring': feats.get('early_spring'),
        'summer': feats.get('summer'),
        'autumn': feats.get('autumn'),
        'peak_doy': prof.peak_doy,
        'peak_ndvi': feats.get('peak_ndvi'),
        'amplitude': feats.get('amplitude'),
        'green_fraction': feats.get('green_fraction'),
        'season_min': feats.get('season_min'),
        'winter_baseline': feats.get('winter_baseline'),
    }


# ── Матрица признаков ────────────────────────────────────────────────
def fit_impute(rows: Sequence[dict], names: Sequence[str]) -> dict:
    """Медианы признаков по обучающей выборке для заполнения пропусков.

    Медиана, а не среднее: распределения NDVI-признаков скошены, и один
    выброс сдвинул бы подстановку. Признак без единого значения получает
    ноль — после стандартизации это центр выборки, то есть «нейтрально».
    """
    out = {}
    for name in names:
        vals = [r[name] for r in rows if r.get(name) is not None]
        out[name] = float(np.median(vals)) if vals else 0.0
    return out


def vectorize(rows: Sequence[dict], names: Sequence[str],
              impute: dict) -> np.ndarray:
    """Матрица (n_rows × n_names); ``None`` заменяются на ``impute``."""
    X = np.empty((len(rows), len(names)), dtype=np.float64)
    for i, row in enumerate(rows):
        for j, name in enumerate(names):
            value = row.get(name)
            X[i, j] = impute[name] if value is None else float(value)
    return X


# ── Логистическая регрессия (IRLS + L2) ──────────────────────────────
def class_weights(y: np.ndarray) -> np.ndarray:
    """Веса примеров, уравнивающие вклад классов.

    Без них модель на разметке 313 «не обрабатывается» против 115
    «культур» максимизировала бы общую точность, всегда отвечая
    «не обрабатывается». С весами максимизируется сбалансированная
    точность — та же метрика, по которой оценивались пороги.
    """
    y = np.asarray(y, dtype=np.float64)
    n = y.size
    w = np.ones(n)
    for cls in (0.0, 1.0):
        mask = y == cls
        n_cls = int(mask.sum())
        if n_cls:
            w[mask] = n / (2.0 * n_cls)
    return w


def fit_logistic(
    X_scaled: np.ndarray,
    y: np.ndarray,
    l2: float,
    sample_weight: Optional[np.ndarray] = None,
    max_iter: int = 50,
    tol: float = 1e-8,
) -> tuple[np.ndarray, float]:
    """Веса и свободный член логистической регрессии: ``(beta, intercept)``.

    Метод Ньютона (IRLS): на каждом шаге решается взвешенная линейная
    задача ``(XᵀWX + λI) Δ = Xᵀ(y - p) - λβ``. Свободный член НЕ
    штрафуется — иначе модель смещалась бы к вероятности 0.5 независимо
    от данных. Гессиан при почти разделимой выборке плохо обусловлен,
    поэтому есть откат на ``lstsq``, а линейный предиктор ограничен по
    модулю (``exp`` от больших чисел переполняется).
    """
    X_scaled = np.asarray(X_scaled, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    n, p = X_scaled.shape
    Xa = np.hstack([np.ones((n, 1)), X_scaled])
    w = (np.ones(n) if sample_weight is None
         else np.asarray(sample_weight, dtype=np.float64))
    penalty = np.full(p + 1, float(l2))
    penalty[0] = 0.0

    beta = np.zeros(p + 1)
    for _ in range(max_iter):
        prob = _sigmoid(Xa @ beta)
        weights = w * prob * (1.0 - prob) + 1e-9
        grad = Xa.T @ (w * (y - prob)) - penalty * beta
        hess = Xa.T @ (Xa * weights[:, None]) + np.diag(penalty)
        try:
            step = np.linalg.solve(hess, grad)
        except np.linalg.LinAlgError:
            step = np.linalg.lstsq(hess, grad, rcond=None)[0]
        beta = beta + step
        if np.max(np.abs(step)) < tol:
            break
    return beta[1:], float(beta[0])


def _sigmoid(eta: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(eta, -30.0, 30.0)))


def predict_proba(X: np.ndarray, model_state: dict) -> np.ndarray:
    """Вероятности положительного класса для матрицы признаков."""
    mean = np.asarray(model_state['mean'], dtype=np.float64)
    std = np.asarray(model_state['std'], dtype=np.float64)
    beta = np.asarray(model_state['beta'], dtype=np.float64)
    Xs = apply_scaler(np.asarray(X, dtype=np.float64), mean, std)
    return _sigmoid(Xs @ beta + float(model_state['intercept']))


def predict_row(row: dict, model_state: dict) -> tuple[float, str]:
    """Вероятность и метка класса для ОДНОГО угодья: ``(prob, label)``.

    Принимает словарь признаков (результат ``features_*``), поэтому
    вызывающему коду не нужно знать порядок столбцов — он берётся из
    самой модели, что исключает рассинхронизацию train и predict.
    """
    X = vectorize([row], model_state['names'], model_state['impute'])
    prob = float(predict_proba(X, model_state)[0])
    label = (model_state['positive_label']
             if prob >= float(model_state['threshold'])
             else model_state['negative_label'])
    return prob, label


# ── Обучение и оценка по группам ─────────────────────────────────────
def _fit_state(X: np.ndarray, y: np.ndarray, l2: float,
               names: Sequence[str], impute: dict,
               feature_set: str, positive_label: str,
               negative_label: str, threshold: float = 0.5) -> dict:
    """Обучить модель на готовой матрице и упаковать в JSON-совместимый dict."""
    mean, std = fit_scaler(X)
    beta, intercept = fit_logistic(apply_scaler(X, mean, std), y, l2,
                                   sample_weight=class_weights(y))
    return {
        'feature_set': feature_set,
        'names': list(names),
        'impute': {k: float(v) for k, v in impute.items()},
        'mean': [float(v) for v in mean],
        'std': [float(v) for v in std],
        'beta': [float(v) for v in beta],
        'intercept': float(intercept),
        'l2': float(l2),
        'threshold': float(threshold),
        'positive_label': positive_label,
        'negative_label': negative_label,
        'n_train': int(X.shape[0]),
        'n_positive': int(np.sum(y == 1)),
    }


def binary_metrics(y_true: np.ndarray, y_prob: np.ndarray,
                   threshold: float = 0.5) -> dict:
    """Метрики бинарного решения: recall по классам плюс их среднее.

    Ключевая метрика — ``balanced``: она не растёт от угадывания
    преобладающего класса, поэтому сравнима между ступенями и с
    пороговым классификатором (0.5 = монетка).
    """
    y_true = np.asarray(y_true, dtype=np.float64)
    pred = np.asarray(y_prob, dtype=np.float64) >= threshold
    pos, neg = y_true == 1, y_true == 0
    n_pos, n_neg = int(pos.sum()), int(neg.sum())
    recall_pos = float(np.mean(pred[pos])) if n_pos else 0.0
    recall_neg = float(np.mean(~pred[neg])) if n_neg else 0.0
    total = n_pos + n_neg
    return {
        'balanced': (recall_pos + recall_neg) / 2,
        'recall_pos': recall_pos,
        'recall_neg': recall_neg,
        'accuracy': ((recall_pos * n_pos + recall_neg * n_neg) / total
                     if total else 0.0),
        'n_pos': n_pos,
        'n_neg': n_neg,
    }


def group_cv(
    rows: Sequence[dict],
    y: Sequence[float],
    groups: Sequence,
    names: Sequence[str],
    l2: float,
    feature_set: str = '',
) -> Optional[dict]:
    """Leave-one-group-out оценка или ``None``, если групп слишком мало.

    Каждая группа (район, хозяйство) по очереди становится тестовой,
    модель обучается на остальных. Стандартизация, подстановка пропусков
    и веса классов считаются ВНУТРИ фолда — иначе информация о тестовых
    угодьях просочилась бы в обучение и оценка снова стала бы завышенной.

    Фолды, где в train или test не оказалось обоих классов, пропускаются:
    оценить разделение по одному классу невозможно.
    """
    y = np.asarray(y, dtype=np.float64)
    groups = np.asarray(list(groups))
    uniq = sorted(set(groups.tolist()))
    if len(uniq) < MIN_GROUPS_FOR_CV:
        return None

    oof_prob = np.full(y.size, np.nan)
    per_group: dict = {}
    for group in uniq:
        test = groups == group
        train = ~test
        if not _both_classes(y[train]) or not test.any():
            continue
        train_rows = [rows[i] for i in np.flatnonzero(train)]
        impute = fit_impute(train_rows, names)
        X_tr = vectorize(train_rows, names, impute)
        state = _fit_state(X_tr, y[train], l2, names, impute, feature_set,
                           'pos', 'neg')
        test_rows = [rows[i] for i in np.flatnonzero(test)]
        prob = predict_proba(vectorize(test_rows, names, impute), state)
        oof_prob[test] = prob
        per_group[str(group)] = {
            'n': int(test.sum()),
            **binary_metrics(y[test], prob),
        }

    scored = ~np.isnan(oof_prob)
    if not scored.any() or not _both_classes(y[scored]):
        return None
    out = binary_metrics(y[scored], oof_prob[scored])
    out.update({
        'scheme': 'leave-one-group-out',
        'n_groups': len(uniq),
        'n_scored': int(scored.sum()),
        'per_group': per_group,
        'l2': float(l2),
    })
    return out


def _both_classes(y: np.ndarray) -> bool:
    return bool(np.any(y == 1) and np.any(y == 0))


def search_best_l2(
    rows: Sequence[dict],
    y: Sequence[float],
    groups: Sequence,
    names: Sequence[str],
    l2_grid: Sequence[float] = L2_GRID,
    feature_set: str = '',
) -> tuple[Optional[float], dict]:
    """Подбор силы регуляризации по CV-балансу: ``(best_l2, {l2: balanced})``.

    Подбирается ПО ОТЛОЖЕННЫМ ГРУППАМ, поэтому сравнение вариантов не
    вырождается в «чем слабее регуляризация, тем лучше», как было бы на
    обучающей выборке. ``(None, {})`` — если CV недоступна.
    """
    scores: dict[float, float] = {}
    for l2 in l2_grid:
        cv = group_cv(rows, y, groups, names, l2, feature_set)
        if cv is not None:
            scores[float(l2)] = cv['balanced']
    if not scores:
        return None, {}
    return max(scores, key=lambda k: scores[k]), scores


def train_model(
    rows: Sequence[dict],
    y: Sequence[float],
    groups: Sequence,
    names: Sequence[str],
    feature_set: str,
    positive_label: str,
    negative_label: str,
    l2_grid: Sequence[float] = L2_GRID,
) -> tuple[dict, Optional[dict], dict]:
    """Обучить ступень: ``(model_state, cv_report, l2_scores)``.

    Сначала по группам подбирается L2, затем с ним модель обучается на
    ВСЕЙ выборке — итоговая модель видит максимум данных, а честная
    оценка её качества берётся из ``cv_report`` (``None``, если групп
    меньше :data:`MIN_GROUPS_FOR_CV`: обучить можно, измерить нельзя).
    """
    y_arr = np.asarray(y, dtype=np.float64)
    if not _both_classes(y_arr):
        raise ValueError('Нужны примеры обоих классов')

    best_l2, scores = search_best_l2(rows, y_arr, groups, names, l2_grid,
                                     feature_set)
    l2 = best_l2 if best_l2 is not None else float(np.median(l2_grid))
    cv = (group_cv(rows, y_arr, groups, names, l2, feature_set)
          if best_l2 is not None else None)

    impute = fit_impute(rows, names)
    X = vectorize(rows, names, impute)
    state = _fit_state(X, y_arr, l2, names, impute, feature_set,
                       positive_label, negative_label)
    state['in_sample'] = binary_metrics(y_arr, predict_proba(X, state))
    return state, cv, scores


def feature_importance(model_state: dict, top: int = 10) -> list[tuple]:
    """Признаки по убыванию |веса|: ``[(имя, вес), ...]``.

    Признаки стандартизованы, поэтому веса сравнимы между собой — это
    главный практический плюс логрегрессии здесь: видно, на какие
    интервалы сезона модель реально опирается, и совпадает ли это с
    агрономической логикой.
    """
    pairs = list(zip(model_state['names'], model_state['beta']))
    pairs.sort(key=lambda p: abs(p[1]), reverse=True)
    return [(name, float(w)) for name, w in pairs[:top]]
