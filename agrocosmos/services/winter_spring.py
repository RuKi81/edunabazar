"""Классификация угодий на озимые / яровые по сезонному профилю NDVI.

Идея (правило, а не ML — обучающих данных по яровым нет):

* **Озимые** сеют осенью; после зимы они возобновляют вегетацию рано —
  уже в апреле поле зелёное (высокий NDVI ранней весны), SOS ранний,
  профиль часто «двугорбый» (осенний + весенний рост).
* **Яровые** сеют весной; в апреле поле — голая почва (низкий NDVI),
  всходы и рост начинаются в мае-июне (поздний SOS, единственный пик).

Ключевой дискриминатор — **средний NDVI ранней весны** (окно
``EARLY_SPRING_*``, ≈ 1 апреля – 15 мая для средней полосы РФ / Тулы).
Порог по нему калибруется по опорным точкам известных озимых
(слой ``kultury_2026``): берём такой порог, чтобы подавляющее большинство
эталонных озимых полей оказалось выше него.

Все функции — чистые (numpy), без обращений к БД: их удобно тестировать на
синтетических профилях и переиспользовать в команде и в отчётах.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np

# ── Календарные окна (день года, DOY) для средней полосы РФ ───────────
# Ранняя весна: озимые уже вегетируют, яровые ещё не взошли.
EARLY_SPRING_DOY_START = 91   # ~1 апреля
EARLY_SPRING_DOY_END = 135    # ~15 мая
# Зимний покой (для baseline): январь-февраль.
WINTER_DOY_END = 59           # ~28 февраля
# Вегетационный сезон (для поиска пика/SOS).
SEASON_DOY_START = 60
SEASON_DOY_END = 300

# Порог ранневесеннего NDVI по умолчанию (если калибровки нет). Голая
# почва даёт NDVI ≈ 0.15–0.25, вегетирующие озимые в апреле ≈ 0.4+.
DEFAULT_EARLY_SPRING_THRESHOLD = 0.35
# Допустимый диапазон калиброванного порога — защита от вырожденной
# выборки эталонов (например, все точки в один сезон с высоким шумом).
MIN_THRESHOLD = 0.25
MAX_THRESHOLD = 0.50

# Порог амплитуды (пик − baseline): ниже — нет внятного вег. цикла,
# класс не определяем.
MIN_AMPLITUDE = 0.12
# Минимум наблюдений в сезоне для устойчивой классификации.
MIN_SEASON_OBSERVATIONS = 6

# Границы SOS для вторичного сигнала (усиливает/ослабляет уверенность):
# ранний SOS ⇒ озимые, поздний ⇒ яровые.
WINTER_SOS_MAX_DOY = 105      # SOS ≤ ~15 апреля — уверенно озимые
SPRING_SOS_MIN_DOY = 130      # SOS ≥ ~10 мая — уверенно яровые

# Ширина «перехода» уверенности вокруг порога (в единицах NDVI).
CONFIDENCE_SCALE = 0.15


@dataclass
class SeasonProfile:
    """Результат :func:`classify_profile` — класс + фичи + диагностика."""
    season_class: str            # 'winter' | 'spring' | 'unknown'
    confidence: float            # 0..1
    early_spring_ndvi: Optional[float]
    winter_baseline: Optional[float]
    sos_doy: Optional[int]
    peak_doy: Optional[int]
    peak_ndvi: Optional[float]
    n_obs: int

    def as_dict(self) -> dict:
        return {
            'season_class': self.season_class,
            'confidence': round(self.confidence, 4),
            'early_spring_ndvi': (
                None if self.early_spring_ndvi is None
                else round(self.early_spring_ndvi, 4)
            ),
            'winter_baseline': (
                None if self.winter_baseline is None
                else round(self.winter_baseline, 4)
            ),
            'sos_doy': self.sos_doy,
            'peak_doy': self.peak_doy,
            'peak_ndvi': (
                None if self.peak_ndvi is None else round(self.peak_ndvi, 4)
            ),
            'n_obs': self.n_obs,
        }


def _smooth(ndvi: np.ndarray) -> np.ndarray:
    """Лёгкое сглаживание ряда (устойчивость к облачным выбросам)."""
    if len(ndvi) < 5:
        return ndvi
    try:
        from scipy.signal import savgol_filter
        return savgol_filter(ndvi, window_length=5, polyorder=2, mode='nearest')
    except ImportError:
        kernel = np.array([1, 2, 1]) / 4.0
        return np.convolve(ndvi, kernel, mode='same')


def _window_mean(doys: np.ndarray, vals: np.ndarray,
                 lo: int, hi: int) -> Optional[float]:
    mask = (doys >= lo) & (doys <= hi)
    if not mask.any():
        return None
    return float(np.mean(vals[mask]))


def _detect_peak_sos(doys: np.ndarray, vals: np.ndarray, baseline: float):
    """(peak_doy, peak_ndvi, sos_doy) внутри сезона или (None, None, None)."""
    season = (doys >= SEASON_DOY_START) & (doys <= SEASON_DOY_END)
    if season.sum() < 2:
        return None, None, None
    s_doys = doys[season]
    s_vals = vals[season]
    peak_idx = int(np.argmax(s_vals))
    peak_ndvi = float(s_vals[peak_idx])
    peak_doy = int(s_doys[peak_idx])

    amplitude = peak_ndvi - baseline
    if amplitude < MIN_AMPLITUDE:
        return peak_doy, peak_ndvi, None

    threshold = baseline + 0.5 * amplitude
    rise = (s_doys <= peak_doy) & (s_vals >= threshold)
    sos_doy = int(s_doys[rise][0]) if rise.any() else None
    return peak_doy, peak_ndvi, sos_doy


def _confidence(early_spring_ndvi: float, threshold: float,
                sos_doy: Optional[int], season_class: str) -> float:
    """Уверенность в [0.5..0.99] по удалённости от порога + согласие SOS."""
    d = abs(early_spring_ndvi - threshold) / CONFIDENCE_SCALE
    conf = 0.5 + 0.4 * (1.0 - np.exp(-d))  # 0.5 → 0.9 по мере удаления
    # Вторичный сигнал SOS: согласуется — поднимаем, противоречит — снижаем.
    if sos_doy is not None:
        if season_class == 'winter':
            if sos_doy <= WINTER_SOS_MAX_DOY:
                conf += 0.08
            elif sos_doy >= SPRING_SOS_MIN_DOY:
                conf -= 0.12
        else:  # spring
            if sos_doy >= SPRING_SOS_MIN_DOY:
                conf += 0.08
            elif sos_doy <= WINTER_SOS_MAX_DOY:
                conf -= 0.12
    return float(np.clip(conf, 0.5, 0.99))


def classify_profile(
    doys: Sequence[int], ndvi: Sequence[float],
    threshold: float = DEFAULT_EARLY_SPRING_THRESHOLD,
) -> SeasonProfile:
    """Классифицировать один сезонный NDVI-ряд угодья.

    Args:
        doys: дни года наблюдений (1..366).
        ndvi: значения NDVI (сырые или сглаженные) в том же порядке.
        threshold: порог ранневесеннего NDVI (winter, если ≥ порога).

    Возвращает :class:`SeasonProfile`. ``season_class='unknown'`` — данных
    недостаточно (мало точек, нет наблюдений ранней весной, слабая амплитуда).
    """
    doys = np.asarray(doys, dtype=np.int32)
    ndvi = np.asarray(ndvi, dtype=np.float64)
    n_obs = int(len(doys))

    if n_obs < MIN_SEASON_OBSERVATIONS:
        return SeasonProfile('unknown', 0.0, None, None, None, None, None, n_obs)

    order = np.argsort(doys)
    doys = doys[order]
    ndvi = ndvi[order]
    smoothed = _smooth(ndvi)

    winter_baseline = _window_mean(doys, smoothed, 1, WINTER_DOY_END)
    if winter_baseline is None:
        winter_baseline = float(np.min(smoothed))

    peak_doy, peak_ndvi, sos_doy = _detect_peak_sos(doys, smoothed, winter_baseline)

    early_spring = _window_mean(
        doys, smoothed, EARLY_SPRING_DOY_START, EARLY_SPRING_DOY_END,
    )

    # Без ранневесенних наблюдений или без вег. цикла — класс не определён.
    if early_spring is None or peak_doy is None:
        return SeasonProfile(
            'unknown', 0.0, early_spring, winter_baseline,
            sos_doy, peak_doy, peak_ndvi, n_obs,
        )
    if peak_ndvi is not None and (peak_ndvi - winter_baseline) < MIN_AMPLITUDE:
        return SeasonProfile(
            'unknown', 0.0, early_spring, winter_baseline,
            sos_doy, peak_doy, peak_ndvi, n_obs,
        )

    season_class = 'winter' if early_spring >= threshold else 'spring'
    confidence = _confidence(early_spring, threshold, sos_doy, season_class)
    return SeasonProfile(
        season_class, confidence, early_spring, winter_baseline,
        sos_doy, peak_doy, peak_ndvi, n_obs,
    )


def calibrate_threshold(
    winter_early_spring: Sequence[float],
    percentile: float = 10.0,
    default: float = DEFAULT_EARLY_SPRING_THRESHOLD,
) -> float:
    """Подобрать порог по ранневесеннему NDVI эталонных ОЗИМЫХ полей.

    Порог ставим на ``percentile``-й перцентиль распределения (по умолчанию
    10-й): так ~90 % эталонных озимых окажутся ≥ порога и будут корректно
    классифицированы. Результат зажимается в [``MIN_THRESHOLD``,
    ``MAX_THRESHOLD``]; при пустой выборке возвращается ``default``.
    """
    vals = [v for v in winter_early_spring if v is not None]
    if not vals:
        return default
    thr = float(np.percentile(np.asarray(vals, dtype=np.float64), percentile))
    return float(np.clip(thr, MIN_THRESHOLD, MAX_THRESHOLD))


def calibrate_threshold_separating(
    winter_early_spring: Sequence[float],
    spring_early_spring: Sequence[float],
    default: float = DEFAULT_EARLY_SPRING_THRESHOLD,
) -> float:
    """Двусторонняя калибровка порога по эталонам ОЗИМЫХ и ЯРОВЫХ.

    Когда есть опорные точки обоих классов, порог по ранневесеннему NDVI
    ставим в точку наилучшего разделения: перебираем кандидатов (значения
    выборок и середины между ними) и максимизируем *сбалансированную
    точность* — среднее из доли верно распознанных озимых (≥ порога) и
    яровых (< порога). Это устойчивее к дисбалансу классов, чем обычная
    точность.

    Деградации:
    - только озимые → :func:`calibrate_threshold` (10-й перцентиль);
    - только яровые → 90-й перцентиль их значений (≈90 % яровых < порога);
    - нет данных → ``default``.
    Итог зажимается в [``MIN_THRESHOLD``, ``MAX_THRESHOLD``].
    """
    w = [v for v in winter_early_spring if v is not None]
    s = [v for v in spring_early_spring if v is not None]
    if not w and not s:
        return default
    if w and not s:
        return calibrate_threshold(w)
    if s and not w:
        thr = float(np.percentile(np.asarray(s, dtype=np.float64), 90))
        return float(np.clip(thr, MIN_THRESHOLD, MAX_THRESHOLD))

    w_arr = np.asarray(w, dtype=np.float64)
    s_arr = np.asarray(s, dtype=np.float64)
    uniq = np.unique(np.concatenate([w_arr, s_arr]))
    candidates = list(uniq)
    candidates += [(uniq[i] + uniq[i + 1]) / 2 for i in range(len(uniq) - 1)]

    best_thr, best_score = default, -1.0
    for thr in candidates:
        tpr = float(np.mean(w_arr >= thr))   # доля озимых верно
        tnr = float(np.mean(s_arr < thr))    # доля яровых верно
        score = (tpr + tnr) / 2
        if score > best_score:
            best_score, best_thr = score, float(thr)
    return float(np.clip(best_thr, MIN_THRESHOLD, MAX_THRESHOLD))


# ── Сопоставление названия культуры → класс сезона (ground truth) ────
# Правило пользователя (слой kultury_2026, атрибут crop), 3 класса + сады:
#   * озимые    — содержит «озим» (Пшеница озимая, Рожь озимая, …);
#   * сады      — содержит «сад» → ИГНОРИРУЕМ (не бинарь озимая/яровая);
#   * не обраб. — «не использ»/«неудоб»/«сидеральн»/«залеж»/«рекультивац»/«пар»;
#   * яровые    — ВСЕ ОСТАЛЬНЫЕ обрабатываемые (Соя, Ячмень, Вика+Овес, …).
# Порядок проверок важен: «сад» и «озим» — до «пар»/unused
# («Молодой сад (пар)» → игнор, а не unused).
CROP_IGNORE_KEYS = ('сад',)
CROP_WINTER_KEYS = ('озим',)
CROP_UNUSED_KEYS = (
    'не использ', 'неудоб', 'сидеральн', 'залеж', 'рекультивац', 'пар',
)


def classify_crop_value(value: Optional[str]) -> Optional[str]:
    """Название культуры → 'winter' | 'spring' | 'unused' | 'ignore' | None.

    ``None`` — пустое значение. ``ignore`` — сады (в бинаре не участвуют).
    Реализует правило «все остальные обрабатываемые → яровые».
    """
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    if any(k in v for k in CROP_IGNORE_KEYS):
        return 'ignore'
    if any(k in v for k in CROP_WINTER_KEYS):
        return 'winter'
    if any(k in v for k in CROP_UNUSED_KEYS):
        return 'unused'
    return 'spring'


def evaluate_predictions(pairs: Sequence) -> dict:
    """Матрица качества по эталонам: (true_class, pred_class) → сводка.

    Учитываются только эталоны классов winter/spring (бинарь); прогноз
    ``unknown`` считается неверным. Возвращает per-class {n, correct} и
    общую точность.
    """
    out = {
        'winter': {'n': 0, 'correct': 0},
        'spring': {'n': 0, 'correct': 0},
    }
    for true_cls, pred_cls in pairs:
        if true_cls not in out:
            continue
        out[true_cls]['n'] += 1
        if pred_cls == true_cls:
            out[true_cls]['correct'] += 1
    total = out['winter']['n'] + out['spring']['n']
    correct = out['winter']['correct'] + out['spring']['correct']
    out['accuracy'] = (correct / total) if total else None
    out['total'] = total
    return out
