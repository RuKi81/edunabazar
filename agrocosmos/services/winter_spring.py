"""Классификация угодий на озимые / яровые по сезонному профилю NDVI.

Идея (правило, а не ML — обучающих данных по яровым нет):

* **Озимые** сеют осенью; после зимы они возобновляют вегетацию рано —
  уже в апреле поле зелёное (высокий NDVI ранней весны), SOS ранний,
  профиль часто «двугорбый» (осенний + весенний рост).
* **Яровые** сеют весной; в апреле поле — голая почва (низкий NDVI),
  всходы и рост начинаются в мае-июне (поздний SOS, единственный пик).

ОСНОВНОЙ дискриминатор — **день пика NDVI**: озимые достигают максимума
рано (конец мая – июнь) и убираются к июлю, яровые — позже (июль – август).
Порог дня пика калибруется по опорным точкам (слой ``kultury_2026``):
раньше порога ⇒ озимые, позже ⇒ яровые. **Средний NDVI ранней весны**
(окно ``EARLY_SPRING_*``, ≈ 1 апреля – 15 мая) — ВТОРИЧНЫЙ сигнал: озимые
уже зелены в апреле, яровые — голая почва; он корректирует уверенность.

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

# ── ОСНОВНОЙ дискриминатор: время пика NDVI ──────────────────────────
# Озимые достигают максимума рано (конец мая – июнь), к июлю их убирают;
# яровые набирают максимум позже (июль – август). Поэтому день пика —
# физически устойчивый признак: раньше порога ⇒ озимые, позже ⇒ яровые.
# Ранневесенний NDVI остаётся ВТОРИЧНЫМ (корроборация + уверенность).
PEAK_DOY_THRESHOLD_DEFAULT = 185   # ~4 июля
MIN_PEAK_DOY_THRESHOLD = 160       # ~9 июня
MAX_PEAK_DOY_THRESHOLD = 215       # ~3 августа
# Масштаб «перехода» уверенности по пику (в днях).
PEAK_CONFIDENCE_SCALE_DOY = 30.0

# ── ГЕЙТ УБОРКИ: отсев необрабатываемых угодий ───────────────────────
# Обрабатываемая культура убирается — после пика NDVI РЕЗКО падает к
# уровню голой почвы / стерни (озимые к июлю, яровые к августу-сентябрю).
# Необрабатываемые земли (залежь, многолетние травы, сенокос без укоса,
# сидеральный пар) дают вегетационный цикл БЕЗ уборочного обвала: NDVI
# после пика снижается плавно и остаётся высоким. Поэтому наличие
# «уборочного» спада — надёжный признак того, что поле реально
# обрабатывается. Контуры без спада относим к классу ``unused``.
#
# Окно поиска уборки — от дня пика вперёд (уборка обычно в пределах ~3
# месяцев после максимума). Спад меряем до МИНИМУМА в окне, а не до конца
# ряда: так повторный рост сорняков ПОСЛЕ уборки не «прячет» событие.
HARVEST_WINDOW_DAYS = 90
# Минимум наблюдений после пика, чтобы вообще судить об уборке.
HARVEST_MIN_POST_OBS = 2
# Абсолютный спад NDVI (пик − минимум после пика) для «уборки».
HARVEST_MIN_DROP = 0.20
# Доля падения относительно амплитуды (пик − baseline): культура падает
# минимум наполовину к базовой линии, трава — почти не падает.
HARVEST_MIN_DROP_RATIO = 0.50

# ── Диагностические окна для отбора признаков «не обрабатывается» ─────
# Кандидаты в дискриминаторы залежи/трав против культур (см.
# :func:`profile_features`). Работают и в середине сезона, т.к. не требуют
# состоявшейся уборки.
GREEN_NDVI_THRESHOLD = 0.40      # «зелёное» наблюдение (есть растительность)
SUMMER_DOY_START = 196           # ~15 июля
SUMMER_DOY_END = 243             # ~31 августа
AUTUMN_DOY_START = 258           # ~15 сентября
AUTUMN_DOY_END = 305             # ~1 ноября

# ── ГЕЙТ ПОКРОВА (cover gate): отсев необрабатываемых по доле зелени ──
# На реальных данных (Тула-2026) лучший сезон-агностичный дискриминатор —
# ДОЛЯ ЗЕЛЁНЫХ наблюдений (NDVI ≥ GREEN_NDVI_THRESHOLD за весь ряд):
# у культур медиана 0.47–0.58 (p25 ≥ 0.38), у «не обрабатываемых» (залежь,
# неудобья, разреженный покров) — 0.19 (p75 ≤ 0.26). Порог между кластерами
# ≈ 0.33 чисто разделяет классы и, в отличие от гейта уборки, НЕ требует
# состоявшейся уборки — применим и в середине сезона.
GREEN_FRACTION_MIN = 0.33
# Диапазон калиброванного порога (защита от вырожденной выборки эталонов).
MIN_COVER_THRESHOLD = 0.20
MAX_COVER_THRESHOLD = 0.50


@dataclass
class HarvestSignal:
    """Результат :func:`detect_harvest` — признак уборочного спада NDVI."""
    has_harvest: bool
    harvest_doy: Optional[int]
    drop_abs: Optional[float]     # пик − минимум после пика (в NDVI)
    drop_ratio: Optional[float]   # drop_abs / амплитуда (0..1+)


@dataclass
class SeasonProfile:
    """Результат :func:`classify_profile` — класс + фичи + диагностика."""
    season_class: str            # 'winter' | 'spring' | 'unused' | 'unknown'
    confidence: float            # 0..1
    early_spring_ndvi: Optional[float]
    winter_baseline: Optional[float]
    sos_doy: Optional[int]
    peak_doy: Optional[int]
    peak_ndvi: Optional[float]
    n_obs: int
    harvest_doy: Optional[int] = None
    harvest_drop: Optional[float] = None   # доля спада (drop_ratio)
    green_fraction: Optional[float] = None  # доля зелёных наблюдений (покров)

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
            'harvest_doy': self.harvest_doy,
            'harvest_drop': (
                None if self.harvest_drop is None
                else round(self.harvest_drop, 4)
            ),
            'green_fraction': (
                None if self.green_fraction is None
                else round(self.green_fraction, 4)
            ),
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


def detect_harvest(
    doys: np.ndarray, vals: np.ndarray, peak_doy: int, peak_ndvi: float,
    baseline: float,
    min_drop: float = HARVEST_MIN_DROP,
    min_drop_ratio: float = HARVEST_MIN_DROP_RATIO,
    window_days: int = HARVEST_WINDOW_DAYS,
) -> HarvestSignal:
    """Есть ли «уборочный» спад NDVI после пика.

    Смотрим МИНИМУМ сглаженного ряда в окне ``(peak_doy, peak_doy +
    window_days]``. Уборка засчитывается, если спад к этому минимуму
    достаточно глубокий и в абсолюте (``min_drop``), и относительно
    амплитуды сезона (``min_drop_ratio``). Минимум (а не последнее
    значение) делает признак устойчивым к повторному росту сорняков
    после уборки. При нехватке наблюдений после пика (< ``HARVEST_MIN_POST_OBS``)
    — считаем, что уборки нет (``has_harvest=False``, метрики None).
    """
    amplitude = peak_ndvi - baseline
    if amplitude <= 0:
        return HarvestSignal(False, None, None, None)

    post = (doys > peak_doy) & (doys <= peak_doy + window_days)
    if int(post.sum()) < HARVEST_MIN_POST_OBS:
        return HarvestSignal(False, None, None, None)

    p_doys = doys[post]
    p_vals = vals[post]
    trough_idx = int(np.argmin(p_vals))
    trough = float(p_vals[trough_idx])
    drop_abs = peak_ndvi - trough
    drop_ratio = drop_abs / amplitude

    has = drop_abs >= min_drop and drop_ratio >= min_drop_ratio
    harvest_doy = int(p_doys[trough_idx]) if has else None
    return HarvestSignal(has, harvest_doy, float(drop_abs), float(drop_ratio))


def _confidence(peak_doy: int, peak_threshold: float,
                early_spring: Optional[float], es_threshold: float,
                sos_doy: Optional[int], season_class: str) -> float:
    """Уверенность в [0.5..0.99].

    База — удалённость дня пика от порога (основной признак). Затем
    корректируется вторичными сигналами: ранневесенним NDVI и SOS —
    согласие повышает, противоречие снижает.
    """
    d = abs(peak_doy - peak_threshold) / PEAK_CONFIDENCE_SCALE_DOY
    conf = 0.5 + 0.4 * (1.0 - np.exp(-d))  # 0.5 → 0.9 по мере удаления

    # Вторичный сигнал: ранневесенний NDVI (озимые зелены в апреле).
    if early_spring is not None:
        es_winter = early_spring >= es_threshold
        agree = es_winter == (season_class == 'winter')
        conf += 0.08 if agree else -0.15
    else:
        conf -= 0.05  # нет ранневесенней корроборации

    # Вторичный сигнал SOS: ранний ⇒ озимые, поздний ⇒ яровые.
    if sos_doy is not None:
        if season_class == 'winter':
            if sos_doy <= WINTER_SOS_MAX_DOY:
                conf += 0.05
            elif sos_doy >= SPRING_SOS_MIN_DOY:
                conf -= 0.10
        else:  # spring
            if sos_doy >= SPRING_SOS_MIN_DOY:
                conf += 0.05
            elif sos_doy <= WINTER_SOS_MAX_DOY:
                conf -= 0.10
    return float(np.clip(conf, 0.5, 0.99))


def _unused_confidence(drop_ratio: Optional[float],
                       min_drop_ratio: float) -> float:
    """Уверенность класса ``unused``: чем меньше спад, тем увереннее.

    ``drop_ratio=None`` (нет наблюдений после пика) — умеренная уверенность.
    """
    if drop_ratio is None:
        return 0.6
    conf = 0.5 + (min_drop_ratio - drop_ratio)
    return float(np.clip(conf, 0.5, 0.95))


def classify_profile(
    doys: Sequence[int], ndvi: Sequence[float],
    peak_doy_threshold: float = PEAK_DOY_THRESHOLD_DEFAULT,
    early_spring_threshold: float = DEFAULT_EARLY_SPRING_THRESHOLD,
    require_harvest: bool = False,
    harvest_min_drop: float = HARVEST_MIN_DROP,
    harvest_min_drop_ratio: float = HARVEST_MIN_DROP_RATIO,
    require_cover: bool = False,
    cover_min: float = GREEN_FRACTION_MIN,
) -> SeasonProfile:
    """Классифицировать один сезонный NDVI-ряд угодья.

    ОСНОВНОЙ признак — день пика NDVI: пик раньше ``peak_doy_threshold``
    ⇒ озимые (ранний максимум конца мая–июня), позже ⇒ яровые (июль–август).
    Ранневесенний NDVI (``early_spring_threshold``) — ВТОРИЧНЫЙ сигнал,
    влияет только на уверенность.

    **Гейт уборки** (``require_harvest``, по умолчанию ВЫКЛ): перед бинарной
    классификацией проверяем, что после пика был уборочный спад NDVI
    (:func:`detect_harvest`). Если спада нет — угодье не обрабатывается
    (залежь, многолетние травы), класс ``unused`` вместо winter/spring.

    ВНИМАНИЕ: гейт корректен ТОЛЬКО для ЗАВЕРШЁННОГО сезона. В середине
    сезона уборка ещё не произошла (или не попала в ряд) — включать нельзя,
    иначе почти всё уйдёт в ``unused``. Поэтому по умолчанию выключен.

    **Гейт покрова** (``require_cover``, по умолчанию ВЫКЛ): если доля
    «зелёных» наблюдений (NDVI ≥ ``GREEN_NDVI_THRESHOLD``) за ряд ниже
    ``cover_min`` — угодье большую часть сезона не покрыто растительностью
    (залежь, неудобья, разреженный покров) ⇒ класс ``unused``. В отличие от
    гейта уборки этот признак НЕ требует состоявшейся уборки, поэтому
    корректен и в середине сезона. Проверяется ПЕРЕД гейтом уборки.

    Args:
        doys: дни года наблюдений (1..366).
        ndvi: значения NDVI (сырые или сглаженные) в том же порядке.
        peak_doy_threshold: порог дня пика (winter, если пик < порога).
        early_spring_threshold: порог ранневесеннего NDVI (корроборация).
        require_harvest: включить гейт уборки (отсев необрабатываемых).
        harvest_min_drop: абсолютный порог уборочного спада NDVI.
        harvest_min_drop_ratio: относительный порог спада (к амплитуде).

    Возвращает :class:`SeasonProfile`. ``season_class='unknown'`` — данных
    недостаточно (мало точек, нет вег. цикла, слабая амплитуда); ``unused``
    — вег. цикл есть, но нет уборочного спада (не обрабатывается).
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

    green_fraction = float(np.mean(smoothed >= GREEN_NDVI_THRESHOLD))

    winter_baseline = _window_mean(doys, smoothed, 1, WINTER_DOY_END)
    if winter_baseline is None:
        winter_baseline = float(np.min(smoothed))

    peak_doy, peak_ndvi, sos_doy = _detect_peak_sos(doys, smoothed, winter_baseline)

    early_spring = _window_mean(
        doys, smoothed, EARLY_SPRING_DOY_START, EARLY_SPRING_DOY_END,
    )

    # Без вег. цикла или со слабой амплитудой — класс не определён
    # (голая почва, вода, застройка, шум).
    if peak_doy is None:
        return SeasonProfile(
            'unknown', 0.0, early_spring, winter_baseline,
            sos_doy, peak_doy, peak_ndvi, n_obs, green_fraction=green_fraction,
        )
    if peak_ndvi is not None and (peak_ndvi - winter_baseline) < MIN_AMPLITUDE:
        return SeasonProfile(
            'unknown', 0.0, early_spring, winter_baseline,
            sos_doy, peak_doy, peak_ndvi, n_obs, green_fraction=green_fraction,
        )

    # Гейт покрова: мало «зелёных» наблюдений за сезон → большую часть года
    # поле без растительности (залежь, неудобья) → не обрабатывается. Работает
    # и в середине сезона (не требует состоявшейся уборки).
    if require_cover and green_fraction < cover_min:
        return SeasonProfile(
            'unused', _unused_confidence(green_fraction, cover_min),
            early_spring, winter_baseline, sos_doy, peak_doy, peak_ndvi, n_obs,
            green_fraction=green_fraction,
        )

    # Гейт уборки: вег. цикл есть, но без уборочного спада → не обрабатывается.
    harvest = detect_harvest(
        doys, smoothed, peak_doy, peak_ndvi, winter_baseline,
        min_drop=harvest_min_drop, min_drop_ratio=harvest_min_drop_ratio,
    )
    if require_harvest and not harvest.has_harvest:
        return SeasonProfile(
            'unused', _unused_confidence(harvest.drop_ratio, harvest_min_drop_ratio),
            early_spring, winter_baseline, sos_doy, peak_doy, peak_ndvi, n_obs,
            harvest_doy=None, harvest_drop=harvest.drop_ratio,
            green_fraction=green_fraction,
        )

    season_class = 'winter' if peak_doy < peak_doy_threshold else 'spring'
    confidence = _confidence(
        peak_doy, peak_doy_threshold, early_spring, early_spring_threshold,
        sos_doy, season_class,
    )
    return SeasonProfile(
        season_class, confidence, early_spring, winter_baseline,
        sos_doy, peak_doy, peak_ndvi, n_obs,
        harvest_doy=harvest.harvest_doy, harvest_drop=harvest.drop_ratio,
        green_fraction=green_fraction,
    )


def profile_features(doys: Sequence[int], ndvi: Sequence[float]) -> dict:
    """Кандидатные признаки для отделения «не обрабатывается» от культур.

    Диагностическая функция (не влияет на классификацию): считает набор
    сезон-агностичных признаков, разделяющих залежь/многолетние травы от
    убираемых культур. В отличие от гейта уборки НЕ требует состоявшейся
    уборки, поэтому применима и в середине сезона.

    Гипотезы разделения (проверяются на эталонах командой
    ``--reference-features``):

    * ``season_min`` — минимум NDVI за год. У культур есть период голой
      почвы (низкий минимум); залежь/трава держат высокий «пол».
    * ``amplitude`` — размах (пик − минимум). У травы ниже (пологий профиль).
    * ``green_fraction`` — доля «зелёных» наблюдений (NDVI ≥ порога). У
      постоянной растительности высокая.
    * ``early_spring`` / ``summer`` / ``autumn`` — средний NDVI в окнах;
      трава остаётся зелёной поздним летом и осенью.

    Возвращает dict с признаками (значения могут быть ``None`` при нехватке
    наблюдений в окне) и ``n_obs``.
    """
    doys = np.asarray(doys, dtype=np.int32)
    ndvi = np.asarray(ndvi, dtype=np.float64)
    n_obs = int(len(doys))
    feats = {
        'n_obs': n_obs, 'season_min': None, 'peak_ndvi': None,
        'amplitude': None, 'green_fraction': None, 'early_spring': None,
        'summer': None, 'autumn': None, 'winter_baseline': None,
    }
    if n_obs == 0:
        return feats

    order = np.argsort(doys)
    doys = doys[order]
    ndvi = ndvi[order]
    smoothed = _smooth(ndvi)

    season = (doys >= SEASON_DOY_START) & (doys <= SEASON_DOY_END)
    s_vals = smoothed[season] if season.any() else smoothed
    peak = float(np.max(s_vals))
    floor = float(np.min(smoothed))
    feats['season_min'] = floor
    feats['peak_ndvi'] = peak
    feats['amplitude'] = peak - floor
    feats['green_fraction'] = float(np.mean(smoothed >= GREEN_NDVI_THRESHOLD))
    feats['early_spring'] = _window_mean(
        doys, smoothed, EARLY_SPRING_DOY_START, EARLY_SPRING_DOY_END,
    )
    feats['summer'] = _window_mean(doys, smoothed, SUMMER_DOY_START,
                                   SUMMER_DOY_END)
    feats['autumn'] = _window_mean(doys, smoothed, AUTUMN_DOY_START,
                                   AUTUMN_DOY_END)
    feats['winter_baseline'] = _window_mean(doys, smoothed, 1, WINTER_DOY_END)
    return feats


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


def calibrate_peak_doy_threshold(
    winter_peaks: Sequence[float],
    spring_peaks: Sequence[float],
    default: float = PEAK_DOY_THRESHOLD_DEFAULT,
) -> float:
    """Калибровка порога ДНЯ ПИКА по эталонам озимых и яровых.

    Озимые должны оказаться НИЖЕ порога (ранний пик), яровые — ВЫШЕ
    (поздний пик). При наличии обоих классов перебираем кандидатов и
    максимизируем сбалансированную точность. Деградации:
    - только озимые → 90-й перцентиль их пиков (порог выше почти всех);
    - только яровые → 10-й перцентиль их пиков (порог ниже почти всех);
    - нет данных → ``default``.
    Итог зажимается в [``MIN_PEAK_DOY_THRESHOLD``, ``MAX_PEAK_DOY_THRESHOLD``].
    """
    w = [v for v in winter_peaks if v is not None]
    s = [v for v in spring_peaks if v is not None]
    if not w and not s:
        return default
    lo, hi = MIN_PEAK_DOY_THRESHOLD, MAX_PEAK_DOY_THRESHOLD
    if w and not s:
        return float(np.clip(np.percentile(w, 90), lo, hi))
    if s and not w:
        return float(np.clip(np.percentile(s, 10), lo, hi))

    w_arr = np.asarray(w, dtype=np.float64)
    s_arr = np.asarray(s, dtype=np.float64)
    uniq = np.unique(np.concatenate([w_arr, s_arr]))
    candidates = list(uniq)
    candidates += [(uniq[i] + uniq[i + 1]) / 2 for i in range(len(uniq) - 1)]

    best_thr, best_score = default, -1.0
    for thr in candidates:
        tpr = float(np.mean(w_arr < thr))    # доля озимых верно (ранний пик)
        tnr = float(np.mean(s_arr >= thr))   # доля яровых верно (поздний пик)
        score = (tpr + tnr) / 2
        if score > best_score:
            best_score, best_thr = score, float(thr)
    return float(np.clip(best_thr, lo, hi))


def calibrate_cover_threshold(
    crop_green_fractions: Sequence[float],
    unused_green_fractions: Sequence[float],
    default: float = GREEN_FRACTION_MIN,
) -> float:
    """Калибровка порога ДОЛИ ЗЕЛЁНЫХ по эталонам культур и «не обраб.».

    Культуры (озимые+яровые) должны оказаться ВЫШЕ порога (много зелени за
    сезон), «не обрабатываемые» — НИЖЕ. При наличии обоих классов перебираем
    кандидатов и максимизируем сбалансированную точность. Деградации:
    - только культуры → 10-й перцентиль их долей (порог ниже почти всех);
    - только «не обраб.» → 90-й перцентиль их долей (порог выше почти всех);
    - нет данных → ``default``.
    Итог зажимается в [``MIN_COVER_THRESHOLD``, ``MAX_COVER_THRESHOLD``].
    """
    c = [v for v in crop_green_fractions if v is not None]
    u = [v for v in unused_green_fractions if v is not None]
    if not c and not u:
        return default
    lo, hi = MIN_COVER_THRESHOLD, MAX_COVER_THRESHOLD
    if c and not u:
        return float(np.clip(np.percentile(c, 10), lo, hi))
    if u and not c:
        return float(np.clip(np.percentile(u, 90), lo, hi))

    c_arr = np.asarray(c, dtype=np.float64)
    u_arr = np.asarray(u, dtype=np.float64)
    uniq = np.unique(np.concatenate([c_arr, u_arr]))
    candidates = list(uniq)
    candidates += [(uniq[i] + uniq[i + 1]) / 2 for i in range(len(uniq) - 1)]

    best_thr, best_score = default, -1.0
    for thr in candidates:
        tpr = float(np.mean(c_arr >= thr))   # доля культур верно (много зелени)
        tnr = float(np.mean(u_arr < thr))    # доля «не обраб.» верно (мало)
        score = (tpr + tnr) / 2
        if score > best_score:
            best_score, best_thr = score, float(thr)
    return float(np.clip(best_thr, lo, hi))


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
