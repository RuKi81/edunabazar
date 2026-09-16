"""Сборка обучающей выборки для модели класса угодья.

Мост между БД и чистой матчастью ``services/season_model.py``: метки
разметчика (:class:`FarmlandTrainingLabel`) плюс ряды NDVI превращаются в
``rows`` (словари признаков), ``y`` (0/1) и ``groups`` (ключ валидации).

Две ступени собираются из ОДНОЙ загрузки рядов:

* ``cover`` — ``crop`` (озимые+яровые) против ``unused`` (залежь и её
  подклассы). ``hayfield`` и ``ignore`` в обучение не идут: сенокос
  определяется по land-use, сады исключены разметчиком.
* ``season`` — ``winter`` против ``spring``, только по пашне.

ИСТОЧНИК СНИМКОВ ЕДИНЫЙ для текущего и прошлого года (S2/L8/L9 либо
HLS): модель должна обучаться и применяться на одной и той же
физической величине.

ГРУППЫ ДЛЯ ВАЛИДАЦИИ — это не техническая деталь, а суть честной оценки.
Контуры одного массива обрабатываются одной техникой в один день и по
NDVI почти неразличимы. Если они попадут и в train, и в test, модель
получит ответы заранее. Поэтому группа — район; а если вся разметка
собрана в одном-двух районах, группой становится ячейка географической
сетки, чтобы соседние поля всё равно оставались в одном фолде.
"""
from __future__ import annotations

from typing import Optional, Sequence

from django.contrib.gis.db.models.functions import Centroid
from django.db import connection

from agrocosmos.models import Farmland, FarmlandTrainingLabel
from agrocosmos.services.season_model import (
    FEATURE_SET_COVER, FEATURE_SET_SEASON, MIN_GROUPS_FOR_CV,
    SEASON_FEATURE_NAMES, cover_feature_names, features_cover,
    features_season,
)
from agrocosmos.services.winter_spring import (
    GREEN_FRACTION_MAX, classify_profile,
)

STAGE_COVER = 'cover'
STAGE_SEASON = 'season'

RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')
FUSED_SATELLITES = ('hls_fused',)

# Размер ячейки резервной сетки группировки в градусах (~11 км по
# широте). Поля одного хозяйства почти всегда попадают в одну ячейку.
GROUP_CELL_DEG = 0.1

# Метки, участвующие в обучении (подклассы зарастания сворачиваются
# в ``unused`` через ``FarmlandTrainingLabel.family``).
TRAINING_CLASSES = ('winter', 'spring') + FarmlandTrainingLabel.UNUSED_CLASSES


def load_labels(year: int, region_id: Optional[int] = None,
                district_id: Optional[int] = None) -> dict[int, str]:
    """``{farmland_id: базовый класс}`` по разметке scope за год."""
    qs = FarmlandTrainingLabel.objects.filter(
        year=year, true_class__in=TRAINING_CLASSES,
    )
    if district_id is not None:
        qs = qs.filter(farmland__district_id=district_id)
    elif region_id is not None:
        qs = qs.filter(farmland__district__region_id=region_id)
    return {fid: FarmlandTrainingLabel.family(cls)
            for fid, cls in qs.values_list('farmland_id', 'true_class')}


def load_ndvi_series(farmland_ids: Sequence[int], year: int,
                     satellites: Sequence[str]) -> dict[int, tuple]:
    """``{farmland_id: (doys, ndvi)}`` за год по наблюдениям-невыбросам.

    Сырой SQL вместо ORM: выборка сужена до размеченных угодий, поэтому
    это одна короткая агрегация вместо тысяч объектов ``VegetationIndex``.
    """
    if not farmland_ids:
        return {}
    placeholders = ', '.join(['%s'] * len(satellites))
    sql = f"""
        SELECT vi.farmland_id, vi.acquired_date, vi.mean
        FROM agro_vegetation_index vi
        JOIN agro_satellite_scene sc ON sc.id = vi.scene_id
        WHERE vi.index_type = 'ndvi'
          AND vi.is_outlier = false
          AND vi.mean >= -0.2 AND vi.mean <= 1
          AND EXTRACT(year FROM vi.acquired_date) = %s
          AND sc.satellite IN ({placeholders})
          AND vi.farmland_id = ANY(%s)
        ORDER BY vi.farmland_id, vi.acquired_date
    """
    params = [year, *satellites, list(farmland_ids)]
    with connection.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    out: dict[int, tuple[list[int], list[float]]] = {}
    for fl_id, acquired, mean in rows:
        doys, vals = out.setdefault(fl_id, ([], []))
        doys.append(acquired.timetuple().tm_yday)
        vals.append(float(mean))
    return out


def load_prev_series(farmland_ids: Sequence[int], year: int,
                     satellites: Sequence[str]) -> tuple[dict, str]:
    """Ряды прошлого года и источник: ``(series, 'детальный'|'нет')``.

    Озимые сеют в августе-сентябре, поэтому прошлогодняя осень — самый
    прямой признак класса.

    Источник снимков ТОТ ЖЕ, что и для текущего года. Подмены на архив
    низкого разрешения (MODIS, 250 м) здесь СОЗНАТЕЛЬНО нет: на контуре
    в несколько десятков га такой NDVI — смесь с соседними угодьями, то
    есть физически другая величина. Обучение на одном источнике и
    применение на другом дало бы скрытый сдвиг признака. Нет
    прошлогодних снимков — признак честно помечается пропуском
    (``autumn_prev_missing``), и модель решает по остальным.
    """
    if not farmland_ids:
        return {}, 'нет'
    series = load_ndvi_series(farmland_ids, year - 1, satellites)
    return series, ('детальный' if series else 'нет')


def load_groups(farmland_ids: Sequence[int],
                cell_deg: float = GROUP_CELL_DEG) -> tuple[dict, str]:
    """``({farmland_id: группа}, схема)`` для валидации без утечки.

    Схема ``'district'`` — если размеченные угодья охватывают хотя бы
    :data:`MIN_GROUPS_FOR_CV` районов. Иначе ``'grid'``: группой
    становится ячейка сетки ``cell_deg`` по центроиду — фолды всё равно
    разделяют соседние поля, а число групп достаточно для
    leave-one-group-out.
    """
    if not farmland_ids:
        return {}, 'district'
    rows = (Farmland.objects
            .filter(pk__in=list(farmland_ids))
            .annotate(center=Centroid('geom'))
            .values_list('pk', 'district_id', 'center'))

    by_district, by_grid = {}, {}
    for pk, district_id, center in rows:
        by_district[pk] = f'd{district_id}'
        if center is None:
            by_grid[pk] = 'nogeom'
            continue
        by_grid[pk] = (f'g{int(center.x / cell_deg)}'
                       f'_{int(center.y / cell_deg)}')

    if len(set(by_district.values())) >= MIN_GROUPS_FOR_CV:
        return by_district, 'district'
    return by_grid, 'grid'


def build_stage(
    stage: str,
    labels: dict[int, str],
    series: dict[int, tuple],
    groups: dict[int, str],
    prev_series: Optional[dict] = None,
) -> dict:
    """Выборка одной ступени: ``rows``/``y``/``groups``/``names``/статистика.

    Угодья без ряда NDVI или со слишком коротким рядом отбрасываются —
    их число возвращается в ``dropped``, чтобы отчёт показывал, сколько
    разметки не дошло до обучения (обычно это мелкие контуры под
    постоянной облачностью).
    """
    prev_series = prev_series or {}
    if stage == STAGE_COVER:
        positive, negative = ('winter', 'spring'), ('unused',)
        names, feature_set = cover_feature_names(), FEATURE_SET_COVER
    elif stage == STAGE_SEASON:
        positive, negative = ('winter',), ('spring',)
        names, feature_set = SEASON_FEATURE_NAMES, FEATURE_SET_SEASON
    else:
        raise ValueError(f'stage must be {STAGE_COVER}|{STAGE_SEASON}, '
                         f'got {stage!r}')

    rows, y, group_list, ids = [], [], [], []
    dropped = 0
    for fid, cls in labels.items():
        if cls not in positive and cls not in negative:
            continue
        data = series.get(fid)
        if not data:
            dropped += 1
            continue
        if stage == STAGE_COVER:
            feats = features_cover(data[0], data[1])
        else:
            prev = prev_series.get(fid)
            feats = features_season(data[0], data[1],
                                    prev[0] if prev else None,
                                    prev[1] if prev else None)
        if feats is None:
            dropped += 1
            continue
        rows.append(feats)
        y.append(1.0 if cls in positive else 0.0)
        group_list.append(groups.get(fid, 'unknown'))
        ids.append(fid)

    return {
        'stage': stage,
        'feature_set': feature_set,
        'names': list(names),
        'rows': rows,
        'y': y,
        'groups': group_list,
        'farmland_ids': ids,
        'positive_label': 'crop' if stage == STAGE_COVER else 'winter',
        'negative_label': 'unused' if stage == STAGE_COVER else 'spring',
        'n_pos': int(sum(y)),
        'n_neg': int(len(y) - sum(y)),
        'n_groups': len(set(group_list)),
        'dropped': dropped,
    }


def rule_based_baseline(stage: str, series: dict,
                        farmland_ids: Sequence[int]) -> list[float]:
    """Предсказания ПОРОГОВОГО классификатора (0/1) для тех же угодий.

    Нужны, чтобы у модели был честный конкурент: выигрыш измеряется не
    относительно монетки, а относительно текущего прода. Для ступени
    ``cover`` пороговый классификатор запускается с обоими гейтами
    покрова и уборки (так он работает в ``classify_winter_spring``), для
    ``season`` — по одному порогу дня пика, без гейтов.
    """
    out = []
    for fid in farmland_ids:
        doys, ndvi = series[fid][0], series[fid][1]
        if stage == STAGE_COVER:
            prof = classify_profile(
                doys, ndvi, require_cover=True, require_harvest=True,
                cover_max=GREEN_FRACTION_MAX,
            )
            out.append(1.0 if prof.season_class in ('winter', 'spring')
                       else 0.0)
        else:
            prof = classify_profile(doys, ndvi)
            out.append(1.0 if prof.season_class == 'winter' else 0.0)
    return out


def build_dataset(
    year: int,
    region_id: Optional[int] = None,
    district_id: Optional[int] = None,
    source: str = 'raster',
    skip_prev_autumn: bool = False,
) -> dict:
    """Обе ступени плюс сводка загрузки: единый вход для команды обучения."""
    labels = load_labels(year, region_id, district_id)
    satellites = (FUSED_SATELLITES if source == 'fused'
                  else RASTER_SATELLITES)
    series = load_ndvi_series(list(labels), year, satellites)
    groups, group_scheme = load_groups(list(labels))
    if skip_prev_autumn:
        prev_series, prev_source = {}, 'выключен'
    else:
        prev_series, prev_source = load_prev_series(
            list(labels), year, satellites)

    return {
        'year': year,
        'source': source,
        'labels': labels,
        'n_labels': len(labels),
        'n_with_series': len(series),
        'group_scheme': group_scheme,
        'prev_source': prev_source,
        'n_prev_series': len(prev_series),
        'series': series,
        STAGE_COVER: build_stage(STAGE_COVER, labels, series, groups),
        STAGE_SEASON: build_stage(STAGE_SEASON, labels, series, groups,
                                  prev_series),
    }
