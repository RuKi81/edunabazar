"""Подбор порогов классификатора озимые / яровые / не обрабатывается.

Диагностическая команда БЕЗ записи в БД: считает признаки по ручным меткам
(:class:`FarmlandTrainingLabel`, карта-разметчик ``label/``) и печатает
качество КАЖДОГО значения порога на сетке — а не одно «лучшее» число, как
``calibrate_*`` внутри ``classify_winter_spring``. Видно форму кривой:
где плато (порог можно двигать безопасно), где обрыв (цена ошибки высока)
и насколько текущий дефолт далёк от оптимума.

Проверяются три порога:

* **день пика NDVI** — основной дискриминатор озимые/яровые;
* **доля зелёных наблюдений** — гейт покрова (отсев необрабатываемых);
* **глубина уборочного спада** — гейт уборки (доля к амплитуде сезона).

Плюс распределение дня пика по ВСЕМ угодьям региона (не только эталонам):
бимодальность с провалом означает, что признак вообще разделяет классы, и
порог надо ставить в провал. Один горб — порогом задача не решается.

Примеры:
    # Весь регион, метки за 2026 год
    python manage.py tune_season_thresholds --region-id 71 --year 2026

    # Мельче сетка по дню пика, без гистограммы по всем угодьям
    python manage.py tune_season_thresholds --region-id 71 --year 2026 \
        --peak-step 2 --skip-area
"""
import time

import numpy as np
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from agrocosmos.models import District, FarmlandTrainingLabel, Region
from agrocosmos.services.winter_spring import (
    GREEN_FRACTION_MIN, HARVEST_MIN_DROP_RATIO, MAX_COVER_THRESHOLD,
    MAX_PEAK_DOY_THRESHOLD, MIN_COVER_THRESHOLD, MIN_PEAK_DOY_THRESHOLD,
    PEAK_DOY_THRESHOLD_DEFAULT, best_split, classify_profile,
    profile_features, sweep_threshold,
)

RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')
FUSED_SATELLITES = ('hls_fused',)
# Прошлогодняя осень нужна для признака «всходы озимых»: если S2/L8
# за предыдущий год нет, пробуем архив MODIS (он есть с 2000-х).
MODIS_SATELLITES = ('modis_terra', 'modis_aqua')

# Кандидаты в дискриминаторы: ключ фичи → подпись и формат.
# Порядок только для читаемости — в отчёте признаки сортируются по
# разделяющей силе. ``autumn_prev`` — главный физический признак
# озимых: осенью прошлого года они всходят и зеленеют, а под яровые
# поле стоит под стернёй или вспашкой.
FEATURES = (
    ('autumn_prev', 'NDVI сен-ноя ПРОШЛ. года', '{:.3f}'),
    ('early_spring', 'NDVI апр-май', '{:.3f}'),
    ('peak_doy', 'день пика', '{:.0f}'),
    ('sos_doy', 'SOS (день)', '{:.0f}'),
    ('summer', 'NDVI июл-авг', '{:.3f}'),
    ('autumn', 'NDVI сен-ноя', '{:.3f}'),
    ('drop_ratio', 'спад после пика', '{:.2f}'),
    ('green_fraction', 'доля зелени', '{:.2f}'),
    ('amplitude', 'амплитуда', '{:.3f}'),
    ('peak_ndvi', 'пик NDVI', '{:.3f}'),
    ('season_min', 'минимум за год', '{:.3f}'),
    ('winter_baseline', 'зимний baseline', '{:.3f}'),
)

# Ниже этого баланса признак практически бесполезен (0.5 = монетка).
USELESS_BALANCED = 0.60
# Доля эталонов с прошлогодним рядом, ниже которой пробуем MODIS.
PREV_COVERAGE_MIN = 0.5

# Классы меток, участвующие в подборе порогов. ``hayfield``/``ignore``
# не участвуют: сенокос получает класс по land-use, сады исключены.
SWEEP_CLASSES = ('winter', 'spring', 'unused')

# Ниже этого числа эталонов в классе оценка порога недостоверна
# (доверительный интервал шире, чем разница между кандидатами).
MIN_LABELS_PER_CLASS = 30

# Сетки порогов гейтов (для дня пика шаг задаётся аргументом).
COVER_GRID_STEP = 0.02
HARVEST_RATIO_GRID = (0.20, 0.80, 0.05)

HIST_BIN_DAYS = 10
BAR_WIDTH = 32


def _grid(lo, hi, step):
    """Сетка порогов [lo..hi] включительно (без выхода за hi из-за float)."""
    return np.arange(lo, hi + step / 2, step)


class Command(BaseCommand):
    help = ('Подбор порогов озимые/яровые/не обрабатывается по ручным '
            'меткам: качество на сетке значений, без записи в БД.')

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int)
        parser.add_argument('--district-id', type=int)
        parser.add_argument('--year', type=int, required=True)
        parser.add_argument('--source', choices=['raster', 'fused'],
                            default='raster',
                            help='raster = S2/L8 (по умолчанию), fused = HLS')
        parser.add_argument('--peak-step', type=int, default=5,
                            help='Шаг сетки по дню пика в днях (по умолч. 5).')
        parser.add_argument('--skip-area', action='store_true',
                            help='Не считать распределение дня пика по всем '
                                 'угодьям региона (только эталоны).')
        parser.add_argument('--skip-prev-autumn', action='store_true',
                            help='Не грузить ряды предыдущего года (признак '
                                 '«всходы озимых осенью» будет пропущен).')

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        region, district = self._resolve_scope(options)
        year = options['year']
        source = options['source']

        labels = self._load_labels(region, district, year)
        if not labels:
            raise CommandError(
                f'Нет меток обучающей выборки за {year} год в этом scope. '
                'Разметьте угодья на странице label/ или укажите другой год.'
            )

        satellites = FUSED_SATELLITES if source == 'fused' else RASTER_SATELLITES
        t0 = time.time()
        series = self._load_series(list(labels), year, satellites)
        self.stdout.write(
            f'Ряды NDVI ({source}): {len(series)} из {len(labels)} эталонов '
            f'за {time.time() - t0:.1f}s'
        )

        prev_series = ({} if options['skip_prev_autumn']
                       else self._load_prev_series(list(labels), year,
                                                   satellites))

        feats = self._features(labels, series, prev_series)
        self._report_labels(labels, feats)
        self._sweep_peak(feats, options['peak_step'])
        self._sweep_cover(feats)
        self._sweep_harvest(feats)
        self._rank_features(
            feats, 'ОЗИМЫЕ vs ЯРОВЫЕ', ('winter',), ('spring',),
            'озимые', 'яровые',
        )
        self._rank_features(
            feats, 'КУЛЬТУРЫ vs НЕ ОБРАБАТЫВАЕТСЯ',
            ('winter', 'spring'), ('unused',), 'культуры', 'не обраб.',
        )
        if not options['skip_area']:
            self._report_peak_histogram(region, district, year, source)

    # ----------------------------------------------------------------- scope

    def _resolve_scope(self, options):
        district_id = options.get('district_id')
        region_id = options.get('region_id')
        if district_id:
            try:
                district = District.objects.select_related('region').get(
                    pk=district_id)
            except District.DoesNotExist:
                raise CommandError(f'District {district_id} not found')
            self.stdout.write(
                f'Scope: район {district.name} ({district.region.name})'
            )
            return district.region, district
        if region_id:
            try:
                region = Region.objects.get(pk=region_id)
            except Region.DoesNotExist:
                raise CommandError(f'Region {region_id} not found')
            self.stdout.write(f'Scope: регион {region.name}')
            return region, None
        raise CommandError('Укажите --region-id или --district-id')

    # ------------------------------------------------------------------ data

    @staticmethod
    def _load_labels(region, district, year) -> dict:
        """{farmland_id: true_class} по меткам scope за год."""
        qs = FarmlandTrainingLabel.objects.filter(
            year=year, true_class__in=SWEEP_CLASSES,
        )
        if district is not None:
            qs = qs.filter(farmland__district_id=district.pk)
        else:
            qs = qs.filter(farmland__district__region_id=region.pk)
        return dict(qs.values_list('farmland_id', 'true_class'))

    @staticmethod
    def _load_series(farmland_ids, year, satellites) -> dict:
        """{farmland_id: (doys, ndvi)} за год по не-выбросам.

        Выборка сужена до эталонных угодий, поэтому дешёвая (сотни строк
        на угодье), в отличие от полной загрузки региона в
        ``classify_winter_spring``.
        """
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

    def _load_prev_series(self, farmland_ids, year, satellites) -> dict:
        """Ряды ПРЕДЫДУЩЕГО года — для признака «всходы озимых осенью».

        Озимые сеют в августе-сентябре, и до зимы поле зеленеет — это
        прямой признак класса, в отличие от косвенного дня пика. Если
        детальный мониторинг за прошлый год покрывает меньше половины
        эталонов, переходим на архив MODIS.
        """
        prev = year - 1
        series = self._load_series(farmland_ids, prev, satellites)
        used = 'детальный'
        if len(series) < PREV_COVERAGE_MIN * len(farmland_ids):
            modis = self._load_series(farmland_ids, prev, MODIS_SATELLITES)
            if len(modis) > len(series):
                series, used = modis, 'MODIS'
        self.stdout.write(
            f'Ряды NDVI за {prev} ({used}): {len(series)} из '
            f'{len(farmland_ids)} эталонов'
            + ('' if series else ' — признак осени недоступен')
        )
        return series

    @staticmethod
    def _features(labels, series, prev_series=None) -> list[dict]:
        """Признаки эталонов: [{'cls', <ключи FEATURES>, 'n_obs'}].

        Признаки НЕ зависят от порогов, поэтому считаются один раз с
        дефолтными параметрами, а пороги перебираются уже по ним.
        ``autumn_prev`` берётся из ряда предыдущего года и может быть
        ``None`` — такие значения отбрасываются при ранжировании.
        """
        prev_series = prev_series or {}
        out = []
        for fid, cls in labels.items():
            data = series.get(fid)
            if not data:
                continue
            prof = classify_profile(data[0], data[1])
            feats = profile_features(data[0], data[1])
            prev = prev_series.get(fid)
            row = {
                'cls': cls,
                'n_obs': prof.n_obs,
                'peak_doy': prof.peak_doy,
                'sos_doy': prof.sos_doy,
                'green': prof.green_fraction,
                'drop_ratio': prof.harvest_drop,
                'autumn_prev': (
                    profile_features(prev[0], prev[1]).get('autumn')
                    if prev else None
                ),
            }
            for key in ('early_spring', 'summer', 'autumn', 'amplitude',
                        'peak_ndvi', 'season_min', 'winter_baseline',
                        'green_fraction'):
                row[key] = feats.get(key)
            out.append(row)
        return out

    # --------------------------------------------------------------- reports

    def _report_labels(self, labels, feats):
        counts = {c: 0 for c in SWEEP_CLASSES}
        for cls in labels.values():
            counts[cls] = counts.get(cls, 0) + 1
        with_series = {c: 0 for c in SWEEP_CLASSES}
        for f in feats:
            with_series[f['cls']] += 1

        self.stdout.write('\nЭталоны (метка → с рядом NDVI):')
        for cls in SWEEP_CLASSES:
            warn = ('  ← мало для достоверной оценки'
                    if with_series[cls] < MIN_LABELS_PER_CLASS else '')
            self.stdout.write(
                f'  {cls:<8} {counts[cls]:>4} → {with_series[cls]:>4}{warn}'
            )

    def _sweep_peak(self, feats, step):
        """Развёртка порога дня пика: озимые ниже порога, яровые выше."""
        winter = [f['peak_doy'] for f in feats if f['cls'] == 'winter']
        spring = [f['peak_doy'] for f in feats if f['cls'] == 'spring']
        grid = _grid(MIN_PEAK_DOY_THRESHOLD, MAX_PEAK_DOY_THRESHOLD, step)
        points = sweep_threshold(winter, spring, grid, pos_side='below')
        self._print_sweep(
            'ПОРОГ ДНЯ ПИКА (озимые < порога)', points,
            pos_name='озимые', neg_name='яровые',
            current=PEAK_DOY_THRESHOLD_DEFAULT, fmt='{:.0f}',
        )

    def _sweep_cover(self, feats):
        """Развёртка гейта покрова: культуры выше порога, «не обраб.» ниже."""
        crop = [f['green'] for f in feats if f['cls'] in ('winter', 'spring')]
        unused = [f['green'] for f in feats if f['cls'] == 'unused']
        grid = _grid(MIN_COVER_THRESHOLD, MAX_COVER_THRESHOLD, COVER_GRID_STEP)
        points = sweep_threshold(crop, unused, grid, pos_side='above')
        self._print_sweep(
            'ПОРОГ ДОЛИ ЗЕЛЁНЫХ (культуры ≥ порога, ниже → unused)', points,
            pos_name='культуры', neg_name='не обраб.',
            current=GREEN_FRACTION_MIN, fmt='{:.2f}',
        )

    def _sweep_harvest(self, feats):
        """Развёртка гейта уборки по доле спада к амплитуде сезона."""
        crop = [f['drop_ratio'] for f in feats
                if f['cls'] in ('winter', 'spring')]
        unused = [f['drop_ratio'] for f in feats if f['cls'] == 'unused']
        grid = _grid(*HARVEST_RATIO_GRID)
        points = sweep_threshold(crop, unused, grid, pos_side='above')
        self._print_sweep(
            'ПОРОГ УБОРОЧНОГО СПАДА (культуры ≥ порога, ниже → unused)',
            points, pos_name='культуры', neg_name='не обраб.',
            current=HARVEST_MIN_DROP_RATIO, fmt='{:.2f}',
        )

    def _print_sweep(self, title, points, pos_name, neg_name, current, fmt):
        """Таблица качества по сетке + отметки оптимума и текущего порога."""
        self.stdout.write(f'\n{title}')
        if not points or (points[0].n_pos == 0 or points[0].n_neg == 0):
            self.stdout.write(
                f'  Недостаточно эталонов ({pos_name}={points[0].n_pos if points else 0}, '
                f'{neg_name}={points[0].n_neg if points else 0}) — '
                'нужны оба класса, развёртка пропущена.'
            )
            return

        best = max(points, key=lambda p: p.balanced)
        nearest = min(points, key=lambda p: abs(p.threshold - current))
        self.stdout.write(
            f'  эталонов: {pos_name}={points[0].n_pos}, '
            f'{neg_name}={points[0].n_neg}'
        )
        self.stdout.write(
            f'  {"порог":>7}  {pos_name:>9}  {neg_name:>9}  '
            f'{"баланс":>7}  {"точн.":>6}'
        )
        for p in points:
            mark = ''
            if p is best:
                mark += ' ← лучший'
            if p is nearest:
                mark += ' ← текущий дефолт'
            bar = '#' * int(round(p.balanced * BAR_WIDTH))
            self.stdout.write(
                f'  {fmt.format(p.threshold):>7}  {p.pos_recall:>9.2f}  '
                f'{p.neg_recall:>9.2f}  {p.balanced:>7.3f}  '
                f'{p.accuracy:>6.2f}  {bar}{mark}'
            )
        delta = best.balanced - nearest.balanced
        self.stdout.write(
            f'  Оптимум {fmt.format(best.threshold)} '
            f'(баланс {best.balanced:.3f}); текущий '
            f'{fmt.format(nearest.threshold)} → {nearest.balanced:.3f}; '
            f'выигрыш {delta:+.3f}'
        )

    def _rank_features(self, feats, title, pos_cls, neg_cls,
                       pos_name, neg_name):
        """Ранжирование признаков по разделяющей силе на эталонах.

        Для каждого кандидата ищется лучший разрез с автовыбором
        стороны (:func:`best_split`) — направление разделения заранее
        неизвестно. Сортировка по сбалансированной точности: сразу
        видно, есть ли вообще признак, на котором стоит строить правило.
        """
        self.stdout.write(f'\nРАЗДЕЛЯЮЩАЯ СИЛА ПРИЗНАКОВ: {title}')
        self.stdout.write(
            f'  {"признак":<24}  {pos_name:>9}  {neg_name:>9}  '
            f'{"порог":>8}  {"баланс":>7}'
        )
        rows = []
        for key, label, fmt in FEATURES:
            src = 'green' if key == 'green_fraction' else key
            pos = [f.get(src) for f in feats if f['cls'] in pos_cls]
            neg = [f.get(src) for f in feats if f['cls'] in neg_cls]
            point, side = best_split(pos, neg)
            if point is None:
                continue
            rows.append((point, side, label, fmt,
                         self._median(pos), self._median(neg)))

        if not rows:
            self.stdout.write('  Нет эталонов обоих классов — пропущено.')
            return

        rows.sort(key=lambda r: r[0].balanced, reverse=True)
        for point, side, label, fmt, med_pos, med_neg in rows:
            sign = '<' if side == 'below' else '≥'
            bar = '#' * int(round(point.balanced * BAR_WIDTH))
            note = '  ← не разделяет' if point.balanced < USELESS_BALANCED else ''
            self.stdout.write(
                f'  {label:<24}  {self._fmt(med_pos, fmt):>9}  '
                f'{self._fmt(med_neg, fmt):>9}  '
                f'{sign}{self._fmt(point.threshold, fmt):>7}  '
                f'{point.balanced:>7.3f}  {bar}{note}'
            )
        best = rows[0][0]
        if best.balanced < USELESS_BALANCED:
            self.stdout.write(
                '  НИ ОДИН признак не разделяет эти классы лучше '
                f'{USELESS_BALANCED:.2f} — пороговым правилом задача не '
                'решается (либо шум в метках/рядах).'
            )
        self.stdout.write(
            '  Колонки классов — медианы; знак у порога показывает '
            f'условие для «{pos_name}».'
        )

    @staticmethod
    def _median(values):
        vals = [v for v in values if v is not None]
        return float(np.median(vals)) if vals else None

    @staticmethod
    def _fmt(value, fmt):
        return '—' if value is None else fmt.format(value)

    def _report_peak_histogram(self, region, district, year, source):
        """Распределение дня пика по ВСЕМ угодьям scope (бимодальность?).

        Считается по уже сохранённым ``FarmlandCropSeason.peak_doy`` — это
        обычный GROUP BY, без пересчёта пайплайна.
        """
        where = ['cs.year = %s', 'cs.source = %s', 'cs.peak_doy IS NOT NULL']
        params = [year, source]
        if district is not None:
            where.append('f.district_id = %s')
            params.append(district.pk)
        else:
            where.append('f.district_id IN '
                         '(SELECT id FROM agro_district WHERE region_id = %s)')
            params.append(region.pk)
        sql = f"""
            SELECT (cs.peak_doy / {HIST_BIN_DAYS}) * {HIST_BIN_DAYS} AS bin,
                   COUNT(*), COALESCE(SUM(f.area_ha), 0)
            FROM agro_farmland_crop_season cs
            JOIN agro_farmland f ON f.id = cs.farmland_id
            WHERE {' AND '.join(where)}
            GROUP BY bin
            ORDER BY bin
        """
        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        self.stdout.write(
            f'\nРАСПРЕДЕЛЕНИЕ ДНЯ ПИКА по всем угодьям '
            f'(год {year}, источник {source})'
        )
        if not rows:
            self.stdout.write(
                '  Нет строк FarmlandCropSeason — сначала прогоните '
                'classify_winter_spring для этого scope.'
            )
            return

        max_n = max(r[1] for r in rows)
        total = sum(r[1] for r in rows)
        for bin_start, n, area in rows:
            bar = '#' * max(1, int(round(n / max_n * BAR_WIDTH)))
            self.stdout.write(
                f'  {int(bin_start):>3}  {n:>7}  {float(area):>12.0f} га  {bar}'
            )
        self.stdout.write(f'  Всего угодий с пиком: {total}')

        trough = self._find_trough(rows)
        if trough is None:
            self.stdout.write(
                '  Одна мода (провала нет) — порогом по дню пика классы '
                'не разделяются, нужен другой признак.'
            )
        else:
            self.stdout.write(
                f'  Провал между модами: бин {trough} → порог стоит ставить '
                f'в район {trough + HIST_BIN_DAYS // 2}'
            )

    @staticmethod
    def _find_trough(rows):
        """Начало бина-минимума между двумя крупнейшими модами (или None).

        Ищем два самых населённых бина; если они не соседние — минимум
        между ними и есть точка разделения классов. Соседние моды означают
        один горб: провала нет, порог поставить некуда.
        """
        if len(rows) < 3:
            return None
        ordered = sorted(rows, key=lambda r: r[1], reverse=True)
        first, second = int(ordered[0][0]), int(ordered[1][0])
        lo, hi = min(first, second), max(first, second)
        if hi - lo <= HIST_BIN_DAYS:
            return None
        between = [r for r in rows if lo < int(r[0]) < hi]
        if not between:
            return None
        return int(min(between, key=lambda r: r[1])[0])
