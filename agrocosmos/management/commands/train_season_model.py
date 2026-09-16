"""Обучение модели класса угодья по ручной разметке (диагностика).

Альтернатива подбору порогов (``tune_season_thresholds``): вместо того
чтобы вручную искать разрез по одному признаку, обучается логистическая
регрессия на всём профиле NDVI, а качество измеряется на ОТЛОЖЕННЫХ
группах угодий (район или ячейка сетки — контуры одного массива почти
дублируют друг друга и не должны попадать в train и test одновременно).

Две ступени обучаются независимо:

* ``cover`` — культуры против «не обрабатывается» (разметки много,
  поэтому в вектор идёт весь профиль по полумесяцам);
* ``season`` — озимые против яровых (разметки мало, поэтому компактный
  вектор физически осмысленных признаков, главный — прошлогодняя осень).

В отчёте рядом с качеством модели печатается качество ТЕКУЩЕГО
порогового классификатора на тех же угодьях: без этой колонки нельзя
сказать, даёт ли модель выигрыш, или красивая цифра — просто следствие
дисбаланса классов.

Команда НИЧЕГО НЕ ПИШЕТ в БД: пока это инструмент решения, стоит ли
переводить прод на модель. Сохранение появится отдельным шагом.

Примеры:
    # Регион целиком, метки за 2026 год
    python manage.py train_season_model --region-id 71 --year 2026

    # Только ступень озимые/яровые, без прошлогодней осени
    python manage.py train_season_model --region-id 71 --year 2026 \
        --stage season --skip-prev-autumn
"""
from __future__ import annotations

import time

import numpy as np
from django.core.management.base import BaseCommand, CommandError

from agrocosmos.models import District, Region
from agrocosmos.services.season_dataset import (
    STAGE_COVER, STAGE_SEASON, build_dataset, rule_based_baseline,
)
from agrocosmos.services.season_model import (
    L2_GRID, MIN_GROUPS_FOR_CV, binary_metrics, feature_importance,
    train_model,
)

# Ниже этого числа примеров в классе обучение бессмысленно: модель
# запомнит отдельные угодья, а доверительный интервал качества будет
# шире, чем разница с пороговым классификатором.
MIN_PER_CLASS = 20

STAGE_TITLES = {
    STAGE_COVER: 'КУЛЬТУРЫ vs НЕ ОБРАБАТЫВАЕТСЯ',
    STAGE_SEASON: 'ОЗИМЫЕ vs ЯРОВЫЕ',
}


class Command(BaseCommand):
    help = ('Обучить модель класса угодья по ручной разметке и сравнить '
            'её с пороговым классификатором (без записи в БД).')

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int)
        parser.add_argument('--district-id', type=int)
        parser.add_argument('--year', type=int, required=True)
        parser.add_argument('--source', choices=['raster', 'fused'],
                            default='raster',
                            help='raster = S2/L8 (по умолчанию), fused = HLS')
        parser.add_argument('--stage', choices=[STAGE_COVER, STAGE_SEASON],
                            help='Обучить только одну ступень.')
        parser.add_argument('--skip-prev-autumn', action='store_true',
                            help='Не грузить ряды предыдущего года '
                                 '(признак «всходы озимых» будет пропущен).')
        parser.add_argument('--top-features', type=int, default=10,
                            help='Сколько весов печатать (по умолч. 10).')

    def handle(self, *args, **options):
        region, district = self._resolve_scope(options)
        year = options['year']

        t0 = time.time()
        data = build_dataset(
            year=year,
            region_id=region.pk if region else None,
            district_id=district.pk if district else None,
            source=options['source'],
            skip_prev_autumn=options['skip_prev_autumn'],
        )
        if not data['n_labels']:
            raise CommandError(
                f'Нет меток обучающей выборки за {year} год в этом scope. '
                'Разметьте угодья на странице label/ или укажите другой год.'
            )
        self._report_data(data, time.time() - t0)

        stages = [options['stage']] if options['stage'] else [STAGE_COVER,
                                                              STAGE_SEASON]
        for stage in stages:
            self._run_stage(data, stage, options['top_features'])

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
                f'Scope: район {district.name} ({district.region.name})')
            return district.region, district
        if region_id:
            try:
                region = Region.objects.get(pk=region_id)
            except Region.DoesNotExist:
                raise CommandError(f'Region {region_id} not found')
            self.stdout.write(f'Scope: регион {region.name}')
            return region, None
        raise CommandError('Укажите --region-id или --district-id')

    # --------------------------------------------------------------- reports

    def _report_data(self, data, seconds):
        scheme = ('районы' if data['group_scheme'] == 'district'
                  else f'ячейки сетки (районов < {MIN_GROUPS_FOR_CV})')
        self.stdout.write(
            f'Меток: {data["n_labels"]}, из них с рядом NDVI '
            f'({data["source"]}): {data["n_with_series"]} '
            f'за {seconds:.1f}s\n'
            f'Прошлогодняя осень: {data["prev_source"]} '
            f'({data["n_prev_series"]} угодий)\n'
            f'Группы валидации: {scheme}'
        )

    def _run_stage(self, data, stage, top):
        stage_data = data[stage]
        self.stdout.write('')
        self.stdout.write(self.style.MIGRATE_HEADING(
            f'═══ {STAGE_TITLES[stage]} ═══'))
        self.stdout.write(
            f'  {stage_data["positive_label"]}: {stage_data["n_pos"]}, '
            f'{stage_data["negative_label"]}: {stage_data["n_neg"]}, '
            f'групп: {stage_data["n_groups"]}, '
            f'без ряда/короткий ряд: {stage_data["dropped"]}'
        )
        if stage == STAGE_SEASON:
            self._report_autumn_prev(stage_data)
        if min(stage_data['n_pos'], stage_data['n_neg']) < MIN_PER_CLASS:
            self.stdout.write(self.style.WARNING(
                f'  Пропуск: нужно ≥ {MIN_PER_CLASS} примеров в каждом '
                'классе. Доразметьте угодья на странице label/.'
            ))
            return

        state, cv, scores = train_model(
            stage_data['rows'], stage_data['y'], stage_data['groups'],
            stage_data['names'], stage_data['feature_set'],
            stage_data['positive_label'], stage_data['negative_label'],
        )
        self._report_l2(scores, state['l2'])
        self._report_quality(stage, stage_data, data['series'], state, cv)
        self._report_weights(state, top)

    def _report_autumn_prev(self, stage_data):
        """Заполненность главного признака озимых — до, а не после весов.

        Признак ``autumn_prev`` (NDVI 15 сентября — 1 ноября ПРОШЛОГО
        года) физически отличает озимые: только у них к зиме есть всходы.
        Но ряд за прошлый год может обрываться раньше сентября — тогда
        признак пуст У ВСЕХ, вес его в модели около нуля, и это легко
        принять за «осень не информативна». Поэтому заполненность
        печатается явно.
        """
        total = stage_data['n_pos'] + stage_data['n_neg']
        filled = stage_data.get('n_autumn_prev', 0)
        if not total:
            return
        line = f'  Прошлогодняя осень заполнена: {filled}/{total}'
        if filled == 0:
            self.stdout.write(self.style.WARNING(
                line + ' — главный признак озимых недоступен: за прошлый '
                'год нет снимков в окне 15 сентября — 1 ноября. Модель '
                'решает по косвенным признакам; догрузите осень '
                'прошлого года командой run_ndvi_pipeline.'
            ))
        else:
            self.stdout.write(line)

    def _report_l2(self, scores, chosen):
        if not scores:
            self.stdout.write(self.style.WARNING(
                f'  Групп меньше {MIN_GROUPS_FOR_CV} — кросс-валидация '
                f'невозможна, взят L2={chosen:g} по умолчанию. Качество '
                'модели НЕ измерено.'
            ))
            return
        self.stdout.write('  Подбор L2 (сбаланс. точность на отложенных '
                          'группах):')
        for l2 in sorted(scores):
            mark = ' ← выбран' if l2 == chosen else ''
            self.stdout.write(f'    L2={l2:>6g}  {scores[l2]:.3f}{mark}')

    def _report_quality(self, stage, stage_data, series, state, cv):
        base = binary_metrics(
            np.asarray(stage_data['y'], dtype=np.float64),
            np.asarray(rule_based_baseline(stage, series,
                                           stage_data['farmland_ids'])),
        )
        rows = [('пороговый классификатор', base),
                ('модель (на обучении)', state['in_sample'])]
        if cv is not None:
            rows.append(('модель (отложенные группы)', cv))

        self.stdout.write('')
        self.stdout.write(
            f'  {"":<28}{"сбаланс.":>9}{"recall+":>9}{"recall−":>9}'
            f'{"точность":>10}'
        )
        for title, m in rows:
            self.stdout.write(
                f'  {title:<28}{m["balanced"]:>9.3f}{m["recall_pos"]:>9.3f}'
                f'{m["recall_neg"]:>9.3f}{m["accuracy"]:>10.3f}'
            )
        if cv is None:
            return

        gain = cv['balanced'] - base['balanced']
        if gain > 0.02:
            self.stdout.write(self.style.SUCCESS(
                f'  → Модель лучше порогов на {gain:+.3f} сбаланс. точности '
                '(на отложенных группах).'
            ))
        elif gain < -0.02:
            self.stdout.write(self.style.WARNING(
                f'  → Модель ХУЖЕ порогов на {gain:+.3f}: разметки мало или '
                'признаки не разделяют классы. Прод переводить нельзя.'
            ))
        else:
            self.stdout.write(
                f'  → Разница с порогами в пределах шума ({gain:+.3f}): '
                'нужна дополнительная разметка.'
            )
        self._report_per_group(cv)

    def _report_per_group(self, cv):
        """Худшие группы: где модель проваливается, там и искать причину.

        В группе с ОДНИМ классом сбалансированная точность не определена:
        recall отсутствующего класса считается нулём и тянет метрику к
        0.5, поэтому район, где все 48 угодий угаданы верно, выглядел бы
        таким же провальным, как район с ошибками. Такие группы
        ранжируются и печатаются по доле верных ответов с пометкой ``*``.
        """
        ranked = []
        for name, m in cv['per_group'].items():
            both = bool(m['n_pos'] and m['n_neg'])
            ranked.append((name, m, m['balanced'] if both else m['accuracy'],
                           both))
        if not ranked:
            return
        ranked.sort(key=lambda item: item[2])
        self.stdout.write('  Худшие группы (сбаланс. точность; '
                          '* — один класс, доля верных):')
        for name, m, score, both in ranked[:5]:
            mark = '' if both else '*'
            self.stdout.write(
                f'    {name:<12} n={m["n"]:<4} {score:.3f}{mark:<2}'
                f'(+{m["n_pos"]}/−{m["n_neg"]})'
            )

    def _report_weights(self, state, top):
        self.stdout.write('')
        self.stdout.write('  Веса (на стандартизованных признаках, '
                          '+ → в пользу '
                          f'«{state["positive_label"]}»):')
        for name, weight in feature_importance(state, top):
            self.stdout.write(f'    {name:<22}{weight:+.3f}')
        self.stdout.write(f'    {"intercept":<22}{state["intercept"]:+.3f}')
        self.stdout.write(
            f'  L2={state["l2"]:g}, признаков={len(state["names"])}, '
            f'обучено на {state["n_train"]} угодьях'
        )
        if state['l2'] == max(L2_GRID):
            self.stdout.write(
                '  L2 на краю сетки — вероятно, признаки слабые и модель '
                'приходится сильно «придавливать».'
            )
