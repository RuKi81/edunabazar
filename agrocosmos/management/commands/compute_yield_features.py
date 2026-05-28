"""Батч-вычисление NDVI-фичей для прогноза урожайности.

Идёт по парам (регион × год × культура) и для каждой считает 6 фичей
через ``services.yield_features.compute_region_features``, сохраняя
результат в ``YieldFeatures`` (upsert по версии feature-set).

По умолчанию обрабатывает все пары, для которых ЕСТЬ факт-урожайность
в ``CropYieldStat`` (это то, что нужно для обучения). Опционально
можно посчитать фичи и для пар без yield-факта (для применения
обученной модели в режиме прогноза).

Примеры:
    # Все обучающие пары (region × year × grains_total) с yield-фактом
    python manage.py compute_yield_features

    # Один регион, все доступные годы
    python manage.py compute_yield_features --region "Краснодарский край"

    # Конкретный год — нужно посчитать прогнозные фичи
    python manage.py compute_yield_features --year 2025 --include-no-yield

    # Тестовый запуск: только посмотреть, ничего не пишем
    python manage.py compute_yield_features --year 2022 --dry-run --verbose
"""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Min, Max

from agrocosmos.models import (
    CropYieldStat, DistrictNdviSeries, Region, YieldCrop, YieldFeatures,
)
from agrocosmos.services.yield_features import (
    compute_region_features, is_season_complete,
)


# Соответствие модельной культуры → фильтр crop_type в DistrictNdviSeries.
# V1: только зерновые → 'arable' (пашня). В V2 расширим.
CROP_TO_NDVI_FILTER: dict[str, str] = {
    YieldCrop.GRAINS_TOTAL: 'arable',
    YieldCrop.WHEAT: 'arable',
    YieldCrop.WHEAT_WINTER: 'arable',
    YieldCrop.WHEAT_SPRING: 'arable',
    YieldCrop.BARLEY: 'arable',
    YieldCrop.CORN_GRAIN: 'arable',
    YieldCrop.SUNFLOWER: 'arable',
    YieldCrop.SOY: 'arable',
    YieldCrop.RAPESEED: 'arable',
    YieldCrop.SUGAR_BEET: 'arable',
}


class Command(BaseCommand):
    help = 'Батч-вычисление NDVI-фичей для прогноза урожайности по регионам'

    def add_arguments(self, parser):
        parser.add_argument(
            '--year', type=int, default=None,
            help='Конкретный год (по умолчанию все годы с NDVI-данными)',
        )
        parser.add_argument(
            '--region', default=None,
            help='Имя или код региона (по умолчанию все регионы РФ)',
        )
        parser.add_argument(
            '--crop', default=YieldCrop.GRAINS_TOTAL,
            choices=[c.value for c in YieldCrop],
            help='Культура (по умолчанию grains_total)',
        )
        parser.add_argument(
            '--feature-set-version', default='v1',
            help='Метка версии фичей для записи в YieldFeatures (default: v1)',
        )
        parser.add_argument(
            '--include-no-yield', action='store_true',
            help='Считать фичи и для пар без yield-факта в CropYieldStat. '
                 'Нужно для прогнозных лет (2024+).',
        )
        parser.add_argument(
            '--force', action='store_true',
            help='Пересчитать даже если YieldFeatures для пары уже есть.',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Только посчитать и вывести сводку, в БД ничего не писать.',
        )
        parser.add_argument(
            '--verbose', action='store_true',
            help='Печатать каждую вычисленную пару.',
        )

    def handle(self, *args, **opts):
        crop = opts['crop']
        version = opts['feature_set_version']
        crop_type = CROP_TO_NDVI_FILTER.get(crop, 'arable')

        # ── Определяем рабочий набор пар (region, year) ──────────────
        regions = self._select_regions(opts['region'])
        if not regions:
            self.stdout.write(self.style.ERROR('Регионы не найдены — нечего считать.'))
            return

        years = self._select_years(opts['year'])
        if not years:
            self.stdout.write(self.style.ERROR(
                'Не найдено ни одного года с NDVI-данными в DistrictNdviSeries.'
            ))
            return

        pairs = self._build_pairs(
            regions=regions, years=years, crop=crop,
            include_no_yield=opts['include_no_yield'],
        )
        self.stdout.write(self.style.SUCCESS(
            f'К обработке: {len(pairs)} пар (регион × год)\n'
            f'  Регионов: {len(regions)}\n'
            f'  Годы:     {sorted(years)}\n'
            f'  Культура: {crop}\n'
            f'  NDVI crop_type: {crop_type}\n'
            f'  Версия фичей:   {version}'
        ))

        # Skip уже посчитанных, если не --force
        if not opts['force']:
            existing = set(
                YieldFeatures.objects
                .filter(crop=crop, feature_set_version=version, district__isnull=True)
                .values_list('region_id', 'year')
            )
            before = len(pairs)
            pairs = [(r, y) for (r, y) in pairs if (r.id, y) not in existing]
            if before != len(pairs):
                self.stdout.write(
                    f'  Пропущено уже посчитанных: {before - len(pairs)} '
                    f'(используйте --force, чтобы пересчитать)'
                )

        # ── Прогон ───────────────────────────────────────────────────
        stats = defaultdict(int)
        results: list[tuple[Region, int, object]] = []
        for region, year in pairs:
            features = compute_region_features(region, year, crop_type=crop_type)
            if features is None:
                stats['skipped'] += 1
                if opts['verbose']:
                    self.stdout.write(
                        self.style.WARNING(
                            f'  ✗ {region.name} / {year} — фенология не детектируется'
                        )
                    )
                continue
            stats['computed'] += 1
            results.append((region, year, features))
            if opts['verbose']:
                f = features
                self.stdout.write(
                    f'  ✓ {region.name:30s} / {year}  '
                    f'peak={f.peak_ndvi:.3f} @DOY{f.peak_ndvi_doy:3d}  '
                    f'SOS={f.sos_doy:3d}  LOS={f.length_of_season:3d}d  '
                    f'iNDVI_tot={f.indvi_total:5.1f}  '
                    f'iNDVI_rep={f.indvi_repro:4.1f}  '
                    f'(n={f.n_observations})'
                )

        # ── Сохранение ───────────────────────────────────────────────
        if opts['dry_run']:
            self._print_summary(stats, results, dry_run=True, version=version)
            return

        with transaction.atomic():
            for region, year, features in results:
                _, created = YieldFeatures.objects.update_or_create(
                    region=region, district=None,
                    year=year, crop=crop,
                    feature_set_version=version,
                    defaults={
                        'features': {**features.as_dict(), **features.diagnostics()},
                        'season_complete': is_season_complete(year),
                    },
                )
                if created:
                    stats['created'] += 1
                else:
                    stats['updated'] += 1

        self._print_summary(stats, results, dry_run=False, version=version)

    # ── Хелперы выбора входных данных ────────────────────────────────
    def _select_regions(self, region_arg: str | None) -> list[Region]:
        qs = Region.objects.all().order_by('name')
        if region_arg:
            # Ищем по точному имени или по коду.
            r = qs.filter(name=region_arg).first() or qs.filter(code=region_arg).first()
            if r is None:
                self.stdout.write(self.style.ERROR(
                    f'Регион "{region_arg}" не найден ни по имени, ни по коду.'
                ))
                return []
            return [r]
        return list(qs)

    def _select_years(self, year_arg: int | None) -> list[int]:
        if year_arg is not None:
            return [year_arg]
        agg = DistrictNdviSeries.objects.aggregate(
            min_d=Min('acquired_date'), max_d=Max('acquired_date'),
        )
        if agg['min_d'] is None:
            return []
        return list(range(agg['min_d'].year, agg['max_d'].year + 1))

    def _build_pairs(
        self, regions: list[Region], years: list[int],
        crop: str, include_no_yield: bool,
    ) -> list[tuple[Region, int]]:
        if include_no_yield:
            return [(r, y) for r in regions for y in years]

        # Берём только пары, где ЕСТЬ yield-факт в CropYieldStat —
        # бессмысленно считать фичи без таргета (для обучения).
        with_yield = set(
            CropYieldStat.objects
            .filter(crop=crop, district__isnull=True)
            .values_list('region_id', 'year')
        )
        return [
            (r, y) for r in regions for y in years
            if (r.id, y) in with_yield
        ]

    # ── Сводка ───────────────────────────────────────────────────────
    def _print_summary(self, stats, results, *, dry_run: bool, version: str):
        self.stdout.write('')
        self.stdout.write(self.style.MIGRATE_HEADING('═══ Сводка ═══'))
        self.stdout.write(f'  Вычислено фичей:    {stats["computed"]}')
        self.stdout.write(f'  Пропущено (нет данных / низкая амплитуда): {stats["skipped"]}')

        if results:
            n_obs_avg = sum(f.n_observations for _, _, f in results) / len(results)
            peaks = [f.peak_ndvi for _, _, f in results]
            losses = [f.length_of_season for _, _, f in results]
            self.stdout.write(
                f'  Средн. n_observations: {n_obs_avg:.1f}\n'
                f'  Peak NDVI диапазон: {min(peaks):.3f}..{max(peaks):.3f}\n'
                f'  Length of season:   {min(losses)}..{max(losses)} дней'
            )

        if dry_run:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING('Dry-run: в БД ничего не записано.'))
        else:
            self.stdout.write('')
            self.stdout.write(self.style.SUCCESS(
                f'  Создано: {stats["created"]}, обновлено: {stats["updated"]} '
                f'(feature_set_version={version})'
            ))
