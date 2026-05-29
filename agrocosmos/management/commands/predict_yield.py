"""Применение обученной модели прогноза урожайности.

Берёт PRODUCTION-модель для пары (scope=NATIONAL, crop), идёт по всем
региональным YieldFeatures данного года и сохраняет YieldForecast.

Если для (region, year, crop) уже есть прогнозы — старые помечаются
``is_latest=False``, новый — ``is_latest=True`` (история уточнений).

Примеры:
    # Прогноз 2024 — все регионы
    python manage.py predict_yield --year 2024

    # Только посчитать, ничего не писать
    python manage.py predict_yield --year 2025 --dry-run --verbose

    # Конкретный регион
    python manage.py predict_yield --year 2024 --region "Краснодарский край"
"""
from __future__ import annotations

import dataclasses
import datetime as _dt

from django.core.management.base import BaseCommand
from django.db import transaction

from agrocosmos.models import (
    Region, YieldCrop, YieldFeatures, YieldForecast, YieldForecastModel,
)
from agrocosmos.services.yield_features import SEASON_DOY_END, SEASON_DOY_START
from agrocosmos.services.yield_model import model_predict


@dataclasses.dataclass
class _SyntheticYF:
    """In-memory stand-in for ``YieldFeatures`` used by the trivial-model
    fallback path. Only fields actually read by ``predict_yield`` are
    populated — ``region``, ``region_id`` and an empty ``features`` dict.
    The trivial model ignores ``features`` (no NDVI-coefficients), so this
    is sufficient to drive the prediction loop and the persistence step.
    """
    region: object
    region_id: int
    features: dict


class Command(BaseCommand):
    help = 'Применить активную модель прогноза урожайности к фичам данного года'

    def add_arguments(self, parser):
        parser.add_argument('--year', type=int, required=True)
        parser.add_argument(
            '--crop', default=YieldCrop.GRAINS_TOTAL,
            choices=[c.value for c in YieldCrop],
        )
        parser.add_argument(
            '--feature-set-version', default='v1',
        )
        parser.add_argument(
            '--region', default=None,
            help='Имя или код региона (по умолчанию все регионы).',
        )
        parser.add_argument(
            '--model-id', type=int, default=None,
            help='Использовать конкретную модель по id (по умолчанию — '
                 'PRODUCTION для (NATIONAL, crop)).',
        )
        parser.add_argument('--dry-run', action='store_true')
        parser.add_argument('--verbose', action='store_true')

    def handle(self, *args, **opts):
        crop = opts['crop']
        year = opts['year']
        version = opts['feature_set_version']

        # ── Найти модель ────────────────────────────────────────────
        model = self._load_model(crop, opts['model_id'])
        if model is None:
            return

        self.stdout.write(self.style.MIGRATE_HEADING(
            f'═══ Прогноз {crop} / {year} (model id={model.id}, '
            f'{model.model_version}, R²_cv={model.r2_cv:.3f}) ═══'
        ))

        # Состояние модели для predict — собираем из полей + diagnostics.
        diag = model.diagnostics or {}
        model_state = {
            'feature_names': model.feature_names,
            'coefficients': model.coefficients,
            'intercept': model.intercept,
            'feature_scaler': model.feature_scaler,
            'residuals_cv': model.residuals_cv,
            'regional_baselines': diag.get('regional_baselines', {}),
        }

        # ── Подобрать YieldFeatures ─────────────────────────────────
        yf_qs = YieldFeatures.objects.filter(
            crop=crop, year=year,
            feature_set_version=version,
            district__isnull=True,
            region__isnull=False,
        ).select_related('region')

        if opts['region']:
            r = (
                Region.objects.filter(name=opts['region']).first()
                or Region.objects.filter(code=opts['region']).first()
            )
            if r is None:
                self.stdout.write(self.style.ERROR(
                    f'Регион "{opts["region"]}" не найден.'
                ))
                return
            yf_qs = yf_qs.filter(region=r)

        yf_list = list(yf_qs)
        self.stdout.write(f'YieldFeatures найдено: {len(yf_list)}')

        # Trivial-модель (feature_names == []) не использует NDVI-фичи —
        # прогноз = baseline по году. Если YieldFeatures для запрошенного
        # года ещё не посчитаны (типичный сценарий для текущего года, где
        # сезон не завершён), генерируем синтетический список из самих
        # регионов, для которых известна baseline. Это позволяет получить
        # ранний прогноз тренда сразу после обновления EMISS-данных.
        if not yf_list and not model.feature_names:
            rb = model_state.get('regional_baselines') or {}
            baseline_ids = {int(k) for k in rb.keys()}
            regions_qs = Region.objects.filter(id__in=baseline_ids)
            if opts['region']:
                regions_qs = regions_qs.filter(name=opts['region']) | \
                             regions_qs.filter(code=opts['region'])
            yf_list = [
                _SyntheticYF(region=r, region_id=r.id, features={})
                for r in regions_qs
            ]
            self.stdout.write(self.style.WARNING(
                f'  [trivial-fallback] YieldFeatures отсутствуют — '
                f'итерируем по {len(yf_list)} регионам с известной baseline.'
            ))

        if not yf_list:
            return

        # ── Подсчёт прогноза для каждого ────────────────────────────
        season_progress = self._estimate_season_progress(year)
        forecasted_at = _dt.date.today()

        results = []
        skipped_no_baseline = 0
        skipped_bad_features = 0

        for yf in yf_list:
            pred = model_predict(yf.features, model_state, yf.region_id, year)
            if pred is None:
                rb = model_state['regional_baselines']
                has_bl = (str(yf.region_id) in rb) or (yf.region_id in rb)
                if not has_bl:
                    skipped_no_baseline += 1
                else:
                    skipped_bad_features += 1
                if opts['verbose']:
                    self.stdout.write(self.style.WARNING(
                        f'  ✗ {yf.region.name} — '
                        f'{"нет baseline" if not has_bl else "битые фичи"}'
                    ))
                continue
            results.append((yf, pred))
            if opts['verbose']:
                self.stdout.write(
                    f'  ✓ {yf.region.name:30s}  '
                    f'baseline={pred["baseline"]:.2f}  '
                    f'anom={pred["anomaly"]:+.3f}  '
                    f'forecast={pred["forecast_t_per_ha"]:.2f}  '
                    f'CI80=[{pred["ci_lower"]:.2f}; {pred["ci_upper"]:.2f}]'
                )

        # ── Сводка ──────────────────────────────────────────────────
        self.stdout.write('')
        self.stdout.write(self.style.MIGRATE_HEADING('═══ Сводка ═══'))
        self.stdout.write(f'  Прогнозов сделано: {len(results)}')
        if skipped_no_baseline:
            self.stdout.write(
                f'  Пропущено (нет baseline ≥5 лет факт-данных): '
                f'{skipped_no_baseline}'
            )
        if skipped_bad_features:
            self.stdout.write(
                f'  Пропущено (битые фичи): {skipped_bad_features}'
            )
        if results:
            forecasts = [p['forecast_t_per_ha'] for _, p in results]
            anoms = [p['anomaly'] for _, p in results]
            self.stdout.write(
                f'  Прогноз диапазон:  '
                f'{min(forecasts):.2f} .. {max(forecasts):.2f} т/га'
            )
            self.stdout.write(
                f'  Аномалия диапазон: '
                f'{min(anoms):+.3f} .. {max(anoms):+.3f} т/га'
            )
            self.stdout.write(
                f'  Средняя аномалия: {sum(anoms)/len(anoms):+.3f} т/га'
            )

        # ── Сохранение ──────────────────────────────────────────────
        if opts['dry_run']:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING('Dry-run: прогнозы не сохранены.'))
            return

        n_created = 0
        n_replaced = 0
        with transaction.atomic():
            for yf, pred in results:
                # Сбрасываем is_latest у предыдущих прогнозов этой пары.
                old = YieldForecast.objects.filter(
                    region=yf.region, district=None,
                    year=year, crop=crop,
                    is_latest=True,
                )
                n_replaced += old.update(is_latest=False)

                YieldForecast.objects.create(
                    region=yf.region, district=None,
                    year=year, crop=crop,
                    forecasted_at=forecasted_at,
                    season_progress=season_progress,
                    forecast_t_per_ha=pred['forecast_t_per_ha'],
                    ci_lower=pred['ci_lower'],
                    ci_upper=pred['ci_upper'],
                    features_used=yf.features,
                    features_completeness=1.0,
                    model=model,
                    is_latest=True,
                )
                n_created += 1

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(
            f'✓ Сохранено прогнозов: {n_created} '
            f'(заменено предыдущих is_latest: {n_replaced})'
        ))

    # ── Хелперы ──────────────────────────────────────────────────────
    def _load_model(
        self, crop: str, model_id: int | None,
    ) -> YieldForecastModel | None:
        if model_id is not None:
            m = YieldForecastModel.objects.filter(id=model_id).first()
            if m is None:
                self.stdout.write(self.style.ERROR(
                    f'YieldForecastModel id={model_id} не найдена.'
                ))
            return m
        m = YieldForecastModel.objects.filter(
            scope=YieldForecastModel.Scope.NATIONAL,
            region__isnull=True,
            crop=crop,
            is_production=True,
        ).order_by('-trained_at').first()
        if m is None:
            self.stdout.write(self.style.ERROR(
                f'PRODUCTION-модель для (NATIONAL, {crop}) не найдена. '
                f'Обучите: python manage.py train_yield_model '
                f'--crop {crop} --activate'
            ))
        return m

    def _estimate_season_progress(self, year: int) -> float:
        """0.0 до сева, 1.0 после уборки. Линейно по DOY в окне сезона."""
        today = _dt.date.today()
        if year < today.year:
            return 1.0
        if year > today.year:
            return 0.0
        doy = today.timetuple().tm_yday
        if doy <= SEASON_DOY_START:
            return 0.0
        if doy >= SEASON_DOY_END:
            return 1.0
        return round((doy - SEASON_DOY_START) / (SEASON_DOY_END - SEASON_DOY_START), 2)
