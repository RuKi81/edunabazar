"""API эндпоинты прогноза урожайности (V1.4).

Три эндпоинта поверх ``YieldForecast`` / ``YieldForecastModel`` /
``CropYieldStat``. Никакого DRF — простые ``JsonResponse``-views в одном
стиле с остальным API агрокосмоса.

* ``/agrocosmos/api/yield/forecast/?year=YYYY&crop=grains_total``
    Список прогнозов на указанный год для всех регионов (для choropleth).

* ``/agrocosmos/api/yield/forecast/region/<region_id>/?crop=grains_total``
    История прогнозов + факт-урожайность для региона (для попапа/графика).

* ``/agrocosmos/api/yield/models/``
    Список моделей (PROD + history) с метриками — для admin-панели и
    «прозрачности» прогноза.

Все ответы кэшируются ETag'ом по составу данных. Прогноз/модель
обновляется редко (раз в неделю при cron-предикте), так что
``Cache-Control: max-age=3600`` совершенно безопасен.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any

from django.http import HttpRequest, HttpResponseNotModified, JsonResponse

from ..models import (
    CropYieldStat, Region, YieldCrop, YieldForecast, YieldForecastModel,
)
from ._helpers import _safe_round, rate_limit
from .geojson import _conditional_json


# ── Утилиты ──────────────────────────────────────────────────────────
def _validate_crop(crop: str | None) -> str | None:
    """Проверка culture-параметра. Возвращает crop или None если невалид."""
    if not crop:
        return YieldCrop.GRAINS_TOTAL
    valid = {c.value for c in YieldCrop}
    if crop not in valid:
        return None
    return crop


def _model_dict(m: YieldForecastModel) -> dict[str, Any]:
    """Сериализатор YieldForecastModel — без коэффициентов и residuals.

    Для публичного API мы НЕ отдаём коэффициенты, scaler и остатки — это
    внутренняя кухня обучения. Только метрики и идентификация.
    """
    return {
        'id': m.id,
        'version': m.model_version,
        'crop': m.crop,
        'scope': m.scope,
        'region_id': m.region_id,
        'is_production': m.is_production,
        'r2_cv': _safe_round(m.r2_cv, 3),
        'rmse_cv_t_per_ha': _safe_round(m.rmse_cv, 3),
        'rmse_pct': _safe_round(m.rmse_pct, 1),
        'n_samples': m.n_samples,
        'train_years': m.train_years,
        'feature_names': m.feature_names,
        'trained_at': m.trained_at.isoformat() if m.trained_at else None,
    }


def _forecast_dict(
    f: YieldForecast, *, include_features: bool = False,
) -> dict[str, Any]:
    """Сериализатор одного YieldForecast с derived-полями.

    ``baseline_t_per_ha`` восстанавливаем из CI80: середина (lo+hi)/2 это
    предсказание модели, а baseline = forecast − anomaly. Поскольку
    trivial_v1 предсказывает anomaly=0, для неё baseline = forecast.
    Для ridge_v1 — пока пишем 0 как anomaly (не сохраняли в БД явно);
    будущая миграция добавит явное поле, см. TODO.
    """
    return {
        'region_id': f.region_id,
        'region_name': f.region.name if f.region_id else None,
        'region_code': f.region.code if f.region_id else None,
        'district_id': f.district_id,
        'district_name': f.district.name if f.district_id else None,
        'year': f.year,
        'crop': f.crop,
        'forecast_t_per_ha': _safe_round(f.forecast_t_per_ha, 2),
        'ci_lower': _safe_round(f.ci_lower, 2),
        'ci_upper': _safe_round(f.ci_upper, 2),
        'season_progress': _safe_round(f.season_progress, 2),
        'forecasted_at': f.forecasted_at.isoformat() if f.forecasted_at else None,
        'features_completeness': _safe_round(f.features_completeness, 2),
        'model': {
            'id': f.model_id,
            'version': f.model.model_version,
            'r2_cv': _safe_round(f.model.r2_cv, 3),
            'rmse_cv_t_per_ha': _safe_round(f.model.rmse_cv, 3),
        },
        **({'features': f.features_used} if include_features else {}),
    }


def _resolve_baseline(
    f: YieldForecast, baseline_cache: dict[int, dict],
) -> float | None:
    """Подсчёт baseline для (region, year) из ``model.diagnostics``.

    Кэшируем dict baselines по model_id чтобы не парсить JSON на каждой
    записи.
    """
    diag = baseline_cache.get(f.model_id)
    if diag is None:
        diag = (f.model.diagnostics or {}).get('regional_baselines') or {}
        baseline_cache[f.model_id] = diag
    bl = diag.get(str(f.region_id)) or diag.get(f.region_id)
    if not bl:
        return None
    try:
        return float(bl['intercept']) + float(bl['slope']) * f.year
    except (KeyError, TypeError, ValueError):
        return None


# ── 1. Список прогнозов на год ───────────────────────────────────────
@rate_limit('60/m')
def api_yield_forecast(request: HttpRequest) -> JsonResponse:
    """Прогноз урожайности на год для всех регионов.

    Параметры:
        ``year``  — обязательный, ``[2020 .. текущий+1]``
        ``crop``  — default ``grains_total``

    Возвращает список ``forecasts[]`` с прогнозом, baseline, аномалией
    и CI80 для каждого региона. Используется choropleth-слоем UI.
    """
    try:
        year = int(request.GET.get('year', _dt.date.today().year))
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year'}, status=400)

    crop = _validate_crop(request.GET.get('crop'))
    if crop is None:
        return JsonResponse({'ok': False, 'error': 'invalid crop'}, status=400)

    qs = (
        YieldForecast.objects
        .filter(year=year, crop=crop, is_latest=True, region__isnull=False)
        .select_related('region', 'model')
        .order_by('region__name')
    )
    forecasts_raw = list(qs)

    # Один проход — собираем dict + baseline.
    baseline_cache: dict[int, dict] = {}
    forecasts: list[dict] = []
    for f in forecasts_raw:
        d = _forecast_dict(f)
        bl = _resolve_baseline(f, baseline_cache)
        if bl is not None:
            d['baseline_t_per_ha'] = round(bl, 2)
            d['anomaly_t_per_ha'] = round(f.forecast_t_per_ha - bl, 2)
        forecasts.append(d)

    # Активная модель для информации.
    active_model = (
        YieldForecastModel.objects
        .filter(scope='national', region__isnull=True, crop=crop, is_production=True)
        .order_by('-trained_at')
        .first()
    )

    # Сводка для UI (для отображения над картой).
    if forecasts:
        vals = [f['forecast_t_per_ha'] for f in forecasts]
        summary = {
            'n_regions': len(forecasts),
            'mean_t_per_ha': round(sum(vals) / len(vals), 2),
            'min_t_per_ha': round(min(vals), 2),
            'max_t_per_ha': round(max(vals), 2),
        }
    else:
        summary = {'n_regions': 0}

    payload = {
        'ok': True,
        'year': year,
        'crop': crop,
        'model': _model_dict(active_model) if active_model else None,
        'summary': summary,
        'forecasts': forecasts,
    }

    # ETag по composite-ключу: год+crop+max(forecasted_at)+model_id.
    last_at = max(
        (f.forecasted_at for f in forecasts_raw if f.forecasted_at),
        default=None,
    )
    etag = 'W/"yf-v1-{y}-{c}-{n}-{d}-{m}"'.format(
        y=year, c=crop, n=len(forecasts),
        d=last_at.isoformat() if last_at else 'none',
        m=active_model.id if active_model else 0,
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=3600, stale-while-revalidate=86400',
    )


# ── 2. История прогнозов и фактов по региону ─────────────────────────
@rate_limit('60/m')
def api_yield_forecast_region(
    request: HttpRequest, region_id: int,
) -> JsonResponse:
    """История прогноза + факт-урожайность для одного региона.

    Параметры:
        ``crop``      — default ``grains_total``
        ``year_from`` — default 2010
        ``year_to``   — default текущий+1

    Используется в попапе при клике на регион — рисует график
    «факт vs прогноз» по годам.
    """
    region = Region.objects.filter(id=region_id).first()
    if region is None:
        return JsonResponse({'ok': False, 'error': 'region not found'}, status=404)

    crop = _validate_crop(request.GET.get('crop'))
    if crop is None:
        return JsonResponse({'ok': False, 'error': 'invalid crop'}, status=400)

    today = _dt.date.today()
    try:
        year_from = int(request.GET.get('year_from', 2010))
        year_to = int(request.GET.get('year_to', today.year + 1))
    except (TypeError, ValueError):
        return JsonResponse({'ok': False, 'error': 'invalid year range'}, status=400)

    # Фактические данные.
    facts_qs = (
        CropYieldStat.objects
        .filter(
            region=region, district__isnull=True, crop=crop,
            year__gte=year_from, year__lte=year_to,
        )
        .order_by('year')
    )
    facts = [
        {
            'year': fct.year,
            'yield_t_per_ha': _safe_round(fct.yield_t_per_ha, 2),
            'area_ha': _safe_round(fct.area_ha, 0) if fct.area_ha else None,
            'gross_t': _safe_round(fct.gross_t, 0) if fct.gross_t else None,
            'source': fct.source,
        }
        for fct in facts_qs
    ]

    # Прогнозы (только is_latest).
    fc_qs = (
        YieldForecast.objects
        .filter(
            region=region, district__isnull=True, crop=crop,
            is_latest=True,
            year__gte=year_from, year__lte=year_to,
        )
        .select_related('model')
        .order_by('year')
    )
    fc_raw = list(fc_qs)
    baseline_cache: dict[int, dict] = {}
    forecasts = []
    for f in fc_raw:
        d = _forecast_dict(f, include_features=True)
        bl = _resolve_baseline(f, baseline_cache)
        if bl is not None:
            d['baseline_t_per_ha'] = round(bl, 2)
            d['anomaly_t_per_ha'] = round(f.forecast_t_per_ha - bl, 2)
        forecasts.append(d)

    # Активная модель для контекста.
    active_model = (
        YieldForecastModel.objects
        .filter(scope='national', region__isnull=True, crop=crop, is_production=True)
        .order_by('-trained_at')
        .first()
    )

    payload = {
        'ok': True,
        'region': {'id': region.id, 'name': region.name, 'code': region.code},
        'crop': crop,
        'year_from': year_from,
        'year_to': year_to,
        'model': _model_dict(active_model) if active_model else None,
        'facts': facts,
        'forecasts': forecasts,
    }

    etag = 'W/"yfr-v1-{r}-{c}-{n_f}-{n_p}-{m}"'.format(
        r=region.id, c=crop,
        n_f=len(facts), n_p=len(forecasts),
        m=active_model.id if active_model else 0,
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=3600, stale-while-revalidate=86400',
    )


# ── 3. Список моделей ────────────────────────────────────────────────
@rate_limit('30/m')
def api_yield_models(request: HttpRequest) -> JsonResponse:
    """Все обученные модели прогноза урожайности.

    Параметры:
        ``crop`` — фильтр (default — все культуры)
        ``only_production`` — ``1`` чтобы вернуть только PROD-модели

    Используется в admin/диагностической панели для прозрачности — какая
    модель сейчас в проде, какие были раньше, какие метрики и т.п.
    """
    qs = YieldForecastModel.objects.all().order_by('-trained_at')

    crop = request.GET.get('crop')
    if crop:
        crop_v = _validate_crop(crop)
        if crop_v is None:
            return JsonResponse({'ok': False, 'error': 'invalid crop'}, status=400)
        qs = qs.filter(crop=crop_v)

    if request.GET.get('only_production') == '1':
        qs = qs.filter(is_production=True)

    models = [_model_dict(m) for m in qs]

    payload = {'ok': True, 'count': len(models), 'models': models}

    etag = 'W/"yfm-v1-{n}-{ts}"'.format(
        n=len(models),
        ts=models[0]['trained_at'] if models else 'none',
    )
    return _conditional_json(
        request, payload,
        etag=etag,
        cache_control='public, max-age=300, stale-while-revalidate=3600',
    )
