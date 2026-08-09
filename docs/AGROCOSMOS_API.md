# Agrocosmos API Reference

Все endpoint'ы доступны под префиксом `/agrocosmos/`. Публичные API защищены
IP-based rate-limiting (см. колонку «Лимит»). Тяжёлые агрегаты кешируются в
Redis на 5 минут (`cache_page`).

**Базовый ответ:** `{"ok": true, ...}` при успехе, `{"ok": false, "error": "..."}` при ошибке.
Статусы: `200` OK · `400` invalid params · `404` not found · `429` rate-limit.

---

## Краткая таблица

| Endpoint | Метод | Лимит | Cache | Назначение |
|---|---|---|---|---|
| `/` | GET | — | — | HTML: главный дашборд |
| `/raster/` | GET | — | — | HTML: растровый дашборд |
| `/report/region/` | GET | — | — | HTML: отчёт по региону/району (MODIS) |
| `/report/district-detailed/` | GET | — | — | HTML: свод по району (S2/L8) |
| `/report/screening/` | GET | — | — | HTML: проблемные поля района |
| `/report/unused/` | GET | — | — | HTML: неиспользуемые земли (ЗСН) |
| `/report/farmland/` | GET | — | — | HTML: паспорт поля |
| `/api/regions/` | GET | — | — | GeoJSON регионов |
| `/api/districts/` | GET | — | — | GeoJSON районов в регионе |
| `/api/farmlands/` | GET | **60/m** | — | GeoJSON полей в районе |
| `/api/tiles/{z}/{x}/{y}.pbf` | GET | **300/m** | 10 мин | Mapbox Vector Tiles полей |
| `/api/raster-tile/{z}/{x}/{y}.png` | GET | **300/m** | — | NDVI PNG-тайлы из GeoTIFF |
| `/api/raster-composites/` | GET | — | — | Список доступных растровых композитов |
| `/api/raster-preview/` | GET | **120/m** | — | PNG-превью композита по bbox поля |
| `/api/farmland/raster-frames/` | GET | **60/m** | — | Последние N композитов, покрывающих поле |
| `/api/farmland/ndvi/` | GET | **60/m** | — | NDVI time series одного поля |
| `/api/ndvi-stats/` | GET | **30/m** | **5 мин** | Агрегированная NDVI-статистика по региону/району |
| `/api/phenology/` | GET | **30/m** | — | Фенологические метрики (SOS/POS/EOS/LOS) |
| `/api/report/country/` | GET | **30/m** | **15 мин** | Данные для странового отчёта |
| `/api/report/region/` | GET | **30/m** | **5 мин** | Данные для региональной страницы отчёта |
| `/api/report/district/` | GET | **30/m** | **5 мин** | Данные для районной страницы отчёта |
| `/api/report/district-detailed/` | GET | **30/m** | **10 мин** | Свод района по детальному мониторингу |
| `/api/report/screening/` | GET | **30/m** | **10 мин** | Рейтинг проблемных полей района |
| `/api/report/unused/` | GET | **30/m** | **10 мин** | Скрининг неиспользуемых земель |
| `/api/report/farmland/` | GET | **30/m** | **5 мин** | Данные паспорта поля |

---

## GeoJSON API

### `GET /api/regions/`

Все регионы с геометрией (MultiPolygon).

**Ответ:** `{"type": "FeatureCollection", "features": [...]}`

### `GET /api/districts/?region=<id>`

Районы региона.

**Параметры:** `region` (обязательный, int).

### `GET /api/farmlands/?district=<id>`

Поля одного района. Для региональной карты используйте MVT (см. ниже).

**Параметры:** `district` (обязательный, int).
**Лимит:** 60 req/min / IP.

---

## Tile API

### `GET /api/tiles/{z}/{x}/{y}.pbf`

Mapbox Vector Tiles с полигонами полей. Используется для отрисовки всех полей
региона на Leaflet/MapLibre. Zoom 6–15.

**Content-Type:** `application/x-protobuf`
**Лимит:** 300 req/min / IP.
**Cache:** 10 мин в Redis.

### `GET /api/raster-tile/{z}/{x}/{y}.png`

NDVI псевдоцветная PNG-подложка, склеенная из GeoTIFF-композита.

**Параметры:**
- `composite=<filename>` — имя файла композита (из `/api/raster-composites/`).

**Лимит:** 300 req/min / IP.

### `GET /api/raster-composites/`

Список доступных растровых композитов (NDVI GeoTIFF) с метаданными.

### `GET /api/raster-preview/?farmland=<id>&sensor=s2|l8&scope=<scope>&date=<from>_<to>`

Мини-превью (PNG, псевдоцвет NDVI) композита, обрезанное по bbox поля
(+25% паддинг) с контуром угодья поверх. Используется во фреймах «Снимки
NDVI» паспорта поля. Пустой ответ 204 — bbox вне растра или нет данных.

**Лимит:** 120 req/min / IP.

### `GET /api/farmland/raster-frames/?farmland=<id>[&year=<y>][&limit=<n>]`

Последние N (по умолчанию 5, максимум 12) растровых композитов,
покрывающих угодье, для превью в паспорте поля. Скоуп подбирается
автоматически: район (`d<id>`) → регион; сенсор: S2 → L8.

**Ответ:**
```json
{
  "ok": true,
  "farmland_id": 17,
  "year": "2025",
  "sensor": "s2",
  "scope": "d5",
  "frames": [
    {"date_from": "2025-07-01", "date_to": "2025-07-05", "date": "2025-07-01_2025-07-05"}
  ]
}
```

---

## NDVI API

### `GET /api/farmland/ndvi/?farmland=<id>[&year=<y>][&source=modis|raster]`

NDVI time series одного поля.

**Параметры:**
- `farmland` (обязательный, int)
- `year` (опциональный, int)
- `source` (опциональный, `modis` или `raster`)

**Ответ:**
```json
{
  "ok": true,
  "data": [
    {"date": "2025-06-10", "mean": 0.72, "mean_smooth": 0.71, "is_outlier": false}
  ],
  "last_period_end": "2025-06-18"
}
```

`last_period_end` возвращается только для MODIS — это дата окончания
16-дневного композита (midpoint + 8 дней), нужна для dashed extension line на
графиках.

**Лимит:** 60 req/min / IP.

### `GET /api/ndvi-stats/?region=<id>[&district=<id>][&year=<y>][&date_from=...][&date_to=...][&crop_types=...][&fact_isp=used|unused][&source=modis|raster]`

Агрегированная NDVI-статистика по региону или району:

- `by_crop_type`: area-weighted среднее NDVI по типам культур
- `by_period`: time series с z-score относительно baseline
- `summary`: общий охват и средний NDVI
- `farmland_summary`: распределение полей по культурам
- `usage_summary`: разбивка по `Fact_isp` (используется / не используется)
- `baseline`: историческое среднее по DOY (день года)

**Параметры:**
- `region` (обязательный)
- `district` (опциональный) — сужает до района
- `year` (опциональный) — по умолчанию все годы
- `date_from`, `date_to` (опциональные) — ISO-даты
- `crop_types` (опциональный) — comma-separated, например `arable,hayfield`
- `fact_isp` (опциональный) — `used` / `unused`
- `source` (опциональный) — `modis` / `raster` / `fused`

**Лимит:** 30 req/min / IP.
**Cache:** 5 мин Redis (ключ варьируется по полному URL со query string).

> **`source=fused`** возвращает HLS-style объединённый ряд Sentinel-2 +
> Landsat (см. ниже раздел «HLS Fusion»). Требует предварительного
> прогона `compute_fused_ndvi`.

> **Производительность:** cold ~30s для крупного региона (2M+ VI-строк), warm <10ms.
> Реализация делает **один** `SELECT … JOIN` и агрегирует результат в Python
> (single-pass). См. `agrocosmos/views/ndvi.py`.

### `GET /api/phenology/?region=<id>[&district=<id>]&year=<y>[&crop_types=...][&fact_isp=...]`

Фенологические метрики, посчитанные MODIS-pipeline'ом:

- SOS (start of season)
- POS (peak of season)
- EOS (end of season)
- LOS (length of season, дни)
- `avg_max_ndvi`, `avg_mean_ndvi`

Возвращает массив записей по крупам культур.

**Лимит:** 30 req/min / IP.

---

## Report API

Линейка отчётов (сквозная навигация от страны до поля):
страновой/региональный MODIS-отчёт → свод района (S2/L8) →
проблемные поля → неиспользуемые земли → паспорт поля.

### `GET /api/report/country/?year=<y>`

Данные для странового отчёта (читает пре-агрегат `DistrictNdviSeries`,
просуммированный до `(region, date)`):

- `regions[]` — для каждого региона: series, latest_ndvi, z-score, baseline, assessment
- `country_overall_series`, `country_baseline`, `last_period_end`

**Лимит:** 30 req/min / IP. **Cache:** 15 мин Redis.

### `GET /api/report/region/?region=<id>&year=<y>`

Данные для региональной страницы:

- `districts[]` — для каждого района: series, latest_ndvi, z-score, baseline, assessment
- `region_overall_series` — общая area-weighted NDVI по всем полям региона
- `region_baseline` — усреднённый по районам baseline
- `last_period_end` — для dashed extension line

**Лимит:** 30 req/min / IP. **Cache:** 5 мин Redis.

### `GET /api/report/district/?district=<id>&year=<y>`

Данные для районной страницы:

- `crop_types[]` — по каждой культуре: series, phenology, baseline, latest_ndvi, z-score, assessment
- `overall_series` — общая по району
- `overall_baseline`, `region_overall_series`
- `last_period_end`

**Лимит:** 30 req/min / IP. **Cache:** 5 мин Redis.

### `GET /api/report/district-detailed/?district=<id>&year=<y>`

Свод района по детальному мониторингу (S2/L8/fused):

- `coverage` — всего угодий / с детальными данными / площадь с данными
- `categories` — распределение полей по правилам скрининга
  (`anomaly` / `below` / `normal` / `nodata`), с количеством и площадью
- `overall_series` — area-weighted NDVI района, `baseline` — полоса нормы
- `crops[]` — по культурам: полей, площадь, взвешенный NDVI последних
  наблюдений, число проблемных, series
- `alerts_summary` — неразрешённые алерты за сезон по типам

**Лимит:** 30 req/min / IP. **Cache:** 10 мин Redis.

### `GET /api/report/screening/?district=<id>&year=<y>[&limit=<n>]`

Рейтинг угодий района по неблагополучию (детальный мониторинг).
Берётся **последнее** детальное наблюдение каждого поля
(`DISTINCT ON (farmland_id)`), балл = глубина провала под baseline
(`max(0, −z)`) + 2 × доля пикселей < 0.4 в гистограмме + неразрешённые
алерты (потолок 3).

- `farmlands[]` — топ-N худших: NDVI, z-score, low_pct, alerts, score,
  category (`anomaly`/`below`/`normal`), series для sparkline
- `farmlands_total`, `farmlands_with_data` — покрытие

**Параметры:** `limit` — размер топа, default 20, max 100.
**Лимит:** 30 req/min / IP. **Cache:** 10 мин Redis.

### `GET /api/report/unused/?district=<id>&year=<y>[&limit=<n>]`

Скрининг неиспользуемых земель (контроль ЗСН). Сверка заявленного
`is_used` со спутниковыми сигналами сезона по **всем** источникам
(вкл. MODIS, чтобы отсутствие безоблачных S2-сцен не давало ложных
срабатываний):

- `no_vegetation` — max NDVI сезона < 0.35
- `no_cycle` — амплитуда < 0.15 без детектированного SOS
- минимум 3 наблюдения, иначе поле вне анализа

Ответ:

- `suspects[]` — заявлено используемым/неизвестно, но есть сигналы
  (severity `high`/`medium`, сортировка high → площадь)
- `reactivated[]` — заявлено неиспользуемым, но max NDVI ≥ 0.5 + SOS
- `totals` — счётчики и площади, `thresholds` — использованные пороги

**Параметры:** `limit` — max строк на список, default 50, max 200.
**Лимит:** 30 req/min / IP. **Cache:** 10 мин Redis.

### `GET /api/report/farmland/?farmland=<id>&year=<y>`

Данные паспорта поля:

- `farmland` — атрибуты угодья (культура, площадь, кадастр, район)
- `detailed_series` — fused HLS если есть, иначе сырые S2/L8
  (`detailed_source` указывает источник); точки содержат по-пиксельные
  NDVI-гистограммы
- `modis_series` — референсный MODIS-ряд, `last_period_end`
- `baseline` — норма района для культуры поля
- `latest` — последнее наблюдение: NDVI, z-score, assessment
- `phenology` / `district_phenology` — SOS/POS/EOS/LOS поля против
  среднего по району
- `alerts[]` — алерты сезона

**Лимит:** 30 req/min / IP. **Cache:** 5 мин Redis.

---

## Rate limiting

Реализовано через `django-ratelimit` с Redis backend. Декоратор — в
`agrocosmos/views/_helpers.py`:

```python
@rate_limit('60/m')            # JSON 429 response
@rate_limit('300/m', binary=True)   # HTTP 429 без тела (для тайлов)
```

Ключ лимита — IP (`key='ip'`). Лимит шарится между всеми gunicorn-воркерами
через Redis. При превышении:

- Обычные endpoint'ы: `{"ok": false, "error": "rate limit exceeded", "rate": "30/m"}` + статус 429
- Tile endpoint'ы: пустой 429 response (фронтенд-map-библиотеки не умеют в JSON)

---

## Кеширование (`cache_page`)

Применяется к тяжёлым endpoint'ам. Ключ кеша — полный URL со всеми query-параметрами,
так что `?region=37&year=2025` и `?region=37&year=2024` кешируются отдельно.

TTL:
- `api_ndvi_stats`, `api_report_region`, `api_report_district`, `api_report_farmland`: **5 минут**
- `api_report_screening`, `api_report_district_detailed`, `api_report_unused`, `api_tile`: **10 минут**
- `api_report_country`: **15 минут**

**Инвалидация** после обновления NDVI-данных (MODIS pipeline):

```bash
# Очистить весь Redis (грубо, но надёжно — сессии тоже живут в Redis,
# так что пользователи разлогинятся — делать редко)
ssh root@10.0.0.10 "docker exec edunabazar-redis-1 redis-cli FLUSHDB"
```

Более точная инвалидация — `cache.delete_many()` по префиксам, пока не реализовано.

---

## Модели данных

Источник:  `agrocosmos/models.py`. Краткая сводка:

| Модель | Назначение | Ключевые поля |
|---|---|---|
| `Region` | Субъект РФ | `name`, `geometry` (MultiPolygon) |
| `District` | Муниципальный район | `region`, `name`, `geometry` |
| `Farmland` | Поле / угодье | `district`, `crop_type`, `area_ha`, `geometry`, `properties` (JSON) |
| `SatelliteScene` | Метаданные снимка | `satellite`, `acquired_date`, `cloud_cover` |
| `VegetationIndex` | Зональная статистика | `farmland`, `scene`, `index_type`, `acquired_date`, `mean`, `is_anomaly` |
| `NdviBaseline` | Исторический baseline | `district`, `crop_type`, `day_of_year`, `mean_ndvi`, `std_ndvi` |
| `FarmlandPhenology` | Фенология полигона | `farmland`, `year`, `sos_date`, `pos_date`, `eos_date`, `los_days` |

Индексы для производительности (`0015_perf_indexes`):

- `idx_vi_ndvi_farm_date` — partial на `VegetationIndex(farmland_id, acquired_date)`
  WHERE `index_type='ndvi' AND is_anomaly=false`
- `idx_scene_sat_date` — composite на `SatelliteScene(satellite, acquired_date)`

---

## HLS Fusion (Sentinel-2 + Landsat)

**Задача.** Закрыть пропуски в S2-ряде (5-дневный cadence) наблюдениями
Landsat 8/9 (16-дневный cadence, уже гармонизированный к S2-шкале
через Roy et al. 2016). Подход по мотивам NASA HLS (Harmonized Landsat
Sentinel).

**Команда:** `python manage.py compute_fused_ndvi --region-id <id> --year <y> [--overwrite]`

### Алгоритм

1. **Один SELECT** над `VegetationIndex` с фильтром
   `scene.satellite IN ('sentinel2', 'landsat8')` для указанной
   `(region, year)`.
2. **Группировка в памяти:** per-farmland — два списка `s2` и `l`
   (каждый элемент: `date, mean, valid_pixel_count`).
3. **Fusion:**
   - Для каждой S2-точки ищется **ближайшая** Landsat-запись в пределах
     `±8 дней`. Если есть — взвешенное среднее:
     `fused_mean = (s2.m * s2.n + l.m * l.n) / (s2.n + l.n)`.
   - Landsat-записи, **не попавшие** в ±8 дней ни от одной S2-точки,
     добавляются как самостоятельные fused-точки (gap-fill при долгих
     облачных периодах).
4. **Запись в БД:**
   - `SatelliteScene` — один на `(district, acquired_date)`, `satellite='hls_fused'`.
   - `VegetationIndex` — один на `(farmland, scene)`, `mean=fused_mean`,
     `valid_pixel_count = s2.n + l.n`.

### Параметры

| Параметр | Значение | Комментарий |
|---|---|---|
| Grid | S2-native (5 дней) | Плотный таймлайн |
| L pairing window | ±8 дней от центра S2 | Landsat revisit = 16 дней ⇒ окно покрывает половину цикла |
| Весы | `valid_pixel_count` | Пропорционально информативности (S2 10м ≫ L 30м по числу пикселей) |
| Валидация | Наследуется от источников | `min_valid_ratio=0.70` уже отработал в `zonal_stats` |

### Использование

```bash
# Собрать fused-ряд по региону за год
python manage.py compute_fused_ndvi --region-id 37 --year 2025

# Пересобрать (удалить старые записи и сделать заново)
python manage.py compute_fused_ndvi --region-id 37 --year 2025 --overwrite

# Dry-run: проверить сколько точек получится, без записи в БД
python manage.py compute_fused_ndvi --region-id 37 --year 2025 --dry-run

# Только один район
python manage.py compute_fused_ndvi --district-id 5 --year 2025
```

После прогона fused-ряд доступен в API:

```
GET /agrocosmos/api/ndvi-stats/?region=37&year=2025&source=fused
GET /agrocosmos/api/farmland/ndvi/?farmland=123&source=fused
```

### Идемпотентность

- Без `--overwrite`: `bulk_create(update_conflicts=True)` по
  `unique_fields=['farmland','scene','index_type']` — повторный прогон
  обновляет значения, не дублируя строки.
- С `--overwrite`: удаляются все `hls_fused` VI-записи и orphan-scene'ы
  за целевой `(region/district, year)`, затем строится заново.

Логирование каждого запуска — в `PipelineRun` (`task_type='raster_ndvi'`,
`description` содержит scope, `records_count` — число записанных VI,
`log` — traceback при ошибке).
