# Модель данных модуля «Агрокосмос»

> Источник истины: `@agrocosmos/models.py`.
> Все таблицы живут в PostgreSQL + PostGIS (VM2, база `enb_DB`).
> SRID геометрий — **4326** (WGS84).
> Документ: 2026-04-28.

---

## Оглавление

1. [Справочники территорий](#1-справочники-территорий)
   - [1.1 `Region` — субъект РФ](#11-region--субъект-рф--agro_region)
   - [1.2 `District` — муниципальный район](#12-district--муниципальный-район--agro_district)
2. [Земельный вектор](#2-земельный-вектор)
   - [2.1 `Farmland` — с/х угодье](#21-farmland--сх-угодье--agro_farmland)
3. [Спутниковые данные](#3-спутниковые-данные)
   - [3.1 `SatelliteScene` — сцена](#31-satellitescene--сцена-снимок--agro_satellite_scene)
   - [3.2 `VegetationIndex` — индекс по угодью](#32-vegetationindex--вегетационный-индекс-по-угодью--agro_vegetation_index)
   - [3.3 `NdviBaseline` — историческая норма](#33-ndvibaseline--историческая-норма-ndvi--agro_ndvi_baseline)
   - [3.4 `FarmlandPhenology` — фенология сезона](#34-farmlandphenology--фенология-сезона--agro_farmland_phenology)
4. [Мониторинг и алерты](#4-мониторинг-и-алерты)
   - [4.1 `MonitoringTask` — задача мониторинга](#41-monitoringtask--задача-мониторинга--agro_monitoring_task)
   - [4.2 `VegetationAlert` — алерт вегетации](#42-vegetationalert--алерт-вегетации--agro_vegetation_alert)
   - [4.3 `AgroSubscription` — подписка пользователя](#43-agrosubscription--подписка-пользователя--agro_subscription)
5. [Служебные таблицы](#5-служебные-таблицы)
   - [5.1 `PipelineRun` — лог запусков](#51-pipelinerun--лог-запусков-пайплайна--agro_pipeline_run)
   - [5.2 `GeeApiMetric` — квота GEE](#52-geeapimetric--дневная-статистика-google-earth-engine--agro_gee_api_metric)
6. [ER-диаграмма связей](#6-er-диаграмма-связей)
7. [Словари значений](#7-словари-значений)

---

## 1. Справочники территорий

### 1.1 `Region` — субъект РФ — `agro_region`

Список субъектов Российской Федерации с границами.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | Идентификатор субъекта |
| `name` | varchar(255) | нет | Название (напр. «Московская область», «Республика Крым») |
| `code` | varchar(100), uniq | нет | Машинный код субъекта (напр. `moskovskaya_obl`) — используется в staging-таблицах и URL |
| `osm_id` | bigint, uniq | да | ID relation'а в OpenStreetMap (источник геометрий) |
| `geom` | MultiPolygon (SRID 4326) | нет | Административные границы |
| `created_at` | timestamp | нет | Когда запись появилась в БД |

**Использование.** Главный справочник. На него ссылаются `District`, `Farmland`, `MonitoringTask`, `PipelineRun`, `AgroSubscription`.

---

### 1.2 `District` — муниципальный район — `agro_district`

Муниципальные районы / городские округа внутри субъекта.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | Идентификатор района |
| `region_id` | FK → `Region` | нет | К какому субъекту относится (`ON DELETE CASCADE`) |
| `name` | varchar(255) | нет | Название района |
| `code` | varchar(150) | да (по умолчанию `""`) | Машинный код района |
| `osm_id` | bigint, uniq | да | ID relation'а в OSM |
| `geom` | MultiPolygon (SRID 4326) | нет | Границы района |
| `created_at` | timestamp | нет | Когда добавлен |

**Использование.** Назначается угодьям постфактум (spatial-join в команде `assign_farmland_district`). Используется как скоуп подписок и мониторинга.

---

## 2. Земельный вектор

### 2.1 `Farmland` — с/х угодье — `agro_farmland`

Один полигон сельхоз. угодья (пашня / пастбище / сенокос / многолетние насаждения / залежь). Главная таблица модуля — на неё опираются все агрегаты NDVI, алерты и отчёты.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | Идентификатор угодья |
| `region_id` | FK → `Region` | да (`SET NULL`) | Субъект РФ |
| `district_id` | FK → `District` | да (`SET NULL`) | Муниципальный район. Назначается постфактум spatial-join'ом |
| `crop_type` | varchar(20), enum | нет | Вид угодья, см. [словарь CropType](#crop-type-вид-угодья) |
| `is_used` | boolean | да | Факт использования: `True` — используется, `False` — заброшено/не используется, `NULL` — неизвестно |
| `cadastral_number` | varchar(50) | да (`""`) | Кадастровый номер (если есть в источнике) |
| `area_ha` | float | нет (0) | Площадь, гектары |
| `geom` | MultiPolygon (SRID 4326) | нет | Границы угодья |
| `properties` | JSONB | да | Сырые исходные атрибуты для аудита (напр. `{"Neisp_All": "пашня обрабатываемая", "name_raw": "Пашня"}`) |
| `source` | varchar(40) | да (`""`) | Идентификатор исходной схемы (напр. `mosobl_2021`, `rosreestr_zsn/altai`) — позволяет перезалить один источник не трогая остальные |
| `created_at` | timestamp | нет | Когда загружено |

**Индексы.** `(region, crop_type)`, `(region, is_used)`, `(district, crop_type)`, `cadastral_number`.

**Источники загрузки.**
- Шейпы Росреестра ЗСН 5.3 — команда `import_farmlands_rosreestr` (~19.6 млн полигонов, 73 субъекта).
- Одиночные GeoJSON-выгрузки (как Московская область 2021) — команда `import_farmland_geojson`.

---

## 3. Спутниковые данные

### 3.1 `SatelliteScene` — сцена (снимок) — `agro_satellite_scene`

**Метаданные** (не пиксели) спутникового снимка. Сами растры лежат на диске (`/data/modis`) или скачиваются через GEE — в БД хранится только ссылка и охват.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `satellite` | varchar(20), enum | нет | Платформа, см. [словарь Satellite](#satellite-платформа-снимка) |
| `scene_id` | varchar(255), uniq | нет | Идентификатор сцены провайдера (напр. `S2A_MSIL2A_20250615T083601_N0511_R064_T37UDB`) |
| `acquired_date` | date | нет | Дата съёмки |
| `cloud_cover` | float | нет (0) | Процент облачности по сцене |
| `bbox` | Polygon (SRID 4326) | да | Охват сцены |
| `file_path` | varchar(500) | да (`""`) | Локальный путь к растру (для MODIS-архива) |
| `metadata` | JSONB | да | Всё остальное от провайдера (band info, QA, solar angles…) |
| `processed` | boolean | нет (false) | Обработана ли сцена пайплайном (признак, что индексы посчитаны) |
| `created_at` | timestamp | нет | Когда добавлена |

**Индексы.** `(satellite, acquired_date)` — базовый индекс для всех дашборд/отчётных выборок, где идёт JOIN с `VegetationIndex` и фильтрация по типу спутника.

---

### 3.2 `VegetationIndex` — вегетационный индекс по угодью — `agro_vegetation_index`

Зональная статистика вегетационного индекса по конкретному угодью на конкретный снимок. **Главный источник данных для графиков NDVI и алертов.** Самая большая таблица в БД по числу строк.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | bigint (PK) | — | — |
| `farmland_id` | FK → `Farmland` | нет (`CASCADE`) | К какому угодью относится |
| `scene_id` | FK → `SatelliteScene` | нет (`CASCADE`) | К какой сцене относится |
| `index_type` | varchar(10), enum | нет | Тип индекса, см. [словарь IndexType](#indextype-тип-индекса) |
| `acquired_date` | date | нет | Дата съёмки (денормализация от `scene.acquired_date` — для быстрых индексных выборок) |
| `mean` | float | нет | Среднее значение по пикселям угодья |
| `median` | float | нет (0) | Медиана |
| `min_val` | float | нет (0) | Минимум |
| `max_val` | float | нет (0) | Максимум |
| `std_val` | float | нет (0) | Стандартное отклонение |
| `pixel_count` | integer | нет (0) | Всего пикселей, попавших на угодье |
| `valid_pixel_count` | integer | нет (0) | Валидных (без масок облаков / SCL / nodata) |
| `is_outlier` | boolean | нет (false) | **Технический** выброс (облако / снег / блик) — исключается из сглаживания. Это НЕ биологическая аномалия — для тех есть `VegetationAlert` |
| `mean_smooth` | float | да | Сглаженное (Савицкий-Голай) NDVI — именно оно идёт в график и в детектор аномалий |
| `created_at` | timestamp | нет | Когда рассчитано |

**Ограничения.** `UNIQUE(farmland, scene, index_type)` — один ряд на связку.

**Индексы.**
- `(farmland, index_type, acquired_date)` — обычная выборка «временной ряд NDVI по угодью».
- `(acquired_date, index_type)` — по-дате (для отчётов).
- Партиальный `(farmland, acquired_date) WHERE index_type='ndvi' AND is_outlier=false` — покрывает 95% «горячих» запросов дашборда.

---

### 3.3 `NdviBaseline` — историческая норма NDVI — `agro_ndvi_baseline`

Среднемноголетнее NDVI по району на каждый день года (day-of-year, 1–366). Используется детектором аномалий для z-score и на графиках как «норма». Пересчитывается раз в год (7 января) по всем годам кроме текущего.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `district_id` | FK → `District` | нет (`CASCADE`) | Район |
| `day_of_year` | smallint | нет | День года, 1–366 |
| `mean_ndvi` | float | нет | Средний NDVI по всем годам |
| `std_ndvi` | float | нет (0) | Стандартное отклонение между годами |
| `years_count` | smallint | нет (0) | Сколько лет входит в расчёт |
| `crop_type` | varchar(20) | да (`""`) | Вид угодья — пусто значит «по всем сразу» |
| `updated_at` | timestamp | нет | Последний пересчёт |

**Ограничения.** `UNIQUE(district, day_of_year, crop_type)`.

---

### 3.4 `FarmlandPhenology` — фенология сезона — `agro_farmland_phenology`

Фенологические метрики по угодью за конкретный год (по сглаженному ряду NDVI).

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `farmland_id` | FK → `Farmland` | нет (`CASCADE`) | Угодье |
| `year` | integer | нет | Год сезона |
| `source` | varchar(10), enum | нет | `modis` (16-дневный архив) или `raster` (S2/L8 оперативный) |
| `sos_date` | date | да | Start Of Season — начало вегетации |
| `eos_date` | date | да | End Of Season — конец вегетации |
| `pos_date` | date | да | Peak Of Season — пик |
| `max_ndvi` | float | да | Максимум NDVI за сезон |
| `mean_ndvi` | float | да | Среднее за сезон |
| `los_days` | integer | да | Length Of Season — длительность вегетации в днях |
| `total_ndvi` | float | да | Time Integral — интеграл NDVI за сезон (продуктивность) |
| `created_at` | timestamp | нет | Когда рассчитано |

**Ограничения.** `UNIQUE(farmland, year, source)`.

---

## 4. Мониторинг и алерты

### 4.1 `MonitoringTask` — задача мониторинга — `agro_monitoring_task`

Подписка «пайплайна» на регулярный пересчёт NDVI для региона (или конкретного района). Воркеры опрашивают активные задачи и тянут свежие сцены.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `task_type` | varchar(20), enum | нет | `modis` (архив 16 дн.) или `raster` (оперативный S2+L8) |
| `region_id` | FK → `Region` | нет (`CASCADE`) | Субъект |
| `district_id` | FK → `District` | да (`CASCADE`) | Опционально — только один район |
| `year` | integer | нет | Год мониторинга |
| `status` | varchar(20), enum | нет | `active` / `paused` / `completed` |
| `last_check` | timestamp | да | Когда воркер последний раз проверял задачу |
| `last_date_to` | date | да | До какой даты доведён пересчёт |
| `records_total` | integer | нет (0) | Счётчик обработанных строк |
| `log` | text | да (`""`) | Короткий лог последнего прогона |
| `created_at` / `updated_at` | timestamp | нет | — |

**Ограничения.** `UNIQUE(task_type, region, district, year)` — одна запись на скоуп.

---

### 4.2 `VegetationAlert` — алерт вегетации — `agro_vegetation_alert`

**Биологическое** отклонение, обнаруженное детектором `detect_vegetation_alerts` по сглаженному ряду NDVI против `NdviBaseline`. НЕ путать с техническим выбросом `VegetationIndex.is_outlier` (снег/облако).

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `farmland_id` | FK → `Farmland` | нет (`CASCADE`) | Угодье |
| `alert_type` | varchar(30), enum | нет | `baseline_deviation` — несколько наблюдений подряд ниже нормы (z ≤ −1.5); `rapid_drop` — падение NDVI ≥ 0.15 за ~16 дней |
| `severity` | varchar(10), enum | нет | `warning` / `critical` |
| `status` | varchar(15), enum | нет | `active` / `acknowledged` / `resolved` |
| `detected_on` | date | нет | Дата наблюдения, спровоцировавшего алерт |
| `triggered_at` | timestamp | нет | Когда алерт создан |
| `acknowledged_at` | timestamp | да | Когда принят оператором |
| `acknowledged_by_id` | FK → `auth.User` | да (`SET NULL`) | Кто принял |
| `resolved_at` | timestamp | да | Когда разрешён |
| `context` | JSONB | да | z-score, значения NDVI, baseline и т.п. |
| `message` | varchar(500) | да (`""`) | Человеко-читаемое описание |

**Индексы.** `(status, -triggered_at)` — «активные сверху» на панели; `(farmland, alert_type, status)`.

---

### 4.3 `AgroSubscription` — подписка пользователя — `agro_subscription`

Подписка пользователя кабинета (legacy-маркетплейса) на уведомления по региону или конкретному району.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `legacy_user_id` | integer | нет | `legacy_user.id` (unmanaged FK без constraint — Django не строит миграции для `managed=False`) |
| `region_id` | FK → `Region` | да (`CASCADE`) | Субъект (если скоуп — весь регион, `district` пустой) |
| `district_id` | FK → `District` | да (`CASCADE`) | Район (если скоуп — один район) |
| `notify_anomalies` | boolean | нет (true) | Слать email при появлении `VegetationAlert` |
| `notify_updates` | boolean | нет (false) | Слать ежедневный дайджест при появлении свежих `VegetationIndex` |
| `last_update_notified_at` | timestamp | да | Когда последний раз отправили дайджест |
| `created_at` / `updated_at` | timestamp | нет | — |

**Ограничения.**
- `CHECK (region_id IS NOT NULL OR district_id IS NOT NULL)` — подписка обязана иметь скоуп.
- `UNIQUE(legacy_user_id, region, district)` — одна подписка на скоуп от пользователя.

---

## 5. Служебные таблицы

### 5.1 `PipelineRun` — лог запусков пайплайна — `agro_pipeline_run`

Каждый запуск любого процесса (загрузка региона, расчёт NDVI, мониторинг) пишется сюда.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `task_type` | varchar(30), enum | нет | `upload_region` / `upload_districts` / `upload_farmlands` / `archive_ndvi` / `raster_ndvi` / `monitoring` |
| `status` | varchar(20), enum | нет | `queued` / `running` / `completed` / `failed` |
| `launch_args` | JSONB | нет (`{}`) | CLI-аргументы для воркера (используется при `status=queued`) |
| `region_id` | FK → `Region` | да (`SET NULL`) | Скоуп регионом (если применимо) |
| `year` | integer | да | Год |
| `description` | varchar(500) | да (`""`) | Человеко-читаемое описание |
| `log` | text | да (`""`) | Вывод процесса |
| `records_count` | integer | нет (0) | Сколько строк обработано |
| `started_at` | timestamp | нет | — |
| `finished_at` | timestamp | да | — |
| `pid` | integer | да | PID отсоединённого процесса |
| `log_file` | varchar(255) | да (`""`) | Путь к файлу лога на диске |
| `heartbeat_at` | timestamp | да | Последний heartbeat воркера |

### 5.2 `GeeApiMetric` — дневная статистика Google Earth Engine — `agro_gee_api_metric`

У Earth Engine **нет** публичного quota-API — считаем расходы сами.

| Поле | Тип | NULL | Описание |
|------|-----|:---:|----------|
| `id` | integer (PK) | — | — |
| `day` | date, uniq | нет | Сутки |
| `calls` | bigint | нет (0) | Успешных вызовов `computePixels` |
| `errors` | integer | нет (0) | Ошибок |
| `throttled` | integer | нет (0) | Переповторов из-за rate-limit |
| `bytes_downloaded` | bigint | нет (0) | Объём скачанных данных |
| `last_error` | text | да (`""`) | Текст последней ошибки |
| `updated_at` | timestamp | нет | — |

---

## 6. ER-диаграмма связей

```
                ┌─────────┐
                │ Region  │◄───────────────┐
                └────┬────┘                │
                     │ 1:N                 │ 1:N (SET NULL)
                     ▼                     │
                ┌─────────┐                │
                │District │◄──────────┐    │
                └────┬────┘           │    │
                     │ 1:N (SET NULL) │    │
                     │                │    │
                     ▼                │    │
                ┌────────┐            │    │
                │Farmland├────────────┼────┘
                └───┬────┘            │
                    │ 1:N (CASCADE)   │
         ┌──────────┼─────────────────┤
         ▼          ▼                 ▼
  ┌────────────┐ ┌───────────┐ ┌────────────────┐
  │Vegetation  │ │Farmland   │ │Vegetation      │
  │Index       │ │Phenology  │ │Alert           │
  └─────┬──────┘ └───────────┘ └────────────────┘
        │ N:1
        ▼
  ┌────────────┐
  │Satellite   │
  │Scene       │
  └────────────┘

  ┌────────────┐     ┌────────────┐
  │NdviBaseline│     │Monitoring  │  ┌──────────────┐
  │ (district) │     │Task        │  │AgroSubscri-  │
  └────────────┘     └────────────┘  │ption         │
                                     └──────────────┘
```

---

## 7. Словари значений

### `CropType` (вид угодья)

Используется в `Farmland.crop_type` и `NdviBaseline.crop_type`.

| Код | Лейбл |
|-----|-------|
| `arable` | Пашня |
| `fallow` | Залежь |
| `hayfield` | Сенокос |
| `pasture` | Пастбище |
| `perennial` | Многолетние насаждения |
| `other_agri` | Иные с.-х. земли |
| `other` | Прочее |

### `Satellite` (платформа снимка)

| Код | Описание |
|-----|----------|
| `sentinel2` | Sentinel-2 (ESA, 10–20 м) |
| `landsat8` | Landsat 8 (NASA/USGS, 30 м) |
| `landsat9` | Landsat 9 |
| `modis_terra` | MODIS Terra (250 м) |
| `modis_aqua` | MODIS Aqua |
| `hls_fused` | HLS Fused — Harmonized Sentinel-2 + Landsat |

### `IndexType` (тип индекса)

| Код | Расшифровка |
|-----|-------------|
| `ndvi` | Normalized Difference Vegetation Index — основной индекс вегетации |
| `evi` | Enhanced Vegetation Index — устойчивее к атмосфере |
| `msavi` | Modified Soil-Adjusted VI — хорош для ранних фаз (много голой земли) |
| `ndwi` | Normalized Difference Water Index — влагосодержание |
| `ndmi` | Normalized Difference Moisture Index — влага в листьях |

### `Farmland.is_used` (факт использования)

| Значение | Смысл |
|----------|-------|
| `TRUE` | Используется по назначению (обрабатывается / есть с/х деятельность) |
| `FALSE` | Не используется (заросло, залесено, нет следов с/х > 3 лет, нецелевое использование) |
| `NULL` | Источник данных не содержит информации |

### `VegetationIndex.is_outlier` vs `VegetationAlert`

Это **разные** понятия, их легко перепутать:

| Признак | `VegetationIndex.is_outlier` | `VegetationAlert` |
|---------|------------------------------|-------------------|
| Природа | Технический выброс | Биологическая аномалия |
| Источник | Алгоритм сглаживания (облака, снег, маски) | Детектор `detect_vegetation_alerts` |
| Следствие | Исключается из Савицкого-Голая | Создаётся запись в `agro_vegetation_alert`, шлётся уведомление подписчикам |
| Пример | «12 июня 2025 — облако закрыло поле» | «С 5 по 25 июля NDVI на 2σ ниже нормы района» |

---

## Приложение. Источники загрузки данных

| Слой / таблица | Источник | Команда загрузки |
|----------------|----------|------------------|
| `Region` | OSM relation id | `import_regions` |
| `District` | OSM relation id | `import_districts` |
| `Farmland` (Росреестр ЗСН 5.3) | Шейпы Минсельхоза | `import_farmlands_rosreestr` |
| `Farmland` (отдельные GeoJSON) | Региональные выгрузки (напр. Московская область 2021) | `import_farmland_geojson` |
| `Farmland.district_id` | Spatial-join с `District.geom` | `assign_farmland_district` |
| `SatelliteScene` + `VegetationIndex` (MODIS архив) | MOD13Q1/MYD13Q1 с диска | `import_modis_ndvi` / `calculate_vegetation_indices` |
| `SatelliteScene` + `VegetationIndex` (S2/L8 оперативно) | Google Earth Engine | `run_raster_pipeline` |
| `NdviBaseline` | Пересчёт по `VegetationIndex` | `recompute_ndvi_baselines` (ежегодно 7 января) |
| `FarmlandPhenology` | По сглаженному ряду NDVI | `compute_phenology` |
| `VegetationAlert` | Детектор по `mean_smooth` vs `NdviBaseline` | `detect_vegetation_alerts` |
