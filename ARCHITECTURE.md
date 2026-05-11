# Архитектура проекта «Еду на базар» (edunabazar)

> **Репозиторий:** <https://github.com/RuKi81/edunabazar>
> **Домен:** edunabazar.ru / www.edunabazar.ru
> **Дата:** 2026-05-11

> См. также: [`README.md`](./README.md) (overview, quick start) ·
> [`docs/AGROCOSMOS_API.md`](./docs/AGROCOSMOS_API.md) (API reference) ·
> [`docs/AGROCOSMOS_DATA_MODEL.md`](./docs/AGROCOSMOS_DATA_MODEL.md) (полная схема таблиц + cron-карта)

---

## 1. Общая схема инфраструктуры

```
                  ┌─────────────────────────────────────────────┐
                  │            Интернет / Пользователь           │
                  └────────────────────┬────────────────────────┘
                                       │  :80 / :443
                  ┌────────────────────▼────────────────────────┐
                  │   PVE-шлюз (195.47.196.46)                  │
                  │   DNAT :80→10.0.0.10:80, :443→10.0.0.10:443│
                  └────────────────────┬────────────────────────┘
                                       │  NAT
                  ┌────────────────────▼────────────────────────┐
                  │   VM1 — App-сервер (10.0.0.10)              │
                  │                                             │
                  │  ┌─────────┐   ┌──────────┐  ┌───────────┐ │
                  │  │  Nginx  │──▶│ Gunicorn  │  │   Redis   │ │
                  │  │ :80/:443│   │  :8000    │  │   :6379   │ │
                  │  └─────────┘   └──────────┘  └───────────┘ │
                  │  ┌──────────┐                               │
                  │  │ Certbot  │  (Let's Encrypt)              │
                  │  └──────────┘                               │
                  └────────────────────┬────────────────────────┘
                                       │  :5432 (TCP)
                  ┌────────────────────▼────────────────────────┐
                  │   VM2 — DB-сервер (10.0.0.11)               │
                  │                                             │
                  │  ┌────────────────────────────────────────┐ │
                  │  │  PostgreSQL 16 + PostGIS 3.4 (Docker)  │ │
                  │  │  :5432                                 │ │
                  │  └────────────────────────────────────────┘ │
                  └─────────────────────────────────────────────┘
```

---

## 2. Серверы и IP-адреса

| Роль | Внешний IP | Внутренний IP | Доступ по SSH | ОС |
|------|-----------|--------------|---------------|---|
| **PVE-шлюз** | `195.47.196.46` | — | `ssh root@195.47.196.46` | Proxmox VE |
| **VM1 — App-сервер** | — (за NAT) | `10.0.0.10` | `ssh root@195.47.196.46 "ssh root@10.0.0.10"` | Ubuntu (Docker) |
| **VM2 — DB-сервер** | — (за NAT) | `10.0.0.11` | `ssh root@195.47.196.46 "ssh root@10.0.0.11"` | Ubuntu (Docker) |

### Открытые порты

| Сервер | Порт | Протокол | Сервис | Доступ |
|--------|------|----------|--------|--------|
| VM1 | 80 | TCP | Nginx (HTTP → HTTPS redirect) | Публичный |
| VM1 | 443 | TCP | Nginx (HTTPS) | Публичный |
| VM1 | 22 | TCP | SSH | Администрирование |
| VM2 | 5432 | TCP | PostgreSQL | Только с VM1 (`195.47.196.46`) |
| VM2 | 22 | TCP | SSH | Администрирование |

---

## 3. Стек технологий

| Компонент | Технология | Версия |
|-----------|-----------|--------|
| Язык | Python | 3.13 |
| Фреймворк | Django | 5.1.15 |
| WSGI-сервер | Gunicorn | latest |
| Веб-сервер / реверс-прокси | Nginx | 1.27-alpine |
| БД | PostgreSQL + PostGIS | 16 + 3.4 |
| Кэш / сессии | Redis | 7-alpine |
| SSL | Let's Encrypt (Certbot) | — |
| Контейнеризация | Docker / Docker Compose | — |
| CI/CD | GitHub Actions | — |
| REST API | Django REST Framework + drf-spectacular | — |
| Геоданные | GeoDjango (GDAL, GEOS, PostGIS) | — |
| Email | Yandex SMTP (`smtp.yandex.ru:587`) | — |
| SMS | SMSC.ru | — |
| LLM (рерайт новостей) | GigaChat (Sber) | — |

---

## 4. Структура проекта

```
edunabazar/
├── enb_django/              # Конфигурация Django-проекта
│   ├── settings.py
│   ├── urls.py
│   ├── wsgi.py / asgi.py
│
├── legacy/                  # Основное приложение (маркетплейс)
│   ├── models.py            # Advert, LegacyUser, Seller, Catalog, Categories,
│   │                        #   Review, Message, News, EmailCampaign, Favorite, ...
│   ├── views.py
│   ├── api.py / api_urls.py # REST API v1
│   ├── templates/legacy/
│   ├── static/legacy/
│   └── management/commands/ # Management-команды (новости, рассылки и т.д.)
│
├── agrocosmos/              # Приложение «Агрокосмос» (ГИС + спутники)
│   ├── models.py            # Region, District, Farmland, SatelliteScene,
│   │                        #   VegetationIndex, NdviBaseline, FarmlandPhenology,
│   │                        #   DistrictNdviStatus, DistrictNdviSeries,
│   │                        #   MonitoringTask, PipelineRun, GeeApiMetric,
│   │                        #   AgroSubscription, VegetationAlert
│   ├── views/               # Views-пакет (разделён по доменам)
│   │   ├── __init__.py      #   — re-export для обратной совместимости
│   │   ├── _helpers.py      #   — константы, rate_limit, satellite_filter
│   │   ├── pages.py         #   — HTML-страницы (dashboard, report_*)
│   │   ├── geojson.py       #   — GeoJSON endpoints (regions/districts/farmlands)
│   │   ├── tiles.py         #   — MVT + raster PNG tiles
│   │   ├── ndvi.py          #   — NDVI time series, stats, phenology
│   │   ├── reports.py       #   — данные для отчётов region/district
│   │   └── cabinet.py       #   — страница /me/agrocosmos/ (подписки)
│   ├── services/
│   │   ├── district_ndvi_series.py  # Предагрегат per-district×date×crop
│   │   ├── notifications.py         # Email для алертов и дайджестов
│   │   └── gee_client.py            # Google Earth Engine wrapper
│   ├── static/agrocosmos/js/ndvi_chart.js  # Общий helper для Chart.js
│   ├── templates/agrocosmos/
│   └── management/commands/
│
├── deploy/                  # Деплой-скрипты и конфигурации
│   ├── nginx.conf
│   ├── deploy.sh
│   ├── setup-app.sh         # Первоначальная настройка VM1
│   ├── setup-db.sh          # Первоначальная настройка VM2
│   └── db/
│       ├── docker-compose.yml
│       ├── pg_hba_custom.conf
│       ├── backup.sh
│       └── .env.example
│
├── .github/workflows/ci.yml # CI/CD pipeline
├── docker-compose.yml       # Продакшен (только web-сервис, БД внешняя)
├── Dockerfile
├── requirements.txt
├── .env.example
└── manage.py
```

---

## 5. Django-приложения и модели

### 5.1 `legacy` — Маркетплейс объявлений

| Модель | Таблица | managed | Описание |
|--------|---------|---------|----------|
| `Advert` | `advert` | **False** | Объявление (предложение/спрос) |
| `AdvertPhoto` | `advert_photo` | True | Фото объявления |
| `Catalog` | `catalog` | **False** | Каталог (группа категорий) |
| `Categories` | `categories` | **False** | Категория товара |
| `Seller` | `seller` | **False** | Профиль продавца |
| `LegacyUser` | `legacy_user` | **False** | Пользователь (legacy) |
| `Review` | `review` | **False** | Отзыв |
| `Message` | `message` | True | Сообщение между пользователями |
| `News` | `news` | True | Агро-новость (RSS → GigaChat рерайт) |
| `NewsKeyword` | `news_keyword` | True | Ключевое слово для фильтрации новостей |
| `NewsFeedSource` | `news_feed_source` | True | RSS-источник новостей |
| `EmailCampaign` | `email_campaign` | True | Email-рассылка |
| `EmailLog` | `email_log` | True | Лог отправки email |
| `Favorite` | `favorite` | True | Избранное |
| `AdvertView` | `advert_view` | True | Просмотр объявления |

> Модели с `managed = False` — унаследованы из старой PHP-базы, Django не управляет их миграциями.

### 5.2 `agrocosmos` — ГИС-модуль (спутниковый мониторинг)

| Модель | Таблица | Описание |
|--------|---------|----------|
| `Region` | `agro_region` | Субъект РФ (MultiPolygon) |
| `District` | `agro_district` | Муниципальный район (MultiPolygon) |
| `Farmland` | `agro_farmland` | Полигон сельхоз. угодья (пашня, пастбище, сенокос, многолетние насаждения, залежь) |
| `SatelliteScene` | `agro_satellite_scene` | Метаданные спутникового снимка (Sentinel-2, Landsat, MODIS) |
| `VegetationIndex` | `agro_vegetation_index` | Зональная статистика вегетационных индексов по угодью (+ `mean_smooth`, `is_outlier`) |
| `NdviBaseline` | `agro_ndvi_baseline` | Историческое среднее NDVI района на каждый день года (по всем годам кроме текущего) |
| `FarmlandPhenology` | `agro_farmland_phenology` | Фенология по угодью на сезон (SOS/POS/EOS) из `mean_smooth` |
| `DistrictNdviStatus` | `agro_district_ndvi_status` | Кешированный текущий NDVI района для all-Russia choropleth (см. §14.4) |
| `DistrictNdviSeries` | `agro_district_ndvi_series` | Area-weighted NDVI по района×дате×культуре — питает дашборд-графики (см. §14.5) |
| `MonitoringTask` | `agro_monitoring_task` | Активная задача периодического NDVI-мониторинга для региона |
| `PipelineRun` | `agro_pipeline_run` | Лог запуска пайплайнов (MODIS, S2+L8, backfill, baselines) |
| `GeeApiMetric` | `agro_gee_api_metric` | Дневной счётчик вызовов Google Earth Engine (`computePixels`) + трафик |
| `AgroSubscription` | `agro_subscription` | Подписка `LegacyUser` на email-уведомления по региону/району (см. §15) |
| `VegetationAlert` | `agro_vegetation_alert` | Биологическая аномалия на угодье **или** районе/культуре (baseline deviation / rapid drop) (см. §15) |

---

## 6. URL-маршруты

| Путь | Назначение |
|------|-----------|
| `/` | Главная страница (legacy) |
| `/admin/` | Django Admin |
| `/api/v1/` | REST API v1 (legacy) |
| `/api/docs/` | Swagger UI |
| `/api/redoc/` | ReDoc |
| `/api/schema/` | OpenAPI-схема |
| `/agrocosmos/` | Модуль Агрокосмос |
| `/healthz` | Health-check (для Docker / мониторинга) |
| `/robots.txt` | SEO |
| `/sitemap.xml` | Sitemap index (ссылки на sub-sitemaps) |
| `/sitemap-static.xml` | Статические страницы + каталоги/категории |
| `/sitemap-adverts.xml` | Все объявления |
| `/sitemap-sellers.xml` | Все продавцы |
| `/sitemap-news.xml` | Все новости |
| `/turbo-rss.xml` | Yandex Turbo RSS |

---

## 7. Доступы и переменные окружения

### 7.1 Файл `.env` на VM1 (App-сервер)

| Переменная | Описание | Пример (прод) |
|-----------|----------|---------------|
| `DJANGO_SECRET_KEY` | Секретный ключ Django | *(случайная строка 50+ символов)* |
| `DJANGO_DEBUG` | Режим отладки | `0` |
| `DJANGO_ALLOWED_HOSTS` | Разрешённые хосты | `195.47.196.46 edunabazar.ru www.edunabazar.ru` |
| `DJANGO_ADMIN_USERS` | Логины админов | `admin` |
| `DB_HOST` | Хост БД | `10.0.0.11` |
| `DB_PORT` | Порт БД | `5432` |
| `DB_NAME` | Имя БД | `enb_DB` |
| `DB_USER` | Пользователь БД | `enb_app` |
| `DB_PASSWORD` | Пароль БД | *(секрет)* |
| `REDIS_URL` | URL Redis | `redis://redis:6379/0` *(задаётся в compose)* |
| `EMAIL_BACKEND` | Email-бэкенд | `django.core.mail.backends.smtp.EmailBackend` |
| `EMAIL_HOST` | SMTP-сервер | `smtp.yandex.ru` |
| `EMAIL_PORT` | SMTP-порт | `587` |
| `EMAIL_USE_TLS` | TLS | `1` |
| `EMAIL_HOST_USER` | Email-логин | `edunabazar2017@yandex.ru` |
| `EMAIL_HOST_PASSWORD` | Email-пароль | *(секрет — пароль приложения Яндекс)* |
| `DEFAULT_FROM_EMAIL` | Адрес отправителя | `edunabazar2017@yandex.ru` |
| `SMSC_LOGIN` | Логин SMSC.ru | *(секрет)* |
| `SMSC_PASSWORD` | Пароль SMSC.ru | *(секрет)* |
| `SMSC_SENDER` | Отправитель SMS | *(настроить)* |
| `GIGACHAT_AUTH_KEY` | Ключ GigaChat (base64) | *(секрет)* |
| `CSRF_TRUSTED_ORIGINS` | Trusted origins | `https://edunabazar.ru,https://www.edunabazar.ru` |
| `SECURE_SSL_REDIRECT` | Редирект на HTTPS | `1` |
| `SECURE_HSTS_SECONDS` | HSTS | `31536000` |

### 7.2 Файл `.env` на VM2 (DB-сервер)

| Переменная | Описание | Пример |
|-----------|----------|--------|
| `DB_NAME` | Имя БД | `enb_DB` |
| `DB_USER` | Пользователь БД | `enb_app` |
| `DB_PASSWORD` | Пароль БД | *(секрет — должен совпадать с VM1)* |

### 7.3 GitHub Actions Secrets

| Секрет | Описание |
|--------|----------|
| `SERVER_HOST` | IP app-сервера (`195.47.196.46`) |
| `SERVER_USER` | SSH-пользователь |
| `SERVER_SSH_KEY` | Приватный SSH-ключ для деплоя |
| `SERVER_PORT` | SSH-порт (по умолчанию `22`) |

---

## 8. Доступ к БД (pg_hba)

Файл `deploy/db/pg_hba_custom.conf` ограничивает сетевой доступ к PostgreSQL:

| Тип | БД | Пользователь | Адрес | Метод |
|-----|-----|-------------|-------|-------|
| local | all | all | — | trust |
| host | all | all | 127.0.0.1/32 | scram-sha-256 |
| host | all | all | ::1/128 | scram-sha-256 |
| host | enb_DB | enb_app | **195.47.196.46/32** | scram-sha-256 |
| host | all | all | 0.0.0.0/0 | **reject** |

Дополнительно: UFW на VM2 разрешает порт `5432` только с `195.47.196.46`.

---

## 9. Docker-контейнеры (прод)

**VM1** (10.0.0.10):

`docker-compose.yml` содержит **только** сервис `web`. Остальные контейнеры — orphan (созданы отдельно).

| Контейнер | Образ | Порты | В compose | Назначение |
|-----------|-------|-------|-----------|----------|
| `edunabazar-web-1` | *(build из Dockerfile)* | 8000 | **Да** | Django + Gunicorn |
| `edunabazar-nginx-1` | `nginx:1.27-alpine` | 80, 443 | Нет (orphan) | Реверс-прокси, статика, SSL |
| `edunabazar-redis-1` | `redis:7-alpine` | 6379 | Нет (orphan) | Кэш + сессии |
| `edunabazar-certbot-1` | `certbot/certbot` | — | Нет (orphan) | SSL-сертификаты |

> ⚠️ БД НЕ в Docker на VM1. Используется внешняя БД на VM2 (10.0.0.11), настроенная через `.env`.
> **Не добавлять** `db` сервис в `docker-compose.yml` — это создаст пустую локальную БД и сломает сайт.

### Nginx — монтирование файлов

| Хост (VM1) | Контейнер | Режим |
|---|---|---|
| `/opt/edunabazar/deploy/nginx.conf` | `/etc/nginx/conf.d/default.conf` | ro |
| `/opt/edunabazar/deploy/certbot/conf` | `/etc/letsencrypt` | ro |
| `/opt/edunabazar/deploy/certbot/www` | `/var/www/certbot` | — |

**VM2** (`deploy/db/docker-compose.yml`):

| Контейнер | Образ | Порты | Назначение |
|-----------|-------|-------|-----------|
| `db` | `postgis/postgis:16-3.4` | 5432 | PostgreSQL + PostGIS |

---

## 10. CI/CD Pipeline

```
push/PR → main/master
    │
    ├── [lint]     Flake8 (синтаксис, импорты, сложность)
    │
    ├── [test]     PostGIS service → migrate → django check → manage.py test legacy
    │
    └── [docker-build] (только main/master, после lint+test)
         │
         └── [deploy] SSH → VM1:
               git pull → docker compose build → up -d --no-deps → collectstatic
```

### Ручной деплой (с PVE-шлюза)

```bash
ssh root@10.0.0.10 "cd /opt/edunabazar && git pull && docker compose build web && docker compose up -d --no-deps web && docker restart edunabazar-nginx-1"
```

> ⚠️ **Обязательно:**
> - Использовать `--no-deps` чтобы не создать лишний db-контейнер
> - После пересоздания web **перезапустить nginx** (он кеширует DNS upstream)

### Перевыпуск SSL-сертификата

```bash
# 1. Временно переключить nginx на HTTP-only (если SSL сломан)
# 2. Выпустить сертификат:
ssh root@10.0.0.10 "docker exec edunabazar-certbot-1 certbot certonly --webroot -w /var/www/certbot -d edunabazar.ru -d www.edunabazar.ru --non-interactive --agree-tos --email admin@edunabazar.ru"
# 3. Восстановить полный nginx.conf и перезапустить nginx
```

---

## 11. Бэкапы

- **Скрипт:** `deploy/db/backup.sh`
- **Расписание:** cron, ежедневно в 03:00 (`0 3 * * *`)
- **Хранение:** `/mnt/nas/pg_backups/` (14 дней)
- **Формат:** `pg_dump | gzip` → `enb_DB_YYYY-MM-DD_HHMM.sql.gz`

---

## 12. Локальная разработка

```bash
# 1. Скопировать .env
cp .env.example .env
# Отредактировать .env (DB_HOST=127.0.0.1, DJANGO_DEBUG=1)

# 2. Запустить PostgreSQL + приложение
docker compose up -d

# 3. Миграции
docker compose exec web python manage.py migrate

# 4. Приложение доступно на http://localhost:8000
```

**Локальная БД (docker-compose.yml):**
- `POSTGRES_DB`: `enb_DB`
- `POSTGRES_USER`: `admin`
- `POSTGRES_PASSWORD`: `admin`
- Порт: `5432`

---

## 13. Важные пути на серверах

| Путь | Сервер | Описание |
|------|--------|----------|
| `/opt/edunabazar/` | VM1 | Корень проекта |
| `/opt/edunabazar/.env` | VM1 | Переменные окружения (прод) |
| `/opt/edunabazar/deploy/nginx.conf` | VM1 | Конфиг Nginx (bind-mount в контейнер) |
| `/opt/edunabazar/deploy/certbot/conf/` | VM1 | SSL-сертификаты Let's Encrypt |
| `/opt/edunabazar/deploy/certbot/www/` | VM1 | ACME challenge directory |
| `/opt/edunabazar/media/` | VM1 | Загруженные файлы (фото и т.д.) |
| `/opt/edunabazar-db/` | VM2 | Конфигурация БД |
| `/opt/edunabazar-db/.env` | VM2 | Переменные БД |
| `/mnt/nas/pg_backups/` | VM2 | Бэкапы БД |
| `/var/log/pg_backup.log` | VM2 | Лог бэкапов |

---

## 14. Производительность и надёжность (Агрокосмос)

### 14.1 Rate limiting

Все публичные API-endpoint'ы модуля Агрокосмос защищены IP-based
rate-limiting'ом через `django-ratelimit` с Redis-backed счётчиком
(общий между gunicorn-воркерами).

Декоратор `rate_limit` реализован в `agrocosmos/views/_helpers.py`:

| Endpoint | Лимит | Тип 429 ответа |
|---|---|---|
| `/api/farmlands/` | 60/m | JSON |
| `/api/farmland/ndvi/` | 60/m | JSON |
| `/api/ndvi-stats/` | 30/m | JSON |
| `/api/phenology/` | 30/m | JSON |
| `/api/report/region/` | 30/m | JSON |
| `/api/report/district/` | 30/m | JSON |
| `/api/districts/status/` | 20/m | JSON |
| `/api/tiles/{z}/{x}/{y}.pbf` | 300/m | HTTP (без тела) |
| `/api/raster-tile/{z}/{x}/{y}.png` | 300/m | HTTP (без тела) |

Для тайлов возвращается пустой 429 без JSON-тела — карта-библиотеки
(MapLibre/Leaflet) его корректно обрабатывают как отсутствие тайла.

### 14.2 Redis-кеш `cache_page`

Тяжёлые агрегаты кешируются по полному URL на 5 минут (варьируется по
query-string, т.е. `?region=37&year=2025` и `?region=37&year=2024` —
разные записи):

| Endpoint | TTL |
|---|---|
| `/api/ndvi-stats/` | 5 мин |
| `/api/report/region/` | 5 мин |
| `/api/report/district/` | 5 мин |
| `/api/districts/status/` | 60 мин |
| `/api/tiles/{z}/{x}/{y}.pbf` | 10 мин |

**Производительность (регион Оренбургская область, 2025, ~2M VI-строк):**

| Endpoint | Cold (БД) | Warm (Redis) |
|---|---|---|
| `api_ndvi_stats` | ~37s | <10ms |
| `api_report_region` | ~15s | <10ms |
| `api_report_district` | ~22s | <10ms |
| `api_districts_status` | ~20s | ~30ms |

`api_districts_status` отдаёт всю Россию (~2300 районов, ≈4.7 MB GeoJSON
после `ST_SimplifyPreserveTopology(geom, 0.01°)` + `precision=3`). Cold
— это GeoJSON-сериализация и упрощение геометрии в PostGIS; данные NDVI
предрассчитаны (см. §14.4), поэтому холодный запрос укладывается в ~20с
и лишь на первой загрузке после `FLUSHDB`.

Прогрев `prewarm_agro_caches` (вызывается в CI после каждого деплоя)
проходит по всем регионам + all-Russia и форсирует построение GeoJSON,
чтобы первый живой пользователь не платил cold-price.

Инвалидация кеша после NDVI-pipeline (ручная):

```bash
ssh root@10.0.0.10 "docker exec edunabazar-redis-1 redis-cli FLUSHDB"
```

> ⚠️ `FLUSHDB` убивает и сессии — пользователи разлогинятся. Для
> точечной инвалидации планируется `cache.delete_pattern()` по префиксам.

### 14.4 Предрассчитанный статус по районам (`agro_district_ndvi_status`)

Для карты-choropleth «вся Россия» агрегация на лету невозможна:
`agro_vegetation_index` содержит ~25 млн MODIS-строк за окно 60 дней,
агрегат на один район по одной дате — >70 секунд. Решение —
материализованная таблица одной строкой на район:

| Поле | Описание |
|---|---|
| `district_id` | PK, OneToOne → `agro_district` |
| `latest_date` | Последняя MODIS-композита, по которой считалось значение |
| `current_ndvi` | Area-weighted среднее по фермам района на эту дату |
| `baseline_ndvi` | `NdviBaseline.mean_ndvi` для соответствующего DOY (с ±16 fallback) |
| `pct_of_baseline` | `current_ndvi / baseline_ndvi * 100`, NULL если базы нет |
| `computed_at` | `auto_now` |

Пересчёт делает команда `recompute_district_ndvi_status` — один большой
SQL-`INSERT ... ON CONFLICT` с тремя CTE (`latest_per_district` →
`current_ndvi` → `matched_baseline`). Время выполнения на холодном
кеше Postgres — **~35 минут** (`statement_timeout=15min` на уровне
команды по умолчанию, при необходимости увеличить флагом).

Команда автоматически вызывается в конце `python manage.py modis_ndvi`
(см. `agrocosmos/management/commands/modis_ndvi.py`, последний блок
`try/except`). Падение пересчёта **не валит** MODIS-пайплайн —
table-cache некритичен.

Ручной запуск (например, после ручной заливки VI или изменения логики):

```bash
ssh root@195.47.196.46 "ssh root@10.0.0.10 \
  'cd /opt/edunabazar && docker compose exec -T web \
   python manage.py recompute_district_ndvi_status'"
```

### 14.5 Предагрегат `DistrictNdviSeries`

Ещё один шаг оптимизации поверх `DistrictNdviStatus` — в той таблице
всего одна строка на район (последняя композита), а дашборд-графику
«Динамика NDVI по району/региону» нужен **полный временной ряд**. Для
Московской области это ~14 М сырых VI-строк × 5 лет × 5 культур —
`api_ndvi_stats` в лоб сканировал ~37 с.

`agro_district_ndvi_series` хранит area-weighted NDVI **per district ×
date × crop_type × source**: несколько тысяч строк на регион вместо
миллионов. Даёт доступ ко всем осям (культура, источник, год) без JOIN'а
с `Farmland` в горячем пути.

| Поле | Описание |
|---|---|
| `district_id` | FK на `agro_district` |
| `source` | `modis` / `raster` / `fused` |
| `crop_type` | 5 категорий `Farmland.CropType` |
| `acquired_date` | Дата композиты (для MODIS — mid-date 16-дневного окна) |
| `sum_ndvi_area` / `sum_area` | Числитель / знаменатель area-weighted mean |
| `obs_count` | Количество валидных угодий, вошедших в агрегат |

Пересчёт: команда `recompute_district_ndvi_series` (инкрементальное окно
60 дней — в ежедневном cron; `--rebuild` — full rebuild при изменении
логики). Источник — `recompute_district_ndvi_status` вызывает её под
капотом после успешного MODIS-пайплайна.

**Производительность `api_ndvi_stats` после внедрения (Московская обл.,
район, `breakdown=crop`):**

| Сценарий | Время |
|---|---|
| Район + год + `breakdown=crop` (5 культур с рядом + baseline) | ~700 мс cold |
| Регион-уровень + год | ~2 с cold |
| Cache-warm (`@cache_page` 5 мин) | <10 мс |

Даёт сайдбар-дашборду возможность рисовать 1 общий + N мини-графиков
по типам угодий (`views/ndvi.py::api_ndvi_stats`, параметр
`?breakdown=crop`).

### 14.6 Оптимизация агрегатов (single-pass Python)

Endpoint'ы с агрегатами раньше делали 3–4 отдельных `GROUP BY` по одному
и тому же огромному JOIN — `VegetationIndex × Farmland`. Это повторно
сканировало миллионы строк 3–4 раза.

Рефактор: один `SELECT … VALUES_LIST()` стримит все нужные поля
(`iterator(chunk_size=5000)`), а затем Python-loop строит все агрегаты
за один проход. Код — в `agrocosmos/views/ndvi.py` и `reports.py`,
метка `Single-pass aggregation`.

Эффект на fallback-пути (когда предагрегат пустой): ~50% снижение
cold-time (72s → 37s для `api_ndvi_stats`).

### 14.7 Индексы БД (миграция `0015_perf_indexes`)

| Индекс | Таблица | Определение |
|---|---|---|
| `idx_vi_ndvi_farm_date` | `agro_vegetation_index` | Partial на `(farmland_id, acquired_date)` WHERE `index_type='ndvi' AND is_outlier=false` |
| `idx_scene_sat_date` | `agro_satellite_scene` | Composite на `(satellite, acquired_date)` |

Partial-индекс выигрывает за счёт того, что в реальной выборке
практически всегда фигурируют условия `index_type='ndvi'` и
`is_outlier=false` — отфильтрованный срез получается в разы меньше
полной таблицы.

### 14.8 Мониторинг (TODO)

Ещё не настроено:

- **Sentry** — error tracking и performance monitoring (backlog)
- **Uptime-мониторинг** внешним сервисом (UptimeRobot / Better Uptime)
- **Alert'ы на 5xx** и медленные endpoint'ы (>5s p95)

Пока используется:

- `/healthz` — health-check (проверяется docker compose healthcheck)
- `docker logs -f edunabazar-web-1` — ручная диагностика
- `docker logs -f edunabazar-nginx-1` — access log

---

## 15. Алерты вегетации и подписки

### 15.1 Пайплайн детекции

Два уровня алертов с одинаковыми детекторами, но разной
гранулярностью и разными источниками данных:

**District-level (MODIS) — основной режим, в крон-расписании.**
Команда `detect_district_ndvi_alerts` читает
предагрегат `DistrictNdviSeries` (area-weighted NDVI по
`district × crop_type × acquired_date`, source=`modis`) и считает
z-score против `NdviBaseline` (district + crop_type + DOY).
Scope по умолчанию ограничен районами, на которые есть
`AgroSubscription.notify_anomalies=True` (region-подписки
разворачиваются в районы региона) — это убирает массовый ночной
обход всех угодий РФ. Флаг `--all` запускает полный sweep.

**Per-farmland (S2/L8) — legacy, на паузе.** Команда
`detect_vegetation_alerts` оставлена в репозитории для будущего
кейса с растровыми снимками 10–30 м (где per-farmland разрешение
оправдано), но **из cron убрана** — MODIS 250 м per-farmland
статистически шумный и порождает слишком много дублирующих строк.

Оба детектора смотрят на окно ~30–45 дней и проверяют два паттерна:

| Тип | Условие | Severity |
|---|---|---|
| `baseline_deviation` | 2 наблюдения подряд с z-score ≤ −1.5 vs `NdviBaseline` района/культуры | `warning` (z ≤ −1.5) / `critical` (z ≤ −2.0) |
| `rapid_drop` | Падение NDVI ≥ 0.15 относительно точки ≥16 дней назад | `warning` (≥ 0.15) / `critical` (≥ 0.20) |

Дедупликация:

- District-level: один активный `VegetationAlert` на
  `(district, crop_type, alert_type, source=modis)`.
- Per-farmland (legacy): на `(farmland, alert_type)`.

Новое детектирование **обновляет** severity / `context` существующей
записи, без дублей. Когда метрика восстанавливается — алерт
автоматически переходит в `resolved` без email.

Пороги и окна захардкожены в начале каждой команды (константы
`Z_WARN / Z_CRIT / DROP_WARN / DROP_CRIT / LOOKBACK_DAYS`)
— тонкая настройка через подмену констант.

### 15.2 Подписки (`AgroSubscription`)

Кабинет пользователя `/me/agrocosmos/` (`agrocosmos/views/cabinet.py`)
даёт `LegacyUser` возможность вести строки-подписки:

- **Scope**: либо весь регион (`district=NULL`), либо один район
- **`notify_anomalies`**: email при появлении / эскалации
  `VegetationAlert` — как per-farmland (legacy), так и
  district-level (MODIS) — на угодьях/районах этого scope
- **`notify_updates`**: ежедневный дайджест свежих NDVI-данных
  (команда `send_agrocosmos_updates`)

Админка: `/legacy-admin/agrocosmos/agrosubscription/` — показывает
логин/email/имя подписчика через bulk-lookup в `legacy_user` (нет
реального FK — `LegacyUser` unmanaged), с поиском по
`username / email / name / phone`.

### 15.3 Рассылка (`services/notifications.py`)

- `send_anomaly_email(alert)` — вызывается из `_reconcile` сразу после
  создания нового алерта **или** на эскалации `warning → critical`.
  Не шлёт повторно для того же severity — иначе каждую ночь был бы
  спам по долгоиграющим аномалиям
- Получатели: все `AgroSubscription` с `notify_anomalies=True` чей
  scope покрывает угодье (район точно или регион целиком)
- Транспорт: `EmailMultiAlternatives` через тот же SMTP что и
  `/legacy-admin/campaigns/` (Yandex)
- Ошибки отправки **не блокируют** создание алерта — логируются в
  `logger.exception`

---

## 16. Cron-задачи (VM1, хост-crontab)

Настраиваются в CI (`deploy` job) после каждого успешного деплоя —
существующие строки перезаписываются. Источник правды — блок
`setup-cron` в `.github/workflows/ci.yml`.

| Время (Moscow / UTC) | Команда | Назначение |
|---|---|---|
| `07:00 / 04:00` | `fetch_news --count 3` | RSS → GigaChat рерайт в `News` |
| `09:00 / 06:00` | `check_monitoring` | Запуск MODIS-пайплайнов по `MonitoringTask` |
| `10:00 / 07:00` | `check_raster_monitoring` | S2 + L8 оперативный NDVI |
| `11:00 / 08:00` | `detect_district_ndvi_alerts` | District-level MODIS-алерты по подписанным районам, email подписчикам |
| `12:00 / 09:00` | `send_agrocosmos_updates` | Ежедневный дайджест свежих NDVI |
| `*/10 min` | `cleanup_stale_runs --timeout-min 15` | Закрытие зависших `PipelineRun` |
| `01 Jan 03:00 MSK` | `ensure_all_regions_monitored` | Годовой roll-over `MonitoringTask` |
| `07 Jan` | `recompute_ndvi_baselines` | Пересчёт `NdviBaseline` с учётом прошлого года |

Инкрементальный пересчёт предагрегатов (`DistrictNdviStatus` +
`DistrictNdviSeries`) **не в отдельном cron** — он вызывается из
`check_monitoring` / `modis_ndvi` после успешного пайплайна, чтобы
таблицы обновлялись сразу после поступления новых данных.
