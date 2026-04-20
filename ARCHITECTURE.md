# Архитектура проекта «Еду на базар» (edunabazar)

> **Репозиторий:** <https://github.com/RuKi81/edunabazar>
> **Домен:** edunabazar.ru / www.edunabazar.ru
> **Дата:** 2026-04-20

> См. также: [`README.md`](./README.md) (overview, quick start) ·
> [`docs/AGROCOSMOS_API.md`](./docs/AGROCOSMOS_API.md) (API reference)

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
│   │                        #   VegetationIndex, NdviBaseline, FarmlandPhenology
│   ├── views/               # Views-пакет (разделён по доменам)
│   │   ├── __init__.py      #   — re-export для обратной совместимости
│   │   ├── _helpers.py      #   — константы, rate_limit, satellite_filter
│   │   ├── pages.py         #   — HTML-страницы (dashboard, report_*)
│   │   ├── geojson.py       #   — GeoJSON endpoints (regions/districts/farmlands)
│   │   ├── tiles.py         #   — MVT + raster PNG tiles
│   │   ├── ndvi.py          #   — NDVI time series, stats, phenology
│   │   └── reports.py       #   — данные для отчётов region/district
│   ├── services/
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
| `Farmland` | `agro_farmland` | Полигон сельхоз. угодья (пашня, пастбище и т.д.) |
| `SatelliteScene` | `agro_satellite_scene` | Метаданные спутникового снимка (Sentinel-2, Landsat, MODIS) |
| `VegetationIndex` | `agro_vegetation_index` | Зональная статистика вегетационных индексов по угодью |

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
| `/api/tiles/{z}/{x}/{y}.pbf` | 10 мин |

**Производительность (регион Оренбургская область, 2025, ~2M VI-строк):**

| Endpoint | Cold (БД) | Warm (Redis) |
|---|---|---|
| `api_ndvi_stats` | ~37s | <10ms |
| `api_report_region` | ~15s | <10ms |
| `api_report_district` | ~22s | <10ms |

Инвалидация кеша после NDVI-pipeline (ручная):

```bash
ssh root@10.0.0.10 "docker exec edunabazar-redis-1 redis-cli FLUSHDB"
```

> ⚠️ `FLUSHDB` убивает и сессии — пользователи разлогинятся. Для
> точечной инвалидации планируется `cache.delete_pattern()` по префиксам.

### 14.3 Оптимизация агрегатов (single-pass Python)

Endpoint'ы с агрегатами раньше делали 3–4 отдельных `GROUP BY` по одному
и тому же огромному JOIN — `VegetationIndex × Farmland`. Это повторно
сканировало миллионы строк 3–4 раза.

Рефактор: один `SELECT … VALUES_LIST()` стримит все нужные поля
(`iterator(chunk_size=5000)`), а затем Python-loop строит все агрегаты
за один проход. Код — в `agrocosmos/views/ndvi.py` и `reports.py`,
метка `Single-pass aggregation`.

Эффект: ~50% снижение cold-time (72s → 37s для `api_ndvi_stats`).

### 14.4 Индексы БД (миграция `0015_perf_indexes`)

| Индекс | Таблица | Определение |
|---|---|---|
| `idx_vi_ndvi_farm_date` | `agro_vegetation_index` | Partial на `(farmland_id, acquired_date)` WHERE `index_type='ndvi' AND is_anomaly=false` |
| `idx_scene_sat_date` | `agro_satellite_scene` | Composite на `(satellite, acquired_date)` |

Partial-индекс выигрывает за счёт того, что в реальной выборке
практически всегда фигурируют условия `index_type='ndvi'` и
`is_anomaly=false` — отфильтрованный срез получается в разы меньше
полной таблицы.

### 14.5 Мониторинг (TODO)

Ещё не настроено:

- **Sentry** — error tracking и performance monitoring (backlog)
- **Uptime-мониторинг** внешним сервисом (UptimeRobot / Better Uptime)
- **Alert'ы на 5xx** и медленные endpoint'ы (>5s p95)

Пока используется:

- `/healthz` — health-check (проверяется docker compose healthcheck)
- `docker logs -f edunabazar-web-1` — ручная диагностика
- `docker logs -f edunabazar-nginx-1` — access log
