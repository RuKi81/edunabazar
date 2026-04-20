# Edunabazar

> Маркетплейс сельхоз-объявлений + ГИС-модуль **Агрокосмос** для спутникового мониторинга
> вегетации (NDVI) на основе MODIS / Sentinel-2 / Landsat.

- **Production:** <https://edunabazar.ru> · <https://www.edunabazar.ru>
- **Repo:** <https://github.com/RuKi81/edunabazar>
- **Стек:** Django 5 · PostgreSQL 16 + PostGIS 3.4 · Redis · Docker · Nginx · Gunicorn

---

## Структура проекта

```
edunabazar/
├── enb_django/        # Django settings / urls / wsgi
├── legacy/            # Маркетплейс (объявления, продавцы, новости)
├── agrocosmos/        # ГИС-модуль (регионы/районы/поля/NDVI)
├── deploy/            # nginx.conf, setup-скрипты, конфиги БД
├── docs/              # Документация (архитектура, API, улучшения)
├── docker-compose.yml # Прод: только web (БД внешняя на VM2)
└── Dockerfile
```

Два Django-приложения:

- **`legacy`** — маркетплейс, унаследованный из старой PHP-базы
  (часть моделей с `managed = False`).
- **`agrocosmos`** — ГИС-модуль с зональной статистикой NDVI по полям,
  MODIS 16-дневными композитами, фенологией и Mapbox Vector Tiles.

---

## Документация

| Файл | Что внутри |
|---|---|
| [`ARCHITECTURE.md`](./ARCHITECTURE.md) | Инфраструктура, серверы, доступы, .env, CI/CD, бэкапы |
| [`docs/AGROCOSMOS_API.md`](./docs/AGROCOSMOS_API.md) | Reference всех API-endpoint'ов агрокосмоса (параметры, ответы, лимиты) |
| [`docs/technical_improvements.md`](./docs/technical_improvements.md) | Лог технических улучшений |

---

## Локальный запуск

```bash
# 1. Клонировать и настроить .env
git clone https://github.com/RuKi81/edunabazar.git
cd edunabazar
cp .env.example .env
# Поставить DJANGO_DEBUG=1, DB_HOST=db (или 127.0.0.1)

# 2. Поднять БД + приложение
docker compose up -d

# 3. Миграции
docker compose exec web python manage.py migrate

# 4. Создать суперюзера (опционально)
docker compose exec web python manage.py createsuperuser

# Открыть http://localhost:8000/
```

**Прямые команды без docker:**

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows PowerShell
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver
```

---

## Тесты

```bash
# Все тесты
docker compose exec web python manage.py test

# Только агрокосмос
docker compose exec web python manage.py test agrocosmos

# Один тест
docker compose exec web python manage.py test agrocosmos.tests.TestNdviChart.test_last_period_end
```

CI (GitHub Actions) прогоняет `flake8` + всю тестовую сьюту на каждый push/PR.

---

## Деплой

Авто-деплой в `main` через GitHub Actions:

```
push main → flake8 → tests → docker build → ssh VM1 → git pull + build + up -d
```

Ручной деплой (с PVE-шлюза):

```bash
ssh root@10.0.0.10 "cd /opt/edunabazar && git pull \
    && docker compose build web \
    && docker compose up -d --no-deps web \
    && docker restart edunabazar-nginx-1"
```

Детали, SSL-перевыпуск, и процедуры восстановления — в [`ARCHITECTURE.md`](./ARCHITECTURE.md#10-cicd-pipeline).

---

## Агрокосмос: ключевые особенности

- **Зональная NDVI-статистика** по полигонам полей (area-weighted mean).
- **MODIS 16-дневные композиты** с dashed extension line до конца периода.
- **Фенология:** SOS / POS / EOS / LOS на основе NDVI-time-series.
- **Baseline / z-score** — историческое среднее по дню года, аномалии.
- **Mapbox Vector Tiles** (`/api/tiles/{z}/{x}/{y}.pbf`) — быстрая карта полей.
- **NDVI raster tiles** (`/api/raster-tile/{z}/{x}/{y}.png`) — псевдоцветная подложка из GeoTIFF.
- **Rate limiting (IP-based)** на все публичные API: 30–300 req/min в зависимости от стоимости endpoint'а.
- **Redis `cache_page`** на тяжёлые агрегаты (5 мин TTL) — cold ~15-40s → warm <10ms.

Полный список endpoint'ов — [`docs/AGROCOSMOS_API.md`](./docs/AGROCOSMOS_API.md).

---

## Переменные окружения

Все переменные и их значения описаны в [`ARCHITECTURE.md` §7](./ARCHITECTURE.md#7-доступы-и-переменные-окружения).
Шаблон: [`.env.example`](./.env.example).

Критичные секреты (не коммитить):

- `DJANGO_SECRET_KEY`
- `DB_PASSWORD`
- `EMAIL_HOST_PASSWORD`
- `SMSC_PASSWORD`
- `GIGACHAT_AUTH_KEY`

---

## Команды обслуживания

```bash
# Создать миграции
docker compose exec web python manage.py makemigrations

# Применить миграции (прод)
ssh root@10.0.0.10 "docker compose -f /opt/edunabazar/docker-compose.prod.yml exec -T web python manage.py migrate"

# Собрать статику
docker compose exec web python manage.py collectstatic --noinput

# Просмотр логов
ssh root@10.0.0.10 "docker logs -f --tail 200 edunabazar-web-1"
ssh root@10.0.0.10 "docker logs -f --tail 200 edunabazar-nginx-1"

# Django shell в проде
ssh root@10.0.0.10 "docker compose -f /opt/edunabazar/docker-compose.prod.yml exec web python manage.py shell"
```

---

## Лицензия

Проприетарный код. Все права защищены.
