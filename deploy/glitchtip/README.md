# GlitchTip — self-hosted мониторинг ошибок

Sentry-совместимый трекер: каждое необработанное исключение приложения
(web, worker, management-команды) попадает в UI на
https://errors.edunabazar.ru с трейсбеком, группировкой и e-mail-алертами.

Приложение шлёт события через `sentry-sdk` (см. `enb_django/settings.py`,
блок `SENTRY_DSN`) — SDK полностью выключен, пока DSN не задан в `.env`.

## Установка (разовая, по SSH)

### 0. DNS

A-запись `errors.edunabazar.ru → 195.47.196.46` (тот же IP, что и сайт).

### 1. База на VM2 (10.0.0.11)

```bash
docker exec -it edunabazar-db-db-1 psql -U admin -d postgres -c \
  "CREATE ROLE glitchtip LOGIN PASSWORD '<пароль>';"
docker exec -it edunabazar-db-db-1 psql -U admin -d postgres -c \
  "CREATE DATABASE glitchtip OWNER glitchtip;"
```

`pg_hba` уже разрешает подключения из подсети 10.0.0.0/24 (приложение
ходит так же) — отдельное правило не нужно.

### 2. Конфиг и запуск на VM1

```bash
cd /opt/edunabazar/deploy/glitchtip
cp .env.example .env
openssl rand -hex 32   # -> SECRET_KEY в .env
vi .env                # DATABASE_URL (пароль из шага 1), EMAIL_URL
docker compose up -d
docker compose logs glitchtip-migrate   # миграции должны пройти чисто
```

### 3. Сертификат (расширить существующий на поддомен)

```bash
cd /opt/edunabazar
docker compose -f docker-compose.prod.yml run --rm certbot certonly \
  --webroot -w /var/www/certbot --expand \
  -d edunabazar.ru -d www.edunabazar.ru -d errors.edunabazar.ru
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```

До этого шага https://errors.edunabazar.ru будет отдавать предупреждение
о сертификате — это ожидаемо и не влияет на основной сайт.

### 4. Суперюзер и проект

```bash
cd /opt/edunabazar/deploy/glitchtip
docker compose exec glitchtip-web ./manage.py createsuperuser
```

Дальше в UI (https://errors.edunabazar.ru):
организация → проект (platform: Django) → скопировать DSN.

### 5. Подключить приложение

В `/opt/edunabazar/.env`:

```
SENTRY_DSN=<DSN из шага 4>
SENTRY_ENVIRONMENT=production
```

Перезапустить приложение:

```bash
cd /opt/edunabazar
docker compose -f docker-compose.prod.yml up -d --force-recreate web worker
```

### 6. Проверка

```bash
docker compose -f docker-compose.prod.yml exec web python -c \
  "import sentry_sdk; sentry_sdk.capture_message('glitchtip smoke test'); sentry_sdk.flush()"
```

Событие должно появиться в проекте в течение нескольких секунд.

## Обслуживание

- **Обновление**: поднять тег образа `glitchtip/glitchtip:vX.Y` в
  `docker-compose.yml`, затем `docker compose up -d` (мигрирует сам).
- **Ретеншн событий**: GlitchTip чистит старые события сам
  (`GLITCHTIP_MAX_EVENT_LIFE_DAYS`, по умолчанию 90 дней).
- **Бэкап**: текущий `deploy/db/backup.sh` дампит только `enb_DB`, база
  `glitchtip` в бэкап НЕ попадает. Осознанное решение: данные некритичные
  (история ошибок), при потере — пересоздать базу и проект, обновить DSN.
  Если захочется бэкапить — добавить второй `pg_dump` в `backup.sh`.
