# MinIO — объектное хранилище растров (S3-совместимое)

Хранит геокодированные растры для ГИС-модуля «Мои поля»: оригиналы
загруженных GeoTIFF и производные Cloud-Optimized GeoTIFF (COG). Файлы
бывают в десятки ГБ, поэтому:

- загрузка идёт **напрямую в MinIO** через presigned S3 Multipart Upload
  (браузер → nginx `s3.edunabazar.ru` → MinIO), минуя gunicorn;
- приложение читает COG **по range-запросам** через GDAL `/vsis3/`, не
  скачивая файл целиком.

MinIO живёт на **VM2** (10.0.0.11), рядом с Postgres, в отдельном
docker-compose. Порт S3 (9000) файрволится так же, как 5432 — только с
VM1. Публичный доступ браузера — через nginx на VM1.

> Код спроектирован S3-абстрактным (boto3 + GDAL `/vsis3/`), поэтому позже
> можно переехать на облачный S3 (Yandex/VK/Selectel) сменой `.env` без
> правки кода.

---

## Топология

```
Браузер ──https──▶ nginx VM1 (s3.edunabazar.ru:443) ──http──▶ MinIO VM2 (10.0.0.11:9000)
                                                                     ▲
Django web/worker VM1 ──http (внутр. сеть 10.0.0.11:9000)────────────┘
```

- **Публичный эндпоинт** `https://s3.edunabazar.ru` — только для presigned
  URL, которые отдаются браузеру (прямая загрузка частей).
- **Внутренний эндпоинт** `http://10.0.0.11:9000` — для серверных операций
  приложения (создание multipart, финализация, чтение COG, конвертация).

---

## Установка (разовая, по SSH)

### 0. DNS

A-запись `s3.edunabazar.ru → 195.47.196.46` (тот же публичный IP, что и
сайт).

### 1. Запуск MinIO на VM2 (10.0.0.11)

```bash
ssh root@195.47.196.46 "ssh root@10.0.0.11"

mkdir -p /opt/edunabazar-minio && cd /opt/edunabazar-minio
# Скопировать сюда deploy/minio/docker-compose.yml из репозитория.
# (например, через git clone/sparse или scp с VM1)

cp .env.example .env
openssl rand -hex 20   # -> MINIO_ROOT_PASSWORD в .env
vi .env                # задать MINIO_ROOT_USER / MINIO_ROOT_PASSWORD

docker compose up -d
docker compose logs --tail=30 minio   # "API: ... Console: ..." = ок
```

### 2. Файрвол VM2 — порт 9000 только с VM1

> ⚠️ У VM2 есть публичный IP (`93.95.98.209`), а docker публикует порты в
> обход UFW (правила в цепочке `DOCKER` применяются раньше `INPUT`).
> Поэтому основная защита — **привязка порта к внутреннему интерфейсу**
> `10.0.0.11:9000:9000` в `docker-compose.yml` (S3 API виден только из
> внутренней сети, не из интернета). UFW-правило ниже — второй слой.

Зеркалим правило для Postgres (5432). По текущей конфигурации трафик
между VM приходит с адреса PVE-шлюза `195.47.196.46` (см. `pg_hba` для
5432):

```bash
ufw allow from 195.47.196.46 to any port 9000 proto tcp
ufw status | grep 9000
```

> Порт 9001 (web-консоль) в compose слушает только `127.0.0.1` на VM2 —
> наружу не открывается. Доступ к консоли — по SSH-туннелю (см. ниже).

### 3. Сервисная учётка приложения + бакеты (`mc`)

MinIO-клиент `mc` уже есть внутри контейнера. Создаём бакеты и
отдельного пользователя приложения (не root):

```bash
# alias на локальный MinIO под root-учёткой
docker compose exec minio mc alias set local http://localhost:9000 \
  "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# бакеты
docker compose exec minio mc mb local/raster-uploads
docker compose exec minio mc mb local/raster-cog

# сервисный пользователь приложения (S3_ACCESS_KEY / S3_SECRET_KEY на VM1)
docker compose exec minio mc admin user add local enb_raster_app 'CHANGE_ME_service_account_secret'

# политика: полный доступ только к двум растровым бакетам
docker compose exec minio sh -c 'cat > /tmp/raster-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect":"Allow","Action":["s3:*"],
     "Resource":["arn:aws:s3:::raster-uploads","arn:aws:s3:::raster-uploads/*",
                  "arn:aws:s3:::raster-cog","arn:aws:s3:::raster-cog/*"]}
  ]
}
JSON'
docker compose exec minio mc admin policy create local raster-rw /tmp/raster-policy.json
docker compose exec minio mc admin policy attach local raster-rw --user enb_raster_app
```

> CORS для браузерной загрузки уже разрешён переменной
> `MINIO_API_CORS_ALLOW_ORIGIN` (см. `.env`). Менять список origin — там же
> + `docker compose up -d`.

### 4. Сертификат (расширить существующий на поддомен) — на VM1

```bash
ssh root@195.47.196.46 "ssh root@10.0.0.10"
cd /opt/edunabazar

docker compose -f docker-compose.prod.yml run --rm certbot certonly \
  --webroot -w /var/www/certbot --expand \
  -d edunabazar.ru -d www.edunabazar.ru -d errors.edunabazar.ru -d s3.edunabazar.ru

docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```

> `deploy/nginx.conf` уже содержит server-блок `s3.edunabazar.ru`. До
> расширения сертификата поддомен отдаёт предупреждение — на основной сайт
> не влияет (тот же приём, что для errors.*).

### 5. Переменные приложения на VM1

В `/opt/edunabazar/.env`:

```
S3_ENDPOINT_URL=http://10.0.0.11:9000
S3_PUBLIC_ENDPOINT_URL=https://s3.edunabazar.ru
S3_ACCESS_KEY=enb_raster_app
S3_SECRET_KEY=CHANGE_ME_service_account_secret
S3_REGION=us-east-1
S3_BUCKET_UPLOADS=raster-uploads
S3_BUCKET_COG=raster-cog
```

Проброс этих переменных в контейнеры web/worker появится вместе с кодом
Фазы 1–2 (пока хранилище живёт независимо и проверяется отдельно).

### 6. Проверка

**Сеть с VM1 → MinIO VM2:**

```bash
ssh root@195.47.196.46 "ssh root@10.0.0.10 'curl -sS -o /dev/null -w %{http_code} http://10.0.0.11:9000/minio/health/live'"
# 200
```

**Публичный эндпоинт (после сертификата):**

```bash
curl -sS -o /dev/null -w '%{http_code}\n' https://s3.edunabazar.ru/minio/health/live
# 200
```

**Web-консоль (по SSH-туннелю с локальной машины):**

```bash
ssh -N -L 9001:127.0.0.1:9001 -J root@195.47.196.46 root@10.0.0.11
# затем открыть http://localhost:9001 (логин = MINIO_ROOT_USER/PASSWORD)
```

---

## Обслуживание

- **Обновление**: поднять тег образа в `docker-compose.yml` → `docker compose up -d`.
- **Бэкап**: текущий `deploy/db/backup.sh` растры НЕ покрывает. COG считаем
  воспроизводимым артефактом (пересобирается из оригинала), поэтому
  приоритет — бэкап бакета `raster-uploads`. Вариант: `mc mirror` на NAS
  по cron, либо bucket versioning. Добавить, когда появятся реальные
  данные.
- **Место на диске**: растры тяжёлые — следить за томом `minio_data` на
  VM2. Ретеншн/удаление объектов появится в коде Фазы 5 (drop объекта при
  удалении слоя).
