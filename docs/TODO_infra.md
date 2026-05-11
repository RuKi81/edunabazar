# Инфраструктурные задачи / Tech debt

Задачи, не связанные с алгоритмами NDVI (для этого см. `technical_improvements.md`),
а с производительностью, стабильностью и DX портала.

---

## [x] DONE: OOM-killer на pve, убивающий VM 101 (DB)

**Корневая причина крахов БД при baseline-прогоне (май 2026):**

Во время прогона `run_baseline_ndvi` PostgreSQL во VM 101 (БД, 10.0.0.11)
терял коннекты. `ping 10.0.0.11` показывал `Destination Host
Unreachable` — то есть падала вся VM, а не только Postgres.

В журнале `journalctl -b -1` на pve-хосте найдены строки:

```
qemu.slice: Consumed 1d 3h 24min CPU time, 170.2G memory peak.
qemu.slice: A process of this unit has been killed by the OOM killer.
```

**Конфигурация хоста на момент инцидента:**

| | |
|---|---|
| RAM pve | 187 GiB |
| Swap | **0 B** |
| VM 100 (web/workers) | 90 GiB max |
| VM 101 (Postgres) | 90 GiB max |
| Итого VMs | 180 GiB |
| ZFS ARC max | ~93 GiB (default = 50% RAM) |

При нагрузке от baseline (одновременно VM100 c worker'ами + VM101 c
Postgres-buffers + ZFS ARC) `qemu.slice` доходила до 170+ GiB при 187 GiB
физической RAM и **отсутствии swap**. Linux OOM-killer на pve начинал
прибивать QEMU-процессы целиком — VM падала полностью.

Так все три «краша БД» (1, 3, 4 мая 2026) объясняются одним механизмом.

**Применённые меры (4 мая 2026):**

1. ✅ **Swap 32 GiB** на pve через ZVOL (zfs создаёт sparse файлы → файл
   через `dd if=/dev/zero` swapon отказывается принимать).
   ```bash
   zfs create -V 32G -b 4096 \
     -o compression=zle -o logbias=throughput -o sync=always \
     -o primarycache=metadata -o secondarycache=none \
     -o com.sun:auto-snapshot=false rpool/swap
   mkswap /dev/zvol/rpool/swap
   swapon /dev/zvol/rpool/swap
   echo '/dev/zvol/rpool/swap none swap discard 0 0' >> /etc/fstab
   ```

2. ✅ **ZFS ARC ограничен 16 GiB** (было ~93 GiB):
   ```bash
   echo 'options zfs zfs_arc_max=17179869184' > /etc/modprobe.d/zfs.conf
   echo 17179869184 > /sys/module/zfs/parameters/zfs_arc_max  # runtime
   update-initramfs -u
   ```

**Результат после мер:**

| Параметр | Было | Стало |
|---|---|---|
| RAM pve | 187 GiB | 187 GiB |
| Swap | **0 B** | **32 GiB** |
| ARC max | ~93 GiB | **16 GiB** |
| Свободно при пике (живой baseline) | падал в 0 | ~129 GiB free + 32 GiB swap |

---

## [x] DONE: опечатка в pgdata mount + 64M /dev/shm на VM2 (5 мая 2026)

**Симптом:** тяжёлые запросы дашборда (агрегаты по `agro_vegetation_index`)
падали с `could not resize shared memory segment "/PostgreSQL.XXX": No space
left on device`. Web/worker отваливались по таймауту.

**Корневые причины (две):**

1. В `/opt/edunabazar-db/docker-compose.yml` на VM2 был развёрнут
   **упрощённый ручной вариант** compose-файла (не из репо `deploy/db/`),
   с опечаткой в пути volume:
   `pgdata:/var/lib/postgresql/datal` (лишняя `l`).
   Postgres использовал дефолтный `PGDATA=/var/lib/postgresql/data`,
   на который Docker автоматически создал **анонимный volume**
   (имя `4a4583bf303a...`) — туда и легли все 410 GB данных.
   Named volume `edunabazar-db_pgdata` был смонтирован в бесполезную
   пустую `/datal` (4K).

2. **`/dev/shm` контейнера = 64 MB** (Docker default). Postgres
   parallel workers при hash-join по миллионам строк требуют сильно
   больше — особенно с `max_parallel_workers_per_gather=2`.

**Применённые меры:**

1. ✅ Compose на VM2 переписан:
   - `datal` → `data`
   - `shm_size: '4gb'`
   - volume `pgdata` объявлен `external: true` со `name:` указывающим
     на анонимный `4a4583bf...`. Так точка монтирования сменилась
     **без копирования 410 GB** данных.
2. ✅ Удалены пустой `edunabazar-db_pgdata` volume и снапшот-image.
3. ✅ Репо `deploy/db/docker-compose.yml` обновлён:
   - добавлен `shm_size: '4gb'`
   - PG-тюнинг под 90 GiB RAM (`shared_buffers=22GB`,
     `effective_cache_size=64GB`, `work_mem=64MB`,
     `maintenance_work_mem=2GB`, `max_wal_size=8GB`, ...)
   - `-c hba_file=/etc/postgresql/pg_hba.conf` чтобы кастомный
     pg_hba реально применялся (без флага PG читает PGDATA/pg_hba.conf
     и mounted-файл игнорируется).
4. ✅ `deploy/db/backup.sh` — исправлено имя контейнера
   (`db-db-1` → `edunabazar-db-db-1`) и дефолт `BACKUP_DIR`
   (`/mnt/nas/...` → `/var/backups/postgres`).

**Что ещё надо доделать (см. ниже отдельный пункт):** синхронизировать
VM2 с полной репо-версией compose (тюнинг + healthcheck + pg_hba),
переименовать volume в человеческое имя, проверить статус cron-бэкапов.

---

## [x] DONE: бэкапы PostgreSQL — pull на Synology NAS (6 мая 2026)

**Контекст (6 мая 2026):** обнаружено что cron-бэкап на VM2 ни разу не
сработал — путь `/opt/edunabazar/deploy/db/backup.sh` не существует
(репо-клона на VM2 нет). Лог `/var/log/pg_backup.log` содержит сотни
строк `not found` — БД жила без единого бэкапа.

**Что сделано:**
1. ✅ Скрипт перенесён на VM2: `/opt/edunabazar-db/backup.sh`
2. ✅ Cron поправлен: `0 3 * * * /opt/edunabazar-db/backup.sh ...`
3. ✅ ACL для `nas_pull` user (uid=1000) на `/var/backups/postgres/`
4. ✅ Создан SSH-юзер `nas_pull` на VM2 для pull с Synology
5. ✅ DNAT на PVE: `195.47.196.46:22023 → 10.0.0.11:22` (для прямого SSH с NAS)
6. ✅ SSH-ключ на NAS (`/volume1/scripts/.ssh/nas_pull_key`),
   публичная часть в `/home/nas_pull/.ssh/authorized_keys` на VM2
7. ✅ Pull-скрипт на NAS: `/volume1/scripts/pull_pg_backup.sh` (rsync over SSH)
8. ✅ DSM Task Scheduler: ежедневно в 05:00 запускает pull-скрипт от root
9. ✅ Shared folder `/volume1/pg_backups/` на NAS (доступ только admin/geoadmin)
10. ✅ pigz -p 4 заменил gzip — следующие dumps будут проходить ~30-40мин вместо 2.5ч
11. ✅ Первый dump завершён: 38GB за 2ч38мин (12:12→14:50 UTC, gzip)
12. ✅ Тестовый pull проверен: 26 мин по 11 МБ/с (~87 Mbit), `gunzip -t` → OK

**Архитектура pull (вместо push):**
```
VM2 (ДЦ, public)                       Synology NAS (дом, 192.168.0.87)
─────────────────                      ────────────────────────────────
pg_dump → /var/backups/         ←──    rsync over SSH ежедневно в 04:00
postgres/*.sql.gz                       (Task Scheduler / Hyper Backup)
                                          ↓
                                       /volume1/pg_backups/
```

NAS дома за NAT, VM2 в ДЦ — прямой push невозможен. Pull инициируется
со стороны NAS (исходящий интернет работает), VM2 хостит файлы через
SSH-юзера с read-only ACL.

**Что осталось:**
- [ ] Сгенерировать SSH-ключ на Synology (`/root/.ssh/nas_pull_key`),
  добавить публичную часть в `/home/nas_pull/.ssh/authorized_keys` на VM2
- [ ] Настроить Synology Task Scheduler: ежедневный rsync с VM2 в
  `/volume1/pg_backups/`
- [ ] Проверить retention: 7 дней на VM2 (KEEP_DAYS=7 в backup.sh,
  было 14 — сокращено 11 мая 2026, ~270GB экономии диска),
  90 дней на NAS (через rsync `--max-age` или DSM-чистку)
- [ ] **Оптимизация:** заменить `gzip` на `pigz -p 4` в `backup.sh` —
  ускорит сжатие в 4-8 раз (gzip однопоточный, в первом dump'е был
  bottleneck по CPU). Команды:
  ```bash
  apt-get install -y pigz
  sed -i 's|| gzip|| pigz -p 4|' /opt/edunabazar-db/backup.sh
  ```
  Закоммитить аналогичную правку в `deploy/db/backup.sh`.
- [ ] Алерт если бэкап не сделался / не подтянулся на NAS более 2 суток.

**Приоритет:** высокий. БД 410 GB, bare-metal NVMe, без бэкапа —
один ребут с fs-corruption или OOM-kill (см. историю выше) и теряем всё.

---

## [ ] FIX: оптимизация памяти VM (продолжение анти-OOM)

После аварийных мер выше остаются «причёсывающие» работы, которые сделают
конфигурацию устойчивой к будущим всплескам нагрузки (новые регионы,
S2/L8-фьюжны и т.п.). Не срочно, но желательно.

1. **Снизить RAM у VM 101** до 64 GiB (с 90). Postgres под текущую
   нагрузку не использует столько; `shared_buffers` обычно 25% RAM, всё
   остальное — page cache, который и так делится с хостом. Через PVE UI
   или `qm set 101 --memory 65536`.
2. **Включить ballooning** (`qm config 101 | grep balloon`). Если
   `balloon=0` — отключено; включить
   `qm set 101 --balloon 32768` — VM будет возвращать неиспользуемую
   RAM хосту динамически.
3. **Postgres tuning** на VM 101 — проверить `shared_buffers`,
   `work_mem`, `maintenance_work_mem`, `effective_cache_size` —
   привести в соответствие реальному RAM = 64 GiB. Использовать
   pgtune.leopard.in.ua как стартовую точку.
4. **Мониторинг RAM/Swap** — добавить алерты в Grafana/Prometheus
   или `node_exporter`, чтобы видеть приближение к OOM-зоне до того
   как сработает kill.

**Приоритет:** средний. После пунктов 1-2 + уже сделанных swap+ARC
конфигурация будет выдерживать нагрузку с двукратным запасом.

---

## [ ] FIX: медленный DISTINCT YEAR на главном дашборде
**Файл:** `agrocosmos/views/pages.py` (view главного дашборда Agrocosmos)

**Симптом:** каждый заход на главную `/agrocosmos/` запускает запрос

```sql
SELECT DISTINCT EXTRACT(YEAR FROM acquired_date)
FROM agro_vegetation_index
WHERE index_type = 'ndvi'
ORDER BY 1;
```

который на текущем объёме таблицы `agro_vegetation_index` (сотни миллионов
строк после baseline 2020-2025) крутится **4+ минут**, держит PostgreSQL
backend и gunicorn-worker, после чего клиент получает 504. Наблюдалось
многократно в ходе baseline-прогона (май 2026).

**Где всплывает в `pg_stat_activity`:**
```
state=active, xact_age=200-300s,
query=SELECT DISTINCT EXTRACT(YEAR FROM "agro_vegetation_index"."acquired_date") ...
```

**Корневая причина:** полный скан огромной таблицы ради десятка уникальных годов.

**Предлагаемое решение (по порядку предпочтения):**

1. **Кэш в Redis на 1 час** — минимальный патч, безопасный:
   ```python
   from django.core.cache import cache

   years = cache.get('agrocosmos:ndvi_years')
   if years is None:
       years = list(
           VegetationIndex.objects
           .filter(index_type='ndvi')
           .dates('acquired_date', 'year', order='DESC')
           .values_list('acquired_date__year', flat=True)
       )
       cache.set('agrocosmos:ndvi_years', years, 3600)
   ```
   Трейдофф: новые годы появятся в UI через час после первых записей.

2. **Материализованный список** — держать `MIN/MAX(acquired_date)` в отдельной
   таблице-метаданных, обновлять в signal после сохранения VI (либо
   post-stage в пайплайне). Мгновенный UI, нет устаревания.

3. **Частичный индекс** — `CREATE INDEX ... ON agro_vegetation_index
   (EXTRACT(YEAR FROM acquired_date)) WHERE index_type='ndvi'`. Уменьшит
   стоимость, но всё равно медленнее (1)/(2) на таком объёме.

**Приоритет:** высокий — блокирует нормальную работу публичного дашборда
во время и после baseline-прогона.

**Смежное:** возможно аналогичные `DISTINCT`/`COUNT` есть и в других
местах админки (регион/район фильтры); стоит аудит при фиксе.

---

## [ ] FIX: resumable zonal stats в `modis_ndvi`
**Файл:** `agrocosmos/management/commands/modis_ndvi.py` (Step 2, цикл по `chunks`)

**Симптом:** при падении пайплайна (OOM, краш БД, рестарт worker'а) регион
перезапускается с нуля по зональной статистике. Пример (май 2026): после
падения ВМ БД Алтайский край и Амурская область получили `failed`, потом
`run_baseline_ndvi` их переэнкьюил — и они пошли считать все ~137
композитов заново, хотя большая часть `VegetationIndex` уже была в БД.

**Текущее поведение:**
- Download: ✅ пропускает уже скачанные `.tif` (`os.path.exists`).
- Zonal stats: ❌ проходит все `chunks` подряд, вызывает
  `compute_zonal_stats(...)` для каждого композита.
- Запись в БД: ✅ безопасна (UPSERT через
  `bulk_create(update_conflicts=True, unique_fields=['farmland','scene','index_type'])`).

То есть данные не дублируются, но CPU-время тратится повторно —
а это 60-70% времени пайплайна региона, в случае Алтайского края это сутки.

**Предлагаемое решение:**
Перед дорогостоящим `compute_zonal_stats` проверять, покрыт ли уже
композит в `VegetationIndex`. Примерный шаблон:

```python
mid_date = cf + (ct - cf) / 2
# «Покрыт» = записей столько же, сколько фарм-угодий в регионе/районе.
have = (VegetationIndex.objects
        .filter(
            index_type='ndvi',
            acquired_date=mid_date,
            farmland__district__region=region if not district else None,
            farmland__district=district if district else None,
        )
        .count())
if have >= len(fl_geoms) * 0.99:   # 99% = допускаем пустые полигоны
    self.stdout.write(
        f'  [{i+1}/{len(chunks)}] {cf}..{ct} — already in DB ({have}), skip'
    )
    continue
```

Два уточнения:
1. Ключ к однозначности композита — `(index_type, acquired_date, scene)`.
   Если `acquired_date` вычисляется детерминированно от `(cf, ct)` — всё
   ок. Сейчас так и есть: `mid_date = cf + (ct - cf) / 2`.
2. Порог 99% нужен потому что часть полигонов могла не получить валидных
   пикселей (облачность), и их в `VegetationIndex` для этого композита
   нет. Альтернатива — считать уникальные `farmland_id` в `scene`
   созданной для этого композита (у MODIS `scene_id = f'modis_{date}_{district_id}'`),
   но это та же семантика.

**Альтернатива:** отдельная служебная таблица `PipelineCheckpoint(region,
composite_date, status)` — более явная, но лишняя сущность для одной
команды.

**Приоритет:** средний. Сейчас «съедает» только повторные часы-сутки при
редких крашах. После стабилизации инфры баsline-прогона можно убрать
вообще, но полезно для будущих массовых пересчётов.

---
