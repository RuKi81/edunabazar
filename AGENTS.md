# Заметки для агентов

## Тесты и проверки (локально, Windows)

- Интерпретатор — системный `python` (3.14); в `.venv` и `.venv313` нет
  зависимостей проекта.
- Тесты: `python manage.py test agrocosmos my_fields legacy --keepdb --noinput`
  (нужен локальный PostgreSQL + PostGIS из `.env`). Один модуль:
  `python manage.py test agrocosmos.tests.test_report_country --keepdb --noinput`.
- Перед запуском выставить `PYTHONUTF8=1` и `PROJ_LIB` на PROJ от OSGeo4W
  (`%LOCALAPPDATA%\Programs\OSGeo4W\share\proj`). С системным `PROJ_LIB`
  (PostGIS) падают ~30 тестов с пересчётом площадей/проекций
  (`my_fields.tests.test_api`, `agrocosmos.tests.test_import_vector`) — это
  окружение, не код.
- `--parallel` локально не работает (нет `tblib`); в CI работает.
- Линтер: `python -m flake8 agrocosmos my_fields legacy` (конфиг `.flake8`).
- Миграции: `python manage.py makemigrations --check --dry-run` — проверяется в CI.
- Локальная dev-БД отстала по миграциям: проверять на ней отчёты и карты нельзя.
  Страницы проверяются на тестовой БД (`StaticLiveServerTestCase` + Playwright).

## Код

- Окончания строк в рабочей копии смешанные (часть файлов CRLF) — при правке
  сохранять те, что есть в файле.
- Агрокосмос многострановой: `Region.country`, реестр
  `agrocosmos/services/countries.py`. Выборки регионов без явной страны
  должны фильтроваться по России (`DEFAULT_COUNTRY`).

## Деплой

- Push в `main` → CI (flake8, тесты) → автодеплой на VM1; см. `ARCHITECTURE.md` §10.
