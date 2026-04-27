"""Import a single-region farmland GeoJSON into ``agro_farmland``.

Сделано под выгрузку **«Московская область 2021.geojson»**, у которой
фиксированная схема (4 атрибута, 100 % заполнены)::

    name        text   — вид угодий (Пашня / Пастбище / Сенокос / …)
    Neisp_All   text   — факт использования
    S_ha        float  — площадь, га
    unused_sig  text   — служебный флаг (попадает в properties JSON)

Геометрия — ``MultiPolygon`` в EPSG:3857; команда репроецирует в 4326
через ``ogr2ogr -t_srs``.

Тот же бинарный путь, что и у ``import_farmlands_rosreestr``: ogr2ogr →
staging-таблица → ``INSERT ... SELECT`` с маппингом → DROP staging.
В отличие от Росреестра, тут нет детектирования схемы — колонки
известны жёстко.

Usage::

    # Сухой прогон (схема, план, без записи в БД):
    python manage.py import_farmland_geojson \\
        --geojson "C:/.../Московская область 2021.geojson" \\
        --region "Московская область" \\
        --dry-run

    # Загрузка с предварительным удалением всего вектора региона:
    python manage.py import_farmland_geojson \\
        --geojson "C:/.../Московская область 2021.geojson" \\
        --region "Московская область" \\
        --source mosobl_2021 \\
        --replace
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from agrocosmos.models import Farmland, Region


# ---------------------------------------------------------------------------
# Маппинги, идентичные подходу farmland_importer (но статичные —
# схема GeoJSON фиксирована, схема-детект тут не нужен).
# ---------------------------------------------------------------------------

# name → CropType. Для незнакомых текстовых лейблов выставляем 'other'
# (а НЕ NULL, потому что ``crop_type`` NOT NULL). Колонка ``properties``
# сохранит исходный текст для аудита.
_CROP_CASES: tuple[tuple[str, str], ...] = (
    ('пашня',                    'arable'),
    ('залежь',                   'fallow'),
    ('сенокос',                  'hayfield'),
    ('сенокосы',                 'hayfield'),
    ('пастбище',                 'pasture'),
    ('пастбища',                 'pasture'),
    ('многолетние насаждения',   'perennial'),
    ('многолетнее насаждение',   'perennial'),
    # Подмосковный мусор (по 1 строке) — однолетние травы и культура
    # «ячмень» по сути это пашня.
    ('одн травы',                'arable'),
    ('однолетние травы',         'arable'),
    ('ячмень',                   'arable'),
)


# Neisp_All → is_used (BOOLEAN/NULL).
#   used:    «пашня обрабатываемая», «другие используемые угодья»
#   unused:  «зарощенные (залесенные) сельскохозяйственные угодья»,
#            «нет следов сельхоздеятельности более 3 лет»,
#            «нецелевое использование сельхозугодий»
# Нецелевое использование — формально «занято», но фактически не по
# назначению; для агрокосмической логики (мониторинг с/х активности)
# трактуется как «не используется».
_USED_TRUE: tuple[str, ...] = (
    'пашня обрабатываемая',
    'другие используемые угодья',
)
_USED_FALSE: tuple[str, ...] = (
    'зарощенные (залесенные) сельскохозяйственные угодья',
    'нет следов сельхоздеятельности более 3 лет',
    'нецелевое использование сельхозугодий',
)


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _pg_dsn() -> str:
    db = settings.DATABASES['default']
    parts = [
        f"host={db['HOST']}",
        f"port={db.get('PORT') or 5432}",
        f"dbname={db['NAME']}",
        f"user={db['USER']}",
    ]
    if db.get('PASSWORD'):
        parts.append(f"password={db['PASSWORD']}")
    return 'PG:' + ' '.join(parts)


# ---------------------------------------------------------------------------
# Команда
# ---------------------------------------------------------------------------


class Command(BaseCommand):
    help = (
        'Импорт GeoJSON с с/х угодьями (схема: name, Neisp_All, S_ha, '
        'unused_sig) в agro_farmland с маппингом атрибутов в модель.'
    )

    # Системные checks (особенно URL-резолвинг) тянут весь legacy-стек
    # с requests/openpyxl/Pillow. Команде они не нужны — мы ходим только
    # в agrocosmos.models и в БД через connection.cursor(). Отключаем,
    # чтобы загрузчик был запускаем из минимального venv.
    requires_system_checks: list[str] = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument(
            '--geojson', required=True,
            help='Путь к GeoJSON-файлу.',
        )
        parser.add_argument(
            '--region', required=True,
            help='Имя субъекта РФ как в Region.name (например, "Московская область").',
        )
        parser.add_argument(
            '--source', default='',
            help='Значение для Farmland.source (по умолчанию: имя файла без расширения).',
        )
        parser.add_argument(
            '--replace', action='store_true',
            help='Перед импортом удалить все существующие Farmland для этого региона.',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Не трогать БД: только показать план и SQL.',
        )
        parser.add_argument(
            '--ogr2ogr', default='ogr2ogr',
            help='Путь к бинарю ogr2ogr (по умолчанию — из PATH).',
        )
        parser.add_argument(
            '--analyze', action='store_true',
            help='Запустить ANALYZE agro_farmland в конце.',
        )

    # ------------------------------------------------------------------

    def handle(self, *args, **opts):
        geojson = Path(opts['geojson']).expanduser()
        if not geojson.is_file():
            raise CommandError(f'GeoJSON not found: {geojson}')

        binary = opts['ogr2ogr']
        if not opts['dry_run'] and not shutil.which(binary):
            raise CommandError(
                f'{binary!r} не найден в PATH. Установите gdal-bin или передайте --ogr2ogr.'
            )

        region = Region.objects.filter(name__iexact=opts['region']).first()
        if region is None:
            raise CommandError(f'Region not found: {opts["region"]!r}')

        source_id = (opts['source'] or geojson.stem)[:40]
        staging = f'staging_farmland_geojson_{self._slug(region.code)}'

        existing = Farmland.objects.filter(region=region).count()
        self.stdout.write(self.style.NOTICE(
            f'Регион: {region.name} (id={region.pk}, code={region.code})\n'
            f'GeoJSON: {geojson}  ({geojson.stat().st_size / 1024 / 1024:.1f} MB)\n'
            f'Уже в БД для этого региона: {existing:,} угодий\n'
            f'Источник (Farmland.source): {source_id!r}\n'
            f'Staging: {staging}\n'
            f'Replace: {opts["replace"]}    Dry-run: {opts["dry_run"]}'
        ))

        if opts['dry_run']:
            self.stdout.write('\n--- INSERT SQL preview (первые 1500 символов) ---')
            self.stdout.write(self._build_insert_sql(staging, region.pk, source_id)[:1500])
            return

        # 1) Удалить старые угодья региона
        if opts['replace'] and existing > 0:
            t0 = time.time()
            self.stdout.write(f'  удаляю {existing:,} существующих Farmland для региона…')
            # Через ORM, чтобы каскадно почистить vegetation_index/alert/phenology
            # (Django ON DELETE CASCADE на уровне приложения).
            deleted, by_model = Farmland.objects.filter(region=region).delete()
            self.stdout.write(self.style.WARNING(
                f'  удалено: {deleted:,} строк ({by_model})  за {time.time() - t0:.1f}s'
            ))

        # 2) ogr2ogr → staging
        t0 = time.time()
        self.stdout.write(f'  ogr2ogr → {staging}…')
        rc = self._run_ogr2ogr(geojson, staging, binary)
        if rc != 0:
            self.stderr.write(self.style.WARNING(
                f'  ogr2ogr rc={rc} (продолжаю — часть фич могла быть пропущена)'
            ))
        self.stdout.write(f'  ogr2ogr done in {time.time() - t0:.1f}s')

        # 3) Подсчитать staged, INSERT, DROP
        with connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{staging}";')
            staged = cur.fetchone()[0]
            self.stdout.write(f'  staged {staged:,} строк')

            if staged == 0:
                cur.execute(f'DROP TABLE IF EXISTS "{staging}";')
                self.stdout.write(self.style.WARNING('  staging пустой — нечего вставлять'))
                return

            t0 = time.time()
            sql = self._build_insert_sql(staging, region.pk, source_id)
            with transaction.atomic():
                cur.execute(sql)
                inserted = cur.rowcount
            cur.execute(f'DROP TABLE IF EXISTS "{staging}";')

        self.stdout.write(self.style.SUCCESS(
            f'  inserted={inserted:,}  за {time.time() - t0:.1f}s'
        ))

        if opts['analyze']:
            with connection.cursor() as cur:
                cur.execute('ANALYZE agro_farmland;')
            self.stdout.write('ANALYZE agro_farmland done.')

    # ------------------------------------------------------------------
    # Внутренние помощники
    # ------------------------------------------------------------------

    @staticmethod
    def _slug(text: str) -> str:
        s = re.sub(r'[^A-Za-z0-9]+', '_', text or '').strip('_').lower()
        return s or 'unknown'

    def _run_ogr2ogr(self, geojson: Path, staging: str, binary: str) -> int:
        """Стейджит GeoJSON в Postgres с репроекцией 3857→4326.

        Используем те же -lco что и в шейп-импортёре: GEOMETRY_NAME,
        FID, отсутствие spatial-index (не нужно на временной таблице),
        LAUNDER=NO (сохраняем оригинальные имена ``Neisp_All``/``S_ha``)
        и PRECISION=NO (на случай если драйвер угадает узкую numeric).
        """
        args = [
            binary,
            '-f', 'PostgreSQL',
            _pg_dsn(),
            str(geojson),
            '-nln', staging,
            '-nlt', 'MULTIPOLYGON',
            '-t_srs', 'EPSG:4326',
            '-lco', 'GEOMETRY_NAME=wkb_geometry',
            '-lco', 'FID=ogc_fid',
            '-lco', 'SPATIAL_INDEX=NONE',
            '-lco', 'LAUNDER=NO',
            '-lco', 'PRECISION=NO',
            '-overwrite',
            '-skipfailures',
            '--config', 'PG_USE_COPY', 'YES',
            '--config', 'OGR_TRUNCATE', 'NO',
        ]
        env = os.environ.copy()
        proc = subprocess.run(
            args, env=env, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, errors='replace',
        )
        if proc.stdout:
            self.stdout.write(proc.stdout)
        if proc.stderr:
            self.stdout.write(proc.stderr)
        return proc.returncode

    def _build_insert_sql(self, staging: str, region_id: int, source_id: str) -> str:
        crop_when = '\n            '.join(
            f"WHEN {_sql_quote(lbl)} THEN {_sql_quote(ct)}"
            for lbl, ct in _CROP_CASES
        )
        used_true_in = ', '.join(_sql_quote(v) for v in _USED_TRUE)
        used_false_in = ', '.join(_sql_quote(v) for v in _USED_FALSE)

        # Колонки в staging (при LAUNDER=NO ogr2ogr сохраняет регистр):
        #   "name", "Neisp_All", "S_ha", "unused_sig", wkb_geometry
        return f"""
INSERT INTO agro_farmland (
    region_id, district_id, crop_type, is_used, cadastral_number,
    area_ha, geom, properties, source, created_at
)
SELECT
    {int(region_id)}::int,
    NULL::int,
    CASE LOWER(TRIM(s."name"::text))
            {crop_when}
            ELSE 'other'
        END,
    CASE
            WHEN s."Neisp_All" IS NULL THEN NULL
            WHEN LOWER(TRIM(s."Neisp_All"::text)) IN ({used_true_in}) THEN TRUE
            WHEN LOWER(TRIM(s."Neisp_All"::text)) IN ({used_false_in}) THEN FALSE
            ELSE NULL
        END,
    '',
    COALESCE(NULLIF(s."S_ha"::text, '')::double precision, 0),
    ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Force2D(s.wkb_geometry)), 3))
        ::geometry(MultiPolygon, 4326) AS geom,
    jsonb_strip_nulls(jsonb_build_object(
        'Neisp_All',  NULLIF(TRIM(s."Neisp_All"::text), ''),
        'name_raw',   NULLIF(TRIM(s."name"::text), ''),
        'unused_sig', NULLIF(TRIM(s."unused_sig"::text), '')
    )),
    {_sql_quote(source_id)},
    NOW()
FROM "{staging}" s
WHERE s.wkb_geometry IS NOT NULL
  AND NOT ST_IsEmpty(s.wkb_geometry);
""".strip()
