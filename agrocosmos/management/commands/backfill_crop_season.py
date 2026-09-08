"""Бэкофилл классификации озимые/яровые/сенокос + признак «убрано».

Прогоняет ``classify_winter_spring`` по ВСЕМ уже загруженным данным
детального мониторинга: находит все комбинации (регион, год, источник),
для которых есть зональная статистика NDVI (S2/L8 → ``raster``,
HLS Fused → ``fused``), и классифицирует каждую.

Опорные точки культур здесь НЕ подключаются (они есть не для всех
регионов) — используются пороги по умолчанию/гейты сервиса. Для регионов
с разметкой (например, Тула) точечно перезапустите ``classify_winter_spring``
с ``--reference-layer``/``--reference-shp`` для калибровки.

Примеры:
    # Всё, что загружено (все регионы/годы/источники)
    python manage.py backfill_crop_season

    # Только один регион и год
    python manage.py backfill_crop_season --region-id 71 --year 2026

    # Только fused-источник, показать план без записи
    python manage.py backfill_crop_season --source fused --dry-run
"""
import time
from io import StringIO

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connection

RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')
FUSED_SATELLITES = ('hls_fused',)


class Command(BaseCommand):
    help = ('Бэкофилл classify_winter_spring по всем загруженным данным '
            'детального мониторинга (регион x год x источник).')

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int, default=None,
                            help='Ограничить одним регионом')
        parser.add_argument('--year', type=int, default=None,
                            help='Ограничить одним годом')
        parser.add_argument('--source', choices=['raster', 'fused'],
                            default=None,
                            help='Ограничить одним источником (по умолч. оба)')
        parser.add_argument('--dry-run', action='store_true',
                            help='Только показать план (без классификации)')

    def handle(self, *args, **options):
        combos = self._resolve_combos(options)
        if not combos:
            self.stdout.write(self.style.WARNING(
                'Нет загруженных данных детального мониторинга под фильтр.'
            ))
            return

        self.stdout.write(
            f'Найдено комбинаций (регион x год x источник): {len(combos)}'
        )
        for region_id, year, source in combos:
            self.stdout.write(f'  - регион={region_id}, год={year}, '
                              f'источник={source}')
        if options['dry_run']:
            self.stdout.write('[DRY RUN] классификация не запускалась.')
            return

        t0 = time.time()
        ok = 0
        for region_id, year, source in combos:
            self.stdout.write(
                f'\n=== classify: регион={region_id}, год={year}, '
                f'источник={source} ==='
            )
            out = StringIO()
            try:
                call_command(
                    'classify_winter_spring',
                    region_id=region_id, year=year, source=source,
                    stdout=out, stderr=out,
                )
                ok += 1
            except Exception as exc:  # noqa: BLE001 — продолжаем остальные
                self.stderr.write(f'  ОШИБКА: {exc}')
            # Прокидываем итоговую строку сводки классификатора.
            for line in out.getvalue().splitlines():
                if 'Классифицировано' in line:
                    self.stdout.write('  ' + line.strip())

        self.stdout.write(
            f'\nГотово: {ok}/{len(combos)} комбинаций за '
            f'{time.time() - t0:.0f}с.'
        )

    def _resolve_combos(self, options):
        """[(region_id, year, source), ...] с данными (регион x год x источник)."""
        satellites = []
        if options['source'] in (None, 'raster'):
            satellites += list(RASTER_SATELLITES)
        if options['source'] in (None, 'fused'):
            satellites += list(FUSED_SATELLITES)

        where = [
            "vi.index_type = 'ndvi'",
            "vi.mean >= -0.2 AND vi.mean <= 1",
            f"sc.satellite IN ({', '.join(['%s'] * len(satellites))})",
        ]
        params = list(satellites)
        if options['region_id'] is not None:
            where.append('d.region_id = %s')
            params.append(options['region_id'])
        if options['year'] is not None:
            where.append('EXTRACT(year FROM vi.acquired_date) = %s')
            params.append(options['year'])

        sql = f"""
            SELECT DISTINCT
                d.region_id,
                EXTRACT(year FROM vi.acquired_date)::int AS yr,
                CASE
                    WHEN sc.satellite IN ('hls_fused') THEN 'fused'
                    ELSE 'raster'
                END AS src
            FROM agro_vegetation_index vi
            JOIN agro_farmland f ON f.id = vi.farmland_id
            JOIN agro_district d ON d.id = f.district_id
            JOIN agro_satellite_scene sc ON sc.id = vi.scene_id
            WHERE {' AND '.join(where)} AND d.region_id IS NOT NULL
            ORDER BY d.region_id, yr, src
        """
        with connection.cursor() as cur:
            cur.execute(sql, params)
            return [(int(r[0]), int(r[1]), r[2]) for r in cur.fetchall()]
