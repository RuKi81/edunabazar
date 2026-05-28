"""Импорт фактической урожайности с ЕМИСС (Росстат) в ``CropYieldStat``.

ЕМИСС публикует выгрузки в формате XLS/XLSX с такой структурой:

    row 0:  заголовок («Урожайность сельскохозяйственных культур … ц/га …»)
    row 2:  «Хозяйства всех категорий»            ← фильтр категорий
    row 3:  «Зерновые и зернобобовые культуры»    ← фильтр культуры
    row 4:  пустая ячейка | 2010 | 2011 | … | 2023   ← годы по колонкам
    row 5+: «Российская Федерация» / «… федеральный округ» / субъект
            и значения урожайности в ц/га

Команда:
    - читает любой sheet, где обнаруживает строку с годами (DOY-fallback нет);
    - пропускает строки-агрегаты (РФ, федеральные округа);
    - пропускает строки-дубликаты («... (кроме ... автономного округа)»);
    - матчит название субъекта на ``Region.name`` (точное → без скобок →
      без регистра); неудачи логирует, не падает;
    - конвертирует ц/га → т/га (÷ 10);
    - делает upsert в ``CropYieldStat`` через ``update_or_create`` по
      (region, year, crop, source).

Использование:
    python manage.py import_emiss_yield "data (1).xls"
    python manage.py import_emiss_yield path/to/yield.xlsx --crop grains_total
    python manage.py import_emiss_yield file.xls --dry-run        # ничего не пишет
    python manage.py import_emiss_yield file.xls --sheet 0        # выбрать лист
    python manage.py import_emiss_yield file.xls --source-note "ЕМИСС 06.04.2026"
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from agrocosmos.models import CropYieldStat, Region, YieldCrop


# Строки, которые точно НЕ субъекты (агрегаты или служебные).
_AGGREGATE_RE = re.compile(
    r'^(российская\s+федерация|.+?\s+федеральный\s+округ)\b',
    re.IGNORECASE,
)

# «(кроме … автономного округа)» — дубликат родительского субъекта.
_EXCL_PARENTHETICAL_RE = re.compile(r'\s*\(кроме\b.*?\)\s*$', re.IGNORECASE)

# Любая концевая скобка вида «(Адыгея)», «(с 29.07.2016)» и т.п.
_TAIL_PARENS_RE = re.compile(r'\s*\([^()]*\)\s*$')

# Дефисный «двойник»: «Кемеровская область - Кузбасс» → «Кемеровская область».
# Сохраняем дефис БЕЗ окружающих пробелов («Северная Осетия-Алания» не трогаем).
_TAIL_DASH_ALIAS_RE = re.compile(r'\s+-\s+\S.*$')


def _normalize_region_name(raw: str) -> str:
    """Подготовить название из XLS к матчингу с ``Region.name``."""
    s = raw.strip()
    # «Республика Адыгея (Адыгея)» → «Республика Адыгея»
    s = _TAIL_PARENS_RE.sub('', s).strip()
    # Двойные пробелы внутри
    s = re.sub(r'\s+', ' ', s)
    return s


def _normalize_region_name_aggressive(raw: str) -> str:
    """Более жёсткая нормализация — fallback, когда обычная не сработала.

    Дополнительно убирает «двойное» название через ` - …` суффикс
    («Кемеровская область - Кузбасс» → «Кемеровская область»).
    """
    s = _normalize_region_name(raw)
    s = _TAIL_DASH_ALIAS_RE.sub('', s).strip()
    return s


def _is_aggregate(name: str) -> bool:
    return bool(_AGGREGATE_RE.match(name.strip()))


def _is_excluding_subregion(name: str) -> bool:
    """«Архангельская область (кроме Ненецкого автономного округа)»."""
    return bool(_EXCL_PARENTHETICAL_RE.search(name))


def _to_float(value) -> Optional[float]:
    """Преобразовать ячейку из XLS (число / строка / NaN) во float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        # pandas даёт NaN; явная проверка
        return f if f == f else None  # NaN != NaN
    s = str(value).strip().replace(',', '.').replace('\u00a0', '')
    if not s or s in ('-', '—', '…', 'н/д', 'нд'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _find_header_row(df) -> Optional[int]:
    """Найти строку, в которой по колонкам идут числа-годы.

    Возвращаем индекс строки (0-based). Признак — в строке ≥ 4 ячеек
    с целыми числами в диапазоне 1990..2100.
    """
    for i in range(min(20, len(df))):
        row = df.iloc[i]
        year_like = 0
        for cell in row:
            v = _to_float(cell)
            if v is not None and v.is_integer() and 1990 <= v <= 2100:
                year_like += 1
        if year_like >= 4:
            return i
    return None


class Command(BaseCommand):
    help = 'Импорт фактической урожайности из XLS-выгрузки ЕМИСС в CropYieldStat'

    def add_arguments(self, parser):
        parser.add_argument('path', help='Путь к XLS/XLSX-файлу ЕМИСС')
        parser.add_argument(
            '--sheet', default=0,
            help='Имя или индекс листа Excel (по умолчанию первый)',
        )
        parser.add_argument(
            '--crop', default=YieldCrop.GRAINS_TOTAL,
            choices=[c.value for c in YieldCrop],
            help='Культура (по умолчанию grains_total — общий ряд по зерновым)',
        )
        parser.add_argument(
            '--source-note', default='',
            help='Комментарий к источнику (имя файла, дата выгрузки и т.п.)',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Только разобрать файл и показать сводку, БЕЗ записи в БД',
        )

    def handle(self, *args, **opts):
        path = Path(opts['path'])
        if not path.exists():
            raise CommandError(f'Файл не найден: {path}')

        try:
            import pandas as pd
        except ImportError as e:
            raise CommandError(
                'Требуется pandas. Установите: pip install pandas xlrd openpyxl'
            ) from e

        # pandas сам выберет engine: xlrd для .xls, openpyxl для .xlsx
        sheet = opts['sheet']
        try:
            sheet_idx = int(sheet)
            sheet_arg: object = sheet_idx
        except (TypeError, ValueError):
            sheet_arg = sheet

        try:
            df = pd.read_excel(path, sheet_name=sheet_arg, header=None)
        except Exception as e:
            raise CommandError(f'Не удалось прочитать XLS: {e}') from e

        header_row = _find_header_row(df)
        if header_row is None:
            raise CommandError(
                'Не найдена строка с годами. Это точно выгрузка ЕМИСС?'
            )

        # Собираем колонки → годы
        col_to_year: dict[int, int] = {}
        for col_idx, cell in enumerate(df.iloc[header_row]):
            v = _to_float(cell)
            if v is not None and v.is_integer() and 1990 <= v <= 2100:
                col_to_year[col_idx] = int(v)

        if not col_to_year:
            raise CommandError('Заголовок найден, но не извлечено ни одного года.')

        self.stdout.write(self.style.SUCCESS(
            f'Файл:     {path.name}\n'
            f'Заголовок: строка {header_row}, годы {min(col_to_year.values())}..{max(col_to_year.values())} '
            f'({len(col_to_year)} лет)'
        ))

        # Предзагружаем регионы для матчинга.
        regions_by_name: dict[str, Region] = {
            r.name.strip(): r for r in Region.objects.all().only('id', 'name')
        }
        regions_by_norm: dict[str, Region] = {
            _normalize_region_name(r.name).lower(): r for r in regions_by_name.values()
        }

        crop = opts['crop']
        note = opts['source_note'] or path.name

        # Статистика прохода
        stats = {
            'rows_seen': 0,
            'aggregates_skipped': 0,
            'duplicates_skipped': 0,
            'unmatched_regions': [],
            'records_created': 0,
            'records_updated': 0,
            'records_unchanged': 0,
            'cells_empty': 0,
        }
        seen_normalized: set[str] = set()

        # Парсим в память, БД-операции — единой транзакцией.
        to_upsert: list[dict] = []
        for row_idx in range(header_row + 1, len(df)):
            label_raw = df.iat[row_idx, 0]
            if label_raw is None or (isinstance(label_raw, float) and label_raw != label_raw):
                continue
            label = str(label_raw).strip()
            if not label:
                continue
            stats['rows_seen'] += 1

            if _is_aggregate(label):
                stats['aggregates_skipped'] += 1
                continue
            if _is_excluding_subregion(label):
                stats['duplicates_skipped'] += 1
                continue

            norm = _normalize_region_name(label)
            if norm.lower() in seen_normalized:
                stats['duplicates_skipped'] += 1
                continue
            seen_normalized.add(norm.lower())

            region = (
                regions_by_name.get(label)
                or regions_by_name.get(norm)
                or regions_by_norm.get(norm.lower())
                # Fallback: убрать « - …» суффикс
                # («Кемеровская область - Кузбасс» → «Кемеровская область»).
                or regions_by_norm.get(_normalize_region_name_aggressive(label).lower())
            )
            if region is None:
                stats['unmatched_regions'].append(label)
                continue

            for col_idx, year in col_to_year.items():
                value = _to_float(df.iat[row_idx, col_idx])
                if value is None:
                    stats['cells_empty'] += 1
                    continue
                # ЕМИСС: ц/га → т/га
                yield_t_ha = round(value / 10.0, 4)
                to_upsert.append({
                    'region_id': region.id,
                    'year': year,
                    'crop': crop,
                    'source': CropYieldStat.Source.EMISS,
                    'yield_t_per_ha': yield_t_ha,
                    'note': note,
                })

        if opts['dry_run']:
            self._print_summary(stats, to_upsert, dry_run=True)
            return

        # Запись в БД
        with transaction.atomic():
            for rec in to_upsert:
                obj, created = CropYieldStat.objects.update_or_create(
                    region_id=rec['region_id'],
                    district=None,
                    year=rec['year'],
                    crop=rec['crop'],
                    source=rec['source'],
                    defaults={
                        'yield_t_per_ha': rec['yield_t_per_ha'],
                        'source_note': rec['note'],
                    },
                )
                if created:
                    stats['records_created'] += 1
                else:
                    # update_or_create возвращает False для created даже
                    # если все поля совпали; различаем по фактическому
                    # обновлению нельзя без дополнительного SELECT.
                    # Сводим под «updated» — это безопасно для отчёта.
                    stats['records_updated'] += 1

        self._print_summary(stats, to_upsert, dry_run=False)

    def _print_summary(self, stats, to_upsert, *, dry_run: bool):
        self.stdout.write('')
        self.stdout.write(self.style.MIGRATE_HEADING('═══ Сводка ═══'))
        self.stdout.write(f'  Строк просмотрено:           {stats["rows_seen"]}')
        self.stdout.write(f'  Агрегатов пропущено (РФ/ФО): {stats["aggregates_skipped"]}')
        self.stdout.write(f'  Дубликатов пропущено:        {stats["duplicates_skipped"]}')
        self.stdout.write(f'  Пустых ячеек:                {stats["cells_empty"]}')
        self.stdout.write(f'  К сохранению (region, year): {len(to_upsert)}')

        if stats['unmatched_regions']:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING(
                f'  Не сматчилось субъектов: {len(stats["unmatched_regions"])}'
            ))
            for name in stats['unmatched_regions'][:30]:
                self.stdout.write(f'    • {name!r}')
            if len(stats['unmatched_regions']) > 30:
                self.stdout.write(f'    … и ещё {len(stats["unmatched_regions"]) - 30}')

        if dry_run:
            self.stdout.write('')
            self.stdout.write(self.style.WARNING('Dry-run: в БД ничего не записано.'))
            # Покажем пример записей
            if to_upsert:
                self.stdout.write('  Примеры (первые 5):')
                for rec in to_upsert[:5]:
                    self.stdout.write(
                        f'    region_id={rec["region_id"]:>4d}  '
                        f'year={rec["year"]}  crop={rec["crop"]}  '
                        f'yield={rec["yield_t_per_ha"]:.2f} т/га'
                    )
        else:
            self.stdout.write('')
            self.stdout.write(self.style.SUCCESS(
                f'  Создано/обновлено в БД: {stats["records_created"] + stats["records_updated"]} '
                f'(новых: {stats["records_created"]})'
            ))
