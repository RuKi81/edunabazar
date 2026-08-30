"""Заменить ЗСН (Росреестр, ``agro_farmland``) на территории слоя-границы
полигонами из другого загруженного ГИС-слоя.

Разовая админ-операция для «пересадки» датасета ЗСН по одному субъекту
(например, Тульской области): удаляем существующие ``Farmland`` в пределах
контура слоя-границы и вставляем полигоны из слоя-источника. Оба слоя —
записи реестра ``myf_gis_layer`` с физическими таблицами PostGIS
(``gis_up_*``, колонка ``geom geometry(Geometry, 4326)``).

Безопасность (операция затрагивает общий референс-датасет, отдаётся всем
как MVT-тайлы):

* ``--inspect``  — печатает метаданные обоих слоёв (атрибуты, число
  объектов, геометрия) и выходит; БД не трогается;
* ``--dry-run``  — считает, сколько строк было бы удалено/вставлено, без
  записи;
* реальное изменение требует явного ``--yes`` и выполняется в ОДНОЙ
  транзакции (delete + insert): падение откатывает всё.

Атрибутивный маппинг (опционально): исходные Русские подписи вида угодья
маппятся в ``Farmland.CropType`` через существующий
``agrocosmos.services.farmland_crop_mapping.MAPPING``; факт использования —
приблизительно из текстовой колонки. Все исходные поля объекта сохраняются
в ``Farmland.properties`` (JSONB).

Примеры::

    # 1) Посмотреть колонки слоя-источника (ничего не меняет):
    python manage.py replace_farmlands_from_layer \\
        --boundary-layer "1.1 Границы субъекта" \\
        --source-layer "Sovremennyye granitsy Tulskaya" --inspect

    # 2) Прогон вхолостую (счётчики delete/insert):
    python manage.py replace_farmlands_from_layer \\
        --boundary-layer "1.1 Границы субъекта" \\
        --source-layer "Sovremennyye granitsy Tulskaya" \\
        --region "Тульская область" --dry-run

    # 3) Применить с маппингом полей:
    python manage.py replace_farmlands_from_layer \\
        --boundary-layer "1.1 Границы субъекта" \\
        --source-layer "Sovremennyye granitsy Tulskaya" \\
        --region "Тульская область" \\
        --crop-field vid_ugod --usage-field fact_isp \\
        --cadastral-field kadastr --source-tag "gis_layer/tula_2026" --yes

После применения имеет смысл проставить районы:
``python manage.py assign_farmland_district --region "Тульская область"``.
"""
from __future__ import annotations

import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from psycopg import sql

from agrocosmos.models import Region
from agrocosmos.services.farmland_crop_mapping import MAPPING
from my_fields.models import GisLayer


_PREDICATES = ('intersects', 'within', 'centroid')


class Command(BaseCommand):
    help = (
        'Заменить agro_farmland (ЗСН) в пределах слоя-границы полигонами из '
        'слоя-источника (оба — загруженные ГИС-слои gis_up_*).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--boundary-layer', required=True,
            help='Слой-граница территории (id или подстрока названия).',
        )
        parser.add_argument(
            '--source-layer', default=None,
            help='Слой-источник геометрии (id или подстрока названия). '
                 'Необязателен: без него команда только УДАЛЯЕТ ЗСН.',
        )
        parser.add_argument(
            '--delete-only', action='store_true',
            help='Только удалить ЗСН в границах, без вставки (источник не нужен).',
        )
        parser.add_argument(
            '--region', default=None,
            help='Регион для region_id вставляемых угодий (id/код/название). '
                 'Если не задан — region_id остаётся NULL.',
        )
        parser.add_argument(
            '--crop-field', default=None,
            help='Колонка слоя-источника с видом угодья (маппится в CropType).',
        )
        parser.add_argument(
            '--usage-field', default=None,
            help='Колонка слоя-источника с фактом использования (текст).',
        )
        parser.add_argument(
            '--cadastral-field', default=None,
            help='Колонка слоя-источника с кадастровым номером.',
        )
        parser.add_argument(
            '--default-crop', default='arable',
            help='CropType по умолчанию, если вид угодья не распознан '
                 '(default: arable).',
        )
        parser.add_argument(
            '--source-tag', default=None,
            help='Значение Farmland.source для вставленных строк '
                 '(default: gis_layer/<table_name>).',
        )
        parser.add_argument(
            '--delete-predicate', default='intersects', choices=_PREDICATES,
            help='Как отбирать удаляемые угодья относительно контура границы: '
                 'intersects (пересекают), within (целиком внутри), '
                 'centroid (центроид внутри). Default: intersects.',
        )
        parser.add_argument(
            '--inspect', action='store_true',
            help='Только показать метаданные слоёв и выйти (БД не трогается).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Посчитать delete/insert без записи.',
        )
        parser.add_argument(
            '--yes', action='store_true',
            help='Подтвердить реальное изменение данных (обязателен для записи).',
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **opts):
        boundary = self._resolve_layer(opts['boundary_layer'], 'boundary-layer')
        source = None
        if opts.get('source_layer'):
            source = self._resolve_layer(opts['source_layer'], 'source-layer')
        # Режим только-удаление: явный флаг ИЛИ отсутствие слоя-источника.
        delete_only = bool(opts.get('delete_only')) or source is None

        if opts['inspect']:
            self._print_layer(boundary, 'ГРАНИЦА')
            if source is not None:
                self._print_layer(source, 'ИСТОЧНИК')
            return

        predicate = opts['delete_predicate']
        boundary_tbl = sql.Identifier(boundary.table_name)
        del_count = self._count_delete(boundary_tbl, predicate)

        self.stdout.write(self.style.NOTICE(
            f'[replace] граница={boundary.title!r} ({boundary.table_name}, '
            f'{boundary.feature_count} об.)  predicate={predicate}'
        ))

        # Параметры вставки готовим только если есть источник.
        source_tbl = region_id = source_tag = None
        crop_field = usage_field = cad_field = None
        if not delete_only:
            crop_field = self._check_field(source, opts.get('crop_field'), 'crop-field')
            usage_field = self._check_field(source, opts.get('usage_field'), 'usage-field')
            cad_field = self._check_field(source, opts.get('cadastral_field'), 'cadastral-field')
            region = self._resolve_region(opts.get('region'))
            region_id = region.id if region else None
            source_tag = opts.get('source_tag') or f'gis_layer/{source.table_name}'
            source_tbl = sql.Identifier(source.table_name)
            ins_count = self._count_source(source_tbl)
            self.stdout.write(self.style.NOTICE(
                f'[replace] источник={source.title!r} ({source.table_name}, '
                f'{source.feature_count} об.)  регион='
                f'{region.name if region else "—"} (region_id={region_id})  '
                f'source={source_tag!r}'
            ))
            self.stdout.write(self.style.WARNING(
                f'[replace] УДАЛИТЬ ЗСН: {del_count:,}   ВСТАВИТЬ из источника: '
                f'{ins_count:,}'
            ))
        else:
            self.stdout.write(self.style.WARNING(
                f'[replace] РЕЖИМ УДАЛЕНИЯ (без вставки). УДАЛИТЬ ЗСН: {del_count:,}'
            ))

        if opts['dry_run']:
            self.stdout.write(self.style.NOTICE('[replace] dry-run — БД не изменена.'))
            return
        if not opts['yes']:
            raise CommandError(
                'Отказ: реальное изменение общего датасета требует флага --yes. '
                'Сначала прогоните с --dry-run.'
            )

        self._apply(
            boundary_tbl, source_tbl, predicate, region_id, source_tag,
            crop_field, usage_field, cad_field, opts['default_crop'],
            delete_only=delete_only,
        )

    # ------------------------------------------------------------------
    # SQL-построители
    # ------------------------------------------------------------------
    @staticmethod
    def _boundary_cte(boundary_tbl):
        """CTE ``b(g)`` — объединённый валидный контур слоя-границы (4326)."""
        return sql.SQL(
            'WITH b AS (SELECT ST_Union(ST_MakeValid(ST_Force2D(geom))) AS g '
            'FROM {tbl} WHERE geom IS NOT NULL)'
        ).format(tbl=boundary_tbl)

    @staticmethod
    def _predicate_sql(predicate):
        if predicate == 'within':
            return sql.SQL('ST_Within(f.geom, b.g)')
        if predicate == 'centroid':
            return sql.SQL('ST_Contains(b.g, ST_PointOnSurface(f.geom))')
        return sql.SQL('f.geom && b.g AND ST_Intersects(f.geom, b.g)')

    def _count_delete(self, boundary_tbl, predicate) -> int:
        stmt = sql.SQL('{cte} SELECT count(*) FROM agro_farmland f, b WHERE {pred}').format(
            cte=self._boundary_cte(boundary_tbl), pred=self._predicate_sql(predicate),
        )
        with connection.cursor() as cur:
            cur.execute(stmt)
            return int(cur.fetchone()[0] or 0)

    @staticmethod
    def _count_source(source_tbl) -> int:
        stmt = sql.SQL(
            'SELECT count(*) FROM {tbl} st WHERE st.geom IS NOT NULL AND NOT '
            'ST_IsEmpty(ST_CollectionExtract(ST_MakeValid(ST_Force2D(st.geom)), 3))'
        ).format(tbl=source_tbl)
        with connection.cursor() as cur:
            cur.execute(stmt)
            return int(cur.fetchone()[0] or 0)

    @staticmethod
    def _crop_expr(crop_field, default_crop):
        if not crop_field:
            return sql.Literal(default_crop)
        whens = [
            sql.SQL('WHEN {lbl} THEN {crop}').format(
                lbl=sql.Literal(label), crop=sql.Literal(crop))
            for label, crop in MAPPING.items() if crop is not None
        ]
        return sql.SQL('CASE lower(btrim({col}::text)) {whens} ELSE {dflt} END').format(
            col=sql.Identifier(crop_field), whens=sql.SQL(' ').join(whens),
            dflt=sql.Literal(default_crop),
        )

    @staticmethod
    def _usage_expr(usage_field):
        if not usage_field:
            return sql.SQL('NULL::boolean')
        col = sql.Identifier(usage_field)
        return sql.SQL(
            "CASE WHEN lower(btrim({c}::text)) LIKE {neg} THEN false "
            "WHEN lower(btrim({c}::text)) LIKE {pos} THEN true "
            "ELSE NULL::boolean END"
        ).format(c=col, neg=sql.Literal('не %использ%'), pos=sql.Literal('%использ%'))

    @staticmethod
    def _cad_expr(cad_field):
        if not cad_field:
            return sql.Literal('')
        return sql.SQL("COALESCE(left({col}::text, 50), '')").format(
            col=sql.Identifier(cad_field))

    def _insert_stmt(self, source_tbl, region_id, source_tag,
                     crop_field, usage_field, cad_field, default_crop):
        """``INSERT ... SELECT`` из слоя-источника в agro_farmland."""
        return sql.SQL(
            'INSERT INTO agro_farmland '
            '(region_id, district_id, crop_type, is_used, cadastral_number, '
            'area_ha, geom, properties, source, created_at) '
            'SELECT {region}, NULL, {crop}, {usage}, {cad}, '
            'ST_Area(mgeom::geography) / 10000.0, mgeom, props, {src}, now() '
            'FROM ('
            '  SELECT st.*, '
            '    ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Force2D(st.geom)), 3)) AS mgeom, '
            "    (to_jsonb(st) - 'geom' - 'id') AS props "
            '  FROM {tbl} st WHERE st.geom IS NOT NULL'
            ') q WHERE mgeom IS NOT NULL AND NOT ST_IsEmpty(mgeom)'
        ).format(
            region=sql.Literal(region_id), crop=self._crop_expr(crop_field, default_crop),
            usage=self._usage_expr(usage_field), cad=self._cad_expr(cad_field),
            src=sql.Literal(source_tag), tbl=source_tbl,
        )

    # ------------------------------------------------------------------
    def _apply(self, boundary_tbl, source_tbl, predicate, region_id, source_tag,
               crop_field, usage_field, cad_field, default_crop, delete_only=False):
        t0 = time.monotonic()
        del_stmt = sql.SQL('{cte} DELETE FROM agro_farmland f USING b WHERE {pred}').format(
            cte=self._boundary_cte(boundary_tbl), pred=self._predicate_sql(predicate),
        )
        inserted = 0
        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute(del_stmt)
                deleted = cur.rowcount or 0
                if not delete_only:
                    cur.execute(self._insert_stmt(
                        source_tbl, region_id, source_tag,
                        crop_field, usage_field, cad_field, default_crop,
                    ))
                    inserted = cur.rowcount or 0
        self.stdout.write(self.style.SUCCESS(
            f'[replace] ГОТОВО за {time.monotonic() - t0:.1f}s: удалено '
            f'{deleted:,}, вставлено {inserted:,}.'
        ))
        if delete_only:
            self.stdout.write(self.style.NOTICE(
                '[replace] Вставка пропущена (режим удаления). '
                'Кэш MVT-тайлов обновится ~10 мин (или сбросьте вручную).'
            ))
        else:
            self.stdout.write(self.style.NOTICE(
                '[replace] Рекомендуется: assign_farmland_district '
                '(проставить районы) и сброс кэша MVT-тайлов.'
            ))

    # ------------------------------------------------------------------
    # Резолверы / вывод
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_layer(arg: str, opt_name: str) -> GisLayer:
        qs = GisLayer.objects.all()
        layer = None
        if str(arg).isdigit():
            layer = qs.filter(id=int(arg)).first()
        if layer is None:
            matches = list(qs.filter(title__icontains=arg)[:5])
            if len(matches) > 1:
                titles = '; '.join(f'#{m.id} {m.title!r}' for m in matches)
                raise CommandError(
                    f'--{opt_name}={arg!r}: найдено несколько слоёв: {titles}. '
                    'Уточните по id.'
                )
            layer = matches[0] if matches else None
        if layer is None:
            raise CommandError(f'--{opt_name}={arg!r}: слой не найден.')
        return layer

    @staticmethod
    def _check_field(layer: GisLayer, field, opt_name):
        if not field:
            return None
        dbs = {a.get('db') for a in (layer.attributes or [])}
        if field not in dbs:
            raise CommandError(
                f'--{opt_name}={field!r}: колонки нет в слое {layer.title!r}. '
                f'Доступные: {sorted(d for d in dbs if d)}'
            )
        return field

    @staticmethod
    def _resolve_region(region_arg):
        if not region_arg:
            return None
        qs = Region.objects.all()
        region = None
        if str(region_arg).isdigit():
            region = qs.filter(id=int(region_arg)).first()
        region = region or qs.filter(code=region_arg).first() \
            or qs.filter(name=region_arg).first() \
            or qs.filter(name__iexact=region_arg).first() \
            or qs.filter(name__icontains=region_arg).first()
        if region is None:
            raise CommandError(f'--region={region_arg!r}: регион не найден.')
        return region

    def _print_layer(self, layer: GisLayer, tag: str):
        self.stdout.write(self.style.NOTICE(
            f'=== {tag}: #{layer.id} {layer.title!r} ==='
        ))
        self.stdout.write(
            f'  table={layer.table_name}  geom_kind={layer.geom_kind}  '
            f'geom_type={layer.geom_type}  srid_orig={layer.srid_original}  '
            f'features={layer.feature_count}'
        )
        self.stdout.write('  Атрибуты (db → тип):')
        for a in (layer.attributes or []):
            self.stdout.write(
                f'    {a.get("db")}  ←  {a.get("name")!r}  [{a.get("type")}]'
            )
