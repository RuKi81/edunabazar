"""Импорт шейп-файлов (в ZIP) в отдельные таблицы PostGIS.

Пользователь бросает ZIP-архив в окно ГИС-портала. Внутри может быть один
или несколько наборов шейп-файлов (``.shp`` + ``.shx`` + ``.dbf`` [+ ``.prj``
+ ``.cpg``]). Каждый ``.shp`` импортируется в СВОЮ таблицу PostGIS:

* геометрия репроецируется в EPSG:4326 (как у остальных слоёв портала);
* атрибуты сохраняются (имена sanitized/dedup'нуты под идентификаторы Postgres);
* создаётся GIST-индекс по ``geom`` — чтобы MVT-эндпоинт работал быстро;
* метаданные слоя пишутся в реестр :class:`my_fields.models.GisLayer`.

Читаем через ``django.contrib.gis.gdal.DataSource`` (ctypes-обёртка GDAL,
идёт в комплекте GeoDjango — доп. пакет ``osgeo`` не нужен). DDL/INSERT
собираем через ``psycopg.sql`` (безопасное квотирование идентификаторов),
чтобы произвольные имена полей шейп-файла не открывали SQL-инъекцию.
"""
from __future__ import annotations

import logging
import os
import re
import zipfile

from django.contrib.gis.gdal import CoordTransform, DataSource, SpatialReference
from django.contrib.gis.gdal.field import (
    OFTDate, OFTDateTime, OFTInteger, OFTInteger64, OFTReal, OFTTime,
)
from django.db import connection, transaction
from django.db.models import Max
from psycopg import sql

logger = logging.getLogger('my_fields')

TABLE_PREFIX = 'gis_up_'
MAX_IDENT = 63  # предел длины идентификатора в PostgreSQL

# Палитра для авто-раскраски слоёв на карте (round-robin по числу слоёв).
LAYER_COLORS = [
    '#e91e63', '#3f51b5', '#009688', '#ff9800', '#9c27b0',
    '#2196f3', '#4caf50', '#f44336', '#00bcd4', '#795548',
]


class ShapefileImportError(Exception):
    """Ошибка импорта конкретного шейп-файла (сообщение — для UI)."""


def slugify_identifier(name: str, fallback: str = 'layer') -> str:
    """Привести произвольную строку к безопасному Postgres-идентификатору.

    Нижний регистр, только ``[a-z0-9_]``, не начинается с цифры. Пусто →
    ``fallback``. Длину здесь НЕ режем (это делает вызывающий с учётом
    префикса/суффикса уникальности).
    """
    s = re.sub(r'[^a-z0-9_]+', '_', name.strip().lower())
    s = re.sub(r'_+', '_', s).strip('_')
    if not s:
        s = fallback
    if s[0].isdigit():
        s = f'_{s}'
    return s


def _unique_table_name(base: str) -> str:
    """Сгенерировать уникальное имя таблицы ``gis_up_<base>[_N]`` ≤ 63 симв."""
    from my_fields.models import GisLayer

    core = slugify_identifier(base)
    room = MAX_IDENT - len(TABLE_PREFIX)
    core = core[:room]
    candidate = f'{TABLE_PREFIX}{core}'
    i = 1
    existing = set(
        GisLayer.objects.values_list('table_name', flat=True),
    )
    while candidate in existing or _table_exists(candidate):
        suffix = f'_{i}'
        candidate = f'{TABLE_PREFIX}{core[:room - len(suffix)]}{suffix}'
        i += 1
    return candidate


def _table_exists(name: str) -> bool:
    with connection.cursor() as cur:
        cur.execute('SELECT to_regclass(%s)', [f'public.{name}'])
        return cur.fetchone()[0] is not None


def _pg_type_for(ogr_field_cls) -> str:
    """OGR-тип поля → тип колонки PostgreSQL."""
    if ogr_field_cls is OFTInteger:
        return 'integer'
    if ogr_field_cls is OFTInteger64:
        return 'bigint'
    if ogr_field_cls is OFTReal:
        return 'double precision'
    if ogr_field_cls is OFTDate:
        return 'date'
    if ogr_field_cls in (OFTDateTime, OFTTime):
        return 'timestamptz' if ogr_field_cls is OFTDateTime else 'time'
    return 'text'


def _geom_kind(ogr_geom_type: str) -> str:
    """Упростить OGR geom type до point/line/polygon/other для рендера."""
    t = (ogr_geom_type or '').lower()
    if 'point' in t:
        return 'point'
    if 'line' in t or 'linestring' in t:
        return 'line'
    if 'polygon' in t:
        return 'polygon'
    return 'other'


def _build_columns(field_names, field_types):
    """Составить список колонок (dedup'нуть, обойти reserved id/geom).

    Возвращает ``(columns, attributes_meta)`` где columns —
    ``[(db_name, pg_type, ogr_name), ...]``.
    """
    used = {'id', 'geom'}
    columns = []
    meta = []
    for ogr_name, ftype in zip(field_names, field_types):
        db = slugify_identifier(ogr_name, fallback='attr')[:MAX_IDENT]
        base = db
        i = 1
        while db in used:
            suffix = f'_{i}'
            db = f'{base[:MAX_IDENT - len(suffix)]}{suffix}'
            i += 1
        used.add(db)
        pg_type = _pg_type_for(ftype)
        columns.append((db, pg_type, ogr_name))
        meta.append({'name': ogr_name, 'db': db, 'type': pg_type})
    return columns, meta


def import_zip(uploaded_file, owner=None, archive_name: str = '') -> dict:
    """Распаковать ZIP и импортировать каждый ``.shp`` в свою таблицу.

    Args:
        uploaded_file: файловый объект (Django ``UploadedFile`` или любой
            с ``.read()``/``chunks``).
        owner: Django-пользователь (или None) — записывается в реестр.
        archive_name: исходное имя архива (для метаданных/логов).

    Returns:
        ``{'created': [GisLayer, ...], 'errors': [{'file': str, 'error': str}]}``.
    """
    import tempfile

    archive_name = archive_name or getattr(uploaded_file, 'name', 'upload.zip')
    created, errors = [], []

    with tempfile.TemporaryDirectory(prefix='gis_shp_') as tmp:
        zip_path = os.path.join(tmp, 'archive.zip')
        with open(zip_path, 'wb') as fh:
            for chunk in _iter_chunks(uploaded_file):
                fh.write(chunk)

        extract_dir = os.path.join(tmp, 'extracted')
        os.makedirs(extract_dir, exist_ok=True)
        try:
            _safe_extract(zip_path, extract_dir)
        except (zipfile.BadZipFile, ShapefileImportError) as e:
            raise ShapefileImportError(f'Некорректный ZIP-архив: {e}')

        shp_paths = _find_shapefiles(extract_dir)
        if not shp_paths:
            raise ShapefileImportError(
                'В архиве не найдено ни одного .shp файла.')

        for shp_path in shp_paths:
            base = os.path.splitext(os.path.basename(shp_path))[0]
            try:
                layer = import_shapefile(
                    shp_path, title=base, owner=owner,
                    archive_name=archive_name,
                )
                created.append(layer)
            except Exception as e:  # noqa: BLE001 — конкретный shp не должен рушить всё
                logger.warning('SHP import failed for %s: %s', shp_path, e)
                errors.append({'file': os.path.basename(shp_path), 'error': str(e)})

    return {'created': created, 'errors': errors}


def import_shapefile(shp_path: str, title: str, owner=None,
                     archive_name: str = ''):
    """Импортировать один ``.shp`` в новую таблицу + запись реестра."""
    from my_fields.models import GisLayer

    ds = DataSource(shp_path)
    if len(ds) == 0:
        raise ShapefileImportError('Слой пуст (нет OGR-layer).')
    layer = ds[0]

    ogr_geom_type = str(layer.geom_type)
    kind = _geom_kind(ogr_geom_type)

    # Определяем исходную проекцию и трансформацию в 4326.
    # SpatialReference(4326) строим ЛЕНИВО — только когда реально нужно
    # репроецировать: конструктор трогает proj.db, а у шейп-файлов без .prj
    # (srs=None) координаты уже в lon/lat, и лишний вызов PROJ ни к чему.
    src_srs = layer.srs
    srid_original = 0
    ct = None
    if src_srs is not None:
        try:
            srid_original = int(src_srs.srid or 0)
        except (TypeError, ValueError):
            srid_original = 0
        if srid_original != 4326:
            try:
                ct = CoordTransform(src_srs, SpatialReference(4326))
            except Exception:  # pragma: no cover — экзотические CRS
                ct = None

    columns, attr_meta = _build_columns(layer.fields, layer.field_types)
    table_name = _unique_table_name(title)
    color = LAYER_COLORS[GisLayer.objects.count() % len(LAYER_COLORS)]
    next_order = (
        GisLayer.objects.aggregate(m=Max('sort_order'))['m'] or 0
    ) + 1

    _create_table(table_name, columns)
    feature_count = _copy_features(table_name, layer, columns, ct)
    _finalize_table(table_name)
    extent = _table_extent(table_name)

    return GisLayer.objects.create(
        title=title[:200],
        table_name=table_name,
        original_filename=os.path.basename(shp_path)[:255],
        source_archive=(archive_name or '')[:255],
        geom_kind=kind,
        geom_type=ogr_geom_type[:40],
        srid_original=srid_original,
        feature_count=feature_count,
        attributes=attr_meta,
        extent=extent,
        color=color,
        sort_order=next_order,
        owner=owner if getattr(owner, 'is_authenticated', False) else None,
    )


# ── Низкоуровневые операции с БД ────────────────────────────────────────

def _create_table(table_name: str, columns) -> None:
    col_defs = [sql.SQL('id serial PRIMARY KEY')]
    for db_name, pg_type, _ in columns:
        col_defs.append(sql.SQL('{} {}').format(
            sql.Identifier(db_name), sql.SQL(pg_type)))
    col_defs.append(sql.SQL('geom geometry(Geometry, 4326)'))
    ddl = sql.SQL('CREATE TABLE {} ({})').format(
        sql.Identifier(table_name), sql.SQL(', ').join(col_defs))
    with connection.cursor() as cur:
        cur.execute(ddl)


def _copy_features(table_name: str, layer, columns, ct) -> int:
    col_idents = [sql.Identifier(db) for db, _, _ in columns]
    placeholders = [sql.Placeholder()] * len(columns)
    insert = sql.SQL('INSERT INTO {} ({}) VALUES ({}, ST_GeomFromText(%s, 4326))').format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(col_idents + [sql.Identifier('geom')]),
        sql.SQL(', ').join(placeholders),
    )
    ogr_names = [ogr for _, _, ogr in columns]

    count = 0
    batch = []
    with transaction.atomic(), connection.cursor() as cur:
        for feat in layer:
            geom = feat.geom
            if geom is None:
                continue
            if ct is not None:
                try:
                    geom.transform(ct)
                except Exception:
                    continue
            values = [_field_value(feat, name) for name in ogr_names]
            values.append(geom.wkt)
            batch.append(values)
            count += 1
            if len(batch) >= 1000:
                cur.executemany(insert, batch)
                batch = []
        if batch:
            cur.executemany(insert, batch)
    return count


def _finalize_table(table_name: str) -> None:
    idx = sql.Identifier(f'{table_name}_geom_gix')
    with connection.cursor() as cur:
        cur.execute(sql.SQL('CREATE INDEX {} ON {} USING GIST (geom)').format(
            idx, sql.Identifier(table_name)))


def _table_extent(table_name: str):
    with connection.cursor() as cur:
        cur.execute(sql.SQL(
            'SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) '
            'FROM (SELECT ST_Extent(geom) AS e FROM {}) s'
        ).format(sql.Identifier(table_name)))
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return [float(v) for v in row]


def drop_layer(layer) -> None:
    """Удалить физическую таблицу слоя и запись реестра."""
    table_name = layer.table_name
    with connection.cursor() as cur:
        cur.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(
            sql.Identifier(table_name)))
    layer.delete()


# ── Чтение / правка атрибутов объектов слоя ──────────────────────────────

# Типы колонок, которые мы сами создаём в _pg_type_for — только они
# допускаются как явный CAST в UPDATE (значения из meta слоя, не из запроса,
# но список всё равно фиксируем, чтобы не собирать произвольный SQL-тип).
_ALLOWED_CAST_TYPES = frozenset({
    'integer', 'bigint', 'double precision', 'date', 'timestamptz',
    'time', 'text',
})


def _attr_db_types(layer) -> dict:
    """``{db_column: pg_type}`` из meta слоя (только атрибутивные колонки)."""
    out = {}
    for a in (layer.attributes or []):
        db = a.get('db')
        if db:
            out[db] = a.get('type', 'text')
    return out


def _json_safe(value):
    """Привести значение колонки к JSON-совместимому виду."""
    import datetime
    import decimal
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def list_features(layer, limit: int = 1000, offset: int = 0) -> dict:
    """Постранично прочитать объекты слоя (id + атрибуты, без геометрии).

    Возвращает ``{'total': int, 'results': [{'id': int, 'props': {...}}]}``.
    Геометрию не отдаём — таблица атрибутов её не показывает, а полигоны
    могут быть тяжёлыми.
    """
    types = _attr_db_types(layer)
    cols = list(types.keys())
    table = sql.Identifier(layer.table_name)

    select_cols = sql.SQL(', ').join(
        [sql.Identifier('id')] + [sql.Identifier(c) for c in cols])
    query = sql.SQL(
        'SELECT {cols} FROM {table} ORDER BY id LIMIT %s OFFSET %s'
    ).format(cols=select_cols, table=table)

    with connection.cursor() as cur:
        cur.execute(query, [limit, offset])
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
        cur.execute(sql.SQL('SELECT count(*) FROM {}').format(table))
        total = cur.fetchone()[0]

    results = []
    for row in rows:
        d = dict(zip(colnames, row))
        fid = d.pop('id', None)
        props = {k: _json_safe(v) for k, v in d.items()}
        results.append({'id': fid, 'props': props})
    return {'total': total, 'results': results}


def update_feature(layer, fid: int, props: dict) -> int:
    """Обновить атрибуты одного объекта. Возвращает число затронутых строк.

    Обновляются только колонки, присутствующие в meta слоя (остальные ключи
    игнорируются). Пустая строка/``None`` для не-текстовых типов → ``NULL``.
    """
    types = _attr_db_types(layer)
    set_parts = []
    params = []
    for key, val in (props or {}).items():
        pg_type = types.get(key)
        if pg_type is None:
            continue  # неизвестная/защищённая колонка — молча пропускаем
        if pg_type not in _ALLOWED_CAST_TYPES:
            pg_type = 'text'
        is_blank = val is None or (isinstance(val, str) and val.strip() == '')
        if is_blank and pg_type != 'text':
            set_parts.append(sql.SQL('{} = NULL').format(sql.Identifier(key)))
        else:
            set_parts.append(sql.SQL('{} = %s::{}').format(
                sql.Identifier(key), sql.SQL(pg_type)))
            params.append(val)
    if not set_parts:
        return 0
    params.append(fid)
    query = sql.SQL('UPDATE {} SET {} WHERE id = %s').format(
        sql.Identifier(layer.table_name), sql.SQL(', ').join(set_parts))
    with connection.cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount


# ── Вспомогательное ─────────────────────────────────────────────────────

def _field_value(feat, ogr_name):
    """Безопасно достать значение атрибута (None при любой ошибке чтения)."""
    try:
        return feat.get(ogr_name)
    except Exception:
        return None


def _iter_chunks(uploaded_file):
    if hasattr(uploaded_file, 'chunks'):
        yield from uploaded_file.chunks()
    else:
        uploaded_file.seek(0)
        while True:
            chunk = uploaded_file.read(1024 * 1024)
            if not chunk:
                break
            yield chunk


def _safe_extract(zip_path: str, dest: str) -> None:
    """Распаковать ZIP с защитой от zip-slip (path traversal)."""
    dest_abs = os.path.abspath(dest)
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            target = os.path.abspath(os.path.join(dest, member))
            if not (target == dest_abs or target.startswith(dest_abs + os.sep)):
                raise ShapefileImportError(f'Небезопасный путь в архиве: {member}')
        zf.extractall(dest)


def _find_shapefiles(root: str) -> list:
    """Найти все ``.shp`` рекурсивно (без учёта регистра расширения)."""
    result = []
    for dirpath, _dirs, files in os.walk(root):
        for fname in files:
            if fname.lower().endswith('.shp'):
                result.append(os.path.join(dirpath, fname))
    return sorted(result)
