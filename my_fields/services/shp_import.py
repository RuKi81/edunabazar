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

import io
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


def _layer_field_names(layer):
    """Имена полей слоя с устойчивым декодированием.

    Имя поля в DBF ограничено 10 БАЙТАМИ — кириллическое имя (UTF-8) может быть
    обрезано посреди многобайтового символа, и штатный ``layer.fields`` падает
    ``UnicodeDecodeError`` ещё до импорта. Читаем имена через тот же GDAL-capi,
    но декодируем снисходительно (``errors='replace'``), чтобы импорт не
    рушился (сами имена колонок в БД всё равно sanitized через slugify).
    """
    try:
        return list(layer.fields)
    except UnicodeDecodeError:
        from django.contrib.gis.gdal.prototypes import ds as capi
        names = []
        for i in range(layer.num_fields):
            try:
                raw = capi.get_field_name(
                    capi.get_field_defn(layer._ldefn, i))
            except Exception:
                raw = None
            if isinstance(raw, (bytes, bytearray)):
                names.append(bytes(raw).decode('utf-8', 'replace'))
            elif raw is None:
                names.append(f'attr_{i + 1}')
            else:
                names.append(str(raw))
        return names


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

    columns, attr_meta = _build_columns(
        _layer_field_names(layer), layer.field_types)
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


# ── Создание нового пустого слоя (тип геометрии + атрибуты) ─────────────

# geom_kind → OGR-подобная строка типа для реестра (geom-колонка всё равно
# generic Geometry(4326), как и у импортированных SHP).
_KIND_TO_GEOM_TYPE = {
    'point': 'Point',
    'line': 'LineString',
    'polygon': 'Polygon',
}

# Допустимые типы атрибутов для слоёв, создаваемых пользователем в UI
# (значение из формы → тип колонки PostgreSQL). Ограничены подмножеством
# _ALLOWED_CAST_TYPES, чтобы правка атрибутов (update_feature) работала.
NEW_LAYER_ATTR_TYPES = {
    'text': 'text',
    'integer': 'integer',
    'double precision': 'double precision',
    'date': 'date',
}


def _build_columns_typed(field_names, pg_types):
    """Как :func:`_build_columns`, но типы колонок заданы явно (pg-типы).

    Возвращает ``(columns, attributes_meta)`` где columns —
    ``[(db_name, pg_type, display_name), ...]``. Имена dedup'нуты и не
    пересекаются с reserved ``id``/``geom``.
    """
    used = {'id', 'geom'}
    columns = []
    meta = []
    for name, pg_type in zip(field_names, pg_types):
        db = slugify_identifier(name, fallback='attr')[:MAX_IDENT]
        base = db
        i = 1
        while db in used:
            suffix = f'_{i}'
            db = f'{base[:MAX_IDENT - len(suffix)]}{suffix}'
            i += 1
        used.add(db)
        columns.append((db, pg_type, name))
        meta.append({'name': name, 'db': db, 'type': pg_type})
    return columns, meta


def create_empty_layer(title: str, geom_kind: str, attributes=None, owner=None):
    """Создать новый ПУСТОЙ слой: таблица PostGIS + запись реестра.

    Args:
        title: название слоя (обязательное, непустое).
        geom_kind: ``point`` | ``line`` | ``polygon``.
        attributes: ``[{'name': str, 'type': str}, ...]`` — отображаемое имя
            и тип из :data:`NEW_LAYER_ATTR_TYPES`. Пустые имена пропускаются.
        owner: Django-пользователь (или None) — записывается в реестр.

    Returns:
        Созданный :class:`my_fields.models.GisLayer`.

    Raises:
        ShapefileImportError: некорректный заголовок/тип геометрии/атрибута
            (сообщение пригодно для показа в UI).
    """
    from my_fields.models import GisLayer

    title = (title or '').strip()
    if not title:
        raise ShapefileImportError('Укажите название слоя.')
    if geom_kind not in _KIND_TO_GEOM_TYPE:
        raise ShapefileImportError('Недопустимый тип геометрии.')

    field_names, field_pg = [], []
    for a in (attributes or []):
        if not isinstance(a, dict):
            raise ShapefileImportError('Некорректное описание атрибута.')
        name = str(a.get('name', '')).strip()
        if not name:
            continue  # безымянный атрибут — пропускаем
        pg_type = NEW_LAYER_ATTR_TYPES.get(a.get('type'))
        if pg_type is None:
            raise ShapefileImportError(
                f'Недопустимый тип атрибута: {a.get("type")!r}.')
        field_names.append(name)
        field_pg.append(pg_type)

    columns, attr_meta = _build_columns_typed(field_names, field_pg)
    table_name = _unique_table_name(title)
    color = LAYER_COLORS[GisLayer.objects.count() % len(LAYER_COLORS)]
    next_order = (
        GisLayer.objects.aggregate(m=Max('sort_order'))['m'] or 0
    ) + 1

    _create_table(table_name, columns)
    _finalize_table(table_name)

    return GisLayer.objects.create(
        title=title[:200],
        table_name=table_name,
        original_filename='',
        source_archive='',
        geom_kind=geom_kind,
        geom_type=_KIND_TO_GEOM_TYPE[geom_kind],
        srid_original=4326,
        feature_count=0,
        attributes=attr_meta,
        extent=None,
        color=color,
        sort_order=next_order,
        owner=owner if getattr(owner, 'is_authenticated', False) else None,
    )


# ── Материализация слоя из SELECT (фундамент выборки/оверлеев) ──────────

def create_layer_from_select(
    title: str, geom_kind: str, attr_meta, select_sql, params=None, *,
    owner=None, source_note: str = '', drop_empty: bool = True,
):
    """Создать новый слой из произвольного ``SELECT`` (``CREATE TABLE AS``).

    Общий механизм для SQL-выборки («сохранить результат как слой») и
    оверлейных операций. Вызывающий код собирает безопасный (через
    ``psycopg.sql``) ``SELECT``, который возвращает атрибутивные колонки под
    именами ``attr_meta[*]['db']`` и последнюю колонку геометрии с алиасом
    ``geom`` в EPSG:4326. Здесь мы материализуем результат в таблицу
    ``gis_up_*``, добавляем ``id serial PK`` и типобезопасную колонку
    ``geom geometry(Geometry, 4326)``, строим GIST-индекс и пишем запись
    реестра :class:`my_fields.models.GisLayer`.

    Args:
        title: название нового слоя (как в плашке).
        geom_kind: ``point`` | ``line`` | ``polygon`` | ``other``.
        attr_meta: ``[{'name', 'db', 'type'}, ...]`` — метаданные атрибутов
            результата (порядок совпадает с колонками ``SELECT`` до ``geom``).
        select_sql: ``psycopg.sql.SQL``/``Composed`` — тело ``SELECT`` целиком.
        params: параметры для ``select_sql``.
        owner: Django-пользователь (или None).
        source_note: пометка происхождения (пишется в ``source_archive``).
        drop_empty: удалить строки с NULL/пустой геометрией (для оверлеев,
            где пересечение может дать пустой результат).

    Returns:
        Созданный :class:`my_fields.models.GisLayer`.
    """
    from my_fields.models import GisLayer

    title = (title or '').strip()
    if not title:
        raise ShapefileImportError('Укажите название слоя.')

    table_name = _unique_table_name(title)
    table = sql.Identifier(table_name)
    color = LAYER_COLORS[GisLayer.objects.count() % len(LAYER_COLORS)]
    next_order = (
        GisLayer.objects.aggregate(m=Max('sort_order'))['m'] or 0
    ) + 1

    # Тяжёлые оверлеи/выборки на больших слоях не должны упираться в
    # глобальный statement_timeout — снимаем его на время построения таблицы.
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute('SET LOCAL statement_timeout = 0')
        cur.execute(
            sql.SQL('CREATE TABLE {t} AS {sel}').format(t=table, sel=select_sql),
            params or [],
        )
        if drop_empty:
            cur.execute(sql.SQL(
                'DELETE FROM {t} WHERE geom IS NULL OR ST_IsEmpty(geom)'
            ).format(t=table))
        # id serial PK — заполняется последовательно для уже вставленных строк.
        cur.execute(sql.SQL(
            'ALTER TABLE {t} ADD COLUMN id serial PRIMARY KEY'
        ).format(t=table))
        # Приводим geom к типобезопасной 2D-колонке 4326 (как у SHP-слоёв).
        cur.execute(sql.SQL(
            'ALTER TABLE {t} ALTER COLUMN geom TYPE geometry(Geometry, 4326) '
            'USING ST_SetSRID(ST_Force2D(geom), 4326)'
        ).format(t=table))
        cur.execute(sql.SQL(
            'CREATE INDEX {i} ON {t} USING GIST (geom)'
        ).format(i=sql.Identifier(f'{table_name}_geom_gix'), t=table))
        cur.execute(sql.SQL('SELECT count(*) FROM {t}').format(t=table))
        feature_count = int(cur.fetchone()[0] or 0)

    extent = _table_extent(table_name)
    geom_type = _KIND_TO_GEOM_TYPE.get(geom_kind, 'Geometry')

    return GisLayer.objects.create(
        title=title[:200],
        table_name=table_name,
        original_filename='',
        source_archive=(source_note or '')[:255],
        geom_kind=geom_kind if geom_kind in ('point', 'line', 'polygon') else 'other',
        geom_type=geom_type[:40],
        srid_original=4326,
        feature_count=feature_count,
        attributes=list(attr_meta or []),
        extent=extent,
        color=color,
        sort_order=next_order,
        owner=owner if getattr(owner, 'is_authenticated', False) else None,
    )


def create_layer_from_query(layer, title: str, filter_spec=None,
                            query_text: str = '', owner=None):
    """Материализовать результат SQL-выборки слоя в новый слой.

    Отбирает объекты ``layer`` по визуальному фильтру/поиску (та же логика,
    что в таблице атрибутов — :func:`list_features`) и сохраняет их (со всеми
    атрибутами и геометрией) в новый слой через :func:`create_layer_from_select`.
    """
    from .layer_query import build_where

    types = _attr_db_types(layer)
    cols = list(types.keys())
    where_sql, params = build_where(layer, cols, filter_spec, query_text)
    select_cols = [sql.Identifier(c) for c in cols] + [
        sql.SQL('geom AS {}').format(sql.Identifier('geom'))]
    select_sql = sql.SQL('SELECT {cols} FROM {table}{where}').format(
        cols=sql.SQL(', ').join(select_cols),
        table=sql.Identifier(layer.table_name),
        where=where_sql,
    )
    return create_layer_from_select(
        title, layer.geom_kind, list(layer.attributes or []),
        select_sql, params, owner=owner,
        source_note=f'query:{layer.table_name}',
    )


def duplicate_layer(layer, *, owner=None, title: str = ''):
    """Создать полную копию слоя (все объекты и атрибуты) в новый слой.

    Копирует геометрию и все атрибуты через :func:`create_layer_from_query`
    без фильтра, затем переносит оформление исходного слоя (цвет, стиль,
    папку). Название по умолчанию — с префиксом ``копия_``.
    """
    new_title = (title or f'копия_{layer.title}').strip()
    new_layer = create_layer_from_query(
        layer, new_title, filter_spec=None, query_text='', owner=owner)
    update_fields = []
    if layer.color:
        new_layer.color = layer.color
        update_fields.append('color')
    if isinstance(layer.style, dict) and layer.style:
        new_layer.style = dict(layer.style)
        update_fields.append('style')
    if layer.folder_id:
        new_layer.folder_id = layer.folder_id
        update_fields.append('folder')
    if update_fields:
        new_layer.save(update_fields=update_fields)
    return new_layer


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
    # ST_Force2D сплющивает Z/M-измерения: SHP нередко содержит 3D-геометрию
    # (PolygonZ и т.п.), а колонка geom объявлена 2D (geometry(Geometry,4326)),
    # иначе PostGIS падает с «Geometry has Z dimension but column does not».
    insert = sql.SQL(
        'INSERT INTO {} ({}) VALUES ({}, ST_Force2D(ST_GeomFromText(%s, 4326)))'
    ).format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(col_idents + [sql.Identifier('geom')]),
        sql.SQL(', ').join(placeholders),
    )
    # Читаем значения по ИНДЕКСУ поля (columns[j] ↔ OGR-поле j), а не по имени:
    # имя может быть обрезано/битым (10-байтный лимит DBF), и lookup по имени
    # тогда не находит поле. Индекс устойчив к этому.
    field_count = len(columns)

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
            values = [_field_value(feat, j) for j in range(field_count)]
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


def list_features(layer, limit: int = 1000, offset: int = 0,
                  sort: str = 'id', direction: str = 'asc',
                  query_text: str = '', filter_spec=None) -> dict:
    """Постранично прочитать объекты слоя (id + атрибуты, без геометрии).

    Поддерживает серверную сортировку и поиск — чтобы таблица работала на
    слоях с сотнями тысяч объектов (клиент грузит только текущую страницу).

    Args:
        limit/offset: окно страницы.
        sort: db-имя колонки для сортировки ('id' или колонка из meta слоя;
            неизвестное имя → 'id').
        direction: 'asc' | 'desc'.
        query_text: подстрока для поиска по всем колонкам (ILIKE), пусто — без
            фильтра.
        filter_spec: структурный фильтр визуального конструктора (см.
            :mod:`my_fields.services.layer_query`) или None. Комбинируется с
            ``query_text`` через AND.

    Возвращает ``{'total': int, 'results': [{'id': int, 'props': {...}}]}``,
    где ``total`` — число объектов с учётом фильтра/поиска.
    Геометрию не отдаём — таблица атрибутов её не показывает, а полигоны
    могут быть тяжёлыми.
    """
    from .layer_query import build_where

    types = _attr_db_types(layer)
    cols = list(types.keys())
    table = sql.Identifier(layer.table_name)

    # Сортировка: разрешаем только id или реальную колонку слоя.
    sort_col = sort if (sort == 'id' or sort in types) else 'id'
    dir_sql = sql.SQL('DESC') if str(direction).lower() == 'desc' else sql.SQL('ASC')
    # id — вторичный ключ для стабильного порядка при равных значениях.
    order_by = sql.SQL('ORDER BY {c} {d} NULLS LAST, {id} ASC').format(
        c=sql.Identifier(sort_col), d=dir_sql, id=sql.Identifier('id'))

    # WHERE: структурный фильтр + подстрочный поиск (оба опциональны).
    where_sql, where_params = build_where(layer, cols, filter_spec, query_text)

    select_cols = sql.SQL(', ').join(
        [sql.Identifier('id')] + [sql.Identifier(c) for c in cols])
    query = sql.SQL(
        'SELECT {cols} FROM {table}{where} {order} LIMIT %s OFFSET %s'
    ).format(cols=select_cols, table=table, where=where_sql, order=order_by)

    with connection.cursor() as cur:
        cur.execute(query, where_params + [limit, offset])
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
        count_q = sql.SQL('SELECT count(*) FROM {table}{where}').format(
            table=table, where=where_sql)
        cur.execute(count_q, where_params)
        total = cur.fetchone()[0]

    results = []
    for row in rows:
        d = dict(zip(colnames, row))
        fid = d.pop('id', None)
        props = {k: _json_safe(v) for k, v in d.items()}
        results.append({'id': fid, 'props': props})
    return {'total': total, 'results': results}


def list_feature_ids(layer, query_text: str = '', filter_spec=None) -> list:
    """Все id объектов слоя, удовлетворяющих фильтру/поиску (без пагинации).

    Используется для «выделить все объекты таблицы» — клиент получает полный
    набор id (с учётом текущего структурного фильтра и подстрочного поиска),
    минуя постраничную загрузку. Геометрию/атрибуты не отдаём — только id.
    """
    from .layer_query import build_where

    types = _attr_db_types(layer)
    cols = list(types.keys())
    table = sql.Identifier(layer.table_name)
    where_sql, where_params = build_where(layer, cols, filter_spec, query_text)
    query = sql.SQL('SELECT id FROM {table}{where} ORDER BY id ASC').format(
        table=table, where=where_sql)
    with connection.cursor() as cur:
        cur.execute(query, where_params)
        return [row[0] for row in cur.fetchall()]


def feature_rank(layer, fid: int, sort: str = 'id', direction: str = 'asc',
                 query_text: str = '', filter_spec=None) -> dict:
    """0-based позиция объекта ``fid`` в текущем порядке/фильтре.

    Нужна для двусторонней синхронизации карты и таблицы на слоях с сотнями
    тысяч объектов: клик по полигону → определяем страницу с этой строкой
    (``rank // page_size``) и переходим на неё, вместо тщетного поиска строки
    в уже загруженной странице.

    Порядок и фильтр поиска совпадают с :func:`list_features` (та же ``ORDER
    BY``/``WHERE``), поэтому ``rank`` согласован с постраничной выдачей.

    Возвращает ``{'rank': int|None, 'total': int}``. ``rank`` — ``None``, если
    объект не найден или отфильтрован поиском.
    """
    from .layer_query import build_where

    types = _attr_db_types(layer)
    cols = list(types.keys())
    table = sql.Identifier(layer.table_name)

    sort_col = sort if (sort == 'id' or sort in types) else 'id'
    dir_sql = sql.SQL('DESC') if str(direction).lower() == 'desc' else sql.SQL('ASC')
    order_by = sql.SQL('ORDER BY {c} {d} NULLS LAST, {id} ASC').format(
        c=sql.Identifier(sort_col), d=dir_sql, id=sql.Identifier('id'))

    where_sql, where_params = build_where(layer, cols, filter_spec, query_text)

    query = sql.SQL(
        'SELECT rn, total FROM ('
        'SELECT id, row_number() OVER ({order}) AS rn, '
        'count(*) OVER () AS total FROM {table}{where}'
        ') t WHERE id = %s'
    ).format(order=order_by, table=table, where=where_sql)

    with connection.cursor() as cur:
        cur.execute(query, where_params + [fid])
        row = cur.fetchone()
    if not row:
        # Объект отфильтрован поиском — вернём общий total без rank.
        count_q = sql.SQL('SELECT count(*) FROM {table}{where}').format(
            table=table, where=where_sql)
        with connection.cursor() as cur:
            cur.execute(count_q, where_params)
            total = cur.fetchone()[0]
        return {'rank': None, 'total': total}
    return {'rank': int(row[0]) - 1, 'total': int(row[1])}


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


# ── Правка ГЕОМЕТРИИ объектов слоя ──────────────────────────────────────

# Допустимые GeoJSON-типы геометрии для каждого geom_kind слоя. ``other`` —
# любой тип (в исходном шейпе была смешанная/нестандартная геометрия).
_GEOM_KIND_TYPES = {
    'point': frozenset({'Point', 'MultiPoint'}),
    'line': frozenset({'LineString', 'MultiLineString'}),
    'polygon': frozenset({'Polygon', 'MultiPolygon'}),
}


def geom_kind_allows(layer, geom_type: str) -> bool:
    """True, если тип геометрии совместим с ``layer.geom_kind``."""
    allowed = _GEOM_KIND_TYPES.get(layer.geom_kind)
    return True if allowed is None else geom_type in allowed


def _geos_from_geojson(geometry: dict):
    """GeoJSON-dict → валидная ``GEOSGeometry`` (SRID 4326) или ``ValueError``.

    Пустая/битая геометрия, а также пустой контур → ``ValueError`` (сообщение
    пригодно для показа в UI).
    """
    import json as _json

    from django.contrib.gis.gdal.error import GDALException
    from django.contrib.gis.geos import GEOSGeometry
    from django.contrib.gis.geos.error import GEOSException

    if not isinstance(geometry, dict) or not geometry.get('type'):
        raise ValueError('Ожидается объект геометрии GeoJSON.')
    # Битый GeoJSON может лететь как GEOSException/ValueError/TypeError, так и
    # GDALException (GEOSGeometry парсит JSON через OGR) — класс зависит от
    # версии GDAL; ловим все четыре.
    try:
        geom = GEOSGeometry(_json.dumps(geometry), srid=4326)
    except (GEOSException, GDALException, ValueError, TypeError) as exc:
        raise ValueError(f'Некорректная геометрия: {exc}') from exc
    if geom.empty:
        raise ValueError('Пустая геометрия.')
    if not geom.valid:
        # Пытаемся «починить» самопересечения и т.п. (ST_MakeValid-аналог).
        raise ValueError(f'Невалидная геометрия: {geom.valid_reason}')
    return geom


def get_features_geojson(layer, limit: int = 5000, bbox=None) -> dict:
    """FeatureCollection с ``id`` + точной геометрией (для загрузки в редактор).

    В отличие от MVT-тайлов (квантованных), отдаёт исходные координаты 4326 —
    их и правит mapbox-gl-draw. Ограничено ``limit`` объектов.

    ``bbox`` — необязательный кортеж ``(minx, miny, maxx, maxy)`` в EPSG:4326.
    Если задан, отдаются только объекты, пересекающие видимую область карты
    (``ST_Intersects`` с ``ST_MakeEnvelope``) — так тяжёлые слои грузятся в
    редактор порциями по экстенту, а не целиком до ``limit``.
    """
    import json as _json

    table = sql.Identifier(layer.table_name)
    params: list = []
    where = sql.SQL('geom IS NOT NULL')
    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        where = sql.SQL(
            'geom IS NOT NULL AND ST_Intersects('
            'geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))')
        params.extend([minx, miny, maxx, maxy])
    query = sql.SQL(
        'SELECT id, ST_AsGeoJSON(geom) FROM {table} '
        'WHERE {where} ORDER BY id LIMIT %s'
    ).format(table=table, where=where)
    params.append(max(1, min(int(limit), 20000)))
    feats = []
    with connection.cursor() as cur:
        cur.execute(query, params)
        for fid, gj in cur.fetchall():
            if not gj:
                continue
            feats.append({
                'type': 'Feature', 'id': fid,
                'geometry': _json.loads(gj), 'properties': {},
            })
    return {'type': 'FeatureCollection', 'features': feats}


def feature_extent(layer, fid: int):
    """Охват одного объекта слоя: ``[minx, miny, maxx, maxy]`` (EPSG:4326).

    Возвращает ``None``, если объект не найден или у него пустая геометрия.
    Используется для «перелёта» к объекту по клику 🔍 в таблице атрибутов —
    геометрия объекта в таблицу не грузится, поэтому охват берём с сервера.
    """
    table = sql.Identifier(layer.table_name)
    query = sql.SQL(
        'SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM ('
        'SELECT ST_Extent(geom) AS e FROM {table} WHERE id = %s'
        ') s'
    ).format(table=table)
    with connection.cursor() as cur:
        cur.execute(query, [fid])
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return [row[0], row[1], row[2], row[3]]


def create_feature(layer, geometry: dict) -> int:
    """Создать объект слоя с заданной геометрией (атрибуты — NULL).

    Возвращает ``id`` новой строки. Обновляет ``extent``/``feature_count``
    реестра. ``ValueError`` — если геометрия битая или не совпадает с типом слоя.
    """
    geom = _geos_from_geojson(geometry)
    if not geom_kind_allows(layer, geom.geom_type):
        raise ValueError(
            f'Тип {geom.geom_type} не подходит для слоя ({layer.geom_kind}).')
    query = sql.SQL(
        'INSERT INTO {table} (geom) VALUES (ST_GeomFromText(%s, 4326)) '
        'RETURNING id'
    ).format(table=sql.Identifier(layer.table_name))
    with connection.cursor() as cur:
        cur.execute(query, [geom.wkt])
        fid = cur.fetchone()[0]
    recompute_layer_meta(layer)
    return int(fid)


def update_feature_geom(layer, fid: int, geometry: dict) -> int:
    """Обновить геометрию объекта. Возвращает число затронутых строк.

    ``ValueError`` — битая геометрия или несовместимый с типом слоя тип.
    """
    geom = _geos_from_geojson(geometry)
    if not geom_kind_allows(layer, geom.geom_type):
        raise ValueError(
            f'Тип {geom.geom_type} не подходит для слоя ({layer.geom_kind}).')
    query = sql.SQL(
        'UPDATE {table} SET geom = ST_GeomFromText(%s, 4326) WHERE id = %s'
    ).format(table=sql.Identifier(layer.table_name))
    with connection.cursor() as cur:
        cur.execute(query, [geom.wkt, fid])
        rowcount = cur.rowcount
    if rowcount:
        recompute_layer_meta(layer)
    return rowcount


def delete_feature(layer, fid: int) -> int:
    """Удалить объект слоя. Возвращает число удалённых строк."""
    query = sql.SQL('DELETE FROM {table} WHERE id = %s').format(
        table=sql.Identifier(layer.table_name))
    with connection.cursor() as cur:
        cur.execute(query, [fid])
        rowcount = cur.rowcount
    if rowcount:
        recompute_layer_meta(layer)
    return rowcount


def delete_features(layer, fids) -> int:
    """Пакетно удалить объекты слоя по списку id (один SQL-запрос).

    Заменяет пофайловое удаление на фронте: при «выделить все объекты» тысячи
    параллельных DELETE-запросов упирались в лимит соединений и часть падала —
    объекты «оставались» в таблице. Здесь удаляем всё одним ``id = ANY(%s)``.
    """
    ids = []
    for f in (fids or []):
        try:
            ids.append(int(f))
        except (TypeError, ValueError):
            continue
    if not ids:
        return 0
    query = sql.SQL('DELETE FROM {table} WHERE id = ANY(%s)').format(
        table=sql.Identifier(layer.table_name))
    with connection.cursor() as cur:
        cur.execute(query, [ids])
        rowcount = cur.rowcount
    if rowcount:
        recompute_layer_meta(layer)
    return rowcount


def recompute_layer_meta(layer) -> None:
    """Пересчитать ``extent`` и ``feature_count`` слоя после правки геометрии."""
    with connection.cursor() as cur:
        cur.execute(sql.SQL('SELECT count(*) FROM {}').format(
            sql.Identifier(layer.table_name)))
        count = cur.fetchone()[0]
    layer.extent = _table_extent(layer.table_name)
    layer.feature_count = int(count or 0)
    layer.save(update_fields=['extent', 'feature_count'])


_NUMERIC_PG_TYPES = frozenset({'integer', 'bigint', 'double precision'})


def field_stats(layer, field: str, distinct_limit: int = 60):
    """Статистика по колонке слоя для построения тематической раскраски.

    Для числовых колонок → ``{'numeric': True, 'min', 'max', 'count'}``.
    Для остальных → ``{'numeric': False, 'values': [{'value', 'count'}],
    'truncated': bool}`` (уникальные значения, отсортированы по частоте).

    Возвращает ``None`` если ``field`` не является атрибутом слоя.
    """
    types = _attr_db_types(layer)
    pg_type = types.get(field)
    if pg_type is None:
        return None

    table = sql.Identifier(layer.table_name)
    col = sql.Identifier(field)
    numeric = pg_type in _NUMERIC_PG_TYPES

    with connection.cursor() as cur:
        if numeric:
            cur.execute(sql.SQL(
                'SELECT min({c}), max({c}), count({c}) FROM {t}'
            ).format(c=col, t=table))
            lo, hi, cnt = cur.fetchone()
            return {
                'field': field, 'type': pg_type, 'numeric': True,
                'min': None if lo is None else float(lo),
                'max': None if hi is None else float(hi),
                'count': int(cnt or 0),
            }
        limit = max(1, min(int(distinct_limit), 500))
        cur.execute(sql.SQL(
            'SELECT {c}::text AS v, count(*) AS n FROM {t} '
            'GROUP BY {c} ORDER BY n DESC, v LIMIT %s'
        ).format(c=col, t=table), [limit + 1])
        rows = cur.fetchall()

    truncated = len(rows) > limit
    values = [{'value': r[0], 'count': int(r[1])} for r in rows[:limit]]
    return {
        'field': field, 'type': pg_type, 'numeric': False,
        'values': values, 'truncated': truncated,
    }


def distinct_values(layer, field: str, limit: int = 500):
    """Перечень уникальных значений колонки (для value-фильтра по чекбоксам).

    В отличие от :func:`field_stats`, работает для колонок ЛЮБОГО типа (в т.ч.
    числовых) — возвращает список ``{'value', 'count'}``, отсортированный по
    частоте. Значение ``None`` (SQL ``NULL``) сохраняется как отдельный пункт,
    чтобы в UI можно было отфильтровать пустые ячейки.

    Возвращает ``None`` если ``field`` не является атрибутом слоя, иначе
    ``{'field', 'type', 'values': [{'value', 'count'}], 'truncated': bool,
    'has_null': bool}``.
    """
    types = _attr_db_types(layer)
    pg_type = types.get(field)
    if pg_type is None:
        return None

    table = sql.Identifier(layer.table_name)
    col = sql.Identifier(field)
    limit = max(1, min(int(limit), 2000))

    with connection.cursor() as cur:
        cur.execute(sql.SQL(
            'SELECT {c}::text AS v, count(*) AS n FROM {t} '
            'GROUP BY {c} ORDER BY n DESC, v LIMIT %s'
        ).format(c=col, t=table), [limit + 1])
        rows = cur.fetchall()

    truncated = len(rows) > limit
    rows = rows[:limit]
    has_null = any(r[0] is None for r in rows)
    values = [{'value': r[0], 'count': int(r[1])} for r in rows]
    return {
        'field': field, 'type': pg_type,
        'values': values, 'truncated': truncated, 'has_null': has_null,
    }


# ── Вспомогательное ─────────────────────────────────────────────────────

def _field_value(feat, field_ref):
    """Безопасно достать значение атрибута (None при любой ошибке чтения).

    ``field_ref`` — имя поля ИЛИ его целочисленный индекс. Индекс устойчив к
    битым/обрезанным именам полей (10-байтный лимит DBF), поэтому импорт
    читает значения именно по индексу.
    """
    try:
        return feat.get(field_ref)
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


# ── Экспорт слоя в ZIP (SHP / GeoJSON / Excel) ──────────────────────────

EXPORT_FORMATS = frozenset({'shp', 'geojson', 'xlsx'})

# WGS84 .prj (ESRI WKT) — как у остальных SHP-экспортов портала.
_EXPORT_WGS84_PRJ = (
    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'
)


# Символы, недопустимые в именах файлов (Windows/большинство ФС) + управляющие.
_FS_ILLEGAL_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _export_base_name(layer) -> str:
    """Имя файлов в архиве = название слоя (как в плашке).

    Unicode (кириллица) сохраняется; вырезаются лишь символы, недопустимые
    в именах файлов. Пустое/битое название → ``table_name``.
    """
    title = (layer.title or '').strip()
    base = _FS_ILLEGAL_RE.sub('_', title)
    base = re.sub(r'\s+', ' ', base).strip(' .')
    return (base or layer.table_name)[:120]


def _iter_export_rows(layer):
    """Итератор строк слоя: ``(id, {db_col: value}, geojson_dict|None, wkt|None)``."""
    import json as _json

    cols = [a.get('db') for a in (layer.attributes or []) if a.get('db')]
    select_cols = [sql.Identifier('id')] + [sql.Identifier(c) for c in cols]
    query = sql.SQL(
        'SELECT {cols}, ST_AsGeoJSON(geom), ST_AsText(geom) '
        'FROM {table} ORDER BY id'
    ).format(cols=sql.SQL(', ').join(select_cols),
             table=sql.Identifier(layer.table_name))
    ncols = len(cols)
    # Экспорт больших слоёв сериализует геометрию каждого объекта
    # (ST_AsGeoJSON + ST_AsText) — на десятках тысяч объектов запрос легко
    # упирается в глобальный statement_timeout Postgres и отменяется
    # (QueryCanceled → «export_failed»). Снимаем лимит только на этот тяжёлый
    # read внутри отдельной транзакции (SET LOCAL действует до её конца).
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute('SET LOCAL statement_timeout = 0')
        cur.execute(query)
        for row in cur:
            props = {cols[i]: row[1 + i] for i in range(ncols)}
            gj = row[1 + ncols]
            wkt = row[2 + ncols]
            yield row[0], props, (_json.loads(gj) if gj else None), wkt


def export_layer(layer, fmt: str) -> tuple[bytes, str]:
    """Сформировать ZIP-архив с данными слоя в формате ``fmt``.

    ``fmt`` ∈ :data:`EXPORT_FORMATS` (``shp`` / ``geojson`` / ``xlsx``).
    Возвращает ``(zip_bytes, filename)``. ``ValueError`` — неизвестный формат.
    """
    fmt = (fmt or '').lower()
    if fmt not in EXPORT_FORMATS:
        raise ValueError('unknown_format')
    base = _export_base_name(layer)

    if fmt == 'shp':
        return _export_shp_zip(layer, base), f'{base}.zip'

    if fmt == 'geojson':
        inner_name, payload = base + '.geojson', _export_geojson_bytes(layer)
    else:  # xlsx
        inner_name, payload = base + '.xlsx', _export_xlsx_bytes(layer)

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr(inner_name, payload)
    return zip_buf.getvalue(), f'{base}.zip'


def _export_geojson_bytes(layer) -> bytes:
    """FeatureCollection (EPSG:4326) со всеми объектами слоя — UTF-8 байты."""
    import json as _json

    feats = []
    for fid, props, geom, _wkt in _iter_export_rows(layer):
        feats.append({
            'type': 'Feature', 'id': fid,
            'properties': props, 'geometry': geom,
        })
    fc = {'type': 'FeatureCollection', 'features': feats}
    # default=str — на случай date/Decimal среди атрибутов.
    return _json.dumps(fc, ensure_ascii=False, default=str).encode('utf-8')


def _export_xlsx_bytes(layer) -> bytes:
    """Excel-книга: строка заголовков (id + атрибуты + wkt) и данные слоя."""
    import datetime as _dt

    import openpyxl

    attrs = [a for a in (layer.attributes or []) if a.get('db')]
    cols = [a['db'] for a in attrs]
    headers = ['id'] + [(a.get('name') or a['db']) for a in attrs] + ['wkt']

    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(title='attributes')
    ws.append(headers)
    ok_types = (str, int, float, bool, _dt.date, _dt.datetime)
    for fid, props, _geom, wkt in _iter_export_rows(layer):
        row = [fid]
        for c in cols:
            v = props.get(c)
            row.append(v if (v is None or isinstance(v, ok_types)) else str(v))
        row.append(wkt)
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _geojson_shapetype(gtype, pyshp):
    """GeoJSON-тип → pyshp shapeType (или ``None``, если не поддержан)."""
    if gtype == 'Point':
        return pyshp.POINT
    if gtype == 'MultiPoint':
        return pyshp.MULTIPOINT
    if gtype in ('LineString', 'MultiLineString'):
        return pyshp.POLYLINE
    if gtype in ('Polygon', 'MultiPolygon'):
        return pyshp.POLYGON
    return None


def _orient_ring_export(ring, clockwise: bool):
    """Порядок колец SHP: внешнее — по часовой, дыры — против."""
    s = 0.0
    for (x1, y1), (x2, y2) in zip(ring, ring[1:]):
        s += (x2 - x1) * (y2 + y1)
    is_cw = s > 0
    return list(ring) if is_cw == clockwise else list(reversed(ring))


def _write_shp_geom(w, geom, shapetype, pyshp) -> bool:
    """Записать геометрию в pyshp-Writer. ``False`` — тип не совпал/ошибка."""
    gtype = geom.get('type')
    coords = geom.get('coordinates')
    if not coords or _geojson_shapetype(gtype, pyshp) != shapetype:
        return False
    try:
        if shapetype == pyshp.POINT:
            w.point(coords[0], coords[1])
        elif shapetype == pyshp.MULTIPOINT:
            w.multipoint(coords)
        elif shapetype == pyshp.POLYLINE:
            w.line(coords if gtype == 'MultiLineString' else [coords])
        elif shapetype == pyshp.POLYGON:
            polys = coords if gtype == 'MultiPolygon' else [coords]
            rings = []
            for poly in polys:
                for i, ring in enumerate(poly):
                    rings.append(_orient_ring_export(ring, clockwise=(i == 0)))
            w.poly(rings)
        else:
            return False
    except Exception:
        return False
    return True


def _dbf_field_defs(attrs):
    """DBF-поля из атрибутов слоя: ``[(dbf_name, db_col, ftype, size, dec)]``.

    Имена DBF ограничены 10 символами и дедуплицируются; ``id`` зарезервирован.
    """
    used = {'id'}
    defs = []
    for a in attrs:
        db = a.get('db')
        if not db:
            continue
        nm = db[:10]
        base = nm
        i = 1
        while nm in used:
            suf = str(i)
            nm = base[:10 - len(suf)] + suf
            i += 1
        used.add(nm)
        t = (a.get('type') or 'text').lower()
        if t in ('integer', 'bigint'):
            defs.append((nm, db, 'N', 18, 0))
        elif t in ('double precision', 'real', 'numeric'):
            defs.append((nm, db, 'N', 20, 8))
        elif t == 'date':
            defs.append((nm, db, 'D', 8, 0))
        else:
            defs.append((nm, db, 'C', 254, 0))
    return defs


def _shp_value(v, ftype):
    """Привести значение атрибута к тому, что понимает pyshp для типа ``ftype``."""
    if v is None:
        return '' if ftype == 'C' else None
    if ftype == 'C':
        return str(v)[:254]
    return v


def _export_shp_zip(layer, base) -> bytes:
    """Собрать zipped shapefile (.shp/.shx/.dbf/.prj) со всеми объектами слоя."""
    import shapefile as pyshp

    rows = list(_iter_export_rows(layer))
    # shapeType определяем по первой непустой геометрии (SHP — однотипный).
    shapetype = None
    for _fid, _p, geom, _w in rows:
        if geom and geom.get('type'):
            shapetype = _geojson_shapetype(geom['type'], pyshp)
            if shapetype is not None:
                break
    if shapetype is None:
        shapetype = pyshp.POLYGON

    field_defs = _dbf_field_defs(layer.attributes or [])
    shp_buf, shx_buf, dbf_buf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    with pyshp.Writer(shp=shp_buf, shx=shx_buf, dbf=dbf_buf,
                      shapeType=shapetype) as w:
        w.field('id', 'N', size=18, decimal=0)
        for nm, _db, ftype, size, dec in field_defs:
            w.field(nm, ftype, size=size, decimal=dec)
        for fid, props, geom, _wkt in rows:
            if not geom or not _write_shp_geom(w, geom, shapetype, pyshp):
                continue
            rec = [fid]
            for _nm, db, ftype, _s, _d in field_defs:
                rec.append(_shp_value(props.get(db), ftype))
            w.record(*rec)

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr(f'{base}.shp', shp_buf.getvalue())
        z.writestr(f'{base}.shx', shx_buf.getvalue())
        z.writestr(f'{base}.dbf', dbf_buf.getvalue())
        z.writestr(f'{base}.prj', _EXPORT_WGS84_PRJ)
    return zip_buf.getvalue()
