"""SQL generator + ogr2ogr runner for bulk-loading Rosreestr ЗСН
shapefiles into ``agro_farmland``.

The pipeline per region is::

    ogr2ogr (shp → PostGIS staging table, reprojected to EPSG:4326,
             pre-filtered to agricultural labels)
        ↓
    INSERT INTO agro_farmland … SELECT … FROM staging
        (crop_type mapping, is_used tri-state, cadastral copy,
         JSONB properties of leftover attributes)
        ↓
    DROP staging

``ogr2ogr`` is chosen over ORM bulk_create because the raw COPY path is
30-50× faster on the 26 M-row dataset, and PostGIS-side SQL gives a
clean stream for geometry fixing (``ST_MakeValid`` / ``ST_CollectionExtract``)
without shipping every polygon through Python.
"""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from django.conf import settings

from .farmland_crop_mapping import AGRICULTURAL_LABELS
from .farmland_schemas import FarmlandSchema


# Lowercased / trimmed agricultural labels. Used in the SQL filter.
_AGRI_LABELS_LOWER = tuple(lbl.lower() for lbl in AGRICULTURAL_LABELS)


# Lowercased labels → CropType value. Must agree with
# farmland_crop_mapping.MAPPING (kept duplicated here so the SQL stays
# self-contained and we don't need a round-trip to Python per row).
_CROP_CASES: tuple[tuple[str, str], ...] = (
    ('пашня',                            'arable'),
    ('залежь',                           'fallow'),
    ('сенокос',                          'hayfield'),
    ('сенокосы',                         'hayfield'),
    ('пастбище',                         'pasture'),
    ('пастбища',                         'pasture'),
    ('оленьи пастбища',                  'pasture'),
    ('кормовые угодья',                  'pasture'),
    ('многолетнее насаждение',           'perennial'),
    ('многолетние насаждения',           'perennial'),
    ('виноградники',                     'perennial'),
    ('иные сельскохозяйственные земли',  'other_agri'),
    ('сельскохозяйственные угодья',      'other_agri'),
)


# ---------------------------------------------------------------------------
# ogr2ogr
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Ogr2OgrResult:
    returncode: int
    stdout: str
    stderr: str


def _pg_connection_string() -> str:
    """Build the PG: OGR DSN from Django settings."""
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


def _sql_quote(value: str) -> str:
    """Safely quote a literal for embedding into a SQL predicate string
    passed to ogr2ogr's ``-where`` argument."""
    return "'" + value.replace("'", "''") + "'"


def build_where_for_shp(schema: FarmlandSchema) -> str:
    """Build the ``-where`` predicate for ogr2ogr so only rows whose
    usage column matches one of our agricultural labels are copied to
    the staging table (≈30% reduction in stage volume for most regions).
    """
    if not schema.usage_field:
        return '1=0'  # no usage column — import nothing
    in_list = ','.join(_sql_quote(lbl) for lbl in AGRICULTURAL_LABELS)
    # ogr2ogr's SQL dialect quotes identifiers with double quotes
    return f'"{schema.usage_field}" IN ({in_list})'


def run_ogr2ogr(
    shp_path: Path,
    staging_table: str,
    schema: FarmlandSchema,
    *,
    binary: str = 'ogr2ogr',
    extra_env: dict[str, str] | None = None,
    log_stream=None,
) -> Ogr2OgrResult:
    """Copy a filtered subset of ``shp_path`` into ``staging_table``.

    The table is *dropped-and-recreated* (``-overwrite``) — staging
    tables are single-use.
    """
    args = [
        binary,
        '-f', 'PostgreSQL',
        _pg_connection_string(),
        str(shp_path),
        '-nln', staging_table,
        '-nlt', 'MULTIPOLYGON',
        '-t_srs', 'EPSG:4326',
        '-lco', 'GEOMETRY_NAME=wkb_geometry',
        '-lco', 'FID=ogc_fid',
        '-lco', 'SPATIAL_INDEX=NONE',
        '-lco', 'LAUNDER=NO',
        # PRECISION=NO disables propagating the .dbf field width/decimals
        # into the Postgres column definition. Some Rosreestr files declare
        # S_ha as numeric(18,15) — i.e. max ~999.999… — but actually contain
        # values >1000 (e.g. 1912.45 ha), which makes the COPY abort with
        # "numeric field overflow". With PRECISION=NO ogr2ogr emits a plain
        # numeric/float8 column that accepts the full data range.
        '-lco', 'PRECISION=NO',
        '-oo', f'ENCODING={schema.data_encoding}',
        '-overwrite',
        '-skipfailures',
        '-where', build_where_for_shp(schema),
        '--config', 'PG_USE_COPY', 'YES',
        '--config', 'OGR_TRUNCATE', 'NO',
    ]
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    # ogr2ogr sometimes prints progress bars to stderr; keep bounded.
    proc = subprocess.run(
        args, env=env, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, errors='replace',
    )
    if log_stream is not None:
        if proc.stdout:
            log_stream.write(proc.stdout)
        if proc.stderr:
            log_stream.write(proc.stderr)
    return Ogr2OgrResult(proc.returncode, proc.stdout, proc.stderr)


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------


def _ident(name: str) -> str:
    """Quote a PostgreSQL identifier. We run ogr2ogr with LAUNDER=NO so
    identifiers keep their original case and may contain non-ASCII chars
    (e.g. ``"Площа"``); defensive quoting is mandatory."""
    return '"' + name.replace('"', '""') + '"'


def build_insert_sql(
    schema: FarmlandSchema,
    staging_table: str,
    region_id: int,
    source_id: str,
) -> str:
    """Generate the ``INSERT … SELECT`` statement that promotes
    staging rows to ``agro_farmland``, applying:
        * crop_type mapping (strict agricultural labels only — the rest
          are filtered out in the final WHERE);
        * Fact_isp → is_used tri-state;
        * cadastral_number copy (capped to 50 chars);
        * area_ha parse with regex guard;
        * geometry normalisation (ST_MakeValid + CollectionExtract);
        * JSONB properties with every non-promoted attribute.
    """
    st = _ident(staging_table)

    # ----- crop_type CASE ----------------------------------------------
    # ogr2ogr is invoked with LAUNDER=NO, so staging columns keep their
    # original case and non-ASCII characters verbatim — we just need to
    # quote them defensively.
    usage_col = _ident(schema.usage_field) if schema.usage_field else 'NULL'
    crop_when = '\n            '.join(
        f"WHEN {_sql_quote(lbl)} THEN {_sql_quote(ct)}"
        for lbl, ct in _CROP_CASES
    )
    crop_case = (
        "CASE LOWER(TRIM(s." + usage_col + "::text))\n            "
        + crop_when
        + "\n            ELSE NULL\n        END"
    )

    # ----- is_used CASE ------------------------------------------------
    if schema.fact_isp_field:
        fact_col = _ident(schema.fact_isp_field)
        is_used_case = (
            "CASE\n            "
            f"WHEN s.{fact_col} IS NULL THEN NULL\n            "
            f"WHEN LOWER(TRIM(s.{fact_col}::text)) IN ('используется','использ.','используется, пашня') THEN TRUE\n            "
            f"WHEN LOWER(TRIM(s.{fact_col}::text)) LIKE 'не %' AND LOWER(s.{fact_col}::text) LIKE '%использ%' THEN FALSE\n            "
            f"WHEN LOWER(TRIM(s.{fact_col}::text)) IN ('не используется','неиспользуется','не использ.') THEN FALSE\n            "
            "ELSE NULL\n        END"
        )
    else:
        is_used_case = 'NULL::boolean'

    # ----- cadastral ---------------------------------------------------
    if schema.cadastral_field:
        cad_col = _ident(schema.cadastral_field)
        cad_expr = f"LEFT(COALESCE(s.{cad_col}::text, ''), 50)"
    else:
        cad_expr = "''"

    # ----- area --------------------------------------------------------
    if schema.area_field:
        area_col = _ident(schema.area_field)
        # S_ha is often stored as a string like "1.60e+02". Regex-guard
        # avoids raising when a row has garbage.
        area_expr = (
            f"CASE WHEN s.{area_col}::text ~ '^\\s*[-+]?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?\\s*$' "
            f"THEN s.{area_col}::text::double precision ELSE 0 END"
        )
    else:
        area_expr = "0::double precision"

    # ----- JSONB properties --------------------------------------------
    # Every staging column except our promoted ones + ogr-added fields.
    promoted = {
        schema.usage_field,
        schema.area_field,
        schema.fact_isp_field,
        schema.cadastral_field,
    }
    promoted_set = {n for n in promoted if n}
    ogr_cols = {'ogc_fid', 'wkb_geometry'}
    leftover = [
        orig for orig in schema.all_fields
        if orig not in promoted_set and orig.lower() not in ogr_cols
    ]
    if leftover:
        pairs = ',\n                '.join(
            f"{_sql_quote(orig)}, NULLIF(TRIM(s.{_ident(orig)}::text), '')"
            for orig in leftover
        )
        props_expr = (
            "jsonb_strip_nulls(jsonb_build_object(\n                "
            + pairs
            + "\n            ))"
        )
    else:
        props_expr = "NULL::jsonb"

    # ----- geometry ----------------------------------------------------
    # ST_Force2D strips any Z dimension ogr may have carried over;
    # ST_CollectionExtract(…, 3) keeps only polygonal output from
    # ST_MakeValid, then ST_Multi wraps single → multi.
    geom_expr = (
        "ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Force2D(s.wkb_geometry)), 3))"
        "::geometry(MultiPolygon,4326)"
    )

    # ----- final SQL ----------------------------------------------------
    # crop_type filter is duplicated in WHERE so rows that miss every
    # mapping are skipped without inserting NULL (the column is NOT NULL).
    sql = f"""
INSERT INTO agro_farmland (
    region_id, district_id, crop_type, is_used, cadastral_number,
    area_ha, geom, properties, source, created_at
)
SELECT
    {int(region_id)}::int AS region_id,
    NULL::int AS district_id,
    crop AS crop_type,
    {is_used_case} AS is_used,
    {cad_expr} AS cadastral_number,
    {area_expr} AS area_ha,
    geom_fixed AS geom,
    {props_expr} AS properties,
    {_sql_quote(source_id)} AS source,
    NOW() AS created_at
FROM (
    SELECT
        s.*,
        ({crop_case}) AS crop,
        ({geom_expr}) AS geom_fixed
    FROM {st} s
) s
WHERE s.crop IS NOT NULL
  AND s.geom_fixed IS NOT NULL
  AND NOT ST_IsEmpty(s.geom_fixed)
  AND ST_GeometryType(s.geom_fixed) = 'ST_MultiPolygon';
""".strip()
    return sql


def build_drop_staging_sql(staging_table: str) -> str:
    return f'DROP TABLE IF EXISTS {_ident(staging_table)};'


def build_count_staging_sql(staging_table: str) -> str:
    return f'SELECT COUNT(*) FROM {_ident(staging_table)};'
