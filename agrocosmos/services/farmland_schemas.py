"""Schema detection for the Rosreestr ЗСН shapefile dataset.

The source Shapefiles do not share a single schema. We have catalogued
~30 variants across 85 regional folders (see
``scripts/farmland_inventory.json``). Instead of hard-coding a table
of ``(region → schema)`` we detect each file's columns by priority of
known aliases and return a :class:`FarmlandSchema` describing which
``.dbf`` column to treat as each logical attribute.

This is deliberately forgiving: if a new region arrives with a slight
column-name variation that hits any of the known aliases it will just
work. Unknown files return ``None`` for each missing attribute (the
importer logs and skips rows that cannot be classified).
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Alias lists, ordered by priority. Lowercased comparison. First match wins.
# ---------------------------------------------------------------------------

USAGE_ALIASES: tuple[str, ...] = (
    # 'S_Vid_N' is the most reliable canonical label and must win over
    # Kaluga-style 'Farming' (which actually holds kolhoz names).
    #
    # NOTE: 's_vid_dzz' / 'sovr_vid_c' / 'vid_dzz_n' / 'vid_fact_c' are
    # *numeric* code columns in some Rosreestr dialects (Кемеровская,
    # for instance, ships S_vid_DZZ as Integer alongside the textual
    # Sovr_vid). The textual sibling (sovr_vid / vid_fact_t / dzz_text)
    # must be picked first so the ogr2ogr -where filter on Russian
    # labels and the downstream crop_type CASE both work. Otherwise the
    # numeric column wins, the IN ('Пашня',…) -where silently passes
    # everything (GDAL type-mismatch quirk) and the INSERT … SELECT
    # filters all rows out (observed: Кемеровская staged=985k inserted=0).
    's_vid_n', 's_vid_efis',
    'sovr_vid',
    'vid_efis', 'vid_gfdz',
    'vid_fact_t',  # Primorsky: text sibling of the numeric Vid_Fact_C code
    # Numeric/code fallbacks — only chosen when no textual usage column
    # is present at all. They will not match the Russian -where filter,
    # so an empty staging is the expected outcome (skip the region).
    's_vid_dzz', 'sovr_vid_c', 'vid_dzz_n', 'vid_fact_c',
    'farming',     # Kaluga — tried last, often noise
)

AREA_ALIASES: tuple[str, ...] = ('s_ha', 'area_hec', 'area_ha')

FACT_ISP_ALIASES: tuple[str, ...] = (
    # 'fact_isp' takes priority over 'com_ddz' (Stavropol uses the
    # latter but with low coverage).
    'fact_isp', 'com_isp', 'com_ispol', 'com_ddz',
)

CADASTRAL_ALIASES: tuple[str, ...] = (
    'cad_num', 'cad_num222', 'cadnum', 'kad_num', 'kadnomer',
)

DISTRICT_ALIASES: tuple[str, ...] = (
    # 'mo' (муниципальное образование) is the modern Rosreestr name;
    # 'rayon' / 'district' are older variants. 'ray_num' is numeric.
    'mo', 'rayon', 'district', 'ray_num',
)

KOLHOZ_ALIASES: tuple[str, ...] = ('kolhoz', 'hoz_vo', 'hoz_vo_1')


# ---------------------------------------------------------------------------
# Data structure
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FarmlandSchema:
    """Mapping from logical attributes to concrete ``.dbf`` column names."""

    all_fields: tuple[str, ...]
    usage_field: str | None
    area_field: str | None
    fact_isp_field: str | None
    cadastral_field: str | None
    district_field: str | None
    kolhoz_field: str | None
    data_encoding: str = 'UTF-8'

    @property
    def is_usable(self) -> bool:
        """True if we have at least the minimal usage + geometry (area is
        not strictly required — it can be recomputed from geom)."""
        return self.usage_field is not None

    @property
    def schema_id(self) -> str:
        """Short fingerprint we stamp into Farmland.source to track which
        schema produced a row. Handy for re-running one schema class
        after a fix without touching the rest."""
        return '_'.join(filter(None, (
            self.usage_field,
            self.fact_isp_field,
            self.cadastral_field,
            self.district_field,
        ))).lower() or 'empty'


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------


def _pick(fields: tuple[str, ...], aliases: tuple[str, ...]) -> str | None:
    """Return the first *actual* field name whose lowercased form matches
    the *first* alias in ``aliases`` that appears in ``fields``.

    Priority is determined by the order of ``aliases``, **not** by the
    order of ``fields``. That is exactly what fixes the Kaluga case
    where both ``S_Vid_N`` and ``Farming`` exist — we want ``S_Vid_N``.
    """
    lower_map = {f.lower(): f for f in fields}
    for alias in aliases:
        if alias in lower_map:
            return lower_map[alias]
    return None


def detect_schema_from_fields(
    fields: tuple[str, ...],
    data_encoding: str = 'UTF-8',
) -> FarmlandSchema:
    """Build a :class:`FarmlandSchema` from the raw .dbf field list."""
    return FarmlandSchema(
        all_fields=fields,
        usage_field=_pick(fields, USAGE_ALIASES),
        area_field=_pick(fields, AREA_ALIASES),
        fact_isp_field=_pick(fields, FACT_ISP_ALIASES),
        cadastral_field=_pick(fields, CADASTRAL_ALIASES),
        district_field=_pick(fields, DISTRICT_ALIASES),
        kolhoz_field=_pick(fields, KOLHOZ_ALIASES),
        data_encoding=data_encoding,
    )


# ---------------------------------------------------------------------------
# .dbf header parsing (no external deps)
# ---------------------------------------------------------------------------


def _decode_name(raw: bytes) -> str:
    """Decode the 11-byte field-name slot. Most files use ASCII but a
    few Russian exports stuff cp1251 bytes there."""
    raw = raw.rstrip(b'\x00').rstrip(b' ')
    for enc in ('utf-8', 'cp1251', 'latin-1'):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode('latin-1', errors='replace')


def read_dbf_header(dbf_path: Path) -> tuple[int, int, int, tuple[str, ...]]:
    """Return ``(num_records, header_len, record_len, field_names)``
    without reading the data section."""
    with open(dbf_path, 'rb') as f:
        hdr = f.read(32)
        num_rec, hdr_len, rec_len = struct.unpack('<IHH', hdr[4:12])
        names: list[str] = []
        while True:
            chunk = f.read(32)
            if not chunk or chunk[0:1] == b'\r' or len(chunk) < 32:
                break
            names.append(_decode_name(chunk[0:11]))
    return num_rec, hdr_len, rec_len, tuple(names)


def read_cpg_encoding(shp_path: Path) -> str:
    """Resolve data encoding from the sidecar .cpg file; default UTF-8."""
    cpg = shp_path.with_suffix('.cpg')
    if not cpg.exists():
        return 'UTF-8'
    try:
        txt = cpg.read_text(errors='ignore').strip()
    except OSError:
        return 'UTF-8'
    norm = txt.lower()
    if '1251' in norm or 'win' in norm:
        return 'CP1251'
    if '866' in norm:
        return 'CP866'
    if 'utf' in norm or '65001' in norm:
        return 'UTF-8'
    return txt or 'UTF-8'


def detect_schema(shp_path: Path) -> FarmlandSchema:
    """End-to-end detection for a single ``.shp`` file (reads its .dbf
    header and .cpg sidecar)."""
    dbf = shp_path.with_suffix('.dbf')
    if not dbf.exists():
        raise FileNotFoundError(f'Missing .dbf for {shp_path}')
    _, _, _, fields = read_dbf_header(dbf)
    encoding = read_cpg_encoding(shp_path)
    return detect_schema_from_fields(fields, data_encoding=encoding)
