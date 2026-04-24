"""Normalised mapping of Rosreestr ЗСН usage-category labels to our
:class:`Farmland.CropType` enum.

The Rosreestr ЗСН ("земли сельскохозяйственного назначения") shapefiles
published by different subject-of-federation offices speak a mildly
inconsistent Russian vocabulary; this module flattens it.

Values mapped to ``None`` are intentionally **skipped** during import
(either non-agricultural by zemelny kodeks, or data noise).

The mapping is *keyed by the lowercased raw value*. Use
:func:`resolve_crop_type` for case-insensitive lookup.
"""
from __future__ import annotations

# Canonical mapping: raw label (lowercased, trimmed) → CropType value or None.
MAPPING: dict[str, str | None] = {
    # ------------------------- agricultural -------------------------
    'пашня':                            'arable',

    'залежь':                           'fallow',

    'сенокос':                          'hayfield',
    'сенокосы':                         'hayfield',

    'пастбище':                         'pasture',
    'пастбища':                         'pasture',
    'оленьи пастбища':                  'pasture',
    'кормовые угодья':                  'pasture',

    'многолетнее насаждение':           'perennial',
    'многолетние насаждения':           'perennial',
    'виноградники':                     'perennial',

    'иные сельскохозяйственные земли':  'other_agri',
    'сельскохозяйственные угодья':      'other_agri',

    # ------------------------- non-agri (skip) ---------------------
    'защитные лесные насаждения':                     None,
    'лесные полосы':                                  None,
    'площади, покрытые сельскохозяйственными лесами': None,
    'сельскохозяйственные леса':                      None,
    'несельскохозяйственные земли':                   None,
    'другие несельскохозяйственные угодья':           None,
    'земли под водой':                                None,
    'земли под дорогами и прогонами':                 None,
    'производственные и хозяйственные центры':        None,
    'мелиоративные системы и сооружения':             None,
    'мелиоративные сооружения и объекты':             None,
    'мелиоративные сооружения':                       None,
    'мелиоративные системы':                          None,
    'кустарник':                                      None,
    'болота':                                         None,
    'прочие земли':                                   None,

    # ------------------------- noise / placeholders ----------------
    '':   None,
    '-':  None,
}


def resolve_crop_type(raw: str | None) -> str | None:
    """Return a :class:`Farmland.CropType` value for a raw label, or
    ``None`` if the label is non-agricultural / unknown (caller should
    skip). Case-insensitive, whitespace-trimmed."""
    if raw is None:
        return None
    return MAPPING.get(raw.strip().lower())


# Raw labels (ORIGINAL case as they appear in the .dbf) that we want
# to keep. Used to build the ogr2ogr ``-where`` filter.
AGRICULTURAL_LABELS: tuple[str, ...] = (
    'Пашня',
    'Залежь',
    'Сенокосы', 'Сенокос',
    'Пастбища', 'Пастбище',
    'Оленьи пастбища',
    'Кормовые угодья',
    'Многолетние насаждения', 'Многолетнее насаждение',
    'Виноградники',
    'Иные сельскохозяйственные земли',
    'Сельскохозяйственные угодья',
)


def build_where_clause(field_name: str) -> str:
    """Build a SQL-compatible WHERE clause for ogr2ogr ``-where`` option,
    filtering rows to just the agricultural labels in
    :data:`AGRICULTURAL_LABELS`. Returns e.g.::

        "S_Vid_N" IN ('Пашня','Залежь',…)
    """
    quoted = ",".join("'" + lbl.replace("'", "''") + "'" for lbl in AGRICULTURAL_LABELS)
    return f'"{field_name}" IN ({quoted})'


def resolve_is_used(raw: str | None) -> bool | None:
    """Map a ``Fact_isp`` / ``Com_DDZ`` raw value to True/False/None.

    ``"Используется"`` → True, ``"Не используется"`` → False, anything
    else (including empty, ``"-"`` placeholder, or locality-specific
    noise) → None.
    """
    if raw is None:
        return None
    key = raw.strip().lower()
    if not key or key == '-':
        return None
    if 'не' in key.split() and 'использ' in key:
        return False
    if 'использ' in key and 'не' not in key.split():
        return True
    return None
