"""Stand-alone pre-flight check for the Rosreestr ЗСН import.

Walks every region folder under ``BASE``, runs schema detection on the
largest .shp, and prints the plan:
    folder, resolved schema_id, usage/fact_isp/cad fields, record count.

Does NOT touch the database — no Django setup required. This mirrors
what ``python manage.py import_farmlands_rosreestr --dry-run`` would do
on the server, minus the Region FK resolution step.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make the agrocosmos.services package importable without Django.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from agrocosmos.services.farmland_schemas import detect_schema  # noqa: E402

BASE = Path(r'C:\Users\kiva_\Desktop\КАРТЫ_СХЕМЫ_ЗСН Вектор на РФ')


def largest_shp(region_dir: Path) -> Path | None:
    shps = sorted(region_dir.rglob('*.shp'),
                  key=lambda p: p.stat().st_size, reverse=True)
    return shps[0] if shps else None


def main() -> int:
    if not BASE.is_dir():
        print(f'!! BASE not found: {BASE}', file=sys.stderr)
        return 1

    regions = sorted(p for p in BASE.iterdir() if p.is_dir())
    print(f'Scanning {len(regions)} region folders…\n')

    ok = unusable = noshp = 0
    schemas: dict[str, int] = {}
    missing_fact = missing_cad = 0

    header = (f'{"#":>3}  {"folder":<40}  {"schema_id":<40}  '
              f'{"usage":<16}  {"fact":<12}  {"cad":<12}  {"MB":>7}')
    print(header)
    print('-' * len(header))

    for i, rd in enumerate(regions, 1):
        shp = largest_shp(rd)
        if shp is None:
            print(f'{i:>3}  {rd.name:<40}  NO .shp')
            noshp += 1
            continue
        size_mb = shp.stat().st_size / (1024 * 1024)
        try:
            sch = detect_schema(shp)
        except Exception as exc:
            print(f'{i:>3}  {rd.name:<40}  ERROR {exc}')
            unusable += 1
            continue

        tag = 'OK' if sch.is_usable else 'UNUSABLE'
        schemas[sch.schema_id] = schemas.get(sch.schema_id, 0) + 1
        if sch.is_usable:
            ok += 1
            if not sch.fact_isp_field:
                missing_fact += 1
            if not sch.cadastral_field:
                missing_cad += 1
        else:
            unusable += 1

        print(
            f'{i:>3}  {rd.name:<40.40}  {sch.schema_id:<40.40}  '
            f'{(sch.usage_field or "-"):<16.16}  '
            f'{(sch.fact_isp_field or "-"):<12.12}  '
            f'{(sch.cadastral_field or "-"):<12.12}  '
            f'{size_mb:>7.1f}  [{tag}]'
        )

    print(f'\n=== Totals: ok={ok}, unusable={unusable}, no_shp={noshp}  '
          f'(of {len(regions)})')
    print(f'    missing Fact_isp: {missing_fact}  |  '
          f'missing Cad_num: {missing_cad}')
    print(f'\n=== Schema frequency (top 15)')
    for schema_id, cnt in sorted(schemas.items(), key=lambda kv: -kv[1])[:15]:
        print(f'  {cnt:>3}  {schema_id}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
