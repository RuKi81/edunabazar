"""
Post-process the JSON produced by farmland_inventory.py and print a
clean unified report: which regions picked a suspicious usage field,
global distribution of the top clean usage values, and a suggested
crop-type mapping.
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

OUT_JSON = Path(__file__).parent / 'farmland_inventory.json'

# Values we consider "known clean" (canonical Rosreestr categories).
# Anything else in a region's top-5 is a red flag that we picked the
# wrong field.
CANONICAL = {
    'Пашня', 'Пастбища', 'Сенокосы', 'Сенокос',
    'Иные сельскохозяйственные земли', 'Залежь',
    'Многолетние насаждения', 'Виноградники',
    'Защитные лесные насаждения', 'Лесные полосы',
    'Сельскохозяйственные леса',
    'Площади, покрытые сельскохозяйственными лесами',
    'Производственные и хозяйственные центры',
    'Несельскохозяйственные земли',
    'Другие несельскохозяйственные угодья',
    'Земли под водой', 'Земли под дорогами и прогонами',
    'Оленьи пастбища', 'Кормовые угодья',
    'Мелиоративные системы и сооружения',
    'Мелиоративные сооружения', 'Мелиоративные сооружения и объекты',
    'Мелиоративные системы', 'Болота', 'Кустарник',
    'Прочие земли', 'Сельскохозяйственные угодья',
    '', '-',
}


def main() -> int:
    data = json.loads(OUT_JSON.read_text(encoding='utf-8'))
    regions = data['regions']

    # --- Audit: regions where top value is NOT canonical -------------
    print('=== Regions where the detected usage field looks WRONG ===')
    print('(top-5 usage values are mostly non-canonical — likely a kolhoz/filename column)\n')
    suspicious = []
    for name, info in regions.items():
        if info.get('error'):
            print(f'  {name}: {info["error"]}')
            continue
        uc = collections.Counter(info.get('usage_counts') or {})
        if not uc:
            continue
        top5 = uc.most_common(5)
        clean_hits = sum(1 for v, _ in top5 if v in CANONICAL)
        if clean_hits <= 1:
            suspicious.append(name)
            print(f'  [{name}] field={info["usage_field"]!r}')
            for v, c in top5:
                mark = '✓' if v in CANONICAL else '✗'
                print(f'     {mark} {c:>9,}  {v!r}')
            print()
    print(f'--> {len(suspicious)} suspicious region(s)\n')

    # --- Clean global distribution (canonical only) ------------------
    clean_global = collections.Counter()
    dirty_global = collections.Counter()
    for name, info in regions.items():
        if info.get('error'):
            continue
        if name in suspicious:
            continue  # skip whole region — field choice is wrong
        for v, c in (info.get('usage_counts') or {}).items():
            if v in CANONICAL:
                clean_global[v] += c
            else:
                dirty_global[v] += c

    print('=== Global usage distribution (trusted regions only) ===')
    total = sum(clean_global.values()) + sum(dirty_global.values())
    for v, c in clean_global.most_common():
        pct = 100.0 * c / total if total else 0
        print(f'  {c:>11,}  {pct:>5.1f}%   {v!r}')
    print(f'  {sum(dirty_global.values()):>11,}  (non-canonical leftovers)')
    print(f'  {total:>11,}  GRAND TOTAL (trusted regions)')

    # --- Fact_isp coverage -------------------------------------------
    print('\n=== Fact_isp coverage by region ===')
    fact_total_used = 0
    fact_total_not = 0
    regs_with_fact = 0
    for name, info in regions.items():
        if info.get('error') or not info.get('fact_isp_field'):
            continue
        fc = info.get('fact_counts') or {}
        used = sum(c for v, c in fc.items() if 'спольз' in v.lower() and 'не' not in v.lower())
        notu = sum(c for v, c in fc.items() if 'не ' in v.lower() and 'спольз' in v.lower())
        if used or notu:
            regs_with_fact += 1
            fact_total_used += used
            fact_total_not += notu
            print(f'  {name:<45} field={info["fact_isp_field"]:<10} '
                  f'used={used:>8,}  not_used={notu:>8,}')
    print(f'\n--> {regs_with_fact} regions have meaningful Fact_isp data; '
          f'{fact_total_used:,} used / {fact_total_not:,} not used')

    # --- Cadastral number coverage -----------------------------------
    print('\n=== Cadastral number field coverage ===')
    for name, info in regions.items():
        if info.get('error'):
            continue
        if info.get('cadastral_field'):
            print(f'  {name:<45} cad_field={info["cadastral_field"]}')

    # --- District field coverage -------------------------------------
    print('\n=== District field coverage ===')
    for name, info in regions.items():
        if info.get('error'):
            continue
        if info.get('district_field'):
            print(f'  {name:<45} district_field={info["district_field"]}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
