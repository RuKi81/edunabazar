"""Quick inspector for the Московская область 2021 GeoJSON.

Reads only the head of the file (large file, 140+ MB) plus a streamed
scan of the first N features to summarise:

  * top-level keys / CRS
  * full property keys observed across the first N features
  * sample values for ``name`` (вид угодий) and ``Neisp_All`` (факт
    использования)
  * value frequency for ``name`` and ``Neisp_All``

Usage:
    python scripts/inspect_mosobl_geojson.py [PATH] [N]
"""
from __future__ import annotations

import collections
import json
import sys
from pathlib import Path

DEFAULT_PATH = Path(r'C:\Users\kiva_\Desktop\Вектор сх Мсобласть\Московская область 2021.geojson')


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PATH
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 0  # 0 = all features

    print(f'File: {path}')
    print(f'Size: {path.stat().st_size / 1024 / 1024:.1f} MB')

    # Try to load as a normal GeoJSON FeatureCollection. 140 MB is fine
    # for json.load on a 16 GB box.
    with path.open('r', encoding='utf-8') as f:
        data = json.load(f)

    print(f'Top-level type: {data.get("type")}')
    if 'crs' in data:
        print(f'CRS: {data["crs"]}')
    feats = data.get('features', [])
    print(f'Features: {len(feats)}')

    if not feats:
        return 0

    # Property keys observed
    keys = collections.Counter()
    name_counter = collections.Counter()
    neisp_counter = collections.Counter()
    geom_types = collections.Counter()
    sample = feats[0]

    iterable = feats if limit == 0 else feats[:limit]
    for feat in iterable:
        props = feat.get('properties') or {}
        for k in props:
            keys[k] += 1
        name_counter[props.get('name')] += 1
        neisp_counter[props.get('Neisp_All')] += 1
        geom_types[(feat.get('geometry') or {}).get('type')] += 1

    print('\n--- Property keys (frequency over scanned features) ---')
    for k, v in keys.most_common():
        print(f'  {k}: {v}')

    print('\n--- Geometry types ---')
    for k, v in geom_types.most_common():
        print(f'  {k}: {v}')

    print('\n--- Sample feature[0].properties ---')
    print(json.dumps(sample.get('properties'), ensure_ascii=False, indent=2))

    print('\n--- name (вид угодий) value frequency ---')
    for k, v in name_counter.most_common():
        print(f'  {k!r}: {v}')

    print('\n--- Neisp_All (факт использования) value frequency ---')
    for k, v in neisp_counter.most_common():
        print(f'  {k!r}: {v}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
