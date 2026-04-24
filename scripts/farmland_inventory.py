"""
One-off inventory scan across the Rosreestr ЗСН shapefile dataset.

For every region folder under ``BASE`` it:
  * locates the .dbf file;
  * decodes field names (tries UTF-8 then cp1251 fallback);
  * identifies the usage-type field, area field, fact_isp field,
    cadastral-number field, and (municipal) district field by
    matching known name aliases (case-insensitive);
  * samples ALL records to collect every unique value of the usage
    field across the whole dataset (≈28 M records total).

Writes ``scripts/farmland_inventory.json`` with per-region schema info
and a global frequency table of usage values. Prints a human summary.
"""
from __future__ import annotations

import collections
import json
import os
import struct
import sys
import time
from glob import glob
from pathlib import Path

BASE = Path(r'C:\Users\kiva_\Desktop\КАРТЫ_СХЕМЫ_ЗСН Вектор на РФ')
OUT_JSON = Path(__file__).parent / 'farmland_inventory.json'

# Alias lists (case-insensitive) for each logical column we care about.
USAGE_ALIASES = (
    's_vid_n', 'sovr_vid', 'vid_efis', 'vid_dzz_n',
    'vid_fact_c', 'farming', 'vid_ugodya',
)
AREA_ALIASES = ('s_ha', 'area_hec', 'area_ha')
FACT_ALIASES = ('fact_isp', 'com_ddz', 'com_isp')
CAD_ALIASES = ('cad_num', 'cad_num222', 'cadnum', 'kadnomer')
DISTRICT_ALIASES = ('rayon', 'district', 'mo', 'ray_num', 'district_')


def _decode_name(raw: bytes) -> str:
    """dbf field names are nominally ASCII, but some Russian exports
    put cp1251 bytes in the 11-byte slot. Try utf-8, fall back to
    cp1251, finally latin-1 (loses info but never crashes)."""
    raw = raw.rstrip(b'\x00').rstrip(b' ')
    for enc in ('utf-8', 'cp1251', 'latin-1'):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode('latin-1', errors='replace')


def parse_dbf_header(path: Path):
    """Return ``(num_records, header_len, record_len, fields)`` where
    *fields* is a list of ``(name, offset_in_record, length)`` tuples.
    The offset already accounts for the 1-byte deletion flag."""
    with open(path, 'rb') as f:
        hdr = f.read(32)
        num_rec, hdr_len, rec_len = struct.unpack('<IHH', hdr[4:12])
        fields = []
        off = 1
        while True:
            chunk = f.read(32)
            if not chunk or chunk[0:1] == b'\r':
                break
            if len(chunk) < 32:
                break
            name = _decode_name(chunk[0:11])
            flen = chunk[16]
            fields.append((name, off, flen))
            off += flen
    return num_rec, hdr_len, rec_len, fields


def pick(fields, aliases):
    """Return ``(name, offset, length)`` of the first field whose
    lowercased name matches one of *aliases*, else ``None``."""
    wanted = {a.lower() for a in aliases}
    for name, off, flen in fields:
        if name.lower() in wanted:
            return name, off, flen
    return None


def scan_region(path: Path):
    """Full scan of one .dbf. Returns schema info + Counter of usage
    values + Counter of (usage, fact_isp) pairs when applicable."""
    num_rec, hdr_len, rec_len, fields = parse_dbf_header(path)
    usage = pick(fields, USAGE_ALIASES)
    area = pick(fields, AREA_ALIASES)
    fact = pick(fields, FACT_ALIASES)
    cad = pick(fields, CAD_ALIASES)
    district = pick(fields, DISTRICT_ALIASES)

    usage_counts: collections.Counter = collections.Counter()
    fact_counts: collections.Counter = collections.Counter()
    pair_counts: collections.Counter = collections.Counter()

    # Detect .cpg encoding for data (usually utf-8)
    cpg = path.with_suffix('.cpg')
    data_enc = 'utf-8'
    if cpg.exists():
        try:
            txt = cpg.read_text().strip().lower()
            if '1251' in txt or 'win' in txt:
                data_enc = 'cp1251'
            elif '866' in txt:
                data_enc = 'cp866'
        except OSError:
            pass

    with open(path, 'rb') as f:
        f.seek(hdr_len)
        for _ in range(num_rec):
            rec = f.read(rec_len)
            if len(rec) < rec_len:
                break
            uv = ''
            fv = ''
            if usage:
                _, off, flen = usage
                uv = rec[off:off + flen].decode(data_enc, 'replace').strip()
            if fact:
                _, off, flen = fact
                fv = rec[off:off + flen].decode(data_enc, 'replace').strip()
            usage_counts[uv] += 1
            if fact:
                fact_counts[fv] += 1
                pair_counts[(uv, fv)] += 1

    return {
        'num_records': num_rec,
        'encoding_data': data_enc,
        'fields': [f[0] for f in fields],
        'usage_field': usage[0] if usage else None,
        'area_field': area[0] if area else None,
        'fact_isp_field': fact[0] if fact else None,
        'cadastral_field': cad[0] if cad else None,
        'district_field': district[0] if district else None,
        'usage_counts': dict(usage_counts),
        'fact_counts': dict(fact_counts) if fact else None,
        'pair_counts_top50': [
            {'usage': u, 'fact': fv, 'count': c}
            for (u, fv), c in pair_counts.most_common(50)
        ] if fact else None,
    }


def main() -> int:
    if not BASE.is_dir():
        print(f'!! base dir not found: {BASE}', file=sys.stderr)
        return 1

    per_region: dict[str, dict] = {}
    global_usage: collections.Counter = collections.Counter()
    schema_groups: dict[tuple, list[str]] = collections.defaultdict(list)

    regions = sorted([p for p in BASE.iterdir() if p.is_dir()])
    print(f'Scanning {len(regions)} region folders…')
    t0 = time.time()
    for i, region_dir in enumerate(regions, 1):
        dbfs = [str(p) for p in region_dir.rglob('*.dbf')]
        if not dbfs:
            print(f'  [{i:>2}/{len(regions)}] {region_dir.name}: NO DBF')
            per_region[region_dir.name] = {'error': 'no_dbf'}
            continue
        dbf = Path(dbfs[0])
        t_reg = time.time()
        info = scan_region(dbf)
        dt = time.time() - t_reg
        per_region[region_dir.name] = info
        for v, c in info['usage_counts'].items():
            global_usage[v] += c
        schema_groups[tuple(info['fields'])].append(region_dir.name)
        print(
            f'  [{i:>2}/{len(regions)}] {region_dir.name:<40} '
            f'rec={info["num_records"]:>9,}  '
            f'usage={info["usage_field"] or "-":<10}  '
            f'fact_isp={info["fact_isp_field"] or "-":<10}  '
            f'cad={info["cadastral_field"] or "-":<10}  '
            f'{dt:.1f}s'
        )

    out = {
        'base': str(BASE),
        'regions': per_region,
        'global_usage_counts': dict(global_usage),
        'schema_groups': [
            {'fields': list(flds), 'regions': regs}
            for flds, regs in sorted(schema_groups.items(), key=lambda kv: -len(kv[1]))
        ],
    }
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'\nWrote {OUT_JSON} ({OUT_JSON.stat().st_size / 1e6:.1f} MB)')

    print(f'\nTotal elapsed: {time.time() - t0:.1f}s')
    print(f'\n=== {len(schema_groups)} unique schemas ===')
    for flds, regs in sorted(schema_groups.items(), key=lambda kv: -len(kv[1])):
        total = sum(per_region[r].get('num_records', 0) for r in regs)
        print(f'  {len(regs):>2} regions, {total:>11,} records: {flds}')
        for r in regs:
            print(f'      {r}')

    print(f'\n=== Global usage-value distribution (top 60) ===')
    grand_total = sum(global_usage.values())
    for v, c in global_usage.most_common(60):
        pct = 100.0 * c / grand_total if grand_total else 0
        print(f'  {c:>11,}  {pct:>5.1f}%   {v!r}')
    print(f'  -- total unique usage values: {len(global_usage)}')
    print(f'  -- grand total records: {grand_total:,}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
