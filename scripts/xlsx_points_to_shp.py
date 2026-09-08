"""Convert an Excel table with a "lat, lon" column into a point shapefile.

The default configuration targets the file "Культуры + урожайность 2026.xlsx"
whose columns are:
    A  Координаты участка      -> "lat, lon" (WGS84)
    B  Культура 2026 г         -> crop
    C  Урожайность ПЛАН, т/га  -> yield_plan
    D  Комментарии             -> comment

Usage:
    py scripts/xlsx_points_to_shp.py INPUT.xlsx OUTPUT.shp
    py scripts/xlsx_points_to_shp.py INPUT.xlsx OUTPUT.shp --coord-col 0 --zip
"""
from __future__ import annotations

import argparse
import os
import re
import zipfile

import openpyxl
import shapefile

# WGS84 geographic CRS WKT for the .prj sidecar file.
WGS84_WKT = (
    'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'
)

_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")


def parse_latlon(value):
    """Parse "lat, lon" -> (lon, lat) floats, or None if not parseable."""
    if value is None:
        return None
    nums = _NUM_RE.findall(str(value))
    if len(nums) < 2:
        return None
    lat = float(nums[0].replace(",", "."))
    lon = float(nums[1].replace(",", "."))
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return lon, lat


def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def convert(inp, out, coord_col, crop_col, yield_col, comment_col):
    wb = openpyxl.load_workbook(inp, read_only=True, data_only=True)
    ws = wb.active

    rows = ws.iter_rows(values_only=True)
    header = next(rows, None)  # skip header row

    os.makedirs(os.path.dirname(os.path.abspath(out)) or ".", exist_ok=True)

    written = 0
    skipped = 0
    w = shapefile.Writer(out, shapeType=shapefile.POINT, encoding="utf-8")
    w.field("lat", "N", 18, 8)
    w.field("lon", "N", 18, 8)
    w.field("crop", "C", 120)
    w.field("yield_plan", "N", 12, 3)
    w.field("comment", "C", 254)

    def cell(row, idx):
        return row[idx] if idx is not None and idx < len(row) else None

    for row in rows:
        if row is None or all(v is None for v in row):
            continue
        lonlat = parse_latlon(cell(row, coord_col))
        if lonlat is None:
            skipped += 1
            continue
        lon, lat = lonlat
        crop = cell(row, crop_col)
        w.point(lon, lat)
        w.record(
            lat=lat,
            lon=lon,
            crop=("" if crop is None else str(crop)),
            yield_plan=to_float(cell(row, yield_col)),
            comment=("" if cell(row, comment_col) is None else str(cell(row, comment_col))),
        )
        written += 1

    w.close()

    base = os.path.splitext(out)[0]
    with open(base + ".prj", "w", encoding="utf-8") as f:
        f.write(WGS84_WKT)
    with open(base + ".cpg", "w", encoding="utf-8") as f:
        f.write("UTF-8")

    print(f"Header: {header}")
    print(f"Written points: {written}; skipped rows (no coords): {skipped}")
    return base, written


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument("--coord-col", type=int, default=0)
    ap.add_argument("--crop-col", type=int, default=1)
    ap.add_argument("--yield-col", type=int, default=2)
    ap.add_argument("--comment-col", type=int, default=3)
    ap.add_argument("--zip", action="store_true", help="also produce a .zip bundle")
    args = ap.parse_args()

    base, _ = convert(
        args.input, args.output,
        args.coord_col, args.crop_col, args.yield_col, args.comment_col,
    )

    if args.zip:
        zip_path = base + ".zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
                p = base + ext
                if os.path.exists(p):
                    zf.write(p, os.path.basename(p))
        print(f"Zip: {zip_path}")


if __name__ == "__main__":
    main()
