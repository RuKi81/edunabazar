"""
Filesystem accessors for downloaded NDVI raster composites.

Shared between the ``cleanup_rasters`` management command and the
``/admin/agro/rasters/`` UI.

Storage layout (see ``services/satellite_*_raster.py``)::

    {RASTER_DIR}/{scope_id}/{year}/{prefix}_{scope}_{date_from}_{date_to}.tif

where ``scope_id`` is either a region id (``"37"``) or district id
(``"d123"``).  Filenames embed the period as two ISO dates.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

from django.conf import settings


# Sensor registry — env var name + default path + filename prefix.
SENSORS = {
    's2':    {'env': 'S2_RASTER_DIR',      'default': '/data/s2',      'prefix': 's2_ndvi',       'label': 'Sentinel-2'},
    'l8':    {'env': 'LANDSAT_RASTER_DIR', 'default': '/data/landsat', 'prefix': 'landsat_ndvi',  'label': 'Landsat 8/9'},
    'modis': {'env': 'MODIS_RASTER_DIR',   'default': '/data/modis',   'prefix': 'modis_ndvi',    'label': 'MODIS'},
}

# Trailing "..._YYYY-MM-DD_YYYY-MM-DD.tif" in raster filenames.
DATE_RE = re.compile(r'_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.tif$')


def sensor_root(sensor: str) -> Path:
    """Resolve the filesystem root for a sensor (env var > settings > default)."""
    cfg = SENSORS[sensor]
    return Path(
        os.environ.get(cfg['env'], getattr(settings, cfg['env'], cfg['default']))
    )


@dataclass
class FolderSummary:
    """Aggregate of one ``{root}/{scope}/{year}/`` directory."""
    sensor: str
    scope: str
    year: str
    count: int
    size_bytes: int
    oldest: date | None
    newest: date | None

    @property
    def size_mb(self) -> float:
        return self.size_bytes / 1e6


@dataclass
class RasterFile:
    """One GeoTIFF composite on disk."""
    sensor: str
    scope: str
    year: str
    name: str
    path: str
    date_from: date | None
    date_to: date | None
    size_bytes: int
    age_days: int | None

    @property
    def size_mb(self) -> float:
        return self.size_bytes / 1e6


def list_folders(sensors: Iterable[str] | None = None) -> list[FolderSummary]:
    """Walk each sensor root two levels deep and aggregate per-folder."""
    sensors = list(sensors) if sensors else list(SENSORS)
    summaries: list[FolderSummary] = []

    for sensor in sensors:
        root = sensor_root(sensor)
        prefix = SENSORS[sensor]['prefix']
        if not root.exists():
            continue

        # Each scope/year is one folder summary.
        for scope_dir in sorted(root.iterdir()):
            if not scope_dir.is_dir():
                continue
            for year_dir in sorted(scope_dir.iterdir()):
                if not year_dir.is_dir():
                    continue

                count = 0
                size = 0
                oldest: date | None = None
                newest: date | None = None

                for f in year_dir.glob(f'{prefix}_*.tif'):
                    try:
                        size += f.stat().st_size
                    except OSError:
                        continue
                    count += 1
                    m = DATE_RE.search(f.name)
                    if m:
                        try:
                            d_from = date.fromisoformat(m.group(1))
                            d_to = date.fromisoformat(m.group(2))
                        except ValueError:
                            continue
                        if oldest is None or d_from < oldest:
                            oldest = d_from
                        if newest is None or d_to > newest:
                            newest = d_to

                if count == 0:
                    continue
                summaries.append(FolderSummary(
                    sensor=sensor,
                    scope=scope_dir.name,
                    year=year_dir.name,
                    count=count,
                    size_bytes=size,
                    oldest=oldest,
                    newest=newest,
                ))

    return summaries


def list_files(sensor: str, scope: str, year: str) -> list[RasterFile]:
    """List all rasters in a specific ``{root}/{scope}/{year}/`` folder."""
    if sensor not in SENSORS:
        return []
    root = sensor_root(sensor)
    prefix = SENSORS[sensor]['prefix']
    folder = root / scope / year
    if not folder.exists():
        return []

    today = date.today()
    files: list[RasterFile] = []
    for f in sorted(folder.glob(f'{prefix}_*.tif')):
        try:
            size = f.stat().st_size
        except OSError:
            continue
        m = DATE_RE.search(f.name)
        d_from = d_to = None
        age = None
        if m:
            try:
                d_from = date.fromisoformat(m.group(1))
                d_to = date.fromisoformat(m.group(2))
                age = (today - d_to).days
            except ValueError:
                pass

        files.append(RasterFile(
            sensor=sensor, scope=scope, year=year,
            name=f.name, path=str(f),
            date_from=d_from, date_to=d_to,
            size_bytes=size, age_days=age,
        ))
    return files


def delete_paths(paths: Iterable[str]) -> tuple[int, int]:
    """Delete given absolute paths; returns ``(removed_count, freed_bytes)``.

    Silently ignores files outside of the configured sensor roots
    (defense-in-depth against path traversal from form posts).
    """
    allowed_roots = [sensor_root(s).resolve() for s in SENSORS]
    removed = 0
    freed = 0
    for p in paths:
        path = Path(p).resolve()
        if not any(str(path).startswith(str(root)) for root in allowed_roots):
            continue
        if not path.is_file():
            continue
        try:
            size = path.stat().st_size
            path.unlink()
        except OSError:
            continue
        removed += 1
        freed += size

    # Prune empty year directories.
    for sensor in SENSORS:
        root = sensor_root(sensor)
        if not root.exists():
            continue
        for scope_dir in root.iterdir():
            if not scope_dir.is_dir():
                continue
            for year_dir in scope_dir.iterdir():
                if year_dir.is_dir() and not any(year_dir.iterdir()):
                    try:
                        year_dir.rmdir()
                    except OSError:
                        pass

    return removed, freed


def totals_by_sensor() -> dict[str, dict]:
    """Per-sensor totals for the summary banner."""
    out: dict[str, dict] = {}
    for s in SENSORS:
        root = sensor_root(s)
        if not root.exists():
            out[s] = {'count': 0, 'size_bytes': 0, 'label': SENSORS[s]['label'], 'root': str(root)}
            continue
        count = 0
        size = 0
        for f in root.rglob(f'{SENSORS[s]["prefix"]}_*.tif'):
            try:
                size += f.stat().st_size
            except OSError:
                continue
            count += 1
        out[s] = {
            'count': count,
            'size_bytes': size,
            'size_mb': round(size / 1e6, 1),
            'size_gb': round(size / 1e9, 2),
            'label': SENSORS[s]['label'],
            'root': str(root),
        }
    return out
