"""
Retention cleanup for downloaded NDVI raster composites.

GEE NDVI composites are cached as GeoTIFFs under sensor-specific roots
(S2, Landsat, MODIS).  Zonal statistics are long since persisted to
PostgreSQL, so MODIS rasters exist only as a byproduct of fetching
stats and can be aged out aggressively.

**S2 and Landsat rasters are kept by default** because they are used
by the dashboard's map-overlay layer (NDVI raster-tiles served by
``services/raster_tiles.py``).  Until that use-case is retired or
explicitly scoped to a shorter window, ``--sensor`` defaults to
``modis`` only.  Pass ``--sensor all`` or ``--sensor s2|l8`` to force.

Filename convention (see ``services/satellite_*_raster.py``)::

    {prefix}_{scope}_{date_from}_{date_to}.tif

where prefix is one of ``s2_ndvi``/``landsat_ndvi``/``modis_ndvi``.
The ``date_to`` component is used as the retention reference.

Usage::

    python manage.py cleanup_rasters                     # MODIS, 2y
    python manage.py cleanup_rasters --sensor all        # include S2/L8
    python manage.py cleanup_rasters --keep-days 365
    python manage.py cleanup_rasters --sensor s2
    python manage.py cleanup_rasters --dry-run           # list only

Safe to run daily; idempotent.  Typically invoked manually from the
admin panel at ``/admin/agrocosmos/rasters/`` (preferred) or via SSH
for ad-hoc cleanup.  No cron wiring — we want visibility before
deletion.
"""
from datetime import date, timedelta
from pathlib import Path

from django.core.management.base import BaseCommand

from agrocosmos.services.raster_storage import (
    DATE_RE as _DATE_RE,
    SENSORS,
    sensor_root,
)


class Command(BaseCommand):
    help = 'Delete raster GeoTIFFs older than --keep-days (default 730).'

    def add_arguments(self, parser):
        parser.add_argument('--keep-days', type=int, default=730,
                            help='Retention window in days (default 730 = ~2y).')
        parser.add_argument('--sensor', choices=list(SENSORS) + ['all'],
                            default='modis',
                            help='Which sensor root to clean. Default "modis": '
                                 'S2/L8 are kept because map-overlay layer '
                                 'still consumes them. Pass "all" to force.')
        parser.add_argument('--dry-run', action='store_true',
                            help='List candidates without deleting.')

    def handle(self, *args, **options):
        keep_days = options['keep_days']
        sensor_opt = options['sensor']
        dry = options['dry_run']

        cutoff = date.today() - timedelta(days=keep_days)
        self.stdout.write(
            f'Retention cutoff: {cutoff.isoformat()} '
            f'(keep files with date_to ≥ cutoff)'
            + ('  [DRY RUN]' if dry else '')
        )

        targets = [sensor_opt] if sensor_opt != 'all' else list(SENSORS)
        grand_removed = 0
        grand_bytes = 0

        for sensor in targets:
            cfg = SENSORS[sensor]
            root = sensor_root(sensor)
            if not root.exists():
                self.stdout.write(f'  [{sensor}] {root} — not present, skip')
                continue

            removed, freed, scanned = self._clean_root(root, cfg['prefix'], cutoff, dry)
            grand_removed += removed
            grand_bytes += freed
            self.stdout.write(
                f'  [{sensor}] {root}: scanned {scanned}, '
                f'{"would remove" if dry else "removed"} {removed} files '
                f'({freed / 1e6:.1f} MB)'
            )

        self.stdout.write(self.style.SUCCESS(
            f'Done: {grand_removed} files, {grand_bytes / 1e6:.1f} MB'
            + (' (dry run)' if dry else '')
        ))

    # ------------------------------------------------------------------
    def _clean_root(self, root: Path, prefix: str, cutoff: date, dry: bool):
        """Walk ``root/**/{prefix}_*.tif`` and delete files with date_to < cutoff.

        Returns ``(removed, freed_bytes, scanned)``.
        """
        removed = 0
        freed = 0
        scanned = 0
        pattern = f'**/{prefix}_*.tif'

        for f in root.glob(pattern):
            scanned += 1
            m = _DATE_RE.search(f.name)
            if not m:
                # Filename doesn't match expected format — leave alone.
                continue
            try:
                date_to = date.fromisoformat(m.group(2))
            except ValueError:
                continue

            if date_to >= cutoff:
                continue

            size = f.stat().st_size
            if dry:
                self.stdout.write(f'    would remove: {f} ({size/1e6:.1f} MB, date_to={date_to})')
            else:
                try:
                    f.unlink()
                except OSError as e:
                    self.stderr.write(f'    failed to remove {f}: {e}')
                    continue
            removed += 1
            freed += size

        # Prune now-empty year directories (scope/year/).
        if not dry:
            for year_dir in root.glob('*/*'):
                if year_dir.is_dir() and not any(year_dir.iterdir()):
                    try:
                        year_dir.rmdir()
                    except OSError:
                        pass

        return removed, freed, scanned
