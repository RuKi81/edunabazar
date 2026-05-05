"""Import cropland polygons from ESA WorldCover 2021 (via Google Earth
Engine) for regions that do not have a Rosreestr ЗСН shapefile.

Pipeline:
    1. Download the ESA WorldCover land-cover raster for the region's
       bounding box at ``--scale`` metres (default 100 m).  The raster
       is a single-band uint8 image; class code ``40`` = ``Cropland``.
    2. Vectorise class 40 pixels into polygons using
       ``rasterio.features.shapes``.
    3. Write the polygons to a temp GeoJSON file.
    4. Load them into a staging table via ``ogr2ogr``.
    5. ``INSERT … SELECT`` into ``agro_farmland`` with:
           * clip to ``Region.geom`` via ``ST_Intersection``;
           * ``ST_MakeValid`` + ``CollectionExtract`` + ``ST_Multi``;
           * ``area_ha`` via ``ST_Area(geom::geography)``;
           * filter ``area_ha >= --min-area-ha``;
           * ``crop_type='arable'`` (WorldCover does not split arable /
             pasture / hayfield — it has only one ``Cropland`` class).

Usage::

    python manage.py import_gee_croplands \\
        --regions 19,23,34,41,43,64,70,71,77,17

    python manage.py import_gee_croplands --regions 19 --dry-run
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import ee
from django.contrib.gis.geos import GEOSGeometry
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from agrocosmos.models import Farmland, Region
from agrocosmos.services.farmland_importer import (
    _pg_connection_string,
    build_count_staging_sql,
    build_drop_staging_sql,
)
from agrocosmos.services.gee_download import download_tiled_composite
from agrocosmos.services.satellite_gee import GEEError, initialize as gee_init


logger = logging.getLogger(__name__)

WORLDCOVER_ASSET = 'ESA/WorldCover/v200/2021'
CROPLAND_CLASS = 40
DEFAULT_SOURCE_ID = 'gee_esa_worldcover_2021'


class Command(BaseCommand):
    help = (
        'Import cropland polygons from ESA WorldCover 2021 (via GEE) for '
        'regions without Rosreestr ЗСН shapefiles. Writes to agro_farmland '
        'with source="gee_esa_worldcover_2021".'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--regions', required=True,
            help='Comma-separated Region IDs (e.g. "19,23,34").',
        )
        parser.add_argument(
            '--scale', type=int, default=100,
            help='Raster scale in metres (default: 100). Lower = more '
                 'polygons but slower download + vectorisation.',
        )
        parser.add_argument(
            '--min-area-ha', type=float, default=1.0,
            help='Minimum polygon area in hectares after clipping (default: 1).',
        )
        parser.add_argument(
            '--source-id', default=DEFAULT_SOURCE_ID,
            help=f'Value stored in Farmland.source (default: {DEFAULT_SOURCE_ID}).',
        )
        parser.add_argument(
            '--tmp-dir', default='/tmp/gee_croplands',
            help='Scratch dir for GeoTIFF + GeoJSON (default: /tmp/gee_croplands).',
        )
        parser.add_argument(
            '--ogr2ogr', default='ogr2ogr',
            help='Path to ogr2ogr binary (default: rely on PATH).',
        )
        parser.add_argument(
            '--overwrite', action='store_true',
            help='Re-download raster even if file exists.',
        )
        parser.add_argument(
            '--skip-existing', action='store_true',
            help='Skip regions that already have any Farmland rows.',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Print the plan; do not download or write to DB.',
        )

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def handle(self, *args, **options):
        region_ids = self._parse_ids(options['regions'])
        if not region_ids:
            raise CommandError('--regions parsed to empty list.')

        if not options['dry_run'] and not shutil.which(options['ogr2ogr']):
            raise CommandError(
                f'{options["ogr2ogr"]!r} not found in PATH. Install gdal-bin.'
            )

        tmp_dir = Path(options['tmp_dir']).expanduser().resolve()
        tmp_dir.mkdir(parents=True, exist_ok=True)

        regions = list(Region.objects.filter(pk__in=region_ids).order_by('name'))
        missing = set(region_ids) - {r.pk for r in regions}
        if missing:
            self.stderr.write(self.style.WARNING(
                f'Region IDs not found: {sorted(missing)}'
            ))

        self.stdout.write(self.style.NOTICE(
            f'Will process {len(regions)} region(s) at scale={options["scale"]}m, '
            f'min_area={options["min_area_ha"]}ha, source={options["source_id"]!r}'
        ))

        if not options['dry_run']:
            try:
                gee_init()
            except GEEError as exc:
                raise CommandError(f'GEE init failed: {exc}')

        totals = {'ok': 0, 'failed': 0, 'skipped': 0, 'inserted': 0}
        started = time.time()

        for region in regions:
            t0 = time.time()
            try:
                result = self._process_region(region, options, tmp_dir)
            except Exception as exc:
                logger.exception('[%s] FAILED', region.name)
                self.stderr.write(self.style.ERROR(
                    f'[{region.name}] failed: {exc}'
                ))
                totals['failed'] += 1
                continue

            dt = time.time() - t0
            if result['status'] == 'skipped':
                totals['skipped'] += 1
                self.stdout.write(self.style.WARNING(
                    f'[{region.name}] skipped: {result["reason"]}'
                ))
            else:
                totals['ok'] += 1
                totals['inserted'] += result.get('inserted', 0)
                self.stdout.write(self.style.SUCCESS(
                    f'[{region.name}] inserted={result["inserted"]:,} '
                    f'polygons_raw={result.get("polygons_raw", 0):,} '
                    f'{dt:.1f}s'
                ))

        elapsed = time.time() - started
        self.stdout.write(self.style.NOTICE(
            f'\n=== Totals: ok={totals["ok"]}, failed={totals["failed"]}, '
            f'skipped={totals["skipped"]},  '
            f'inserted={totals["inserted"]:,}  in {elapsed:.1f}s'
        ))

    # ------------------------------------------------------------------
    # Per-region pipeline
    # ------------------------------------------------------------------

    def _process_region(self, region: Region, options: dict, tmp_dir: Path) -> dict:
        if options['skip_existing'] and not options['dry_run']:
            if Farmland.objects.filter(region=region).exists():
                return {'status': 'skipped',
                        'reason': 'already has farmlands (drop --skip-existing to reimport)'}

        # 1. Extent
        if region.geom is None:
            return {'status': 'skipped', 'reason': 'region has no geom'}
        extent = region.geom.extent  # (xmin, ymin, xmax, ymax) in region SRID
        if region.geom.srid and region.geom.srid != 4326:
            # Very rare — Region.geom is SRID 4326 by schema. Fail loudly.
            return {'status': 'skipped',
                    'reason': f'region.geom SRID={region.geom.srid} (expected 4326)'}

        slug = self._slug(region.code or str(region.pk))
        tif_path = tmp_dir / f'worldcover_{slug}_{options["scale"]}m.tif'
        geojson_path = tmp_dir / f'croplands_{slug}.geojson'
        staging = f'staging_gee_croplands_{slug}'

        self.stdout.write(
            f'[{region.name}] extent={tuple(round(x, 3) for x in extent)} '
            f'scale={options["scale"]}m → {tif_path.name}'
        )

        if options['dry_run']:
            self.stdout.write(f'  DRY: would download {tif_path.name} and vectorise')
            return {'status': 'ok', 'inserted': 0, 'polygons_raw': 0}

        # 2. Download WC raster (cropland-only mask)
        if not tif_path.exists() or options['overwrite']:
            self.stdout.write('  Downloading ESA WorldCover from GEE…')
            self._download_worldcover_mask(extent, options['scale'], str(tif_path))
        else:
            self.stdout.write(f'  Raster exists, reusing: {tif_path.name}')

        # 3. Vectorise → GeoJSON
        self.stdout.write('  Vectorising cropland pixels…')
        n_raw = self._vectorise_to_geojson(tif_path, geojson_path)
        if n_raw == 0:
            self._cleanup(tif_path, geojson_path)
            return {'status': 'ok', 'inserted': 0, 'polygons_raw': 0}
        self.stdout.write(f'  Raw polygons: {n_raw:,}')

        # 4. Load GeoJSON → staging table via ogr2ogr
        self.stdout.write(f'  ogr2ogr → {staging}…')
        rc = self._ogr2ogr_geojson_to_pg(geojson_path, staging, options['ogr2ogr'])
        if rc != 0:
            self.stderr.write(self.style.WARNING(
                f'  ogr2ogr rc={rc} (continuing)'
            ))

        with connection.cursor() as cur:
            try:
                cur.execute(build_count_staging_sql(staging))
                staged_rows = cur.fetchone()[0]
            except Exception as exc:
                self._drop_staging_silent(staging)
                self._cleanup(tif_path, geojson_path)
                return {'status': 'skipped',
                        'reason': f'staging missing after ogr2ogr: {exc}'}

            self.stdout.write(f'  Staged: {staged_rows:,} rows, clipping + promoting…')

            # 5. INSERT … SELECT with clip to region.geom and area filter
            min_area_m2 = options['min_area_ha'] * 10000
            source_id = options['source_id'][:40]
            sql = self._build_insert_sql(staging, region.pk, source_id, min_area_m2)
            with transaction.atomic():
                cur.execute(sql)
                inserted = cur.rowcount

            cur.execute(build_drop_staging_sql(staging))

        self._cleanup(tif_path, geojson_path)
        return {'status': 'ok', 'inserted': inserted, 'polygons_raw': n_raw}

    # ------------------------------------------------------------------
    # GEE download
    # ------------------------------------------------------------------

    @staticmethod
    def _download_worldcover_mask(extent: tuple, scale_m: int, out_path: str) -> None:
        """Download WorldCover clipped to cropland class 40 as single-band
        float32 GeoTIFF (reused ``download_tiled_composite`` expects float).

        Pixels outside class 40 are masked (no-data), so vectorisation
        picks only cropland polygons.
        """
        wc = ee.Image(WORLDCOVER_ASSET).select('Map')
        cropland = wc.eq(CROPLAND_CLASS).selfMask().toFloat()
        # download_tiled_composite tiles automatically to fit GEE limits.
        download_tiled_composite(
            composite=cropland,
            extent=extent,
            scale_m=scale_m,
            out_path=out_path,
            n_images=1,
            sensor_label='WorldCover',
        )

    # ------------------------------------------------------------------
    # Vectorisation
    # ------------------------------------------------------------------

    @staticmethod
    def _vectorise_to_geojson(tif_path: Path, out_path: Path) -> int:
        """Vectorise a single-band raster's valid pixels into GeoJSON
        polygons. Returns the number of raw polygons written.

        WorldCover was downloaded cropland-only (non-cropland masked out),
        so every non-nodata pixel is by definition cropland → we vectorise
        the mask directly."""
        import numpy as np
        import rasterio
        from rasterio.features import shapes as rio_shapes

        with rasterio.open(tif_path) as src:
            arr = src.read(1)
            nodata = src.nodata
            transform = src.transform
            # Build boolean mask of valid pixels
            if nodata is None or (isinstance(nodata, float) and np.isnan(nodata)):
                mask = ~np.isnan(arr)
            else:
                mask = arr != nodata
            mask = mask & (arr > 0)

            with open(out_path, 'w', encoding='utf-8') as f:
                f.write('{"type":"FeatureCollection","features":[\n')
                first = True
                count = 0
                for geom, _val in rio_shapes(
                    arr.astype('uint8'), mask=mask, transform=transform,
                ):
                    if not first:
                        f.write(',\n')
                    else:
                        first = False
                    feat = {
                        'type': 'Feature',
                        'properties': {},
                        'geometry': geom,
                    }
                    f.write(json.dumps(feat))
                    count += 1
                f.write('\n]}\n')
        return count

    # ------------------------------------------------------------------
    # ogr2ogr → Postgres staging
    # ------------------------------------------------------------------

    @staticmethod
    def _ogr2ogr_geojson_to_pg(geojson_path: Path, staging: str, binary: str) -> int:
        args = [
            binary,
            '-f', 'PostgreSQL',
            _pg_connection_string(),
            str(geojson_path),
            '-nln', staging,
            '-nlt', 'MULTIPOLYGON',
            '-t_srs', 'EPSG:4326',
            '-s_srs', 'EPSG:4326',
            '-lco', 'GEOMETRY_NAME=wkb_geometry',
            '-lco', 'FID=ogc_fid',
            '-lco', 'SPATIAL_INDEX=NONE',
            '-lco', 'PRECISION=NO',
            '-overwrite',
            '-skipfailures',
            '--config', 'PG_USE_COPY', 'YES',
            '--config', 'OGR_TRUNCATE', 'NO',
        ]
        proc = subprocess.run(
            args, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, errors='replace',
        )
        if proc.stdout:
            logger.debug('ogr2ogr stdout: %s', proc.stdout[-500:])
        if proc.stderr:
            logger.debug('ogr2ogr stderr: %s', proc.stderr[-500:])
        return proc.returncode

    @staticmethod
    def _build_insert_sql(staging: str, region_id: int,
                          source_id: str, min_area_m2: float) -> str:
        st = '"' + staging.replace('"', '""') + '"'
        src_q = "'" + source_id.replace("'", "''") + "'"
        return f"""
INSERT INTO agro_farmland (
    region_id, district_id, crop_type, is_used, cadastral_number,
    area_ha, geom, properties, source, created_at
)
SELECT
    {int(region_id)}::int,
    NULL::int,
    'arable',
    NULL::boolean,
    '',
    ST_Area(clipped::geography) / 10000.0,
    ST_Multi(
        ST_CollectionExtract(
            ST_MakeValid(clipped), 3
        )
    )::geometry(MultiPolygon, 4326),
    jsonb_build_object(
        'source', {src_q},
        'class', {CROPLAND_CLASS}
    ),
    {src_q},
    NOW()
FROM (
    SELECT
        ST_Intersection(
            ST_MakeValid(s.wkb_geometry),
            (SELECT ST_MakeValid(geom) FROM agro_region WHERE id = {int(region_id)})
        ) AS clipped
    FROM {st} s
) AS c
WHERE NOT ST_IsEmpty(c.clipped)
  AND ST_Area(c.clipped::geography) >= {float(min_area_m2)}
  AND ST_GeometryType(
      ST_Multi(ST_CollectionExtract(ST_MakeValid(c.clipped), 3))
  ) = 'ST_MultiPolygon';
""".strip()

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_ids(spec: str) -> list[int]:
        out: list[int] = []
        for tok in spec.split(','):
            tok = tok.strip()
            if not tok:
                continue
            try:
                out.append(int(tok))
            except ValueError:
                raise CommandError(f'--regions: bad id {tok!r}')
        return out

    @staticmethod
    def _slug(text: str) -> str:
        s = re.sub(r'[^A-Za-z0-9]+', '_', text).strip('_').lower()
        return s or 'unknown'

    @staticmethod
    def _drop_staging_silent(staging: str) -> None:
        try:
            with connection.cursor() as cur:
                cur.execute(build_drop_staging_sql(staging))
        except Exception:
            pass

    @staticmethod
    def _cleanup(*paths: Path) -> None:
        for p in paths:
            try:
                if p.exists():
                    p.unlink()
            except OSError:
                pass
