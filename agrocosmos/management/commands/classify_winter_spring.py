"""Классификация угодий на озимые / яровые по профилю NDVI (S2/L8).

Правило (см. ``agrocosmos/services/winter_spring.py``): озимые дают высокий
NDVI ранней весной (апрель) и ранний SOS, яровые — голую почву весной и
поздний пик. Порог ранневесеннего NDVI калибруется по ОПОРНЫМ ТОЧКАМ
известных культур — из shapefile (``--reference-shp``) или загруженного
ГИС-слоя (``--reference-layer``). Значение атрибута культуры раскладывается
на классы (``classify_crop_value``): озимые / яровые / не обрабатываемые /
сады (игнор). Калибровка двусторонняя — по эталонам озимых И яровых.

Примеры:
    # Порог по умолчанию, весь регион, источник S2/L8
    python manage.py classify_winter_spring --region-id 71 --year 2026

    # Калибровка + валидация по опорным точкам прямо из shapefile
    python manage.py classify_winter_spring --district-id 5 --year 2026 \
        --reference-shp /data/import/kultury_2026.shp --reference-attr crop

    # Опорные точки из ранее загруженного ГИС-слоя
    python manage.py classify_winter_spring --region-id 71 --year 2026 \
        --reference-layer kultury_2026 --reference-attr crop

    # Ручной порог дня пика (озимые < 185), классифицировать все угодья
    python manage.py classify_winter_spring --region-id 71 --year 2026 \
        --peak-threshold 185 --crop-types all

    # Только посчитать и показать статистику, без записи в БД
    python manage.py classify_winter_spring --region-id 71 --year 2026 --dry-run
"""
import time
from itertools import groupby
from operator import itemgetter

from django.contrib.gis.geos import GEOSGeometry
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from agrocosmos.models import District, Farmland, FarmlandCropSeason, Region
from agrocosmos.services.winter_spring import (
    DEFAULT_EARLY_SPRING_THRESHOLD, PEAK_DOY_THRESHOLD_DEFAULT,
    calibrate_peak_doy_threshold, calibrate_threshold_separating,
    classify_crop_value, classify_profile, evaluate_predictions,
)

RASTER_SATELLITES = ('sentinel2', 'landsat8', 'landsat9')
FUSED_SATELLITES = ('hls_fused',)

# По умолчанию классифицируем только ПАШНЮ: озимые/яровые — это пашня, а
# луга/пастбища/многолетние/сады зеленеют рано и раздувают «озимые».
DEFAULT_CROP_TYPES = ('arable',)

DB_BATCH = 2000


class Command(BaseCommand):
    help = 'Классификация угодий на озимые/яровые по сезонному NDVI (S2/L8).'

    def add_arguments(self, parser):
        parser.add_argument('--region-id', type=int)
        parser.add_argument('--district-id', type=int)
        parser.add_argument('--year', type=int, required=True)
        parser.add_argument('--source', choices=['raster', 'fused'],
                            default='raster',
                            help='raster = S2/L8 (по умолчанию), fused = HLS')
        parser.add_argument('--peak-threshold', type=float, default=None,
                            help='Ручной порог дня пика (озимые < порога). '
                                 'Переопределяет калибровку.')
        parser.add_argument('--threshold', type=float, default=None,
                            help='Ручной порог early_spring_ndvi (вторичный '
                                 'сигнал; переопределяет калибровку)')
        parser.add_argument('--crop-types', type=str, default='arable',
                            help='Виды угодий через запятую (по умолч. arable). '
                                 '"all"/пусто — без фильтра (все угодья).')
        parser.add_argument('--reference-shp', type=str, default=None,
                            help='Путь к shapefile опорных точек культур')
        parser.add_argument('--reference-layer', type=str, default=None,
                            help='Название/таблица/pk ГИС-слоя опорных точек')
        parser.add_argument('--reference-attr', type=str, default='crop',
                            help='Атрибут культуры (по умолч. crop)')
        parser.add_argument('--dry-run', action='store_true',
                            help='Не писать в БД, только показать сводку')

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        region, district = self._resolve_scope(options)
        if region is None and district is None:
            return
        year = options['year']
        source = options['source']
        satellites = FUSED_SATELLITES if source == 'fused' else RASTER_SATELLITES
        crop_types = self._parse_crop_types(options['crop_types'])

        # --- Опорные точки культур (для калибровки/валидации) ---
        # Резолвим ДО тяжёлой загрузки NDVI: чтение SHP/слоя и spatial-join
        # не зависят от NDVI, а битый путь к SHP валится за секунды, а не
        # после многоминутной выборки временных рядов.
        ref_map = self._reference_farmlands(region, district, options)

        scope_msg = ('все угодья' if crop_types is None
                     else ', '.join(crop_types))
        self.stdout.write(
            f'Loading {source} NDVI series (year {year}; виды: {scope_msg})...'
        )
        t0 = time.time()
        series = self._load_series(region, district, year, satellites, crop_types)
        self.stdout.write(
            f'  {len(series)} farmlands with NDVI in {time.time() - t0:.1f}s'
        )
        if not series:
            self.stdout.write(self.style.WARNING('No data — nothing to do.'))
            return

        # --- Пороги: ручные / калибровка / по умолчанию ---
        peak_threshold = options['peak_threshold']
        es_threshold = options['threshold']
        if (peak_threshold is None or es_threshold is None) and ref_map:
            cal_peak, cal_es = self._calibrate(series, ref_map)
            if peak_threshold is None:
                peak_threshold = cal_peak
            if es_threshold is None:
                es_threshold = cal_es
        if peak_threshold is None:
            peak_threshold = PEAK_DOY_THRESHOLD_DEFAULT
        if es_threshold is None:
            es_threshold = DEFAULT_EARLY_SPRING_THRESHOLD
        self.stdout.write(
            f'  Порог дня пика = {peak_threshold:.0f} (озимые < порога), '
            f'вторичный early_spring_ndvi = {es_threshold:.3f}'
        )

        # --- Классификация + запись ---
        counts = self._classify_all(
            series, peak_threshold, es_threshold, year, source, ref_map,
            options['dry_run'],
        )
        self._report(
            counts, series, ref_map, peak_threshold, es_threshold,
            options['dry_run'],
        )

    @staticmethod
    def _parse_crop_types(raw):
        """'arable,fallow' → ['arable','fallow']; 'all'/'' → None (все)."""
        if raw is None:
            return list(DEFAULT_CROP_TYPES)
        raw = raw.strip().lower()
        if raw in ('', 'all'):
            return None
        return [c.strip() for c in raw.split(',') if c.strip()]

    # ------------------------------------------------------------------ scope

    def _resolve_scope(self, options):
        district_id = options.get('district_id')
        region_id = options.get('region_id')
        if district_id:
            try:
                district = District.objects.select_related('region').get(
                    pk=district_id)
            except District.DoesNotExist:
                self.stderr.write(f'District {district_id} not found')
                return None, None
            return district.region, district
        if region_id:
            try:
                return Region.objects.get(pk=region_id), None
            except Region.DoesNotExist:
                self.stderr.write(f'Region {region_id} not found')
                return None, None
        self.stderr.write('Specify --region-id or --district-id')
        return None, None

    # ------------------------------------------------------------------ data

    def _load_series(self, region, district, year, satellites, crop_types):
        """{farmland_id: (doys[list], ndvi[list])} по не-выбросам за год."""
        where = [
            "vi.index_type = 'ndvi'",
            "vi.is_outlier = false",
            "vi.mean >= -0.2 AND vi.mean <= 1",
            "EXTRACT(year FROM vi.acquired_date) = %s",
        ]
        params = [year]
        if district is not None:
            where.append('f.district_id = %s')
            params.append(district.pk)
        else:
            where.append(
                'f.district_id IN (SELECT id FROM agro_district WHERE region_id = %s)'
            )
            params.append(region.pk)
        if crop_types:
            ct_ph = ', '.join(['%s'] * len(crop_types))
            where.append(f'f.crop_type IN ({ct_ph})')
            params.extend(crop_types)
        placeholders = ', '.join(['%s'] * len(satellites))
        where.append(f'sc.satellite IN ({placeholders})')
        params.extend(satellites)

        sql = f"""
            SELECT vi.farmland_id, vi.acquired_date, vi.mean
            FROM agro_vegetation_index vi
            JOIN agro_farmland f ON f.id = vi.farmland_id
            JOIN agro_satellite_scene sc ON sc.id = vi.scene_id
            WHERE {' AND '.join(where)}
            ORDER BY vi.farmland_id, vi.acquired_date
        """
        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        out = {}
        for fl_id, group in groupby(rows, key=itemgetter(0)):
            recs = list(group)
            doys = [r[1].timetuple().tm_yday for r in recs]
            vals = [float(r[2]) for r in recs]
            out[fl_id] = (doys, vals)
        return out

    # -------------------------------------------------------- reference points

    def _reference_farmlands(self, region, district, options):
        """{farmland_id: {'value', 'class'}} — угодья под опорными точками.

        Источник — shapefile (``--reference-shp``) или ГИС-слой
        (``--reference-layer``). Каждая точка относится к угодью через
        ``geom__contains``; при нескольких точках на угодье приоритет отдаётся
        определённому классу (winter/spring/unused) над сомнительным.
        """
        attr = options['reference_attr']
        if options.get('reference_shp'):
            points = self._points_from_shp(options['reference_shp'], attr)
        elif options.get('reference_layer'):
            points = self._points_from_layer(options['reference_layer'], attr)
        else:
            return {}

        scope = Farmland.objects.all()
        if district is not None:
            scope = scope.filter(district_id=district.pk)
        else:
            scope = scope.filter(district__region_id=region.pk)

        ref_map = {}
        class_stats = {'winter': 0, 'spring': 0, 'unused': 0, 'ignore': 0,
                       'none': 0, 'no_farmland': 0}
        priority = {'winter': 3, 'spring': 3, 'unused': 2, 'ignore': 1}
        for pt, value in points:
            cls = classify_crop_value(value)
            class_stats['none' if cls is None else cls] += 1
            if cls is None or cls == 'ignore':
                continue
            fid = (
                scope.filter(geom__contains=pt)
                .values_list('id', flat=True).first()
            )
            if fid is None:
                class_stats['no_farmland'] += 1
                continue
            prev = ref_map.get(fid)
            if prev is None or priority[cls] > priority[prev['class']]:
                ref_map[fid] = {'value': value, 'class': cls}

        self.stdout.write(
            '  Опорные точки: '
            + ', '.join(f'{k}={v}' for k, v in class_stats.items() if v)
        )
        self.stdout.write(
            f'  Опорных угодий: {len(ref_map)} '
            f'(озимые={sum(1 for r in ref_map.values() if r["class"] == "winter")}, '
            f'яровые={sum(1 for r in ref_map.values() if r["class"] == "spring")}, '
            f'не обраб.={sum(1 for r in ref_map.values() if r["class"] == "unused")})'
        )
        return ref_map

    def _points_from_shp(self, path, attr):
        """[(GEOSGeometry point 4326, value), ...] из shapefile."""
        import os

        from django.contrib.gis.gdal import DataSource
        if not os.path.exists(path):
            raise CommandError(
                f'SHP не найден: {path}. Проверьте, что файл (и компаньоны '
                '.shx/.dbf/.prj) лежит в смонтированном томе '
                '(./import_data → /data/import).'
            )
        for ext in ('.shx', '.dbf'):
            companion = os.path.splitext(path)[0] + ext
            if not os.path.exists(companion):
                raise CommandError(
                    f'Отсутствует обязательный файл shapefile: {companion}'
                )
        ds = DataSource(path)
        layer = ds[0]
        if attr not in layer.fields:
            self.stderr.write(
                f'Атрибут "{attr}" не найден в SHP (есть: {layer.fields}) '
                '— калибровка пропущена'
            )
            return []
        out = []
        for feat in layer:
            geom = feat.geom
            if geom is None:
                continue
            g = geom.geos
            if g.srid is None:
                g.srid = 4326
            elif g.srid != 4326:
                g.transform(4326)
            out.append((g, feat.get(attr)))
        return out

    def _points_from_layer(self, layer_ref, attr):
        """[(GEOSGeometry point 4326, value), ...] из ГИС-слоя (my_fields)."""
        from django.db.models import Q
        from my_fields.models import GisLayer

        layer = GisLayer.objects.filter(
            Q(table_name=layer_ref) | Q(title=layer_ref)
        ).first()
        if layer is None and str(layer_ref).isdigit():
            layer = GisLayer.objects.filter(pk=int(layer_ref)).first()
        if layer is None:
            self.stderr.write(f'ГИС-слой "{layer_ref}" не найден — пропуск')
            return []

        attr_dbs = {a.get('db') for a in (layer.attributes or [])}
        if attr not in attr_dbs:
            self.stderr.write(
                f'Атрибут "{attr}" не найден в слое {layer.table_name} '
                f'(есть: {sorted(attr_dbs)}) — пропуск'
            )
            return []

        from psycopg import sql as _sql
        query = _sql.SQL(
            'SELECT ST_AsEWKT(ST_Transform(geom, 4326)), {attr} FROM {tbl}'
        ).format(attr=_sql.Identifier(attr),
                 tbl=_sql.Identifier(layer.table_name))
        with connection.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        out = []
        for wkt, value in rows:
            if not wkt:
                continue
            out.append((GEOSGeometry(wkt), value))
        return out

    # ------------------------------------------------------------------ calc

    def _calibrate(self, series, ref_map):
        """Калибровка обоих порогов по эталонам озимых и яровых.

        Основной — день пика (:func:`calibrate_peak_doy_threshold`),
        вторичный — ранневесенний NDVI (:func:`calibrate_threshold_separating`).
        Фичи (peak_doy, early_spring) не зависят от порогов, поэтому берём
        их из :func:`classify_profile` с параметрами по умолчанию.
        """
        w_peak, s_peak, w_es, s_es = [], [], [], []
        for fid, ref in ref_map.items():
            data = series.get(fid)
            if not data:
                continue
            prof = classify_profile(data[0], data[1])
            if ref['class'] == 'winter':
                if prof.peak_doy is not None:
                    w_peak.append(prof.peak_doy)
                if prof.early_spring_ndvi is not None:
                    w_es.append(prof.early_spring_ndvi)
            elif ref['class'] == 'spring':
                if prof.peak_doy is not None:
                    s_peak.append(prof.peak_doy)
                if prof.early_spring_ndvi is not None:
                    s_es.append(prof.early_spring_ndvi)
        peak_thr = calibrate_peak_doy_threshold(w_peak, s_peak)
        es_thr = calibrate_threshold_separating(w_es, s_es)
        self.stdout.write(
            f'  Калибровка: озимых эталонов с рядом={len(w_peak)}, '
            f'яровых={len(s_peak)} → день пика {peak_thr:.0f}, '
            f'early_spring {es_thr:.3f}'
        )
        return peak_thr, es_thr

    def _classify_all(self, series, peak_threshold, es_threshold, year,
                      source, ref_map, dry_run):
        counts = {'winter': 0, 'spring': 0, 'unknown': 0}
        batch = []
        for fl_id, (doys, ndvi) in series.items():
            prof = classify_profile(
                doys, ndvi, peak_doy_threshold=peak_threshold,
                early_spring_threshold=es_threshold,
            )
            counts[prof.season_class] += 1
            if dry_run:
                continue
            ref = ref_map.get(fl_id)
            batch.append(FarmlandCropSeason(
                farmland_id=fl_id, year=year, source=source,
                season_class=prof.season_class, confidence=prof.confidence,
                early_spring_ndvi=prof.early_spring_ndvi,
                winter_baseline=prof.winter_baseline,
                sos_doy=prof.sos_doy, peak_doy=prof.peak_doy,
                peak_ndvi=prof.peak_ndvi,
                is_reference=ref is not None,
                reference_crop=(ref['value'] if ref else ''),
                threshold=es_threshold,
                peak_doy_threshold=peak_threshold,
            ))
            if len(batch) >= DB_BATCH:
                self._flush(batch)
                batch = []
        if batch:
            self._flush(batch)
        return counts

    @staticmethod
    def _flush(batch):
        FarmlandCropSeason.objects.bulk_create(
            batch, batch_size=DB_BATCH,
            update_conflicts=True,
            unique_fields=['farmland', 'year', 'source'],
            update_fields=[
                'season_class', 'confidence', 'early_spring_ndvi',
                'winter_baseline', 'sos_doy', 'peak_doy', 'peak_ndvi',
                'is_reference', 'reference_crop', 'threshold',
                'peak_doy_threshold',
            ],
        )

    # ------------------------------------------------------------------ report

    def _report(self, counts, series, ref_map, peak_threshold, es_threshold,
                dry_run):
        total = sum(counts.values())
        self.stdout.write(
            f'\n{"[DRY RUN] " if dry_run else ""}Классифицировано {total}: '
            f'озимые={counts["winter"]}, яровые={counts["spring"]}, '
            f'не определено={counts["unknown"]}'
        )
        if not ref_map:
            return

        # Валидация: матрица ошибок по эталонам озимых/яровых.
        pairs, unused = [], {'n': 0, 'as_winter': 0, 'as_spring': 0, 'unknown': 0}
        for fid, ref in ref_map.items():
            data = series.get(fid)
            if not data:
                continue
            prof = classify_profile(
                data[0], data[1], peak_doy_threshold=peak_threshold,
                early_spring_threshold=es_threshold,
            )
            if ref['class'] in ('winter', 'spring'):
                pairs.append((ref['class'], prof.season_class))
            elif ref['class'] == 'unused':
                unused['n'] += 1
                unused[{'winter': 'as_winter', 'spring': 'as_spring',
                        'unknown': 'unknown'}[prof.season_class]] += 1

        ev = evaluate_predictions(pairs)
        w, s = ev['winter'], ev['spring']
        self.stdout.write('  Валидация по эталонам (озимые/яровые):')
        self.stdout.write(
            f'    озимые:  {w["correct"]}/{w["n"]} верно'
            + (f' ({w["correct"] / w["n"] * 100:.1f}%)' if w['n'] else '')
        )
        self.stdout.write(
            f'    яровые:  {s["correct"]}/{s["n"]} верно'
            + (f' ({s["correct"] / s["n"] * 100:.1f}%)' if s['n'] else '')
        )
        if ev['accuracy'] is not None:
            self.stdout.write(
                f'    общая точность: {ev["accuracy"] * 100:.1f}% '
                f'(n={ev["total"]})'
            )
        if unused['n']:
            self.stdout.write(
                f'  Эталоны «не обрабатываемые» (для контроля ЗСН): '
                f'n={unused["n"]}, распознаны NDVI-классификатором как '
                f'озимые={unused["as_winter"]}, яровые={unused["as_spring"]}, '
                f'не определено={unused["unknown"]}'
            )
