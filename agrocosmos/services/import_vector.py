"""
Import vector data (Shapefile/GeoJSON) into Region/District/Farmland models.
Used by Django admin upload view.
"""
import json
import logging
import os
import tempfile
import zipfile

from django.contrib.gis.geos import GEOSGeometry, MultiPolygon

from agrocosmos.models import Region, District, Farmland

logger = logging.getLogger(__name__)

_CROP_TYPE_MAP = {
    'пашня': 'arable',
    'пашни': 'arable',
    'arable': 'arable',
    'сенокос': 'hayfield',
    'сенокосы': 'hayfield',
    'hayfield': 'hayfield',
    'пастбище': 'pasture',
    'пастбища': 'pasture',
    'pasture': 'pasture',
    'многолетнее насаждение': 'perennial',
    'многолетние насаждения': 'perennial',
    'perennial': 'perennial',
}


def _to_multi(geom):
    if geom.geom_type == 'Polygon':
        return MultiPolygon(geom, srid=geom.srid)
    return geom


def _resolve_crop_type(raw, default='arable'):
    if not raw:
        return default
    return _CROP_TYPE_MAP.get(raw.strip().lower(), default)


def _extract_shp_from_zip(uploaded_file):
    """Extract .shp + sidecar files from a ZIP, return path to .shp."""
    tmp_dir = tempfile.mkdtemp(prefix='agro_shp_')
    with zipfile.ZipFile(uploaded_file, 'r') as zf:
        zf.extractall(tmp_dir)
    # Find .shp file (may be nested)
    for root, dirs, files in os.walk(tmp_dir):
        for f in files:
            if f.lower().endswith('.shp'):
                return os.path.join(root, f), tmp_dir
    raise ValueError('ZIP-архив не содержит .shp файла')


def _save_uploaded_file(uploaded_file):
    """Save an UploadedFile to a temp location, return path."""
    suffix = os.path.splitext(uploaded_file.name)[1]
    fd, path = tempfile.mkstemp(suffix=suffix, prefix='agro_')
    with os.fdopen(fd, 'wb') as f:
        for chunk in uploaded_file.chunks():
            f.write(chunk)
    return path


def import_region_vector(uploaded_file, name_field='NAME', code_field='CODE',
                         encoding='utf-8'):
    """
    Import region boundaries from uploaded file.
    Returns: (created, updated, errors_list)
    """
    fname = uploaded_file.name.lower()
    created = updated = 0
    errors = []

    if fname.endswith('.zip'):
        shp_path, tmp_dir = _extract_shp_from_zip(uploaded_file)
        try:
            return _import_region_shp(shp_path, name_field, code_field, encoding)
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
    elif fname.endswith(('.geojson', '.json')):
        path = _save_uploaded_file(uploaded_file)
        try:
            return _import_region_geojson(path, name_field, code_field, encoding)
        finally:
            os.unlink(path)
    else:
        return 0, 0, ['Неподдерживаемый формат. Используйте ZIP (с .shp) или GeoJSON.']


def _import_region_geojson(path, name_field, code_field, encoding):
    with open(path, 'r', encoding=encoding) as f:
        data = json.load(f)

    features = data.get('features', [])
    created = updated = 0
    errors = []

    for feat in features:
        props = feat.get('properties', {})
        name = str(props.get(name_field, '')).strip()
        code = str(props.get(code_field, '')).strip()
        if not name:
            errors.append(f'Пропущен объект без имени: {props}')
            continue
        try:
            geom = GEOSGeometry(json.dumps(feat['geometry']), srid=4326)
            geom = _to_multi(geom)
            obj, is_new = Region.objects.update_or_create(
                code=code or f'auto_{name[:20]}',
                defaults={'name': name, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1
        except Exception as e:
            errors.append(f'{name}: {e}')

    return created, updated, errors


def _import_region_shp(path, name_field, code_field, encoding):
    from django.contrib.gis.gdal import DataSource
    ds = DataSource(path, encoding=encoding)
    layer = ds[0]
    created = updated = 0
    errors = []

    for feat in layer:
        name = str(feat.get(name_field)).strip()
        code = str(feat.get(code_field)).strip()
        if not name:
            continue
        try:
            geom = GEOSGeometry(feat.geom.wkt, srid=feat.geom.srid or 4326)
            if geom.srid != 4326:
                geom.transform(4326)
            geom = _to_multi(geom)
            obj, is_new = Region.objects.update_or_create(
                code=code or f'auto_{name[:20]}',
                defaults={'name': name, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1
        except Exception as e:
            errors.append(f'{name}: {e}')

    return created, updated, errors


def import_district_vector(uploaded_file, region_id, name_field='NAME',
                           code_field='CODE', encoding='utf-8'):
    """
    Import district boundaries from uploaded file.
    Returns: (created, updated, errors_list)
    """
    region = Region.objects.get(pk=region_id)
    fname = uploaded_file.name.lower()

    if fname.endswith('.zip'):
        shp_path, tmp_dir = _extract_shp_from_zip(uploaded_file)
        try:
            return _import_district_shp(shp_path, region, name_field, code_field, encoding)
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
    elif fname.endswith(('.geojson', '.json')):
        path = _save_uploaded_file(uploaded_file)
        try:
            return _import_district_geojson(path, region, name_field, code_field, encoding)
        finally:
            os.unlink(path)
    else:
        return 0, 0, ['Неподдерживаемый формат. Используйте ZIP (с .shp) или GeoJSON.']


def _import_district_geojson(path, region, name_field, code_field, encoding):
    with open(path, 'r', encoding=encoding) as f:
        data = json.load(f)

    features = data.get('features', [])
    created = updated = 0
    errors = []

    for feat in features:
        props = feat.get('properties', {})
        name = str(props.get(name_field, '')).strip()
        code = str(props.get(code_field, '')).strip()
        if not name:
            errors.append(f'Пропущен объект без имени: {props}')
            continue
        try:
            geom = GEOSGeometry(json.dumps(feat['geometry']), srid=4326)
            geom = _to_multi(geom)
            obj, is_new = District.objects.update_or_create(
                region=region,
                code=code or f'auto_{name[:20]}',
                defaults={'name': name, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1
        except Exception as e:
            errors.append(f'{name}: {e}')

    return created, updated, errors


def _import_district_shp(path, region, name_field, code_field, encoding):
    from django.contrib.gis.gdal import DataSource
    ds = DataSource(path, encoding=encoding)
    layer = ds[0]
    created = updated = 0
    errors = []

    for feat in layer:
        name = str(feat.get(name_field)).strip()
        code = str(feat.get(code_field)).strip()
        if not name:
            continue
        try:
            geom = GEOSGeometry(feat.geom.wkt, srid=feat.geom.srid or 4326)
            if geom.srid != 4326:
                geom.transform(4326)
            geom = _to_multi(geom)
            obj, is_new = District.objects.update_or_create(
                region=region,
                code=code or f'auto_{name[:20]}',
                defaults={'name': name, 'geom': geom},
            )
            if is_new:
                created += 1
            else:
                updated += 1
        except Exception as e:
            errors.append(f'{name}: {e}')

    return created, updated, errors


def import_farmland_vector(uploaded_file, region_id,
                           crop_type_field='LAND_TYPE',
                           area_field='AREA_HA',
                           cadastral_field='CAD_NUM',
                           district_field='DISTRICT',
                           encoding='utf-8',
                           auto_create_districts=True,
                           clear_existing=False):
    """
    Import farmland boundaries from uploaded file.
    Returns: (created, skipped, errors_list)
    """
    region = Region.objects.get(pk=region_id)
    fname = uploaded_file.name.lower()

    if clear_existing:
        Farmland.objects.filter(district__region=region).delete()

    if fname.endswith('.zip'):
        shp_path, tmp_dir = _extract_shp_from_zip(uploaded_file)
        try:
            return _import_farmland_shp(
                shp_path, region, crop_type_field, area_field,
                cadastral_field, district_field, encoding,
                auto_create_districts,
            )
        finally:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
    elif fname.endswith(('.geojson', '.json')):
        path = _save_uploaded_file(uploaded_file)
        try:
            return _import_farmland_geojson(
                path, region, crop_type_field, area_field,
                cadastral_field, district_field, encoding,
                auto_create_districts,
            )
        finally:
            os.unlink(path)
    else:
        return 0, 0, ['Неподдерживаемый формат. Используйте ZIP (с .shp) или GeoJSON.']


def _import_farmland_geojson(path, region, crop_field, area_field,
                              cad_field, dist_field, encoding,
                              auto_create_districts):
    with open(path, 'r', encoding=encoding) as f:
        data = json.load(f)

    features = data.get('features', [])
    districts = {d.name.lower(): d for d in District.objects.filter(region=region)}
    fallback = list(districts.values())[0] if len(districts) == 1 else None

    batch = []
    created = skipped = 0
    errors = []

    for feat in features:
        props = feat.get('properties', {})
        dist_name = str(props.get(dist_field, '')).strip()
        district = _resolve_district(
            dist_name, districts, region, auto_create_districts, fallback,
        )
        if not district:
            skipped += 1
            continue

        crop_type = _resolve_crop_type(str(props.get(crop_field, '')))

        try:
            area = float(props.get(area_field, 0) or 0)
        except (TypeError, ValueError):
            area = 0

        cadastral = str(props.get(cad_field, '') or '').strip()

        try:
            geom = GEOSGeometry(json.dumps(feat['geometry']), srid=4326)
            geom = _to_multi(geom)
        except Exception as e:
            errors.append(str(e))
            skipped += 1
            continue

        if area <= 0:
            try:
                gp = geom.clone()
                gp.transform(32637)
                area = round(gp.area / 10000, 4)
            except Exception:
                area = 0

        batch.append(Farmland(
            district=district, crop_type=crop_type,
            cadastral_number=cadastral, area_ha=area, geom=geom,
            properties={k: str(v) for k, v in props.items()} if props else None,
        ))

        if len(batch) >= 500:
            Farmland.objects.bulk_create(batch)
            created += len(batch)
            batch = []

    if batch:
        Farmland.objects.bulk_create(batch)
        created += len(batch)

    return created, skipped, errors


def _import_farmland_shp(path, region, crop_field, area_field,
                          cad_field, dist_field, encoding,
                          auto_create_districts):
    from django.contrib.gis.gdal import DataSource
    ds = DataSource(path, encoding=encoding)
    layer = ds[0]

    districts = {d.name.lower(): d for d in District.objects.filter(region=region)}
    fallback = list(districts.values())[0] if len(districts) == 1 else None

    batch = []
    created = skipped = 0
    errors = []

    for feat in layer:
        dist_name = ''
        try:
            dist_name = str(feat.get(dist_field))
        except Exception:
            pass

        district = _resolve_district(
            dist_name, districts, region, auto_create_districts, fallback,
        )
        if not district:
            skipped += 1
            continue

        crop_raw = ''
        try:
            crop_raw = str(feat.get(crop_field))
        except Exception:
            pass
        crop_type = _resolve_crop_type(crop_raw)

        try:
            area = float(feat.get(area_field) or 0)
        except Exception:
            area = 0

        try:
            cadastral = str(feat.get(cad_field) or '').strip()
        except Exception:
            cadastral = ''

        try:
            geom = GEOSGeometry(feat.geom.wkt, srid=feat.geom.srid or 4326)
            if geom.srid != 4326:
                geom.transform(4326)
            geom = _to_multi(geom)
        except Exception as e:
            errors.append(str(e))
            skipped += 1
            continue

        if area <= 0:
            try:
                gp = geom.clone()
                gp.transform(32637)
                area = round(gp.area / 10000, 4)
            except Exception:
                area = 0

        extra_props = {}
        for field_name in layer.fields:
            if field_name not in (dist_field, crop_field, area_field, cad_field):
                try:
                    val = feat.get(field_name)
                    if val is not None:
                        extra_props[field_name] = str(val)
                except Exception:
                    pass

        batch.append(Farmland(
            district=district, crop_type=crop_type,
            cadastral_number=cadastral, area_ha=area, geom=geom,
            properties=extra_props if extra_props else None,
        ))

        if len(batch) >= 500:
            Farmland.objects.bulk_create(batch)
            created += len(batch)
            batch = []

    if batch:
        Farmland.objects.bulk_create(batch)
        created += len(batch)

    return created, skipped, errors


def _resolve_district(raw, districts, region, auto_create, fallback):
    if not raw:
        return fallback
    key = raw.strip().lower()
    if key in districts:
        return districts[key]
    for dname, d in districts.items():
        if dname.startswith(key) or key.startswith(dname):
            return d
    if auto_create and raw.strip():
        code = raw.strip().lower().replace(' ', '_')[:100]
        d, _ = District.objects.get_or_create(
            region=region, name=raw.strip(),
            defaults={'code': code, 'geom': region.geom},
        )
        districts[key] = d
        return d
    return fallback
