"""Геометрические утилиты для ``my_fields``.

Все функции принимают/возвращают GEOS-объекты (Django GIS), а не
GeoJSON-словарики — конверсия GeoJSON ↔ GEOS происходит в API-слое,
а сервисы работают со «честными» геометриями. Это упрощает юнит-тесты
и держит зависимость от формата в одном месте.
"""
from __future__ import annotations

from typing import Optional

from django.contrib.gis.db.models.functions import Area, Transform
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon

# Россия — мета-параметры для авто-резолва географии. Equal-area
# проекция «EPSG:3576 — North Pole LAEA Russia» используется для
# точного подсчёта площади (WGS84 даёт ошибку до 2% в высоких широтах).
EQUAL_AREA_SRID = 3576


def ensure_multipolygon(geom: GEOSGeometry) -> MultiPolygon:
    """Привести любую полигональную геометрию к ``MultiPolygon``.

    Если на вход пришёл одиночный ``Polygon`` — оборачиваем. Если уже
    ``MultiPolygon`` — возвращаем как есть. Любой другой тип
    (Point, LineString, GeometryCollection) — ``ValueError``.
    """
    if isinstance(geom, MultiPolygon):
        return geom
    if isinstance(geom, Polygon):
        return MultiPolygon([geom], srid=geom.srid or 4326)
    raise ValueError(
        f'Ожидался Polygon или MultiPolygon, получено: {geom.geom_type}'
    )


def compute_area_ha(geom: GEOSGeometry) -> float:
    """Площадь полигона в гектарах.

    Считаем через equal-area проекцию (EPSG:3576 для России). Для WGS84
    ``ST_Area`` возвращает в градусах², что бесполезно. Точность в
    средних широтах РФ — лучше 0.1%.
    """
    if geom is None or geom.empty:
        return 0.0
    # Клонируем, чтобы не модифицировать вход.
    g = geom.clone()
    if g.srid is None:
        g.srid = 4326
    g.transform(EQUAL_AREA_SRID)
    # Площадь возвращается в м² (единицы проекции).
    return round(g.area / 10_000.0, 4)


def resolve_region_district(geom: GEOSGeometry) -> tuple[Optional[int], Optional[int]]:
    """Найти Region/District по точке centroid'а геометрии.

    Возвращает (region_id, district_id) — оба могут быть None, если
    centroid не попал ни в один полигон справочника (озеро, спорная
    территория, ошибка в данных).

    Импорт моделей ``agrocosmos`` ленивый — чтобы избежать circular
    import при загрузке Django apps.
    """
    from agrocosmos.models import District, Region

    if geom is None or geom.empty:
        return None, None
    centroid = geom.centroid
    if centroid.srid is None:
        centroid.srid = 4326

    # District — приоритетнее: даёт точечное попадание.
    district = (
        District.objects
        .filter(geom__contains=centroid)
        .values('id', 'region_id')
        .first()
    )
    if district:
        return district['region_id'], district['id']

    # Fallback: только Region (например, если в районной геометрии есть
    # дыры — горные нац.парки и т.п.).
    region = (
        Region.objects
        .filter(geom__contains=centroid)
        .values('id')
        .first()
    )
    if region:
        return region['id'], None

    return None, None
