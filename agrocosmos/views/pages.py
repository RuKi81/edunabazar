"""HTML page views: dashboards and report pages."""
from datetime import date

from django.core.cache import cache
from django.db.models import Count, Min, Max, Sum
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from ..models import Region, District, Farmland, VegetationIndex, SatelliteScene
from ._helpers import MODIS_SATELLITES, RASTER_SATELLITES


# Cache TTL for "available years" lookups. Previously these were executed on
# every page load as ``SELECT DISTINCT EXTRACT(YEAR FROM acquired_date)`` on
# agro_vegetation_index (1+ billion rows), which caused 60-80 s dashboards
# and gunicorn worker starvation. We now do a cheap MIN/MAX on the indexed
# ``acquired_date`` column and build the year range in Python, plus cache
# the result in Redis.
_YEARS_CACHE_TTL = 3600  # seconds


def _years_range(first_year: int | None, last_year: int | None,
                 current_year: int) -> list[int]:
    """Build descending list of years covering data range + current year."""
    if not first_year or not last_year:
        return [current_year]
    lo = min(first_year, current_year)
    hi = max(last_year, current_year)
    return list(range(hi, lo - 1, -1))


def _available_ndvi_years(current_year: int) -> list[int]:
    """Years for the main NDVI dashboard (all satellites)."""
    cache_key = 'agrocosmos:years:ndvi_all'
    years = cache.get(cache_key)
    if years is None:
        agg = (VegetationIndex.objects
               .filter(index_type='ndvi')
               .aggregate(first=Min('acquired_date'),
                          last=Max('acquired_date')))
        first = agg['first'].year if agg['first'] else None
        last = agg['last'].year if agg['last'] else None
        years = _years_range(first, last, current_year)
        cache.set(cache_key, years, _YEARS_CACHE_TTL)
    else:
        # Ensure current year is always present (e.g. first day of Jan before
        # the next NDVI composite lands).
        if current_year not in years:
            years = sorted(set(years) | {current_year}, reverse=True)
    return years


def _available_modis_ndvi_years(current_year: int) -> list[int]:
    """Years for MODIS-only NDVI reports.

    Uses ``SatelliteScene`` (small table) instead of ``VegetationIndex``
    (1B+ rows). The MIN/MAX over a JOIN to filter by ``satellite`` cannot
    use the ``acquired_date`` index and degenerates into a full hash join
    that takes 4+ minutes on production data.
    """
    cache_key = 'agrocosmos:years:ndvi_modis'
    years = cache.get(cache_key)
    if years is None:
        agg = (SatelliteScene.objects
               .filter(satellite__in=MODIS_SATELLITES)
               .aggregate(first=Min('acquired_date'),
                          last=Max('acquired_date')))
        first = agg['first'].year if agg['first'] else None
        last = agg['last'].year if agg['last'] else None
        years = _years_range(first, last, current_year)
        cache.set(cache_key, years, _YEARS_CACHE_TTL)
    else:
        if current_year not in years:
            years = sorted(set(years) | {current_year}, reverse=True)
    return years


def _available_raster_years(current_year: int) -> list[int]:
    """Years for the raster (Sentinel-2 / Landsat) dashboard."""
    cache_key = 'agrocosmos:years:raster'
    years = cache.get(cache_key)
    if years is None:
        agg = (SatelliteScene.objects
               .filter(satellite__in=RASTER_SATELLITES)
               .aggregate(first=Min('acquired_date'),
                          last=Max('acquired_date')))
        first = agg['first'].year if agg['first'] else None
        last = agg['last'].year if agg['last'] else None
        years = _years_range(first, last, current_year)
        cache.set(cache_key, years, _YEARS_CACHE_TTL)
    else:
        if current_year not in years:
            years = sorted(set(years) | {current_year}, reverse=True)
    return years


def _get_legacy_user(request):
    """Reuse legacy session auth."""
    from legacy.models import LegacyUser
    uid = request.session.get('legacy_user_id')
    if not uid:
        return None
    try:
        return LegacyUser.objects.get(pk=int(uid))
    except LegacyUser.DoesNotExist:
        return None


def _parse_selected_years(raw):
    """``?year=2024,2025`` → ``{2024, 2025}``. Robust against garbage."""
    out = set()
    if not raw:
        return out
    for part in str(raw).split(','):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


def dashboard(request: HttpRequest) -> HttpResponse:
    """Main Agrocosmos map page — MODIS NDVI monitoring.

    A bare ``/agrocosmos/`` URL with no ``region`` parameter defaults to
    the all-Russia choropleth so first-time visitors land on the
    operational overview rather than an empty map. The sentinel value
    ``'all'`` is the same one the region <select> uses, and the
    front-end JS auto-fires its ``change`` handler to load the layer.
    """
    # Only ``id``/``name``/``code`` are used by the <select>; skip the heavy
    # ``geom`` MultiPolygon to avoid tens of MB of GeoDjango deserialization
    # on every dashboard render.
    regions = Region.objects.only('id', 'name', 'code')
    # ``region`` may legitimately be empty when the user explicitly
    # selects "— Регион —"; only fall back to 'all' when the parameter
    # is missing entirely from the query string.
    if 'region' in request.GET:
        region_id = request.GET.get('region')
    else:
        region_id = 'all'
    district_id = request.GET.get('district')
    selected_years = _parse_selected_years(request.GET.get('year'))
    farmland_id = request.GET.get('farmland') or ''

    districts = District.objects.none()
    if region_id:
        try:
            districts = (District.objects
                         .filter(region_id=int(region_id))
                         .only('id', 'name')
                         .order_by('name'))
        except (TypeError, ValueError):
            pass

    # Summary stats. The unfiltered global aggregate scans ~20M farmlands
    # and takes ~20s; cache it. Filtered (region/district) aggregates use
    # the district_id index and are sub-second, so we don't cache them.
    farmland_qs = Farmland.objects.all()
    scope_key: str | None = None  # ``None`` means "do not cache"
    if district_id:
        try:
            d_id = int(district_id)
            farmland_qs = farmland_qs.filter(district_id=d_id)
        except (TypeError, ValueError):
            pass
    elif region_id:
        try:
            r_id = int(region_id)
            farmland_qs = farmland_qs.filter(district__region_id=r_id)
        except (TypeError, ValueError):
            pass
    else:
        scope_key = 'agrocosmos:farmland_stats:global'

    if scope_key:
        cached = cache.get(scope_key)
    else:
        cached = None
    if cached is not None:
        summary = cached['summary']
        crop_stats = cached['crop_stats']
    else:
        summary = farmland_qs.aggregate(
            total_count=Count('id'),
            total_area=Sum('area_ha'),
        )
        crop_stats = list(
            farmland_qs
            .values('crop_type')
            .annotate(cnt=Count('id'), area=Sum('area_ha'))
            .order_by('-area')
        )
        if scope_key:
            cache.set(scope_key, {
                'summary': summary,
                'crop_stats': crop_stats,
            }, _YEARS_CACHE_TTL)

    # Available years: cheap MIN/MAX + Redis cache (previously DISTINCT
    # EXTRACT(YEAR) over 1B+ rows → 60-80s full scan per request).
    current_year = date.today().year
    years = _available_ndvi_years(current_year)

    return render(request, 'agrocosmos/dashboard.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'summary': summary,
        'crop_stats': crop_stats,
        'crop_type_labels': dict(Farmland.CropType.choices),
        'years': years,
        'selected_years': selected_years,
        'farmland_id': farmland_id,
        'active_page': 'modis',
    })


def raster_dashboard(request: HttpRequest) -> HttpResponse:
    """Detailed raster analysis page — Sentinel-2 / Landsat."""
    regions = Region.objects.only('id', 'name', 'code')
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    selected_years = _parse_selected_years(request.GET.get('year'))
    farmland_id = request.GET.get('farmland') or ''

    districts = District.objects.none()
    if region_id:
        try:
            districts = (District.objects
                         .filter(region_id=int(region_id))
                         .only('id', 'name')
                         .order_by('name'))
        except (TypeError, ValueError):
            pass

    # Available years from raster scenes (cached helper)
    current_year = date.today().year
    years = _available_raster_years(current_year)

    return render(request, 'agrocosmos/raster_dashboard.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'years': years,
        'selected_years': selected_years,
        'farmland_id': farmland_id,
        'active_page': 'raster',
    })


def report_region(request: HttpRequest) -> HttpResponse:
    """Unified MODIS NDVI report page (region or district level)."""
    regions = Region.objects.only('id', 'name', 'code')
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    year = request.GET.get('year')

    current_year = date.today().year
    years = _available_modis_ndvi_years(current_year)

    districts = District.objects.none()
    if region_id:
        try:
            districts = (District.objects
                         .filter(region_id=int(region_id))
                         .only('id', 'name')
                         .order_by('name'))
        except (TypeError, ValueError):
            pass

    return render(request, 'agrocosmos/report_region.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'year': year or str(current_year),
        'years': years,
        'active_page': 'report_region',
    })


