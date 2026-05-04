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
    """Years for MODIS-only NDVI reports."""
    cache_key = 'agrocosmos:years:ndvi_modis'
    years = cache.get(cache_key)
    if years is None:
        agg = (VegetationIndex.objects
               .filter(index_type='ndvi',
                       scene__satellite__in=MODIS_SATELLITES)
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
    """Main Agrocosmos map page — MODIS NDVI monitoring."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    selected_years = _parse_selected_years(request.GET.get('year'))
    farmland_id = request.GET.get('farmland') or ''

    districts = District.objects.none()
    if region_id:
        try:
            districts = District.objects.filter(region_id=int(region_id)).order_by('name')
        except (TypeError, ValueError):
            pass

    # Summary stats
    farmland_qs = Farmland.objects.all()
    if district_id:
        try:
            farmland_qs = farmland_qs.filter(district_id=int(district_id))
        except (TypeError, ValueError):
            pass
    elif region_id:
        try:
            farmland_qs = farmland_qs.filter(district__region_id=int(region_id))
        except (TypeError, ValueError):
            pass

    summary = farmland_qs.aggregate(
        total_count=Count('id'),
        total_area=Sum('area_ha'),
    )

    crop_stats = (
        farmland_qs
        .values('crop_type')
        .annotate(cnt=Count('id'), area=Sum('area_ha'))
        .order_by('-area')
    )

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
        'crop_stats': list(crop_stats),
        'crop_type_labels': dict(Farmland.CropType.choices),
        'years': years,
        'selected_years': selected_years,
        'farmland_id': farmland_id,
        'active_page': 'modis',
    })


def raster_dashboard(request: HttpRequest) -> HttpResponse:
    """Detailed raster analysis page — Sentinel-2 / Landsat."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    selected_years = _parse_selected_years(request.GET.get('year'))
    farmland_id = request.GET.get('farmland') or ''

    districts = District.objects.none()
    if region_id:
        try:
            districts = District.objects.filter(region_id=int(region_id)).order_by('name')
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
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    year = request.GET.get('year')

    current_year = date.today().year
    years = _available_modis_ndvi_years(current_year)

    districts = District.objects.none()
    if region_id:
        try:
            districts = District.objects.filter(region_id=int(region_id)).order_by('name')
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


def report_district(request: HttpRequest) -> HttpResponse:
    """MODIS NDVI report page — district level."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    year = request.GET.get('year')

    current_year = date.today().year
    years = _available_modis_ndvi_years(current_year)

    districts = District.objects.none()
    if region_id:
        try:
            districts = District.objects.filter(region_id=int(region_id)).order_by('name')
        except (TypeError, ValueError):
            pass

    return render(request, 'agrocosmos/report_district.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'year': year or str(current_year),
        'years': years,
        'active_page': 'report_district',
    })
