"""HTML page views: dashboards and report pages."""
import time
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

# Global farmland summary (region=all): a COUNT/SUM over ~20M rows takes
# ~20 s. The data only changes when farmlands are (re)imported, so we cache
# it with no TTL and rotate it explicitly:
#   * ``prewarm_agro_caches`` on deploy (also covers a Redis flush),
#   * lazily on a cache miss, guarded by a dogpile lock so concurrent
#     visitors don't each run their own 20-second aggregate.
_FARMLAND_STATS_KEY = 'agrocosmos:farmland_stats:global'
_FARMLAND_STATS_LOCK = _FARMLAND_STATS_KEY + ':build_lock'
_FARMLAND_STATS_LOCK_TTL = 300   # covers the slowest observed build
_FARMLAND_STATS_POLL_MAX = 30    # seconds a follower waits for the winner


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


def _districts_for_region(region_id) -> object:
    """Districts of the region for the <select>; мусорный id → пустой qs."""
    if region_id:
        try:
            return (District.objects
                    .filter(region_id=int(region_id))
                    .only('id', 'name')
                    .order_by('name'))
        except (TypeError, ValueError):
            pass
    return District.objects.none()


def _to_int_or_none(raw) -> int | None:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _farmland_scope(region_id, district_id):
    """Resolve dashboard scope → (farmland_qs, cache_key | None).

    Important: any region_id that is not a real PK (empty, ``'all'``,
    garbage) collapses to the *global* scope and MUST hit the cache.
    The previous version left ``scope_key = None`` for ``region=all``,
    which made every default dashboard load re-run the 20-second
    aggregate against ~20M farmlands.
    """
    farmland_qs = Farmland.objects.all()
    scope_key: str | None = _FARMLAND_STATS_KEY
    d_id_int = _to_int_or_none(district_id) if district_id else None
    r_id_int = _to_int_or_none(region_id) if region_id else None
    if d_id_int is not None:
        farmland_qs = farmland_qs.filter(district_id=d_id_int)
        scope_key = None
    elif r_id_int is not None:
        farmland_qs = farmland_qs.filter(district__region_id=r_id_int)
        scope_key = None
    return farmland_qs, scope_key


def _compute_farmland_stats(farmland_qs):
    """Run the aggregate SQL. ~20 s for the unfiltered global scope."""
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
    return summary, crop_stats


def refresh_farmland_stats() -> dict:
    """Rebuild the global farmland summary and replace the cached copy.

    ``timeout=None`` — the data only changes on farmland (re)imports;
    rotated by ``prewarm_agro_caches`` on deploy and lazily on a miss.
    Returns the fresh payload so callers can log / inspect it.
    """
    summary, crop_stats = _compute_farmland_stats(Farmland.objects.all())
    payload = {'summary': summary, 'crop_stats': crop_stats}
    cache.set(_FARMLAND_STATS_KEY, payload, timeout=None)
    return payload


def _farmland_stats(farmland_qs, scope_key):
    """Summary stats. The unfiltered global aggregate scans ~20M farmlands
    and takes ~20s; it is cached eternally and rotated explicitly (see
    ``refresh_farmland_stats``). Filtered (region/district) aggregates use
    the district_id index and are sub-second, so we don't cache them.
    """
    if not scope_key:
        return _compute_farmland_stats(farmland_qs)

    cached = cache.get(scope_key)
    if cached is not None:
        return cached['summary'], cached['crop_stats']

    # Cache miss (Redis flush / first run). Dogpile guard: only one
    # worker runs the 20-second aggregate; followers poll the cache and
    # fall through to their own build only if the winner died.
    if not cache.add(_FARMLAND_STATS_LOCK, '1',
                     timeout=_FARMLAND_STATS_LOCK_TTL):
        for _ in range(_FARMLAND_STATS_POLL_MAX):
            time.sleep(1.0)
            cached = cache.get(scope_key)
            if cached is not None:
                return cached['summary'], cached['crop_stats']

    try:
        payload = refresh_farmland_stats()
    finally:
        cache.delete(_FARMLAND_STATS_LOCK)
    return payload['summary'], payload['crop_stats']


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

    districts = _districts_for_region(region_id)

    farmland_qs, scope_key = _farmland_scope(region_id, district_id)
    summary, crop_stats = _farmland_stats(farmland_qs, scope_key)

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

    districts = _districts_for_region(region_id)

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

    districts = _districts_for_region(region_id)

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


def report_farmland(request: HttpRequest) -> HttpResponse:
    """Per-farmland «field passport» report page (detailed monitoring).

    The farmland id normally arrives via the URL (links from the
    dashboards); the page itself only renders the shell — all data is
    fetched from ``/agrocosmos/api/report/farmland/``.
    """
    farmland_id = request.GET.get('farmland') or ''
    year = request.GET.get('year')

    current_year = date.today().year
    years = _available_ndvi_years(current_year)

    return render(request, 'agrocosmos/report_farmland.html', {
        'legacy_user': _get_legacy_user(request),
        'farmland_id': farmland_id,
        'year': year or str(current_year),
        'years': years,
        'active_page': 'report_farmland',
    })


def report_screening(request: HttpRequest) -> HttpResponse:
    """Problem-fields screening report page (district + year).

    «Рабочий стол инспектора»: рейтинг угодий района по неблагополучию
    (детальный мониторинг S2/L8/fused). Data comes from
    ``/agrocosmos/api/report/screening/``.
    """
    regions = Region.objects.only('id', 'name', 'code')
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    year = request.GET.get('year')

    current_year = date.today().year
    years = _available_raster_years(current_year)

    districts = _districts_for_region(region_id)

    return render(request, 'agrocosmos/report_screening.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'year': year or str(current_year),
        'years': years,
        'active_page': 'report_screening',
    })


def report_district_detailed(request: HttpRequest) -> HttpResponse:
    """District detailed-monitoring summary report page.

    Свод по району на данных Sentinel/Landsat: покрытие детальным
    мониторингом, распределение полей по категориям, динамика по
    культурам, сводка алертов. Data comes from
    ``/agrocosmos/api/report/district-detailed/``.
    """
    regions = Region.objects.only('id', 'name', 'code')
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    year = request.GET.get('year')

    current_year = date.today().year
    years = _available_raster_years(current_year)

    districts = _districts_for_region(region_id)

    return render(request, 'agrocosmos/report_district_detailed.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'year': year or str(current_year),
        'years': years,
        'active_page': 'report_district_detailed',
    })
