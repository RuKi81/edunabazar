"""HTML page views: dashboards and report pages."""
from datetime import date

from django.db.models import Count, Sum
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from ..models import Region, District, Farmland, VegetationIndex, SatelliteScene
from ._helpers import MODIS_SATELLITES, RASTER_SATELLITES


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


def dashboard(request: HttpRequest) -> HttpResponse:
    """Main Agrocosmos map page — MODIS NDVI monitoring."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')

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

    # Available years: from NDVI data + current year
    current_year = date.today().year
    data_years = (
        VegetationIndex.objects
        .filter(index_type='ndvi')
        .values_list('acquired_date__year', flat=True)
        .distinct()
        .order_by('-acquired_date__year')
    )
    years = sorted(set(list(data_years) + [current_year]), reverse=True)

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
        'active_page': 'modis',
    })


def raster_dashboard(request: HttpRequest) -> HttpResponse:
    """Detailed raster analysis page — Sentinel-2 / Landsat."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')

    districts = District.objects.none()
    if region_id:
        try:
            districts = District.objects.filter(region_id=int(region_id)).order_by('name')
        except (TypeError, ValueError):
            pass

    # Available years from raster scenes
    current_year = date.today().year
    data_years = (
        SatelliteScene.objects
        .filter(satellite__in=RASTER_SATELLITES)
        .values_list('acquired_date__year', flat=True)
        .distinct()
        .order_by('-acquired_date__year')
    )
    years = sorted(set(list(data_years) + [current_year]), reverse=True)

    return render(request, 'agrocosmos/raster_dashboard.html', {
        'legacy_user': _get_legacy_user(request),
        'regions': regions,
        'districts': districts,
        'region_id': region_id or '',
        'district_id': district_id or '',
        'years': years,
        'active_page': 'raster',
    })


def report_region(request: HttpRequest) -> HttpResponse:
    """Unified MODIS NDVI report page (region or district level)."""
    regions = Region.objects.all()
    region_id = request.GET.get('region')
    district_id = request.GET.get('district')
    year = request.GET.get('year')

    current_year = date.today().year
    data_years = (
        VegetationIndex.objects
        .filter(index_type='ndvi', scene__satellite__in=MODIS_SATELLITES)
        .values_list('acquired_date__year', flat=True)
        .distinct()
        .order_by('-acquired_date__year')
    )
    years = sorted(set(list(data_years) + [current_year]), reverse=True)

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
    data_years = (
        VegetationIndex.objects
        .filter(index_type='ndvi', scene__satellite__in=MODIS_SATELLITES)
        .values_list('acquired_date__year', flat=True)
        .distinct()
        .order_by('-acquired_date__year')
    )
    years = sorted(set(list(data_years) + [current_year]), reverse=True)

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
