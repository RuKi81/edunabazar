"""Quick diagnostic: did the raster pipeline write VI rows for Бахчисарайский / 2026?

Run on server:
    docker compose -f docker-compose.prod.yml exec -T web \
        python manage.py shell < scripts/diag_raster_ndvi.py
"""
from agrocosmos.models import VegetationIndex, SatelliteScene, Farmland
from django.db.models import Count, Min, Max

print('--- VegetationIndex by satellite, year=2026, Бахчисарайский ---')
qs = (VegetationIndex.objects
      .filter(acquired_date__year=2026,
              farmland__district__name__icontains='ахчисарай',
              index_type='ndvi')
      .values('scene__satellite')
      .annotate(n=Count('id'), dmin=Min('acquired_date'), dmax=Max('acquired_date'))
      .order_by('-n'))
for r in qs:
    print(r)

print()
print('--- farmland 10339750 ---')
print(Farmland.objects.filter(pk=10339750)
      .values('id', 'district__name', 'crop_type').first())
qs = (VegetationIndex.objects
      .filter(farmland_id=10339750, index_type='ndvi')
      .values('scene__satellite', 'acquired_date__year')
      .annotate(n=Count('id'))
      .order_by('-acquired_date__year', 'scene__satellite'))
for r in qs:
    print(r)

print()
print('--- All raster-tagged scenes for 2026 in this district ---')
qs = (SatelliteScene.objects
      .filter(acquired_date__year=2026,
              satellite__in=('sentinel2', 'landsat8', 'landsat9', 'hls_fused'))
      .values('satellite')
      .annotate(n=Count('id'), dmin=Min('acquired_date'), dmax=Max('acquired_date')))
for r in qs:
    print(r)
