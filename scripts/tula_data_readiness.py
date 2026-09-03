"""Read-only ревизия готовности данных по Тульской области (Фаза 0).

Ничего не пишет в БД — только считает и печатает сводку по:
  - угодьям (Farmland) региона,
  - покрытию NDVI (VegetationIndex) по источникам и годам,
  - предагрегированному ряду (DistrictNdviSeries),
  - фенологии (FarmlandPhenology),
  - эталонной урожайности (CropYieldStat).

Запуск (там, где работает manage.py):

    # локально
    Get-Content scripts/tula_data_readiness.py | python manage.py shell

    # на проде
    docker compose exec -T web python manage.py shell < scripts/tula_data_readiness.py

При необходимости поменяйте REGION_ID.
"""
from collections import defaultdict

from django.db.models import Count, Sum, Min, Max

from agrocosmos.models import (
    Region, District, Farmland, SatelliteScene, VegetationIndex,
    FarmlandPhenology, DistrictNdviSeries, CropYieldStat,
)

REGION_ID = 71  # Тульская область

RASTER_SATS = {
    SatelliteScene.Satellite.SENTINEL2,
    SatelliteScene.Satellite.LANDSAT8,
    SatelliteScene.Satellite.LANDSAT9,
}
MODIS_SATS = {
    SatelliteScene.Satellite.MODIS_TERRA,
    SatelliteScene.Satellite.MODIS_AQUA,
}


def hr(title):
    print('\n' + '=' * 64)
    print(title)
    print('=' * 64)


region = Region.objects.filter(pk=REGION_ID).first()
if region is None:
    print(f'!!! Region id={REGION_ID} не найден. Прерываю.')
else:
    print(f'Регион: {region.name} (id={region.id}, code={region.code})')

    # ── 1. Угодья ────────────────────────────────────────────────
    hr('1. Угодья (Farmland)')
    fl_qs = Farmland.objects.filter(region_id=REGION_ID)
    total = fl_qs.count()
    area = fl_qs.aggregate(a=Sum('area_ha'))['a'] or 0
    n_districts = District.objects.filter(region_id=REGION_ID).count()
    n_fl_with_district = fl_qs.filter(district__isnull=False).count()
    print(f'  Всего угодий:            {total:,}')
    print(f'  Суммарная площадь, га:   {area:,.0f}')
    print(f'  Районов в регионе:       {n_districts}')
    print(f'  Угодий с district FK:    {n_fl_with_district:,} '
          f'({(100 * n_fl_with_district / total if total else 0):.1f}%)')
    print('  По видам угодий:')
    for row in (fl_qs.values('crop_type')
                .annotate(n=Count('id'), a=Sum('area_ha')).order_by('-n')):
        print(f'    {row["crop_type"]:<12} {row["n"]:>8,}  {row["a"] or 0:>14,.0f} га')

    # ── 2. Покрытие NDVI (VegetationIndex) ───────────────────────
    hr('2. Покрытие NDVI (VegetationIndex, index_type=ndvi)')
    vi_qs = VegetationIndex.objects.filter(
        farmland__region_id=REGION_ID, index_type='ndvi',
    )
    vi_total = vi_qs.count()
    print(f'  Всего NDVI-строк по региону: {vi_total:,}')
    if vi_total:
        rng = vi_qs.aggregate(lo=Min('acquired_date'), hi=Max('acquired_date'))
        print(f'  Диапазон дат: {rng["lo"]} .. {rng["hi"]}')
        # group: (year, raster|modis) -> count
        buckets = defaultdict(int)
        for row in (vi_qs.values('scene__satellite')
                    .annotate(n=Count('id'),
                              lo=Min('acquired_date'), hi=Max('acquired_date'))
                    .order_by('scene__satellite')):
            sat = row['scene__satellite']
            src = ('raster' if sat in RASTER_SATS
                   else 'modis' if sat in MODIS_SATS else sat)
            print(f'    {sat:<14} src={src:<7} {row["n"]:>10,}  '
                  f'{row["lo"]} .. {row["hi"]}')
            buckets[src] += row['n']
        print('  Итого по источникам:')
        for src, n in sorted(buckets.items()):
            print(f'    {src:<8} {n:>12,}')
        # по годам (только raster — это то, что нужно направлениям 2-4)
        print('  RASTER по годам:')
        year_counts = defaultdict(int)
        for row in (vi_qs.filter(scene__satellite__in=RASTER_SATS)
                    .values('acquired_date__year')
                    .annotate(n=Count('id')).order_by('acquired_date__year')):
            year_counts[row['acquired_date__year']] = row['n']
        if year_counts:
            for y, n in sorted(year_counts.items()):
                print(f'    {y}: {n:>10,}')
        else:
            print('    (нет S2/L8 NDVI — пайплайн ещё не прогонялся)')

    # ── 3. Предагрегированный ряд (DistrictNdviSeries) ───────────
    hr('3. DistrictNdviSeries (предагрегат)')
    dns_qs = DistrictNdviSeries.objects.filter(district__region_id=REGION_ID)
    if dns_qs.exists():
        for row in (dns_qs.values('source')
                    .annotate(n=Count('id'),
                              lo=Min('acquired_date'), hi=Max('acquired_date'))
                    .order_by('source')):
            print(f'  {row["source"]:<8} {row["n"]:>8,} строк  '
                  f'{row["lo"]} .. {row["hi"]}')
    else:
        print('  (пусто — нужен recompute_district_ndvi_series --source raster --rebuild)')

    # ── 4. Фенология (FarmlandPhenology) ─────────────────────────
    hr('4. FarmlandPhenology')
    ph_qs = FarmlandPhenology.objects.filter(farmland__region_id=REGION_ID)
    if ph_qs.exists():
        for row in (ph_qs.values('source', 'year')
                    .annotate(n=Count('id')).order_by('source', 'year')):
            print(f'  {row["source"]:<8} {row["year"]}: {row["n"]:>8,}')
    else:
        print('  (пусто — фенология ещё не считалась)')

    # ── 5. Эталон урожайности (CropYieldStat) ────────────────────
    hr('5. CropYieldStat (эталон урожайности)')
    from django.db.models import Q
    cys_qs = CropYieldStat.objects.filter(
        Q(region_id=REGION_ID) | Q(district__region_id=REGION_ID)
    )
    if cys_qs.exists():
        n_region = cys_qs.filter(region_id=REGION_ID).count()
        n_district = cys_qs.filter(district__region_id=REGION_ID).count()
        print(f'  Записей всего: {cys_qs.count()}  '
              f'(region-scope={n_region}, district-scope={n_district})')
        print('  По культуре × источнику × scope:')
        for row in (cys_qs.values('crop', 'source')
                    .annotate(n=Count('id'),
                              lo=Min('year'), hi=Max('year'))
                    .order_by('crop', 'source')):
            print(f'    {row["crop"]:<14} {row["source"]:<14} '
                  f'{row["n"]:>3} лет  {row["lo"]}..{row["hi"]}')
    else:
        print('  (пусто — нужен import_emiss_yield / районный лоадер regional_msx)')

    hr('Готово. Read-only, БД не изменена.')
