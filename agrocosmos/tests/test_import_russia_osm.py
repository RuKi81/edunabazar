"""Тесты команд ``import_russia_regions`` / ``import_russia_districts`` —
страховочная сетка перед рефакторингом (C=13/11/11).

Все сетевые вызовы (Overpass, polygons.osm.fr) мокаются.
"""
import io
from unittest.mock import patch

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import TestCase

from agrocosmos.management.commands.import_russia_regions import (
    _coerce_multipolygon, _name_variants,
)
from agrocosmos.models import District, Region

REGIONS_MOD = 'agrocosmos.management.commands.import_russia_regions'
DISTRICTS_MOD = 'agrocosmos.management.commands.import_russia_districts'

POLY_GEOJSON = {
    'type': 'Polygon',
    'coordinates': [[
        [36.9, 54.9], [37.2, 54.9], [37.2, 55.2], [36.9, 55.2], [36.9, 54.9],
    ]],
}


def _mpoly():
    return MultiPolygon(Polygon((
        (36.9, 54.9), (37.2, 54.9), (37.2, 55.2), (36.9, 55.2), (36.9, 54.9),
    )))


def _rel(osm_id, name, **tags):
    return {'osm_id': osm_id, 'name': name, 'tags': tags}


class HelperTests(TestCase):
    def test_name_variants(self):
        self.assertIn('башкортостан',
                      list(_name_variants('Республика Башкортостан')))
        self.assertIn('татарстан',
                      list(_name_variants('Республика Татарстан (Татарстан)')))
        self.assertEqual(list(_name_variants(None)), [])

    def test_coerce_multipolygon_variants(self):
        self.assertEqual(
            _coerce_multipolygon(POLY_GEOJSON).geom_type, 'MultiPolygon')
        fc = {'type': 'FeatureCollection', 'features': [
            {'type': 'Feature', 'geometry': POLY_GEOJSON},
        ]}
        self.assertEqual(_coerce_multipolygon(fc).geom_type, 'MultiPolygon')
        gc = {'type': 'GeometryCollection',
              'geometries': [{'type': 'Point', 'coordinates': [0, 0]},
                             POLY_GEOJSON]}
        self.assertEqual(_coerce_multipolygon(gc).geom_type, 'MultiPolygon')
        with self.assertRaises(ValueError):
            _coerce_multipolygon({'type': 'FeatureCollection', 'features': []})
        with self.assertRaises(ValueError):
            _coerce_multipolygon({'type': 'Point', 'coordinates': [0, 0]})


class DisplayNameTests(TestCase):
    """Название объекта OSM для русского интерфейса."""

    def test_prefers_russian(self):
        from agrocosmos.services.osm_overpass import display_name
        self.assertEqual(
            display_name({'name': 'Kozaklı', 'name:ru': 'Козаклы'}), 'Козаклы')

    def test_readable_native_name_kept(self):
        from agrocosmos.services.osm_overpass import display_name
        self.assertEqual(
            display_name({'name': 'Kozaklı', 'name:en': 'Kozakli'}), 'Kozaklı')
        self.assertEqual(
            display_name({'name': 'Тульская область'}), 'Тульская область')

    def test_non_readable_script_falls_back_to_english(self):
        from agrocosmos.services.osm_overpass import display_name
        self.assertEqual(
            display_name({'name': 'شهرستان ایوان', 'name:en': 'Eyvan County'}),
            'Eyvan County')
        # Английского нет — остаётся местное, пустым название не делаем.
        self.assertEqual(display_name({'name': 'مديرية'}), 'مديرية')
        self.assertEqual(display_name({}), '')


class ImportRussiaRegionsTests(TestCase):
    def _run(self, *args, relations=None, polygon=POLY_GEOJSON):
        out, err = io.StringIO(), io.StringIO()
        with patch(f'{REGIONS_MOD}.fetch_country_admin_relations',
                   return_value=relations or []) as mock_rel, \
             patch(f'{REGIONS_MOD}.fetch_polygon_geojson',
                   return_value=polygon) as mock_poly, \
             patch(f'{REGIONS_MOD}.time.sleep'):
            call_command('import_russia_regions', *args,
                         stdout=out, stderr=err)
        return out.getvalue(), err.getvalue(), mock_rel, mock_poly

    def test_bulk_import_creates_regions(self):
        rels = [
            _rel(101, 'Регион А', **{'ISO3166-2': 'RU-AAA'}),
            _rel(102, 'Регион Б', ref='RB'),
            _rel(103, ''),  # без имени → failed
        ]
        out, err, _, _ = self._run(relations=rels)
        self.assertEqual(Region.objects.count(), 2)
        a = Region.objects.get(code='RU-AAA')
        self.assertEqual(a.name, 'Регион А')
        self.assertEqual(a.osm_id, 101)
        self.assertEqual(Region.objects.get(code='RB').name, 'Регион Б')
        self.assertIn('2 created, 0 updated, 0 skipped, 1 failed', out)

    def test_skip_existing(self):
        Region.objects.create(name='Регион А', code='RU-AAA', geom=_mpoly())
        rels = [_rel(101, 'Регион А', **{'ISO3166-2': 'RU-AAA'})]
        out, _, _, mock_poly = self._run('--skip-existing', relations=rels)
        mock_poly.assert_not_called()
        self.assertIn('1 skipped', out)

    def test_limit(self):
        rels = [_rel(100 + i, f'Регион {i}', ref=f'R{i}') for i in range(5)]
        self._run('--limit', '2', relations=rels)
        self.assertEqual(Region.objects.count(), 2)

    def test_no_geometry_counts_failed(self):
        rels = [_rel(101, 'Регион А', ref='RA')]
        out, err, _, _ = self._run(relations=rels, polygon=None)
        self.assertFalse(Region.objects.exists())
        self.assertIn('1 failed', out)

    def test_single_osm_id_mode(self):
        out, _, mock_rel, mock_poly = self._run(
            '--osm-id', '72639', '--code', 'RU-CR',
            '--name', 'Республика Крым',
        )
        mock_rel.assert_not_called()
        r = Region.objects.get(code='RU-CR')
        self.assertEqual(r.name, 'Республика Крым')
        self.assertEqual(r.osm_id, 72639)
        self.assertIn('created', out)

    def test_refresh_osm_ids(self):
        r = Region.objects.create(
            name='Республика Башкортостан', code='bash', geom=_mpoly())
        Region.objects.create(
            name='Регион А', code='ra', geom=_mpoly(), osm_id=500)
        Region.objects.create(name='Неведомый', code='x', geom=_mpoly())
        rels = [
            _rel(77, 'Башкортостан', **{'name:ru': 'Башкортостан'}),
            _rel(500, 'Регион А'),
        ]
        out, err, _, mock_poly = self._run('--refresh-osm-ids',
                                           relations=rels)
        mock_poly.assert_not_called()
        r.refresh_from_db()
        self.assertEqual(r.osm_id, 77)
        self.assertIn('1 set, 1 already correct, 1 unmatched', out)
        self.assertIn('Неведомый', err)


class ImportRussiaDistrictsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион А', code='ra', geom=_mpoly(), osm_id=101)
        cls.no_osm = Region.objects.create(
            name='Регион Б', code='rb', geom=_mpoly())

    def _run(self, *args, relations=None, polygon=POLY_GEOJSON):
        out, err = io.StringIO(), io.StringIO()
        with patch(f'{DISTRICTS_MOD}.fetch_admin_relations_in',
                   return_value=relations or []) as mock_rel, \
             patch(f'{DISTRICTS_MOD}.fetch_polygon_geojson',
                   return_value=polygon), \
             patch(f'{DISTRICTS_MOD}.time.sleep'):
            call_command('import_russia_districts', *args,
                         stdout=out, stderr=err)
        return out.getvalue(), err.getvalue(), mock_rel

    def test_imports_districts_for_regions_with_osm_id(self):
        rels = [_rel(201, 'Район 1', **{'ref:OKTMO': '12345'}),
                _rel(202, 'Район 2')]
        out, _, mock_rel = self._run(relations=rels)
        # только регион с osm_id
        mock_rel.assert_called_once_with(101, 6)
        self.assertEqual(District.objects.count(), 2)
        d = District.objects.get(osm_id=201)
        self.assertEqual(d.code, '12345')
        self.assertEqual(d.region_id, self.region.pk)
        self.assertIn('2 created', out)

    def test_region_code_filter_and_unknown(self):
        out, _, mock_rel = self._run(
            '--region-code', 'ra', relations=[_rel(201, 'Район 1')])
        mock_rel.assert_called_once()
        out, err, mock_rel = self._run('--region-code', 'nope')
        self.assertIn('not found', err)
        mock_rel.assert_not_called()

    def test_parent_osm_id_requires_single_region(self):
        Region.objects.filter(pk=self.no_osm.pk).update(osm_id=102)
        out, err, mock_rel = self._run('--parent-osm-id', '999')
        self.assertIn('--parent-osm-id can only be used', err)
        mock_rel.assert_not_called()

    def test_skip_existing_district(self):
        District.objects.create(
            region=self.region, name='Район 1', code='d1',
            geom=_mpoly(), osm_id=201)
        rels = [_rel(201, 'Район 1')]
        out, _, _ = self._run('--skip-existing', relations=rels)
        self.assertEqual(District.objects.count(), 1)
        self.assertIn('1 skipped', out)

    def test_no_regions_with_osm_id(self):
        Region.objects.all().update(osm_id=None)
        out, err, mock_rel = self._run()
        self.assertIn('No Region rows with osm_id', err)
        mock_rel.assert_not_called()


class ImportOtherCountryTests(TestCase):
    """``--country``: субъекты и районы других стран реестра."""

    def test_regions_of_another_country(self):
        rels = [_rel(215743, 'Акмолинская область', **{'ISO3166-2': 'KZ-11'})]
        out = io.StringIO()
        with patch(f'{REGIONS_MOD}.fetch_country_admin_relations',
                   return_value=rels) as mock_rel, \
             patch(f'{REGIONS_MOD}.fetch_polygon_geojson',
                   return_value=POLY_GEOJSON), \
             patch(f'{REGIONS_MOD}.time.sleep'):
            call_command('import_russia_regions', '--country', 'kz', stdout=out)
        mock_rel.assert_called_once_with('KZ', 4)
        region = Region.objects.get(code='KZ-11')
        self.assertEqual(region.country, 'KZ')
        self.assertEqual(region.osm_id, 215743)

    def test_default_country_is_russia(self):
        rels = [_rel(101, 'Регион А', **{'ISO3166-2': 'RU-AAA'})]
        with patch(f'{REGIONS_MOD}.fetch_country_admin_relations',
                   return_value=rels) as mock_rel, \
             patch(f'{REGIONS_MOD}.fetch_polygon_geojson',
                   return_value=POLY_GEOJSON), \
             patch(f'{REGIONS_MOD}.time.sleep'):
            call_command('import_russia_regions', stdout=io.StringIO())
        mock_rel.assert_called_once_with('RU', 4)
        self.assertEqual(Region.objects.get(code='RU-AAA').country, 'RU')

    def test_unknown_country_rejected(self):
        from django.core.management.base import CommandError
        with self.assertRaises(CommandError):
            call_command('import_russia_regions', '--country', 'XX',
                         stdout=io.StringIO())

    def _run_districts(self, *args, relations=None, polygon=POLY_GEOJSON):
        out, err = io.StringIO(), io.StringIO()
        with patch(f'{DISTRICTS_MOD}.fetch_admin_relations_in',
                   return_value=relations or []) as mock_rel, \
             patch(f'{DISTRICTS_MOD}.fetch_polygon_geojson',
                   return_value=polygon), \
             patch(f'{DISTRICTS_MOD}.time.sleep'):
            call_command('import_russia_districts', *args,
                         stdout=out, stderr=err)
        return out.getvalue(), err.getvalue(), mock_rel

    def test_districts_scoped_to_country(self):
        Region.objects.create(
            name='Регион РФ', code='ru', geom=_mpoly(), osm_id=101)
        kz = Region.objects.create(
            name='Область', code='KZ-11', country='KZ', geom=_mpoly(),
            osm_id=215743)
        # По умолчанию — только Россия …
        _, _, mock_rel = self._run_districts(relations=[_rel(201, 'Район РФ')])
        mock_rel.assert_called_once_with(101, 6)
        # … а --country берёт регионы и уровень районов этой страны.
        _, _, mock_rel = self._run_districts(
            '--country', 'KZ', relations=[_rel(301, 'Район КЗ')])
        mock_rel.assert_called_once_with(215743, 6)
        self.assertEqual(District.objects.get(osm_id=301).region_id, kz.pk)

    def test_country_without_district_level(self):
        Region.objects.create(
            name='Штат', code='IN-MH', country='IN', geom=_mpoly(),
            osm_id=1950884)
        _, err, mock_rel = self._run_districts('--country', 'IN')
        mock_rel.assert_not_called()
        self.assertIn('no district level configured for IN', err)
        # Явный уровень снимает ограничение.
        _, _, mock_rel = self._run_districts(
            '--country', 'IN', '--admin-level', '5')
        mock_rel.assert_called_once_with(1950884, 5)

    def test_border_neighbour_is_not_reparented(self):
        """Район соседнего региона, пришедший из Overpass по общей
        границе, не импортируется и не переезжает в чужой регион."""
        home = Region.objects.create(
            name='Сосед', code='home', geom=_mpoly(), osm_id=900)
        far = MultiPolygon(Polygon((
            (60, 60), (61, 60), (61, 61), (60, 61), (60, 60),
        )))
        city = Region.objects.create(
            name='Город', code='city', country='BY', geom=far, osm_id=59195)
        neighbour = District.objects.create(
            region=home, name='Минский район', code='d', geom=_mpoly(),
            osm_id=59190)

        out, _, _ = self._run_districts(
            '--region-code', 'city', relations=[_rel(59190, 'Минский район')])
        neighbour.refresh_from_db()
        self.assertEqual(neighbour.region_id, home.pk)
        self.assertFalse(District.objects.filter(region=city).exists())
        self.assertIn('outside Город', out)
        self.assertIn('1 skipped', out)

    def test_region_fallback_district(self):
        city = Region.objects.create(
            name='Астана', code='KZ-71', country='KZ', geom=_mpoly(),
            osm_id=3087155)
        out, _, _ = self._run_districts('--country', 'KZ', '--region-fallback')
        d = District.objects.get(region=city)
        self.assertEqual((d.name, d.code, d.osm_id), ('Астана', 'region:KZ-71', None))
        self.assertTrue(d.geom.equals(city.geom))
        self.assertIn('1 created', out)
        # Повторный запуск второй район не создаёт.
        self._run_districts('--country', 'KZ', '--region-fallback')
        self.assertEqual(District.objects.filter(region=city).count(), 1)

    def test_region_as_district_country_skips_overpass(self):
        """Египет: границ районов в OSM нет — район равен мухафазе."""
        aswan = Region.objects.create(
            name='Асуан', code='EG-ASN', country='EG', geom=_mpoly(),
            osm_id=3061757)
        out, _, mock_rel = self._run_districts('--country', 'EG')
        mock_rel.assert_not_called()
        d = District.objects.get(region=aswan)
        self.assertEqual((d.name, d.code), ('Асуан', 'region:EG-ASN'))
        self.assertTrue(d.geom.equals(aswan.geom))
        self.assertIn('1 created', out)
        # Идемпотентно.
        self._run_districts('--country', 'EG')
        self.assertEqual(District.objects.filter(region=aswan).count(), 1)

    def test_districts_level_of_iran_and_turkey(self):
        Region.objects.create(
            name='Илам', code='IR-16', country='IR', geom=_mpoly(),
            osm_id=229766)
        Region.objects.create(
            name='Невшехир', code='TR-50', country='TR', geom=_mpoly(),
            osm_id=154449)
        _, _, mock_rel = self._run_districts('--country', 'IR')
        mock_rel.assert_called_once_with(229766, 5)   # шахрестаны
        _, _, mock_rel = self._run_districts('--country', 'TR')
        mock_rel.assert_called_once_with(154449, 6)   # ильче

    def test_no_fallback_when_region_has_districts(self):
        Region.objects.create(
            name='Область', code='KZ-11', country='KZ', geom=_mpoly(),
            osm_id=215743)
        self._run_districts('--country', 'KZ', '--region-fallback',
                            relations=[_rel(301, 'Район')])
        self.assertEqual(
            list(District.objects.values_list('name', flat=True)), ['Район'])
