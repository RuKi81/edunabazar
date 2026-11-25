import json
from pathlib import Path

from django.contrib.gis.geos import GEOSGeometry
from django.core.management.base import BaseCommand, CommandError

from agrocosmos.services.countries import get_country
from agrocosmos.services.osm_overpass import fetch_polygon_geojson

# Страны, чья внешняя граница рисуется поверх районов: код → OSM relation.
COUNTRY_RELATIONS = (
    ('KZ', 214665), ('BY', 59065),
    ('TR', 174737), ('IR', 304938), ('EG', 1473947),
)


class Command(BaseCommand):
    help = ('Download OSM outlines of the monitored foreign countries '
            'into static/agrocosmos/country_outlines.geojson.')

    def add_arguments(self, parser):
        parser.add_argument(
            '--refresh', action='store_true',
            help='Re-download every outline; by default the ones already '
                 'in the file are kept as is.',
        )

    def handle(self, *args, **options):
        destination = (
            Path(__file__).resolve().parents[2]
            / 'static' / 'agrocosmos' / 'country_outlines.geojson'
        )
        existing = {}
        if destination.exists() and not options['refresh']:
            payload = json.loads(destination.read_text(encoding='utf-8'))
            existing = {
                f['properties']['country']: f for f in payload['features']
            }
        features = []
        for code, osm_id in COUNTRY_RELATIONS:
            if code in existing:
                features.append(existing[code])
                self.stdout.write(f'{code}: kept')
                continue
            geometry = fetch_polygon_geojson(osm_id)
            if not geometry:
                raise CommandError(f'Cannot download {code} (OSM {osm_id})')
            geom = GEOSGeometry(json.dumps(geometry), srid=4326)
            if geom.empty or not geom.valid:
                raise CommandError(f'Invalid geometry for {code}')
            outline = geom.simplify(0.002, preserve_topology=True)
            features.append({
                'type': 'Feature',
                'properties': {
                    'country': code,
                    'name': get_country(code).name,
                    'osm_relation': osm_id,
                    'attribution': '© OpenStreetMap contributors, ODbL',
                },
                'geometry': json.loads(outline.geojson),
            })
            self.stdout.write(f'{code}: {outline.num_coords} points')
        destination.write_text(json.dumps({
            'type': 'FeatureCollection', 'features': features,
        }, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
        self.stdout.write(str(destination))
