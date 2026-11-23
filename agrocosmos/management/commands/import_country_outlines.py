import json
from pathlib import Path

from django.contrib.gis.geos import GEOSGeometry
from django.core.management.base import BaseCommand, CommandError

from agrocosmos.services.countries import get_country
from agrocosmos.services.osm_overpass import fetch_polygon_geojson


class Command(BaseCommand):
    help = 'Download OSM country outlines for Kazakhstan and Belarus.'

    def handle(self, *args, **options):
        features = []
        for code, osm_id in (('KZ', 214665), ('BY', 59065)):
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
        destination = (
            Path(__file__).resolve().parents[2]
            / 'static' / 'agrocosmos' / 'country_outlines.geojson'
        )
        destination.write_text(json.dumps({
            'type': 'FeatureCollection', 'features': features,
        }, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
        self.stdout.write(str(destination))
