"""
Management command to forward-geocode advert addresses and update coordinates.

Usage:
    python manage.py geocode_adverts              # update all with address
    python manage.py geocode_adverts --dry        # preview without saving
    python manage.py geocode_adverts --id=11324   # single advert
    python manage.py geocode_adverts --limit=100  # first N adverts
    python manage.py geocode_adverts --force       # overwrite even if coords exist
"""

import time
import urllib.parse
import urllib.request
import json

from django.contrib.gis.geos import Point
from django.core.management.base import BaseCommand

from legacy.models import Advert

NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search'
USER_AGENT = 'enb-legacy/1.0'
REQUEST_DELAY = 1.1  # Nominatim requires max 1 req/sec


def _geocode(address: str):
    """Forward-geocode an address via Nominatim. Returns (lat, lon) or None."""
    if not address or not address.strip():
        return None
    try:
        url = NOMINATIM_URL + '?' + urllib.parse.urlencode({
            'format': 'json',
            'q': address.strip(),
            'limit': 1,
            'countrycodes': 'ru',
        })
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode('utf-8') or '[]')
        if not data:
            return None
        row = data[0]
        lat = float(row.get('lat'))
        lon = float(row.get('lon'))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
        return None
    except Exception:
        return None


class Command(BaseCommand):
    help = 'Forward-geocode advert addresses and update location coordinates'

    def add_arguments(self, parser):
        parser.add_argument('--dry', action='store_true', help='Preview without saving')
        parser.add_argument('--id', type=int, default=0, help='Single advert ID')
        parser.add_argument('--limit', type=int, default=0, help='Max adverts to process (0 = all)')
        parser.add_argument('--force', action='store_true',
                            help='Update even if coordinates already exist')

    def handle(self, *args, **options):
        dry = options['dry']
        force = options['force']

        qs = self._advert_queryset(options['id'], options['limit'])
        self.stdout.write(f'Found {qs.count()} adverts with addresses')

        counters = {'updated': 0, 'skipped': 0, 'failed': 0}
        for advert in qs:
            outcome = self._process_advert(advert, dry, force)
            counters[outcome] += 1

        action = 'Would update' if dry else 'Updated'
        self.stdout.write(self.style.SUCCESS(
            f'\nDone. {action}: {counters["updated"]}, '
            f'Skipped: {counters["skipped"]}, Failed: {counters["failed"]}'
        ))

    @staticmethod
    def _advert_queryset(advert_id, limit):
        qs = Advert.objects.exclude(address__isnull=True).exclude(address='')
        if advert_id:
            qs = qs.filter(pk=advert_id)
        else:
            qs = qs.order_by('id')
        if limit > 0:
            qs = qs[:limit]
        return qs

    @staticmethod
    def _has_coords(advert) -> bool:
        try:
            loc = advert.location
            return bool(loc and loc.x != 0 and loc.y != 0)
        except Exception:
            return False

    @staticmethod
    def _distance_note(advert, new_lat, new_lon, force):
        """(строка ' (Δ N km)', skip) — сравнение с текущими координатами.

        skip=True, когда новая точка ближе 1 км к старой и нет --force.
        """
        try:
            old_lat = float(advert.location.y)
            old_lon = float(advert.location.x)
            # Rough distance in km (1 degree ≈ 111km)
            dlat = abs(new_lat - old_lat) * 111
            dlon = abs(new_lon - old_lon) * 111 * 0.6  # cos(55°) ≈ 0.57
            dist_km = (dlat ** 2 + dlon ** 2) ** 0.5
            return f' (Δ {dist_km:.1f} km)', (dist_km < 1.0 and not force)
        except Exception:
            return '', False

    def _process_advert(self, advert, dry, force) -> str:
        """Геокодировать одно объявление → 'updated' | 'skipped' | 'failed'."""
        address = (advert.address or '').strip()
        if not address:
            return 'skipped'

        result = _geocode(address)
        time.sleep(REQUEST_DELAY)

        if not result:
            self.stdout.write(self.style.WARNING(
                f'  FAIL geocode: [{advert.id}] {address[:60]}'
            ))
            return 'failed'

        new_lat, new_lon = result

        distance_note = ''
        if self._has_coords(advert):
            distance_note, skip = self._distance_note(advert, new_lat, new_lon, force)
            if skip:
                return 'skipped'

        self.stdout.write(
            f'  [{advert.id}] {address[:50]} → '
            f'{new_lat:.6f}, {new_lon:.6f}{distance_note}'
        )

        if not dry:
            advert.location = Point(new_lon, new_lat, srid=4326)
            advert.save(update_fields=['location'])

        return 'updated'
