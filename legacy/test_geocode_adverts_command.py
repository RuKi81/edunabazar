"""
Тесты команды геокодирования (``legacy/management/commands/
geocode_adverts.py``) — страховочная сетка перед рефакторингом
``handle`` (C=15).

Nominatim замокан (`_geocode`), throttle-sleep замокан. Покрывается:
обновление координат, --dry, --id, --limit, --force, скип при совпадении
(<1 км) с существующими координатами, учёт неудачных геокодирований.
"""
import io
from unittest import mock

from django.contrib.gis.geos import Point
from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from .constants import ADVERT_STATUS_PUBLISHED, USER_STATUS_ACTIVE
from .models import Advert, Catalog, Categories, LegacyUser

_MOD = 'legacy.management.commands.geocode_adverts'

# Москва ~ (37.6, 55.7); точка в ~0.5 км и точка в сотнях км
NEAR_MOSCOW = (55.7045, 37.6)      # ~0.5 км от (55.7, 37.6)
SPB = (59.93, 30.31)


def _make_advert(author, category, address, location, title='Объявление'):
    now = timezone.now()
    return Advert.objects.create(
        type=0, category=category, author=author, location=location,
        contacts='+79001234567', title=title, text='Описание', price=100,
        price_unit='кг', wholesale_price=0, min_volume=0, wholesale_volume=0,
        volume=10, priority=0, created_at=now, updated_at=now,
        status=ADVERT_STATUS_PUBLISHED, address=address,
    )


class GeocodeAdvertsTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        now = timezone.now()
        cls.user = LegacyUser.objects.create(
            type=0, username='seller', auth_key='', password_hash='',
            email='s@test.com', currency='RU', name='', address='',
            phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=catalog, title='Пшеница', active=1,
        )

    def _call(self, geocode_result=(55.7, 37.6), **options):
        """geocode_result: (lat, lon) | None | list результатов по очереди."""
        out = io.StringIO()
        if isinstance(geocode_result, list):
            geocode = mock.Mock(side_effect=geocode_result)
        else:
            geocode = mock.Mock(return_value=geocode_result)
        with mock.patch(f'{_MOD}._geocode', geocode), \
                mock.patch(f'{_MOD}.time.sleep'):
            call_command('geocode_adverts', stdout=out, **options)
        return out.getvalue(), geocode

    def test_updates_coords_for_zero_location(self):
        advert = _make_advert(
            self.user, self.category, 'Москва', Point(0, 0, srid=4326),
        )
        out, _ = self._call(geocode_result=(55.7, 37.6))
        advert.refresh_from_db()
        self.assertAlmostEqual(advert.location.y, 55.7, places=4)
        self.assertAlmostEqual(advert.location.x, 37.6, places=4)
        self.assertIn('Updated: 1, Skipped: 0, Failed: 0', out)

    def test_dry_does_not_save(self):
        advert = _make_advert(
            self.user, self.category, 'Москва', Point(0, 0, srid=4326),
        )
        out, _ = self._call(geocode_result=(55.7, 37.6), dry=True)
        advert.refresh_from_db()
        self.assertEqual(advert.location.x, 0)
        self.assertIn('Would update: 1', out)

    def test_failed_geocode_counted(self):
        advert = _make_advert(
            self.user, self.category, 'Неведомое место', Point(0, 0, srid=4326),
        )
        out, _ = self._call(geocode_result=None)
        advert.refresh_from_db()
        self.assertEqual(advert.location.x, 0)
        self.assertIn('FAIL geocode', out)
        self.assertIn('Failed: 1', out)

    def test_existing_coords_within_1km_skipped(self):
        advert = _make_advert(
            self.user, self.category, 'Москва', Point(37.6, 55.7, srid=4326),
        )
        out, _ = self._call(geocode_result=NEAR_MOSCOW)
        advert.refresh_from_db()
        self.assertAlmostEqual(advert.location.y, 55.7, places=4)
        self.assertIn('Skipped: 1', out)

    def test_existing_coords_within_1km_forced(self):
        advert = _make_advert(
            self.user, self.category, 'Москва', Point(37.6, 55.7, srid=4326),
        )
        out, _ = self._call(geocode_result=NEAR_MOSCOW, force=True)
        advert.refresh_from_db()
        self.assertAlmostEqual(advert.location.y, NEAR_MOSCOW[0], places=4)
        self.assertIn('Updated: 1', out)

    def test_far_coords_updated_with_distance_note(self):
        advert = _make_advert(
            self.user, self.category, 'Санкт-Петербург',
            Point(37.6, 55.7, srid=4326),
        )
        out, _ = self._call(geocode_result=SPB)
        advert.refresh_from_db()
        self.assertAlmostEqual(advert.location.y, SPB[0], places=4)
        self.assertIn('Δ', out)

    def test_id_filter(self):
        target = _make_advert(
            self.user, self.category, 'Москва', Point(0, 0, srid=4326),
        )
        other = _make_advert(
            self.user, self.category, 'Тверь', Point(0, 0, srid=4326),
        )
        _, geocode = self._call(geocode_result=(55.7, 37.6), id=target.pk)
        self.assertEqual(geocode.call_count, 1)
        other.refresh_from_db()
        self.assertEqual(other.location.x, 0)

    def test_limit(self):
        for i in range(3):
            _make_advert(
                self.user, self.category, f'Город {i}', Point(0, 0, srid=4326),
            )
        _, geocode = self._call(geocode_result=(55.7, 37.6), limit=2)
        self.assertEqual(geocode.call_count, 2)

    def test_adverts_without_address_not_selected(self):
        _make_advert(self.user, self.category, '', Point(0, 0, srid=4326))
        out, geocode = self._call()
        self.assertEqual(geocode.call_count, 0)
        self.assertIn('Found 0 adverts', out)
