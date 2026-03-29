import io

from django.test import SimpleTestCase, TestCase, Client
from django.core.files.uploadedfile import SimpleUploadedFile
from unittest.mock import MagicMock
from PIL import Image as PILImage

from .templatetags.legacy_extras import format_price, truncate_ellipsis, pick_thumb_url
from .views import _is_admin_user, _normalize_phone, _parse_advert_form


class FormatPriceFilterTests(SimpleTestCase):
    def test_integer(self):
        self.assertEqual(format_price(1000), '1 000.00')

    def test_float(self):
        self.assertEqual(format_price(1234.5), '1 234.50')

    def test_zero(self):
        self.assertEqual(format_price(0), '0.00')

    def test_none_returns_none(self):
        self.assertIsNone(format_price(None))

    def test_invalid_string_returns_value(self):
        self.assertEqual(format_price('abc'), 'abc')


class TruncateEllipsisFilterTests(SimpleTestCase):
    def test_short_string_unchanged(self):
        self.assertEqual(truncate_ellipsis('hello', 10), 'hello')

    def test_long_string_truncated(self):
        result = truncate_ellipsis('a' * 20, 10)
        self.assertEqual(result, 'a' * 10 + '...')

    def test_none_returns_empty(self):
        self.assertEqual(truncate_ellipsis(None, 10), '')

    def test_zero_length_returns_empty(self):
        self.assertEqual(truncate_ellipsis('hello', 0), '')


class PickThumbUrlFilterTests(SimpleTestCase):
    def test_grain_returns_main1(self):
        self.assertIn('legacy/images/main1.jpg', pick_thumb_url('пшеница мягкая'))

    def test_sunflower_returns_main4(self):
        self.assertIn('legacy/images/main4.jpg', pick_thumb_url('подсолнечник'))

    def test_unknown_returns_no_photo(self):
        self.assertIn('legacy/images/no_photo_102_109.jpg', pick_thumb_url(''))

    def test_none_returns_no_photo(self):
        self.assertIn('legacy/images/no_photo_102_109.jpg', pick_thumb_url(None))


class IsAdminUserTests(SimpleTestCase):
    def test_none_user_is_not_admin(self):
        self.assertFalse(_is_admin_user(None))

    def test_superuser_is_admin(self):
        user = MagicMock()
        user.is_superuser = True
        user.username = 'regularuser'
        self.assertTrue(_is_admin_user(user))

    def test_admin_username_is_admin(self):
        user = MagicMock()
        user.is_superuser = False
        user.username = 'admin'
        self.assertTrue(_is_admin_user(user))

    def test_unknown_username_is_not_admin(self):
        user = MagicMock()
        user.is_superuser = False
        user.username = 'johndoe'
        self.assertFalse(_is_admin_user(user))


class NormalizePhoneTests(SimpleTestCase):
    def test_digits_only(self):
        self.assertEqual(_normalize_phone('79001234567'), '+79001234567')

    def test_strips_spaces_and_dashes(self):
        self.assertEqual(_normalize_phone('+7 (900) 123-45-67'), '+79001234567')

    def test_empty_returns_empty(self):
        self.assertEqual(_normalize_phone(''), '')

    def test_none_returns_empty(self):
        self.assertEqual(_normalize_phone(None), '')


class PhotoValidationTests(SimpleTestCase):
    """Tests for photo upload validation in _parse_advert_form."""

    _VALID_POST = {
        'type': '0', 'category': '1', 'title': 'Test', 'text': 'Desc',
        'contacts': '+79001234567', 'address': 'Москва', 'price': '100',
        'price_unit': 'кг', 'volume': '10', 'min_volume': '1',
        'wholesale_volume': '0', 'lat': '55.75', 'lon': '37.62',
    }

    def _make_image_file(self, name='test.jpg', fmt='JPEG', size=(100, 100), content_type='image/jpeg'):
        buf = io.BytesIO()
        PILImage.new('RGB', size, color='red').save(buf, format=fmt)
        buf.seek(0)
        return SimpleUploadedFile(name, buf.read(), content_type=content_type)

    def test_valid_jpeg_accepted(self):
        photo = self._make_image_file()
        files = MagicMock()
        files.getlist.return_value = [photo]
        _, errors, _ = _parse_advert_form(self._VALID_POST, files)
        self.assertNotIn('photos', errors)

    def test_invalid_content_type_rejected(self):
        f = SimpleUploadedFile('hack.exe', b'\x00\x01\x02', content_type='application/octet-stream')
        files = MagicMock()
        files.getlist.return_value = [f]
        _, errors, _ = _parse_advert_form(self._VALID_POST, files)
        self.assertIn('photos', errors)
        self.assertIn('недопустимый формат', errors['photos'])

    def test_oversized_file_rejected(self):
        photo = self._make_image_file()
        photo.size = 11 * 1024 * 1024  # 11 MB
        files = MagicMock()
        files.getlist.return_value = [photo]
        _, errors, _ = _parse_advert_form(self._VALID_POST, files)
        self.assertIn('photos', errors)
        self.assertIn('слишком большой', errors['photos'])

    def test_corrupted_image_rejected(self):
        f = SimpleUploadedFile('bad.jpg', b'not-an-image-at-all', content_type='image/jpeg')
        files = MagicMock()
        files.getlist.return_value = [f]
        _, errors, _ = _parse_advert_form(self._VALID_POST, files)
        self.assertIn('photos', errors)
        self.assertIn('повреждён', errors['photos'])

    def test_too_many_photos_rejected(self):
        photos = [self._make_image_file(name=f'img{i}.jpg') for i in range(11)]
        files = MagicMock()
        files.getlist.return_value = photos
        _, errors, _ = _parse_advert_form(self._VALID_POST, files)
        self.assertIn('photos', errors)


class BulkDeleteUsersTests(TestCase):
    """Verify that bulk-deleting a user also removes related FK rows."""

    def setUp(self):
        from django.utils import timezone
        from django.contrib.gis.geos import Point
        from .models import Advert, AdvertPhoto, LegacyUser, Catalog, Categories, Review, Seller, Message

        self.client = Client()
        now = timezone.now()

        self.admin = LegacyUser.objects.create(
            type=0, username='admin', auth_key='', password_hash='',
            email='admin@test.com', currency='RUB', name='Admin',
            address='', phone='', inn='', status=10,
            created_at=now, updated_at=now, contacts='',
        )
        self.victim = LegacyUser.objects.create(
            type=0, username='victim', auth_key='', password_hash='',
            email='victim@test.com', currency='RUB', name='Victim',
            address='', phone='', inn='', status=10,
            created_at=now, updated_at=now, contacts='',
        )
        catalog = Catalog.objects.create(title='Cat', sort=0, active=1)
        category = Categories.objects.create(catalog=catalog, title='Grains', active=1)
        self.advert = Advert.objects.create(
            type=0, category=category, author=self.victim,
            location=Point(37.6, 55.7, srid=4326), contacts='', title='Test',
            text='', price=0, wholesale_price=0, min_volume=0,
            wholesale_volume=0, volume=0, priority=0,
            created_at=now, updated_at=now, status=10,
        )
        AdvertPhoto.objects.create(advert=self.advert, image='test.jpg', sort=0)
        Review.objects.create(
            type=0, object_id=self.advert.id, points=5, author=self.victim,
            text='Great', created_at=now, updated_at=now, status=10,
        )
        Seller.objects.create(
            user=self.victim, name='VictimShop', logo=0,
            location='', contacts={}, price_list=0, links='', about='',
            created_at=now, updated_at=now, status=10,
        )
        Message.objects.create(
            sender=self.victim, recipient=self.admin,
            text='Hello', is_read=False, created_at=now,
        )

        # Make a dummy request to initialize the session
        self.client.get('/')
        session = self.client.session
        session['legacy_user_id'] = self.admin.pk
        session.save()
        # Ensure cookie is set on the client
        from django.conf import settings as _s
        self.client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key

    def test_bulk_delete_removes_user_and_related(self):
        from .models import Advert, AdvertPhoto, LegacyUser, Review, Seller, Message

        resp = self.client.post(
            '/legacy-admin/users/bulk-delete/',
            {'user_id': [str(self.victim.pk)]},
        )
        self.assertIn(resp.status_code, (302, 301))
        self.assertFalse(LegacyUser.objects.filter(pk=self.victim.pk).exists())
        self.assertFalse(Advert.objects.filter(pk=self.advert.pk).exists())
        self.assertFalse(AdvertPhoto.objects.filter(advert_id=self.advert.pk).exists())
        self.assertFalse(Review.objects.filter(author_id=self.victim.pk).exists())
        self.assertFalse(Seller.objects.filter(user_id=self.victim.pk).exists())
        self.assertFalse(Message.objects.filter(sender_id=self.victim.pk).exists())

    def test_bulk_delete_protects_admin(self):
        from .models import LegacyUser

        resp = self.client.post(
            '/legacy-admin/users/bulk-delete/',
            {'user_id': [str(self.admin.pk)]},
        )
        self.assertIn(resp.status_code, (302, 301))
        self.assertTrue(LegacyUser.objects.filter(pk=self.admin.pk).exists())


class SmokeTests(TestCase):
    """Smoke tests: public pages return 200 and contain expected content."""

    def setUp(self):
        self.client = Client()

    def test_home_page(self):
        resp = self.client.get('/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Еду на базар')

    def test_about_page(self):
        resp = self.client.get('/about/')
        self.assertEqual(resp.status_code, 200)

    def test_contacts_page(self):
        resp = self.client.get('/contacts/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Контакты')

    def test_howto_page(self):
        resp = self.client.get('/howto/')
        self.assertEqual(resp.status_code, 200)

    def test_adverts_list(self):
        resp = self.client.get('/adverts/')
        self.assertEqual(resp.status_code, 200)

    def test_sellers_list(self):
        resp = self.client.get('/sellers/')
        self.assertEqual(resp.status_code, 200)

    def test_map_page(self):
        resp = self.client.get('/map/')
        self.assertEqual(resp.status_code, 200)

    def test_login_page(self):
        resp = self.client.get('/login/')
        self.assertEqual(resp.status_code, 200)

    def test_register_page(self):
        resp = self.client.get('/register/')
        self.assertEqual(resp.status_code, 200)

    def test_robots_txt(self):
        resp = self.client.get('/robots.txt')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Sitemap')

    def test_sitemap_xml(self):
        resp = self.client.get('/sitemap.xml')
        self.assertEqual(resp.status_code, 200)
        self.assertIn('xml', resp['Content-Type'])

    def test_footer_contains_ok_link(self):
        resp = self.client.get('/')
        self.assertContains(resp, 'ok.ru/profile/586187375362')

    def test_redirect_cart(self):
        resp = self.client.get('/cart')
        self.assertEqual(resp.status_code, 301)
        self.assertEqual(resp['Location'], '/adverts/')

    def test_redirect_login_old(self):
        resp = self.client.get('/site/login')
        self.assertEqual(resp.status_code, 301)

    def test_unauthenticated_me_redirects(self):
        resp = self.client.get('/me/')
        self.assertEqual(resp.status_code, 302)

    def test_healthcheck(self):
        resp = self.client.get('/healthz')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data['app'], 'ok')
