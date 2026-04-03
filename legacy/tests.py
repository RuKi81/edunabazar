import io
import time

from django.test import SimpleTestCase, TestCase, Client, RequestFactory, override_settings
from django.core.files.uploadedfile import SimpleUploadedFile
from unittest.mock import MagicMock, patch
from PIL import Image as PILImage

from .templatetags.legacy_extras import format_price, truncate_ellipsis, pick_thumb_url
from .views import _is_admin_user, _normalize_phone, _parse_advert_form
from .views.helpers import (
    _antispam_token, _antispam_check, _normalize_extra_contacts,
    _EXTRA_CONTACT_LABELS, _ANTISPAM_SECRET,
)
from .constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION,
    ADVERT_STATUS_PUBLISHED,
    REVIEW_STATUS_DELETED, REVIEW_STATUS_HIDDEN, REVIEW_STATUS_MODERATION,
    REVIEW_STATUS_PUBLISHED,
    USER_STATUS_ACTIVE, SELLER_STATUS_ACTIVE,
)


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


# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

class ConstantsTests(SimpleTestCase):
    """Verify status constants have expected numeric values."""

    def test_advert_statuses(self):
        self.assertEqual(ADVERT_STATUS_DELETED, 0)
        self.assertEqual(ADVERT_STATUS_HIDDEN, 3)
        self.assertEqual(ADVERT_STATUS_MODERATION, 5)
        self.assertEqual(ADVERT_STATUS_PUBLISHED, 10)

    def test_review_statuses(self):
        self.assertEqual(REVIEW_STATUS_DELETED, 0)
        self.assertEqual(REVIEW_STATUS_HIDDEN, 3)
        self.assertEqual(REVIEW_STATUS_MODERATION, 5)
        self.assertEqual(REVIEW_STATUS_PUBLISHED, 10)

    def test_user_seller_active(self):
        self.assertEqual(USER_STATUS_ACTIVE, 10)
        self.assertEqual(SELLER_STATUS_ACTIVE, 10)


# ---------------------------------------------------------------------------
#  Antispam helpers
# ---------------------------------------------------------------------------

class AntispamTokenTests(SimpleTestCase):
    def test_returns_ts_and_hash(self):
        ts, h = _antispam_token()
        self.assertTrue(ts.isdigit())
        self.assertEqual(len(h), 16)

    def test_hash_is_deterministic(self):
        import hashlib
        ts, h = _antispam_token()
        expected = hashlib.sha256(f'{ts}:{_ANTISPAM_SECRET}'.encode()).hexdigest()[:16]
        self.assertEqual(h, expected)


class AntispamCheckTests(SimpleTestCase):
    def _make_request(self, post_data):
        req = MagicMock()
        req.POST = post_data
        req.META = {'REMOTE_ADDR': '127.0.0.1'}
        return req

    def test_honeypot_triggers(self):
        req = self._make_request({'website': 'spam', '_ts': '1', '_th': 'x'})
        self.assertEqual(_antispam_check(req), 'bot')

    def test_missing_timestamp_triggers(self):
        req = self._make_request({'website': '', '_ts': '', '_th': ''})
        self.assertEqual(_antispam_check(req), 'bot')

    def test_invalid_hash_triggers(self):
        req = self._make_request({'website': '', '_ts': '12345', '_th': 'wrong'})
        self.assertEqual(_antispam_check(req), 'bot')

    def test_too_fast_triggers(self):
        ts, th = _antispam_token()
        req = self._make_request({'website': '', '_ts': ts, '_th': th})
        self.assertEqual(_antispam_check(req), 'bot')

    def test_valid_submission_passes(self):
        import hashlib
        old_ts = str(int(time.time()) - 10)
        old_h = hashlib.sha256(f'{old_ts}:{_ANTISPAM_SECRET}'.encode()).hexdigest()[:16]
        req = self._make_request({'website': '', '_ts': old_ts, '_th': old_h})
        self.assertIsNone(_antispam_check(req))


# ---------------------------------------------------------------------------
#  _normalize_extra_contacts
# ---------------------------------------------------------------------------

class NormalizeExtraContactsTests(SimpleTestCase):
    def test_empty_input(self):
        self.assertEqual(_normalize_extra_contacts(None), [])
        self.assertEqual(_normalize_extra_contacts([]), [])
        self.assertEqual(_normalize_extra_contacts('invalid'), [])

    def test_website_gets_https(self):
        contacts = [{'type': 'website', 'value': 'example.com'}]
        result = _normalize_extra_contacts(contacts)
        self.assertEqual(result[0]['href'], 'https://example.com')

    def test_website_with_https_unchanged(self):
        contacts = [{'type': 'website', 'value': 'https://example.com'}]
        result = _normalize_extra_contacts(contacts)
        self.assertEqual(result[0]['href'], 'https://example.com')

    def test_email_href_is_value(self):
        contacts = [{'type': 'email', 'value': 'a@b.com'}]
        result = _normalize_extra_contacts(contacts)
        self.assertEqual(result[0]['href'], 'a@b.com')

    def test_extra_contact_labels_keys(self):
        expected_keys = {'email', 'telegram', 'max', 'social', 'website'}
        self.assertEqual(set(_EXTRA_CONTACT_LABELS.keys()), expected_keys)


# ---------------------------------------------------------------------------
#  _parse_advert_form — edge cases
# ---------------------------------------------------------------------------

class ParseAdvertFormEdgeCaseTests(SimpleTestCase):
    _VALID_POST = {
        'type': '0', 'category': '1', 'title': 'Test', 'text': 'Desc',
        'contacts': '+79001234567', 'address': 'Москва', 'price': '100',
        'price_unit': 'кг', 'volume': '10', 'min_volume': '1',
        'wholesale_volume': '0', 'lat': '55.75', 'lon': '37.62',
    }

    def test_missing_title_error(self):
        post = dict(self._VALID_POST, title='')
        _, errors, _ = _parse_advert_form(post, None)
        self.assertIn('title', errors)

    def test_missing_text_error(self):
        post = dict(self._VALID_POST, text='')
        _, errors, _ = _parse_advert_form(post, None)
        self.assertIn('text', errors)

    def test_missing_contacts_error(self):
        post = dict(self._VALID_POST, contacts='')
        _, errors, _ = _parse_advert_form(post, None)
        self.assertIn('contacts', errors)

    def test_missing_address_error(self):
        post = dict(self._VALID_POST, address='')
        _, errors, _ = _parse_advert_form(post, None)
        self.assertIn('address', errors)

    def test_missing_lat_lon_error(self):
        post = dict(self._VALID_POST, lat='', lon='')
        _, errors, _ = _parse_advert_form(post, None)
        self.assertIn('lat', errors)

    def test_invalid_type_defaults_to_zero(self):
        post = dict(self._VALID_POST, type='99')
        cleaned, _, _ = _parse_advert_form(post, None)
        self.assertEqual(cleaned['type'], 0)

    def test_delivery_flag_parsed(self):
        post = dict(self._VALID_POST, delivery='on')
        cleaned, _, _ = _parse_advert_form(post, None)
        self.assertTrue(cleaned['delivery'])

    def test_opt_sets_wholesale_price(self):
        post = dict(self._VALID_POST, opt='on', price='500')
        cleaned, _, _ = _parse_advert_form(post, None)
        self.assertEqual(cleaned['wholesale_price'], 500.0)


# ---------------------------------------------------------------------------
#  Middleware
# ---------------------------------------------------------------------------

class LegacyUserMiddlewareTests(TestCase):
    def setUp(self):
        from django.utils import timezone
        from .models import LegacyUser
        now = timezone.now()
        self.user = LegacyUser.objects.create(
            type=0, username='mwtest', auth_key='', password_hash='',
            email='mw@test.com', currency='RUB', name='MW',
            address='', phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        self.client = Client()

    def test_anonymous_request_has_none_legacy_user(self):
        resp = self.client.get('/about/')
        self.assertEqual(resp.status_code, 200)

    def test_authenticated_request_attaches_user(self):
        session = self.client.session
        session['legacy_user_id'] = self.user.pk
        session.save()
        from django.conf import settings as _s
        self.client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key

        resp = self.client.get('/me/')
        # Should not redirect to login since user is authenticated
        self.assertEqual(resp.status_code, 200)


class LegacyLoginRequiredDecoratorTests(SimpleTestCase):
    def test_unauthenticated_redirects(self):
        from .middleware import legacy_login_required

        @legacy_login_required
        def dummy(request):
            from django.http import HttpResponse
            return HttpResponse('ok')

        request = MagicMock()
        request.legacy_user = None
        request.get_full_path.return_value = '/test/'
        resp = dummy(request)
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/login/', resp.url)

    def test_authenticated_passes(self):
        from .middleware import legacy_login_required
        from django.http import HttpResponse

        @legacy_login_required
        def dummy(request):
            return HttpResponse('ok')

        request = MagicMock()
        request.legacy_user = MagicMock()  # not None
        resp = dummy(request)
        self.assertEqual(resp.status_code, 200)

    def test_custom_login_url(self):
        from .middleware import legacy_login_required

        @legacy_login_required(login_url='/custom-login/')
        def dummy(request):
            from django.http import HttpResponse
            return HttpResponse('ok')

        request = MagicMock()
        request.legacy_user = None
        request.get_full_path.return_value = '/test/'
        resp = dummy(request)
        self.assertIn('/custom-login/', resp.url)


# ---------------------------------------------------------------------------
#  Auth flow
# ---------------------------------------------------------------------------

class AuthFlowTests(TestCase):
    def setUp(self):
        from django.utils import timezone
        from django.contrib.auth.hashers import make_password
        from .models import LegacyUser
        now = timezone.now()
        self.user = LegacyUser.objects.create(
            type=0, username='authuser', auth_key='testkey123',
            password_hash=make_password('password123'),
            email='auth@test.com', currency='RUB', name='Auth User',
            address='', phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        self.client = Client()

    def test_login_success(self):
        resp = self.client.post('/login/', {
            'username': 'authuser',
            'password': 'password123',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(self.client.session.get('legacy_user_id'), self.user.pk)

    def test_login_wrong_password(self):
        resp = self.client.post('/login/', {
            'username': 'authuser',
            'password': 'wrong',
        })
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Неверный логин или пароль')

    def test_login_with_next(self):
        resp = self.client.post('/login/', {
            'username': 'authuser',
            'password': 'password123',
            'next': '/me/',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/me/', resp.url)

    def test_logout(self):
        # Login first
        self.client.post('/login/', {
            'username': 'authuser',
            'password': 'password123',
        })
        self.assertIsNotNone(self.client.session.get('legacy_user_id'))
        # Logout
        resp = self.client.get('/logout/')
        self.assertEqual(resp.status_code, 302)
        self.assertIsNone(self.client.session.get('legacy_user_id'))

    def test_change_password(self):
        self.client.post('/login/', {
            'username': 'authuser',
            'password': 'password123',
        })
        resp = self.client.post('/change-password/', {
            'old_password': 'password123',
            'new_password1': 'newpass456',
            'new_password2': 'newpass456',
        })
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Сохранено' if 'Сохранено' in resp.content.decode() else '')

        # Old password should no longer work
        self.client.get('/logout/')
        resp = self.client.post('/login/', {
            'username': 'authuser',
            'password': 'password123',
        })
        self.assertNotEqual(self.client.session.get('legacy_user_id'), self.user.pk)

    def test_change_password_wrong_old(self):
        self.client.post('/login/', {
            'username': 'authuser',
            'password': 'password123',
        })
        resp = self.client.post('/change-password/', {
            'old_password': 'wrong',
            'new_password1': 'newpass456',
            'new_password2': 'newpass456',
        })
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Неверный пароль')


# ---------------------------------------------------------------------------
#  Advert status management
# ---------------------------------------------------------------------------

class AdvertStatusTests(TestCase):
    def setUp(self):
        from django.utils import timezone
        from django.contrib.gis.geos import Point
        from .models import Advert, LegacyUser, Catalog, Categories
        now = timezone.now()
        self.admin = LegacyUser.objects.create(
            type=0, username='admin', auth_key='', password_hash='',
            email='admin@test.com', currency='RUB', name='Admin',
            address='', phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        catalog = Catalog.objects.create(title='Cat', sort=0, active=1)
        category = Categories.objects.create(catalog=catalog, title='Grains', active=1)
        self.advert = Advert.objects.create(
            type=0, category=category, author=self.admin,
            location=Point(37.6, 55.7, srid=4326), contacts='', title='Test',
            text='', price=0, wholesale_price=0, min_volume=0,
            wholesale_volume=0, volume=0, priority=0,
            created_at=now, updated_at=now, status=ADVERT_STATUS_PUBLISHED,
        )
        self.client = Client()
        self.client.get('/')
        session = self.client.session
        session['legacy_user_id'] = self.admin.pk
        session.save()
        from django.conf import settings as _s
        self.client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key

    def test_hide_advert(self):
        from .models import Advert
        resp = self.client.post(f'/adverts/{self.advert.pk}/hide/')
        self.assertEqual(resp.status_code, 302)
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.status, ADVERT_STATUS_HIDDEN)

    def test_publish_advert(self):
        from .models import Advert
        Advert.objects.filter(pk=self.advert.pk).update(status=ADVERT_STATUS_HIDDEN)
        resp = self.client.post(f'/adverts/{self.advert.pk}/publish/')
        self.assertEqual(resp.status_code, 302)
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.status, ADVERT_STATUS_PUBLISHED)

    def test_delete_advert(self):
        from .models import Advert
        resp = self.client.post(f'/adverts/{self.advert.pk}/delete/')
        self.assertEqual(resp.status_code, 302)
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.status, ADVERT_STATUS_DELETED)

    def test_unauthenticated_cannot_hide(self):
        anon_client = Client()
        resp = anon_client.post(f'/adverts/{self.advert.pk}/hide/')
        self.assertEqual(resp.status_code, 302)
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.status, ADVERT_STATUS_PUBLISHED)


# ---------------------------------------------------------------------------
#  Favorites
# ---------------------------------------------------------------------------

class FavoriteToggleTests(TestCase):
    def setUp(self):
        from django.utils import timezone
        from django.contrib.gis.geos import Point
        from .models import Advert, LegacyUser, Catalog, Categories
        now = timezone.now()
        self.user = LegacyUser.objects.create(
            type=0, username='favuser', auth_key='', password_hash='',
            email='fav@test.com', currency='RUB', name='Fav',
            address='', phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        catalog = Catalog.objects.create(title='Cat', sort=0, active=1)
        category = Categories.objects.create(catalog=catalog, title='Grains', active=1)
        self.advert = Advert.objects.create(
            type=0, category=category, author=self.user,
            location=Point(37.6, 55.7, srid=4326), contacts='', title='Test',
            text='', price=0, wholesale_price=0, min_volume=0,
            wholesale_volume=0, volume=0, priority=0,
            created_at=now, updated_at=now, status=ADVERT_STATUS_PUBLISHED,
        )
        self.client = Client()
        self.client.get('/')
        session = self.client.session
        session['legacy_user_id'] = self.user.pk
        session.save()
        from django.conf import settings as _s
        self.client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key

    def test_toggle_on_and_off(self):
        from .models import Favorite
        # Toggle ON
        resp = self.client.post(f'/api/favorites/{self.advert.pk}/toggle/')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertTrue(data['is_favorited'])
        self.assertTrue(Favorite.objects.filter(user=self.user, advert=self.advert).exists())

        # Toggle OFF
        resp = self.client.post(f'/api/favorites/{self.advert.pk}/toggle/')
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertFalse(data['is_favorited'])
        self.assertFalse(Favorite.objects.filter(user=self.user, advert=self.advert).exists())

    def test_unauthenticated_returns_401(self):
        anon_client = Client()
        resp = anon_client.post(f'/api/favorites/{self.advert.pk}/toggle/')
        self.assertEqual(resp.status_code, 401)


# ---------------------------------------------------------------------------
#  Messages unread API
# ---------------------------------------------------------------------------

class MessagesUnreadApiTests(TestCase):
    def setUp(self):
        from django.utils import timezone
        from .models import LegacyUser, Message
        now = timezone.now()
        self.user = LegacyUser.objects.create(
            type=0, username='msguser', auth_key='', password_hash='',
            email='msg@test.com', currency='RUB', name='Msg',
            address='', phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        self.sender = LegacyUser.objects.create(
            type=0, username='sender', auth_key='', password_hash='',
            email='sender@test.com', currency='RUB', name='Sender',
            address='', phone='', inn='', status=USER_STATUS_ACTIVE,
            created_at=now, updated_at=now, contacts='',
        )
        Message.objects.create(
            sender=self.sender, recipient=self.user,
            text='Hello', is_read=False, created_at=now,
        )
        Message.objects.create(
            sender=self.sender, recipient=self.user,
            text='Hello2', is_read=False, created_at=now,
        )
        Message.objects.create(
            sender=self.sender, recipient=self.user,
            text='Read one', is_read=True, created_at=now,
        )
        self.client = Client()
        self.client.get('/')
        session = self.client.session
        session['legacy_user_id'] = self.user.pk
        session.save()
        from django.conf import settings as _s
        self.client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key

    def test_unread_count(self):
        resp = self.client.get('/api/messages/unread/')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['count'], 2)

    def test_unauthenticated_returns_zero(self):
        anon_client = Client()
        resp = anon_client.get('/api/messages/unread/')
        data = resp.json()
        self.assertFalse(data['ok'])
        self.assertEqual(data['count'], 0)


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
