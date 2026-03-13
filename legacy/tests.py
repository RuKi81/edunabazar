import io

from django.test import SimpleTestCase
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
