"""
Тесты полных сценариев регистрации и восстановления доступа
(``legacy/views/auth.py``) — страховочная сетка перед рефакторингом.

Покрывается: SMS-регистрация (запрос кода → подтверждение), email-регистрация,
антиспам (honeypot + time-trap), lockout логина, защита от open redirect,
set-password токен (выдача, повторное использование, валидация пароля).
"""
import hashlib
import time
from unittest import mock

from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import USER_STATUS_ACTIVE
from .models import LegacyUser
from .views.helpers import _ANTISPAM_SECRET, _make_set_password_token


def _antispam_fields(age_seconds=10):
    """Валидные antispam-поля формы: токен возрастом age_seconds."""
    ts = str(int(time.time()) - age_seconds)
    th = hashlib.sha256(f'{ts}:{_ANTISPAM_SECRET}'.encode()).hexdigest()[:16]
    return {'_ts': ts, '_th': th, 'website': ''}


def _make_user(username='user1', email='u1@test.com', phone='', password_hash='',
               auth_key='key-abc'):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key=auth_key, password_hash=password_hash,
        email=email, currency='RU', name='', address='', phone=phone, inn='',
        status=USER_STATUS_ACTIVE, created_at=now, updated_at=now, contacts='',
    )


class LoginLockoutTests(TestCase):
    def setUp(self):
        from django.contrib.auth.hashers import make_password
        self.user = _make_user(password_hash=make_password('correct-pw'))
        self.client = Client()

    def test_lockout_after_max_fails(self):
        for _ in range(10):
            resp = self.client.post('/login/', {
                'username': self.user.username, 'password': 'wrong',
            })
        self.assertContains(resp, 'Слишком много попыток')
        # Даже правильный пароль не пускает, пока действует блокировка
        resp = self.client.post('/login/', {
            'username': self.user.username, 'password': 'correct-pw',
        })
        self.assertContains(resp, 'Слишком много попыток')
        self.assertIsNone(self.client.session.get('legacy_user_id'))

    def test_success_resets_fail_counter(self):
        for _ in range(3):
            self.client.post('/login/', {
                'username': self.user.username, 'password': 'wrong',
            })
        resp = self.client.post('/login/', {
            'username': self.user.username, 'password': 'correct-pw',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertIsNone(self.client.session.get('login_fail_count'))

    def test_open_redirect_is_blocked(self):
        resp = self.client.post('/login/', {
            'username': self.user.username, 'password': 'correct-pw',
            'next': 'https://evil.example.com/phish',
        })
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, '/adverts/')


class SmsRegistrationTests(TestCase):
    def setUp(self):
        self.client = Client()

    def _request_code(self, phone='+79001234567'):
        with mock.patch('legacy.views.auth.send_otp', return_value=True) as m:
            resp = self.client.post(
                '/register/sms/', {'phone': phone, **_antispam_fields()},
            )
        return resp, m

    def test_request_code_success(self):
        resp, send_mock = self._request_code()
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, '/register/sms/confirm/')
        send_mock.assert_called_once()
        state = self.client.session.get('sms_register')
        self.assertEqual(state['phone'], '+79001234567')
        self.assertEqual(len(state['code']), 6)

    def test_request_code_duplicate_phone(self):
        _make_user(phone='+79001234567')
        resp, send_mock = self._request_code()
        self.assertContains(resp, 'уже зарегистрирован')
        send_mock.assert_not_called()

    def test_antispam_honeypot_blocks(self):
        fields = _antispam_fields()
        fields['website'] = 'http://spam.example.com'
        with mock.patch('legacy.views.auth.send_otp', return_value=True) as m:
            resp = self.client.post(
                '/register/sms/', {'phone': '+79001234567', **fields},
            )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, '/register/sms/')
        self.assertIsNone(self.client.session.get('sms_register'))
        m.assert_not_called()

    def test_antispam_too_fast_blocks(self):
        with mock.patch('legacy.views.auth.send_otp', return_value=True):
            resp = self.client.post(
                '/register/sms/',
                {'phone': '+79001234567', **_antispam_fields(age_seconds=0)},
            )
        self.assertEqual(resp.status_code, 302)
        self.assertIsNone(self.client.session.get('sms_register'))

    def _confirm_payload(self, code, **overrides):
        data = {
            'code': code,
            'username': 'newuser',
            'email': 'new@test.com',
            'name': 'Новый',
            'address': '',
        }
        data.update(overrides)
        return data

    def test_confirm_success_creates_user(self):
        self._request_code()
        code = self.client.session['sms_register']['code']
        resp = self.client.post('/register/sms/confirm/', self._confirm_payload(code))
        self.assertEqual(resp.status_code, 302)
        user = LegacyUser.objects.get(username='newuser')
        self.assertEqual(user.phone, '+79001234567')
        self.assertEqual(user.email, 'new@test.com')
        self.assertEqual(user.status, USER_STATUS_ACTIVE)
        self.assertEqual(self.client.session.get('legacy_user_id'), user.pk)
        self.assertIsNone(self.client.session.get('sms_register'))

    def test_confirm_wrong_code(self):
        self._request_code()
        resp = self.client.post('/register/sms/confirm/', self._confirm_payload('000000'))
        # 000000 может совпасть со сгенерированным — исключаем ложный провал
        if self.client.session['sms_register']['code'] != '000000':
            self.assertContains(resp, 'Неверный код')
            self.assertFalse(LegacyUser.objects.filter(username='newuser').exists())
            self.assertEqual(
                self.client.session['sms_register']['verify_attempts'], 1,
            )

    def test_confirm_without_requesting_code(self):
        resp = self.client.post('/register/sms/confirm/', self._confirm_payload('123456'))
        self.assertContains(resp, 'Сначала запросите SMS-код')

    def test_confirm_expired_code(self):
        self._request_code()
        session = self.client.session
        state = session['sms_register']
        stale = timezone.now() - timezone.timedelta(minutes=10)
        state['created_at'] = stale.isoformat()
        session['sms_register'] = state
        session.save()
        resp = self.client.post(
            '/register/sms/confirm/', self._confirm_payload(state['code']),
        )
        self.assertContains(resp, 'Код устарел')

    def test_confirm_duplicate_username(self):
        _make_user(username='newuser', email='other@test.com')
        self._request_code()
        code = self.client.session['sms_register']['code']
        resp = self.client.post('/register/sms/confirm/', self._confirm_payload(code))
        self.assertContains(resp, 'username уже занят')
        self.assertEqual(LegacyUser.objects.filter(username='newuser').count(), 1)


class EmailRegistrationTests(TestCase):
    def setUp(self):
        self.client = Client()

    def _payload(self, **overrides):
        data = {
            'username': 'mailuser',
            'email': 'mail@test.com',
            'name': 'Тест',
            'phone': '',
            **_antispam_fields(),
        }
        data.update(overrides)
        return data

    def test_register_success(self):
        resp = self.client.post('/register/email/', self._payload())
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, '/adverts/')
        user = LegacyUser.objects.get(username='mailuser')
        self.assertEqual(self.client.session.get('legacy_user_id'), user.pk)
        # Пароль случайный — прямой вход без set-password невозможен
        self.assertTrue(user.password_hash)
        self.assertTrue(user.auth_key)

    def test_register_duplicate_email(self):
        _make_user(username='other', email='mail@test.com')
        resp = self.client.post('/register/email/', self._payload())
        self.assertContains(resp, 'email уже занят')
        self.assertFalse(LegacyUser.objects.filter(username='mailuser').exists())

    def test_register_missing_fields(self):
        resp = self.client.post('/register/email/', self._payload(username='', email=''))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(LegacyUser.objects.count(), 0)

    def test_open_redirect_is_blocked(self):
        resp = self.client.post(
            '/register/email/', self._payload(next='https://evil.example.com/'),
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, '/adverts/')


class SetPasswordTests(TestCase):
    def setUp(self):
        self.user = _make_user(auth_key='original-key-123')
        self.client = Client()

    def _token(self):
        return _make_set_password_token(self.user.pk, self.user.auth_key)

    def test_get_shows_form(self):
        resp = self.client.get(f'/set-password/{self._token()}/')
        self.assertEqual(resp.status_code, 200)
        self.assertNotContains(resp, 'недействительна')

    def test_set_password_success_and_rotates_auth_key(self):
        from django.contrib.auth.hashers import check_password
        token = self._token()
        resp = self.client.post(f'/set-password/{token}/', {
            'password1': 'brand-new-pw', 'password2': 'brand-new-pw',
        })
        self.assertEqual(resp.status_code, 302)
        self.user.refresh_from_db()
        self.assertTrue(check_password('brand-new-pw', self.user.password_hash))
        self.assertNotEqual(self.user.auth_key, 'original-key-123')
        self.assertEqual(self.client.session.get('legacy_user_id'), self.user.pk)

    def test_token_is_single_use(self):
        token = self._token()
        self.client.post(f'/set-password/{token}/', {
            'password1': 'first-pw-123', 'password2': 'first-pw-123',
        })
        # auth_key ротирован — старый токен больше не находит пользователя
        resp = Client().post(f'/set-password/{token}/', {
            'password1': 'second-pw-456', 'password2': 'second-pw-456',
        })
        self.assertContains(resp, 'недействительна')

    def test_garbage_token_rejected(self):
        resp = self.client.get('/set-password/not-a-valid-token/')
        self.assertContains(resp, 'недействительна')

    def test_short_password_rejected(self):
        resp = self.client.post(f'/set-password/{self._token()}/', {
            'password1': 'abc', 'password2': 'abc',
        })
        self.assertContains(resp, 'слишком короткий')
        self.user.refresh_from_db()
        self.assertEqual(self.user.auth_key, 'original-key-123')

    def test_password_mismatch_rejected(self):
        resp = self.client.post(f'/set-password/{self._token()}/', {
            'password1': 'password-one', 'password2': 'password-two',
        })
        self.assertContains(resp, 'не совпадают')


@override_settings(CACHES={
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
})
class SetPasswordOpenRedirectTests(TestCase):
    def test_external_next_ignored(self):
        user = _make_user(auth_key='redirect-key-1')
        token = _make_set_password_token(user.pk, user.auth_key)
        resp = Client().post(
            f'/set-password/{token}/?next=https://evil.example.com/',
            {'password1': 'safe-password', 'password2': 'safe-password'},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, '/adverts/')
