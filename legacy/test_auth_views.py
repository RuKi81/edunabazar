"""
Тесты регистрации и установки пароля (``legacy/views/auth.py``) —
страховочная сетка перед рефакторингом ``legacy_register_sms_confirm``
(C=30), ``legacy_set_password`` (C=16) и ``legacy_register_email`` (C=12).

Покрывается: SMS-флоу (запрос кода, подтверждение, TTL, лимит попыток,
создание пользователя с координатами/контактами), email-регистрация
(валидация, антиспам), set-password (токен, ротация auth_key, повторное
использование ссылки, безопасный next).
"""
import hashlib
import time
from unittest import mock

from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import USER_STATUS_ACTIVE
from .models import LegacyUser
from .views.helpers import _ANTISPAM_SECRET, _make_set_password_token

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _make_user(username, **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )
    kwargs.update(overrides)
    return LegacyUser.objects.create(**kwargs)


def _antispam_fields(age_seconds=60):
    """Валидные _ts/_th, «отправленные» age_seconds назад."""
    ts = str(int(time.time()) - age_seconds)
    th = hashlib.sha256(f'{ts}:{_ANTISPAM_SECRET}'.encode()).hexdigest()[:16]
    return {'_ts': ts, '_th': th, 'website': ''}


@override_settings(CACHES=_DUMMY_CACHE)
class RegisterSmsTests(TestCase):
    URL = '/register/sms/'

    def _post(self, phone, **extra):
        data = {'phone': phone}
        data.update(_antispam_fields())
        data.update(extra)
        return Client(), data

    def test_get_renders(self):
        self.assertEqual(Client().get(self.URL).status_code, 200)

    def test_invalid_phone_error(self):
        client, data = self._post('123')
        resp = client.post(self.URL, data)
        self.assertEqual(
            resp.context['errors']['phone'], 'Введите корректный телефон',
        )

    def test_duplicate_phone_error(self):
        _make_user('smsdup', phone='+79001112233')
        client, data = self._post('+7 900 111-22-33')
        resp = client.post(self.URL, data)
        self.assertEqual(
            resp.context['errors']['phone'], 'Этот телефон уже зарегистрирован',
        )

    @mock.patch('legacy.views.auth.send_otp', return_value=True)
    def test_valid_phone_sends_otp_and_redirects(self, mock_send):
        client, data = self._post('+79005556677')
        resp = client.post(self.URL, data)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/register/sms/confirm/')
        mock_send.assert_called_once()
        state = client.session['sms_register']
        self.assertEqual(state['phone'], '+79005556677')
        self.assertEqual(len(state['code']), 6)

    def test_honeypot_redirects_back(self):
        client, data = self._post('+79005556677', website='spam')
        resp = client.post(self.URL, data)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], self.URL)
        self.assertNotIn('sms_register', client.session)


@override_settings(CACHES=_DUMMY_CACHE)
class RegisterSmsConfirmTests(TestCase):
    URL = '/register/sms/confirm/'

    def _client_with_state(self, **overrides):
        client = Client()
        client.get('/')
        session = client.session
        state = {
            'phone': '+79005556677',
            'code': '123456',
            'created_at': timezone.now().isoformat(),
            'attempts': 0,
            'verify_attempts': 0,
        }
        state.update(overrides)
        session['sms_register'] = state
        session.save()
        from django.conf import settings as _s
        client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key
        return client

    def _valid_post(self, **overrides):
        data = {
            'code': '123456',
            'username': 'smsnewuser',
            'email': 'smsnewuser@test.com',
            'name': 'Иван',
            'address': 'Москва',
            'show_address': '1',
            'lat': '55.7',
            'lon': '37.6',
        }
        data.update(overrides)
        return data

    def test_no_state_shows_error(self):
        resp = Client().get(self.URL)
        self.assertEqual(resp.context['errors_all'], 'Сначала запросите SMS-код')

    def test_expired_code_shows_error(self):
        old = (timezone.now() - timezone.timedelta(minutes=10)).isoformat()
        client = self._client_with_state(created_at=old)
        resp = client.get(self.URL)
        self.assertEqual(resp.context['errors_all'], 'Код устарел. Запросите новый')

    def test_wrong_code_increments_attempts(self):
        client = self._client_with_state()
        resp = client.post(self.URL, self._valid_post(code='000000'))
        self.assertEqual(resp.context['errors']['code'], 'Неверный код')
        self.assertEqual(client.session['sms_register']['verify_attempts'], 1)
        self.assertFalse(LegacyUser.objects.filter(username='smsnewuser').exists())

    def test_too_many_attempts_blocks(self):
        client = self._client_with_state(verify_attempts=10)
        resp = client.post(self.URL, self._valid_post())
        self.assertEqual(
            resp.context['errors_all'], 'Слишком много попыток. Запросите новый код',
        )
        self.assertFalse(LegacyUser.objects.filter(username='smsnewuser').exists())

    def test_missing_fields_errors(self):
        client = self._client_with_state()
        resp = client.post(self.URL, self._valid_post(code='', username='', email=''))
        errors = resp.context['errors']
        self.assertIn('code', errors)
        self.assertIn('username', errors)
        self.assertIn('email', errors)

    def test_duplicate_username_and_email_errors(self):
        _make_user('smsnewuser')
        client = self._client_with_state()
        resp = client.post(
            self.URL,
            self._valid_post(email='smsnewuser@test.com'),
        )
        errors = resp.context['errors']
        self.assertEqual(errors['username'], 'Этот username уже занят')
        self.assertEqual(errors['email'], 'Этот email уже занят')

    @mock.patch('legacy.views.auth._send_registration_email', return_value=True)
    def test_valid_post_creates_user(self, mock_email):
        client = self._client_with_state()
        data = self._valid_post()
        data['ec_type'] = ['telegram', 'bogus']
        data['ec_value'] = ['@ivan', 'x']
        resp = client.post(self.URL, data)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/adverts/')

        user = LegacyUser.objects.get(username='smsnewuser')
        self.assertEqual(user.phone, '+79005556677')
        self.assertEqual(user.email, 'smsnewuser@test.com')
        self.assertEqual(user.contacts, '')
        self.assertEqual(user.extra_contacts, [{'type': 'telegram', 'value': '@ivan'}])
        self.assertAlmostEqual(user.location.y, 55.7, places=4)
        self.assertAlmostEqual(user.location.x, 37.6, places=4)

        # залогинен, sms-состояние очищено, письмо отправлено
        self.assertEqual(client.session['legacy_user_id'], user.pk)
        self.assertNotIn('sms_register', client.session)
        mock_email.assert_called_once()

    @mock.patch('legacy.views.auth._send_registration_email', return_value=True)
    def test_show_address_disabled_writes_contacts_flag(self, _):
        client = self._client_with_state()
        data = self._valid_post()
        data.pop('show_address')
        client.post(self.URL, data)
        user = LegacyUser.objects.get(username='smsnewuser')
        self.assertEqual(user.contacts, 'show_address=0')

    @mock.patch('legacy.views.auth._send_registration_email', return_value=True)
    def test_safe_next_redirect(self, _):
        client = self._client_with_state()
        resp = client.post(self.URL, self._valid_post(next='/me/'))
        self.assertEqual(resp['Location'], '/me/')

    @mock.patch('legacy.views.auth._send_registration_email', return_value=True)
    def test_unsafe_next_ignored(self, _):
        client = self._client_with_state()
        resp = client.post(
            self.URL, self._valid_post(next='https://evil.example.com/'),
        )
        self.assertEqual(resp['Location'], '/adverts/')


@override_settings(CACHES=_DUMMY_CACHE)
class RegisterEmailTests(TestCase):
    URL = '/register/email/'

    def _valid_post(self, **overrides):
        data = {
            'username': 'emailnewuser',
            'email': 'emailnewuser@test.com',
            'name': 'Пётр',
            'phone': '+79001231212',
        }
        data.update(_antispam_fields())
        data.update(overrides)
        return data

    def test_get_renders(self):
        self.assertEqual(Client().get(self.URL).status_code, 200)

    def test_missing_fields_errors(self):
        resp = Client().post(self.URL, self._valid_post(username='', email=''))
        errors = resp.context['errors']
        self.assertEqual(errors['username'], 'Введите username')
        self.assertEqual(errors['email'], 'Введите email')

    def test_duplicate_username_and_email_errors(self):
        _make_user('emailnewuser')
        resp = Client().post(self.URL, self._valid_post())
        errors = resp.context['errors']
        self.assertEqual(errors['username'], 'Этот username уже занят')
        self.assertEqual(errors['email'], 'Этот email уже занят')

    def test_honeypot_redirects_back(self):
        resp = Client().post(self.URL, self._valid_post(website='spam'))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], self.URL)
        self.assertFalse(LegacyUser.objects.filter(username='emailnewuser').exists())

    @mock.patch('legacy.views.auth._send_registration_email', return_value=True)
    def test_valid_post_creates_user_and_logs_in(self, mock_email):
        client = Client()
        resp = client.post(self.URL, self._valid_post())
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/adverts/')
        user = LegacyUser.objects.get(username='emailnewuser')
        self.assertEqual(user.email, 'emailnewuser@test.com')
        self.assertEqual(client.session['legacy_user_id'], user.pk)
        mock_email.assert_called_once()

    @mock.patch('legacy.views.auth._send_registration_email', return_value=True)
    def test_safe_next_redirect(self, _):
        resp = Client().post(self.URL, self._valid_post(next='/me/'))
        self.assertEqual(resp['Location'], '/me/')


@override_settings(CACHES=_DUMMY_CACHE)
class SetPasswordTests(TestCase):
    def setUp(self):
        self.user = _make_user('setpwuser', auth_key='oldauthkey1234567890abcd')
        self.token = _make_set_password_token(self.user.pk, self.user.auth_key)
        self.url = f'/set-password/{self.token}/'

    def test_invalid_token_shows_error(self):
        resp = Client().get('/set-password/garbage-token/')
        self.assertEqual(
            resp.context['errors']['token'],
            'Ссылка недействительна или устарела',
        )

    def test_valid_token_renders_form(self):
        resp = Client().get(self.url)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context['errors'], {})

    def test_short_password_error(self):
        resp = Client().post(self.url, {'password1': '123', 'password2': '123'})
        self.assertEqual(resp.context['errors']['password1'], 'Пароль слишком короткий')

    def test_mismatch_error(self):
        resp = Client().post(
            self.url, {'password1': 'secret123', 'password2': 'other123'},
        )
        self.assertEqual(resp.context['errors']['password2'], 'Пароли не совпадают')

    def test_valid_post_sets_password_and_logs_in(self):
        client = Client()
        resp = client.post(
            self.url,
            {'password1': 'secret123', 'password2': 'secret123', 'next': '/me/'},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/me/')
        self.user.refresh_from_db()
        self.assertNotEqual(self.user.auth_key, 'oldauthkey1234567890abcd')
        from django.contrib.auth.hashers import check_password
        self.assertTrue(check_password('secret123', self.user.password_hash))
        self.assertEqual(client.session['legacy_user_id'], self.user.pk)

    def test_token_single_use(self):
        client = Client()
        client.post(self.url, {'password1': 'secret123', 'password2': 'secret123'})
        # auth_key ротирован — старый токен больше не действует
        resp = Client().get(self.url)
        self.assertEqual(
            resp.context['errors']['token'],
            'Ссылка недействительна или устарела',
        )

    def test_unsafe_next_ignored(self):
        resp = Client().post(
            self.url,
            {
                'password1': 'secret123', 'password2': 'secret123',
                'next': 'https://evil.example.com/',
            },
        )
        self.assertEqual(resp['Location'], '/adverts/')
