"""
Тесты кабинета подписок Agrocosmos (``agrocosmos/views/cabinet.py``) —
страховочная сетка перед рефакторингом ``me_agrocosmos`` (C=17).

Покрывается: доступ (redirect на логин), GET-контекст, add (регион/район,
вывод региона из района, валидация scope и флагов, дубликаты),
update (флаги, чужая/несуществующая подписка), delete (только своя),
неизвестное действие.
"""
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.contrib.messages import get_messages
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from legacy.constants import USER_STATUS_ACTIVE
from legacy.models import LegacyUser

from agrocosmos.models import AgroSubscription, District, Region

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}

URL = '/me/agrocosmos/'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _make_user(username='subuser'):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )


def _login(client, user):
    client.get('/')
    session = client.session
    session['legacy_user_id'] = user.pk
    session.save()
    from django.conf import settings as _s
    client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key
    return client


def _messages(resp):
    return [str(m) for m in get_messages(resp.wsgi_request)]


@override_settings(CACHES=_DUMMY_CACHE)
class CabinetTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион К', code='rk-1', geom=_square(30, 50),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район К', geom=_square(30, 50),
        )

    def setUp(self):
        self.user = _make_user('subuser')
        self.client_ = _login(Client(), self.user)

    def _sub(self, user=None, **overrides):
        kwargs = dict(
            legacy_user_id=(user or self.user).pk, region=self.region,
            notify_anomalies=True, notify_updates=False,
        )
        kwargs.update(overrides)
        return AgroSubscription.objects.create(**kwargs)

    # --- access & GET ---

    def test_anonymous_redirected_to_login(self):
        resp = Client().get(URL)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/login/?next=/me/agrocosmos/')

    def test_get_renders_context(self):
        sub = self._sub()
        resp = self.client_.get(URL)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(list(resp.context['subscriptions']), [sub])
        self.assertEqual(list(resp.context['regions']), [self.region])
        districts = resp.context['districts']
        self.assertEqual(districts[0]['id'], self.district.pk)
        self.assertEqual(districts[0]['region__name'], 'Регион К')

    # --- add ---

    def test_add_region_subscription(self):
        resp = self.client_.post(URL, {
            'action': 'add', 'region': str(self.region.pk),
            'notify_anomalies': 'on',
        })
        self.assertEqual(resp['Location'], URL)
        sub = AgroSubscription.objects.get(legacy_user_id=self.user.pk)
        self.assertEqual(sub.region_id, self.region.pk)
        self.assertIsNone(sub.district_id)
        self.assertTrue(sub.notify_anomalies)
        self.assertFalse(sub.notify_updates)
        self.assertIn('Подписка добавлена.', _messages(resp))

    def test_add_district_derives_region(self):
        self.client_.post(URL, {
            'action': 'add', 'district': str(self.district.pk),
            'notify_updates': 'on',
        })
        sub = AgroSubscription.objects.get(legacy_user_id=self.user.pk)
        self.assertEqual(sub.district_id, self.district.pk)
        self.assertEqual(sub.region_id, self.region.pk)
        self.assertTrue(sub.notify_updates)

    def test_add_requires_scope(self):
        resp = self.client_.post(URL, {'action': 'add', 'notify_anomalies': 'on'})
        self.assertIn('Укажите субъект или район.', _messages(resp))
        self.assertFalse(AgroSubscription.objects.exists())

    def test_add_unknown_district(self):
        resp = self.client_.post(URL, {
            'action': 'add', 'district': '999999', 'notify_anomalies': 'on',
        })
        self.assertIn('Район не найден.', _messages(resp))
        self.assertFalse(AgroSubscription.objects.exists())

    def test_add_requires_at_least_one_flag(self):
        resp = self.client_.post(URL, {
            'action': 'add', 'region': str(self.region.pk),
        })
        self.assertIn(
            'Включите хотя бы один тип уведомлений.', _messages(resp),
        )
        self.assertFalse(AgroSubscription.objects.exists())

    def test_add_duplicate_warns(self):
        self._sub()
        resp = self.client_.post(URL, {
            'action': 'add', 'region': str(self.region.pk),
            'notify_anomalies': 'on',
        })
        self.assertEqual(
            AgroSubscription.objects.filter(legacy_user_id=self.user.pk).count(), 1,
        )
        self.assertTrue(any('уже существует' in m for m in _messages(resp)))

    # --- update ---

    def test_update_toggles_flags(self):
        sub = self._sub()
        resp = self.client_.post(URL, {
            'action': 'update', 'subscription_id': str(sub.pk),
            'notify_updates': 'on',
        })
        sub.refresh_from_db()
        self.assertFalse(sub.notify_anomalies)
        self.assertTrue(sub.notify_updates)
        self.assertIn('Настройки сохранены.', _messages(resp))

    def test_update_foreign_subscription_rejected(self):
        other_sub = self._sub(user=_make_user('other'))
        resp = self.client_.post(URL, {
            'action': 'update', 'subscription_id': str(other_sub.pk),
            'notify_updates': 'on',
        })
        other_sub.refresh_from_db()
        self.assertFalse(other_sub.notify_updates)
        self.assertIn('Подписка не найдена.', _messages(resp))

    # --- delete ---

    def test_delete_own_subscription(self):
        sub = self._sub()
        resp = self.client_.post(URL, {
            'action': 'delete', 'subscription_id': str(sub.pk),
        })
        self.assertFalse(AgroSubscription.objects.filter(pk=sub.pk).exists())
        self.assertIn('Подписка удалена.', _messages(resp))

    def test_delete_foreign_subscription_noop(self):
        other_sub = self._sub(user=_make_user('other'))
        self.client_.post(URL, {
            'action': 'delete', 'subscription_id': str(other_sub.pk),
        })
        self.assertTrue(AgroSubscription.objects.filter(pk=other_sub.pk).exists())

    # --- misc ---

    def test_unknown_action(self):
        resp = self.client_.post(URL, {'action': 'frobnicate'})
        self.assertEqual(resp['Location'], URL)
        self.assertIn('Неизвестное действие.', _messages(resp))
