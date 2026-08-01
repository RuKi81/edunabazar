"""
Тесты личного кабинета (``legacy/views/profile.py``) — страховочная
сетка перед рефакторингом ``legacy_me`` (C=28) и
``legacy_me_bulk_adverts`` (C=20).

Покрывается: доступ (anon/user/admin), список своих объявлений,
админ-вкладка с фильтрами status/sort, сохранение профиля (валидация
логина/email, чекбокс show_address, координаты) и bulk-операции
(hide/delete/publish/bump, права, безопасность next-редиректа).
"""
from django.contrib.gis.geos import Point
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION,
    ADVERT_STATUS_PUBLISHED, USER_STATUS_ACTIVE,
)
from .models import Advert, Catalog, Categories, LegacyUser

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


def _make_advert(author, category, title, status=ADVERT_STATUS_PUBLISHED,
                 **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, category=category, author=author,
        location=Point(37.6, 55.7, srid=4326), contacts='+7 900 000-00-00',
        title=title, text='Описание тестового объявления', address='Москва',
        price=100, wholesale_price=0, min_volume=0, wholesale_volume=0,
        volume=0, priority=0, created_at=now, updated_at=now, status=status,
    )
    kwargs.update(overrides)
    return Advert.objects.create(**kwargs)


def _login(client, user):
    client.get('/')
    session = client.session
    session['legacy_user_id'] = user.pk
    session.save()
    from django.conf import settings as _s
    client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key


@override_settings(CACHES=_DUMMY_CACHE)
class LegacyMeGetTests(TestCase):
    URL = '/me/'

    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('meuser')
        cls.other = _make_user('meother')
        catalog = Catalog.objects.create(title='Мекаталог', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=catalog, title='Мекатегория', active=1,
        )
        cls.mine_published = _make_advert(cls.user, cls.category, 'МоёОпубликовано')
        cls.mine_hidden = _make_advert(
            cls.user, cls.category, 'МоёСкрыто', status=ADVERT_STATUS_HIDDEN,
        )
        cls.mine_deleted = _make_advert(
            cls.user, cls.category, 'МоёУдалено', status=ADVERT_STATUS_DELETED,
        )
        cls.foreign = _make_advert(cls.other, cls.category, 'Чужое')
        cls.foreign_moderation = _make_advert(
            cls.other, cls.category, 'ЧужоеМодерация',
            status=ADVERT_STATUS_MODERATION,
        )

    def _client(self, user):
        client = Client()
        _login(client, user)
        return client

    def test_anonymous_redirected_to_login(self):
        resp = Client().get(self.URL)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/login/?next=/me/')

    def test_my_adverts_exclude_deleted_and_foreign(self):
        resp = self._client(self.user).get(self.URL)
        self.assertEqual(resp.status_code, 200)
        titles = [a.title for a in resp.context['my_adverts']]
        self.assertIn('МоёОпубликовано', titles)
        self.assertIn('МоёСкрыто', titles)
        self.assertNotIn('МоёУдалено', titles)
        self.assertNotIn('Чужое', titles)

    def test_non_admin_has_no_admin_tab(self):
        resp = self._client(self.user).get(self.URL)
        self.assertFalse(resp.context['is_admin_cabinet'])
        self.assertEqual(resp.context['admin_adverts'], [])
        self.assertIsNone(resp.context['admin_page'])

    def test_admin_tab_lists_all_but_deleted(self):
        admin = _make_user('admin')
        resp = self._client(admin).get(self.URL)
        self.assertTrue(resp.context['is_admin_cabinet'])
        titles = [a.title for a in resp.context['admin_adverts']]
        self.assertIn('МоёОпубликовано', titles)
        self.assertIn('МоёСкрыто', titles)
        self.assertIn('ЧужоеМодерация', titles)
        self.assertNotIn('МоёУдалено', titles)
        # автору проставляется label
        advert = next(a for a in resp.context['admin_adverts'] if a.title == 'Чужое')
        self.assertEqual(advert.author_label, 'meother')

    def test_admin_status_filter(self):
        admin = _make_user('admin')
        resp = self._client(admin).get(self.URL, {'status': 'moderation'})
        titles = [a.title for a in resp.context['admin_adverts']]
        self.assertEqual(titles, ['ЧужоеМодерация'])
        self.assertEqual(resp.context['admin_filter_status'], 'moderation')

    def test_admin_invalid_filters_fall_back(self):
        admin = _make_user('admin')
        resp = self._client(admin).get(
            self.URL, {'status': 'bogus', 'sort': 'bogus'},
        )
        self.assertEqual(resp.context['admin_filter_status'], 'all')
        self.assertEqual(resp.context['admin_filter_sort'], 'created')

    def test_section_validation(self):
        client = self._client(self.user)
        self.assertEqual(
            client.get(self.URL, {'section': 'adverts'}).context['active_section'],
            'adverts',
        )
        self.assertEqual(
            client.get(self.URL, {'section': 'bogus'}).context['active_section'],
            'credentials',
        )

    def test_garbage_page_param_is_safe(self):
        resp = self._client(self.user).get(self.URL, {'page_my': 'garbage'})
        self.assertEqual(resp.status_code, 200)


@override_settings(CACHES=_DUMMY_CACHE)
class LegacyMePostTests(TestCase):
    URL = '/me/'

    def setUp(self):
        self.user = _make_user('postuser')
        self.taken = _make_user('takenuser')
        self.client_ = Client()
        _login(self.client_, self.user)

    def _valid_post(self, **overrides):
        data = {
            'username': 'postuser',
            'email': 'postuser@test.com',
            'name': 'Иван',
            'phone': '+7 900 111-22-33',
            'address': 'Казань',
            'show_address': '1',
            'lat': '55.79',
            'lon': '49.12',
        }
        data.update(overrides)
        return data

    def test_valid_post_saves_profile(self):
        resp = self.client_.post(self.URL, self._valid_post(name='Пётр'))
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.context['saved'])
        self.user.refresh_from_db()
        self.assertEqual(self.user.name, 'Пётр')
        self.assertEqual(self.user.address, 'Казань')
        self.assertAlmostEqual(self.user.location.y, 55.79, places=4)
        self.assertAlmostEqual(self.user.location.x, 49.12, places=4)

    def test_empty_username_is_error(self):
        resp = self.client_.post(self.URL, self._valid_post(username=''))
        self.assertEqual(resp.context['errors']['username'], 'Введите логин')
        self.assertFalse(resp.context['saved'])

    def test_taken_username_is_error(self):
        resp = self.client_.post(self.URL, self._valid_post(username='takenuser'))
        self.assertEqual(resp.context['errors']['username'], 'Этот логин уже занят')
        self.user.refresh_from_db()
        self.assertEqual(self.user.username, 'postuser')

    def test_taken_email_is_error(self):
        resp = self.client_.post(
            self.URL, self._valid_post(email='takenuser@test.com'),
        )
        self.assertEqual(resp.context['errors']['email'], 'Этот email уже занят')

    def test_show_address_toggle(self):
        data = self._valid_post()
        data.pop('show_address')
        resp = self.client_.post(self.URL, data)
        self.assertFalse(resp.context['show_address_enabled'])
        self.user.refresh_from_db()
        self.assertIn('show_address=0', self.user.contacts)

        resp = self.client_.post(self.URL, self._valid_post())
        self.assertTrue(resp.context['show_address_enabled'])
        self.user.refresh_from_db()
        self.assertNotIn('show_address=0', self.user.contacts)

    def test_out_of_range_coords_ignored(self):
        self.client_.post(self.URL, self._valid_post(lat='95', lon='49.12'))
        self.user.refresh_from_db()
        self.assertIsNone(self.user.location)

    def test_garbage_coords_ignored(self):
        resp = self.client_.post(
            self.URL, self._valid_post(lat='мусор', lon='мусор'),
        )
        self.assertTrue(resp.context['saved'])
        self.user.refresh_from_db()
        self.assertIsNone(self.user.location)


@override_settings(CACHES=_DUMMY_CACHE)
class LegacyMeBulkAdvertsTests(TestCase):
    URL = '/me/adverts/bulk/'

    @classmethod
    def setUpTestData(cls):
        catalog = Catalog.objects.create(title='Балккаталог', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=catalog, title='Балккатегория', active=1,
        )

    def setUp(self):
        self.user = _make_user('bulkuser')
        self.other = _make_user('bulkother')
        self.mine = _make_advert(self.user, self.category, 'МоёДляБалка')
        self.foreign = _make_advert(self.other, self.category, 'ЧужоеДляБалка')
        self.client_ = Client()
        _login(self.client_, self.user)

    def _post(self, client=None, **data):
        return (client or self.client_).post(self.URL, data)

    def test_get_redirects(self):
        resp = self.client_.get(self.URL)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], '/me/')

    def test_anonymous_redirected_to_login(self):
        resp = Client().post(self.URL, {'action': 'hide', 'advert_id': ['1']})
        self.assertEqual(resp['Location'], '/login/?next=/me/')

    def test_hide_own_advert(self):
        resp = self._post(action='hide', advert_id=[str(self.mine.pk)])
        self.assertEqual(resp['Location'], '/me/')
        self.mine.refresh_from_db()
        self.assertEqual(self.mine.status, ADVERT_STATUS_HIDDEN)

    def test_delete_own_advert(self):
        self._post(action='delete', advert_id=[str(self.mine.pk)])
        self.mine.refresh_from_db()
        self.assertEqual(self.mine.status, ADVERT_STATUS_DELETED)

    def test_cannot_touch_foreign_advert(self):
        self._post(action='hide', advert_id=[str(self.foreign.pk)])
        self.foreign.refresh_from_db()
        self.assertEqual(self.foreign.status, ADVERT_STATUS_PUBLISHED)

    def test_publish_requires_admin(self):
        self.mine.status = ADVERT_STATUS_MODERATION
        self.mine.save(update_fields=['status'])
        self._post(action='publish', advert_id=[str(self.mine.pk)])
        self.mine.refresh_from_db()
        self.assertEqual(self.mine.status, ADVERT_STATUS_MODERATION)

    def test_admin_can_publish_foreign(self):
        self.foreign.status = ADVERT_STATUS_MODERATION
        self.foreign.save(update_fields=['status'])
        admin_client = Client()
        _login(admin_client, _make_user('admin'))
        self._post(
            client=admin_client, action='publish',
            advert_id=[str(self.foreign.pk)],
        )
        self.foreign.refresh_from_db()
        self.assertEqual(self.foreign.status, ADVERT_STATUS_PUBLISHED)

    def test_bump_updates_only_published(self):
        # Сравниваем значения, прочитанные из БД: легаси-колонка может
        # быть naive и несравнима с aware timezone.now().
        old = timezone.now() - timezone.timedelta(days=5)
        Advert.objects.filter(pk=self.mine.pk).update(created_at=old, updated_at=old)
        self.mine.refresh_from_db()
        old_db = self.mine.created_at
        self._post(action='bump', advert_id=[str(self.mine.pk)])
        self.mine.refresh_from_db()
        self.assertGreater(self.mine.created_at, old_db)

        hidden = _make_advert(
            self.user, self.category, 'СкрытоБамп', status=ADVERT_STATUS_HIDDEN,
        )
        Advert.objects.filter(pk=hidden.pk).update(created_at=old, updated_at=old)
        hidden.refresh_from_db()
        old_db = hidden.created_at
        self._post(action='bump', advert_id=[str(hidden.pk)])
        hidden.refresh_from_db()
        self.assertEqual(hidden.created_at, old_db)

    def test_unknown_action_is_noop(self):
        self._post(action='explode', advert_id=[str(self.mine.pk)])
        self.mine.refresh_from_db()
        self.assertEqual(self.mine.status, ADVERT_STATUS_PUBLISHED)

    def test_garbage_ids_ignored(self):
        resp = self._post(action='hide', advert_id=['garbage', '-5', '0'])
        self.assertEqual(resp['Location'], '/me/')

    def test_safe_next_redirect(self):
        resp = self._post(
            action='hide', advert_id=[str(self.mine.pk)],
            next='/me/?section=adverts',
        )
        self.assertEqual(resp['Location'], '/me/?section=adverts')

    def test_unsafe_next_ignored(self):
        resp = self._post(
            action='hide', advert_id=[str(self.mine.pk)],
            next='https://evil.example.com/',
        )
        self.assertEqual(resp['Location'], '/me/')
