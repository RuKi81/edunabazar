"""
Тесты пользовательских сценариев объявлений (``legacy/views/adverts.py``)
— страховочная сетка перед рефакторингом.

Покрывается: видимость по статусам в списке, фильтры, redirect со старых
GET-параметров на slug-URL, подача объявления (модерация), редактирование
и права, bump только для опубликованных, счётчик просмотров.
"""
from django.contrib.gis.geos import Point
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import (
    ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION, ADVERT_STATUS_PUBLISHED,
    USER_STATUS_ACTIVE,
)
from .models import Advert, AdvertView, Catalog, Categories, LegacyUser

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _make_user(username='seller', email=None):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=email or f'{username}@test.com', currency='RU', name='',
        address='', phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )


def _make_advert(author, category, title='Пшеница', status=ADVERT_STATUS_PUBLISHED,
                 **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, category=category, author=author,
        location=Point(37.6, 55.7, srid=4326), contacts='+79001234567',
        title=title, text='Описание', price=100, price_unit='кг',
        wholesale_price=0, min_volume=0, wholesale_volume=0, volume=10,
        priority=0, created_at=now, updated_at=now, status=status,
        address='Москва',
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
    return client


@override_settings(CACHES=_DUMMY_CACHE)
class AdvertListVisibilityTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('seller1')
        cls.catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=cls.catalog, title='Пшеница', active=1,
        )
        cls.published = _make_advert(cls.user, cls.category, title='Опубликовано')
        cls.moderation = _make_advert(
            cls.user, cls.category, title='НаМодерации', status=ADVERT_STATUS_MODERATION,
        )
        cls.hidden = _make_advert(
            cls.user, cls.category, title='Скрытое', status=ADVERT_STATUS_HIDDEN,
        )

    def test_anonymous_sees_only_published(self):
        resp = Client().get('/adverts/')
        self.assertContains(resp, 'Опубликовано')
        self.assertNotContains(resp, 'НаМодерации')
        self.assertNotContains(resp, 'Скрытое')

    def test_admin_sees_unpublished(self):
        admin = _make_user('admin', email='admin@test.com')
        client = _login(Client(), admin)
        resp = client.get('/adverts/')
        self.assertContains(resp, 'Опубликовано')
        self.assertContains(resp, 'НаМодерации')
        self.assertContains(resp, 'Скрытое')

    def test_type_filter(self):
        _make_advert(self.user, self.category, title='СпросНаЗерно', type=1)
        resp = Client().get('/adverts/', {'type': 'demand'})
        self.assertContains(resp, 'СпросНаЗерно')
        self.assertNotContains(resp, 'Опубликовано')

    def test_opt_filter(self):
        _make_advert(
            self.user, self.category, title='ОптоваяПартия', wholesale_price=90,
        )
        resp = Client().get('/adverts/', {'opt': '1'})
        self.assertContains(resp, 'ОптоваяПартия')
        self.assertNotContains(resp, 'Опубликовано')

    def test_delivery_filter(self):
        _make_advert(self.user, self.category, title='СДоставкой', delivery=True)
        resp = Client().get('/adverts/', {'delivery': '1'})
        self.assertContains(resp, 'СДоставкой')
        self.assertNotContains(resp, 'Опубликовано')

    def test_invalid_page_size_falls_back(self):
        resp = Client().get('/adverts/', {'page_size': '9999'})
        self.assertEqual(resp.status_code, 200)

    def test_unknown_catalog_slug_404(self):
        resp = Client().get('/adverts/no-such-catalog/')
        self.assertEqual(resp.status_code, 404)

    def test_legacy_get_params_redirect_to_slug(self):
        resp = Client().get('/adverts/', {'catalog': str(self.catalog.pk)})
        self.assertEqual(resp.status_code, 301)
        self.assertEqual(resp.url, '/adverts/zerno/')

    def test_legacy_category_param_redirects_to_slug(self):
        resp = Client().get('/adverts/', {'category': str(self.category.pk)})
        self.assertEqual(resp.status_code, 301)
        self.assertEqual(resp.url, '/adverts/zerno/pshenitsa/')


@override_settings(CACHES=_DUMMY_CACHE)
class AdvertCreateTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('creator')
        cls.catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=cls.catalog, title='Пшеница', active=1,
        )

    def _valid_payload(self, **overrides):
        data = {
            'type': '0',
            'category': str(self.category.pk),
            'title': 'Новое объявление',
            'text': 'Описание товара',
            'contacts': '+79001234567',
            'address': 'Москва',
            'price': '100',
            'price_unit': 'кг',
            'volume': '10',
            'lat': '55.7',
            'lon': '37.6',
        }
        data.update(overrides)
        return data

    def test_anonymous_redirected_to_login(self):
        resp = Client().get('/adverts/add/')
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp.url.startswith('/login/'))

    def test_create_goes_to_moderation(self):
        client = _login(Client(), self.user)
        resp = client.post('/adverts/add/', self._valid_payload())
        self.assertEqual(resp.status_code, 302)
        advert = Advert.objects.get(title='Новое объявление')
        self.assertEqual(advert.status, ADVERT_STATUS_MODERATION)
        self.assertEqual(advert.author_id, self.user.pk)
        self.assertEqual(resp.url, f'/adverts/{advert.pk}/')

    def test_create_requires_location(self):
        client = _login(Client(), self.user)
        resp = client.post('/adverts/add/', self._valid_payload(lat='', lon=''))
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(Advert.objects.filter(title='Новое объявление').exists())

    def test_create_validates_category(self):
        client = _login(Client(), self.user)
        resp = client.post('/adverts/add/', self._valid_payload(category='999999'))
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(Advert.objects.filter(title='Новое объявление').exists())

    def test_create_requires_title_and_text(self):
        client = _login(Client(), self.user)
        resp = client.post('/adverts/add/', self._valid_payload(title='', text=''))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(Advert.objects.count(), 0)


@override_settings(CACHES=_DUMMY_CACHE)
class AdvertEditPermissionTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.owner = _make_user('owner')
        cls.stranger = _make_user('stranger')
        catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=catalog, title='Пшеница', active=1,
        )
        cls.advert = _make_advert(cls.owner, cls.category, title='Моё объявление')

    def _edit_payload(self, **overrides):
        data = {
            'type': '0',
            'category': str(self.category.pk),
            'title': 'Обновлённый заголовок',
            'text': 'Обновлённое описание',
            'contacts': '+79001234567',
            'address': 'Москва',
            'price': '200',
            'price_unit': 'кг',
            'lat': '55.7',
            'lon': '37.6',
        }
        data.update(overrides)
        return data

    def test_owner_can_edit(self):
        client = _login(Client(), self.owner)
        resp = client.post(f'/adverts/{self.advert.pk}/edit/', self._edit_payload())
        self.assertEqual(resp.status_code, 302)
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.title, 'Обновлённый заголовок')
        self.assertEqual(self.advert.price, 200)

    def test_stranger_cannot_edit(self):
        client = _login(Client(), self.stranger)
        resp = client.post(f'/adverts/{self.advert.pk}/edit/', self._edit_payload())
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, f'/adverts/{self.advert.pk}/')
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.title, 'Моё объявление')

    def test_stranger_cannot_bump(self):
        client = _login(Client(), self.stranger)
        before = Advert.objects.get(pk=self.advert.pk).updated_at
        client.post(f'/adverts/{self.advert.pk}/bump/')
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.updated_at, before)

    def test_bump_only_published(self):
        Advert.objects.filter(pk=self.advert.pk).update(
            status=ADVERT_STATUS_HIDDEN,
        )
        before = Advert.objects.get(pk=self.advert.pk).updated_at
        client = _login(Client(), self.owner)
        client.post(f'/adverts/{self.advert.pk}/bump/')
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.updated_at, before)


@override_settings(CACHES=_DUMMY_CACHE)
class AdvertDetailViewCountTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('viewer-seller')
        catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        category = Categories.objects.create(
            catalog=catalog, title='Пшеница', active=1,
        )
        cls.advert = _make_advert(cls.user, category)

    def test_view_counted_once_per_ip_per_day(self):
        client = Client()
        client.get(f'/adverts/{self.advert.pk}/')
        client.get(f'/adverts/{self.advert.pk}/')
        self.assertEqual(
            AdvertView.objects.filter(advert_id=self.advert.pk).count(), 1,
        )

    def test_404_for_missing_advert(self):
        resp = Client().get('/adverts/999999/')
        self.assertEqual(resp.status_code, 404)
