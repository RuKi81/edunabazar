"""
Тесты DRF-эндпоинта объявлений (``legacy/api.py::AdvertViewSet``) —
страховочная сетка перед рефакторингом ``get_queryset`` (C=11).

Покрывается: видимость по статусам (anon видит только опубликованные,
admin — всё кроме удалённых), фильтры q/type/catalog/category,
устойчивость к мусорным значениям фильтров, сортировка.
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

URL = '/api/v1/adverts/'


def _make_user(username='seller'):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
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


def _ids(resp):
    data = resp.json()
    results = data['results'] if isinstance(data, dict) and 'results' in data else data
    return {item['id'] for item in results}


@override_settings(CACHES=_DUMMY_CACHE)
class AdvertApiListTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('seller1')
        cls.grain = Catalog.objects.create(title='Зерно', sort=0, active=1)
        cls.dairy = Catalog.objects.create(title='Молочка', sort=1, active=1)
        cls.wheat = Categories.objects.create(
            catalog=cls.grain, title='Пшеница', active=1,
        )
        cls.milk = Categories.objects.create(
            catalog=cls.dairy, title='Молоко', active=1,
        )
        cls.published = _make_advert(cls.user, cls.wheat, title='Опубликовано')
        cls.hidden = _make_advert(
            cls.user, cls.wheat, title='Скрыто', status=ADVERT_STATUS_HIDDEN,
        )
        cls.moderation = _make_advert(
            cls.user, cls.wheat, title='Модерация', status=ADVERT_STATUS_MODERATION,
        )
        cls.deleted = _make_advert(
            cls.user, cls.wheat, title='Удалено', status=ADVERT_STATUS_DELETED,
        )
        cls.demand = _make_advert(
            cls.user, cls.milk, title='Куплю молоко', type=1,
            text='Ищу поставщика',
        )

    def test_anonymous_sees_only_published(self):
        ids = _ids(self.client.get(URL))
        self.assertEqual(ids, {self.published.pk, self.demand.pk})

    def test_regular_user_sees_only_published(self):
        client = _login(Client(), self.user)
        ids = _ids(client.get(URL))
        self.assertEqual(ids, {self.published.pk, self.demand.pk})

    def test_admin_sees_all_but_deleted(self):
        client = _login(Client(), _make_user('admin'))
        ids = _ids(client.get(URL))
        self.assertEqual(
            ids,
            {self.published.pk, self.hidden.pk, self.moderation.pk, self.demand.pk},
        )

    def test_search_filters_title_and_text(self):
        self.assertEqual(
            _ids(self.client.get(URL, {'q': 'опубликовано'})), {self.published.pk},
        )
        self.assertEqual(
            _ids(self.client.get(URL, {'q': 'поставщика'})), {self.demand.pk},
        )

    def test_type_filter(self):
        self.assertEqual(
            _ids(self.client.get(URL, {'type': 'offer'})), {self.published.pk},
        )
        self.assertEqual(
            _ids(self.client.get(URL, {'type': 'demand'})), {self.demand.pk},
        )
        # мусорное значение игнорируется
        self.assertEqual(
            _ids(self.client.get(URL, {'type': 'garbage'})),
            {self.published.pk, self.demand.pk},
        )

    def test_category_filter(self):
        self.assertEqual(
            _ids(self.client.get(URL, {'category': str(self.milk.pk)})),
            {self.demand.pk},
        )

    def test_catalog_filter(self):
        self.assertEqual(
            _ids(self.client.get(URL, {'catalog': str(self.grain.pk)})),
            {self.published.pk},
        )

    def test_garbage_catalog_and_category_ignored(self):
        ids = _ids(self.client.get(URL, {'catalog': 'abc', 'category': 'xyz'}))
        self.assertEqual(ids, {self.published.pk, self.demand.pk})

    def test_ordering_by_updated_at_desc(self):
        newer = _make_advert(
            self.user, self.wheat, title='Свежее',
            updated_at=timezone.now() + timezone.timedelta(hours=1),
        )
        resp = self.client.get(URL)
        data = resp.json()
        results = data['results'] if isinstance(data, dict) and 'results' in data else data
        self.assertEqual(results[0]['id'], newer.pk)
