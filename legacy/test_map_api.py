"""
Тесты API карты объявлений (``legacy/views/map.py``) — страховочная
сетка перед рефакторингом.

Покрывается: формат ответа, видимость по статусам (anon vs admin),
bbox-фильтрация, фильтры type/catalog/category/opt/delivery,
ограничение limit и устойчивость к мусорным параметрам.
"""
from django.contrib.gis.geos import Point
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_PUBLISHED,
    USER_STATUS_ACTIVE,
)
from .models import Advert, Catalog, Categories, LegacyUser

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _make_user(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )


def _make_advert(author, category, title, lon=37.6, lat=55.7,
                 status=ADVERT_STATUS_PUBLISHED, **overrides):
    now = timezone.now()
    kwargs = dict(
        type=0, category=category, author=author,
        location=Point(lon, lat, srid=4326), contacts='', title=title,
        text='Описание объявления на карте', price=100, wholesale_price=0,
        min_volume=0, wholesale_volume=0, volume=0, priority=0,
        created_at=now, updated_at=now, status=status,
    )
    kwargs.update(overrides)
    return Advert.objects.create(**kwargs)


@override_settings(CACHES=_DUMMY_CACHE)
class MapAdvertsApiTests(TestCase):
    URL = '/api/map/adverts/'

    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('mapseller')
        cls.catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=cls.catalog, title='Пшеница', active=1,
        )
        cls.other_catalog = Catalog.objects.create(title='Техника', sort=1, active=1)
        cls.other_category = Categories.objects.create(
            catalog=cls.other_catalog, title='Трактора', active=1,
        )
        # Москва (внутри тестового bbox) и Новосибирск (снаружи)
        cls.moscow = _make_advert(cls.user, cls.category, 'МоскваОбъявление')
        cls.novosib = _make_advert(
            cls.user, cls.other_category, 'НовосибОбъявление', lon=82.9, lat=55.0,
        )
        cls.hidden = _make_advert(
            cls.user, cls.category, 'СкрытоеОбъявление',
            status=ADVERT_STATUS_HIDDEN,
        )
        cls.deleted = _make_advert(
            cls.user, cls.category, 'УдалённоеОбъявление',
            status=ADVERT_STATUS_DELETED,
        )

    def _titles(self, resp):
        return [a['title'] for a in resp.json()['adverts']]

    def test_response_shape(self):
        resp = Client().get(self.URL)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        advert = next(a for a in data['adverts'] if a['title'] == 'МоскваОбъявление')
        for key in ('id', 'lat', 'lon', 'category_id', 'category_title',
                    'price', 'text_short', 'url', 'thumb_url', 'created_date',
                    'is_opt', 'is_delivery'):
            self.assertIn(key, advert)
        self.assertAlmostEqual(advert['lat'], 55.7, places=4)
        self.assertAlmostEqual(advert['lon'], 37.6, places=4)
        self.assertEqual(advert['url'], f'/adverts/{self.moscow.pk}/')

    def test_anonymous_sees_only_published(self):
        titles = self._titles(Client().get(self.URL))
        self.assertIn('МоскваОбъявление', titles)
        self.assertNotIn('СкрытоеОбъявление', titles)
        self.assertNotIn('УдалённоеОбъявление', titles)

    def test_admin_sees_hidden_but_not_deleted(self):
        admin = _make_user('admin')
        client = Client()
        client.get('/')
        session = client.session
        session['legacy_user_id'] = admin.pk
        session.save()
        from django.conf import settings as _s
        client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key

        titles = self._titles(client.get(self.URL))
        self.assertIn('СкрытоеОбъявление', titles)
        self.assertNotIn('УдалённоеОбъявление', titles)

    def test_bbox_filters_out_of_bounds(self):
        # bbox: sw_lat, sw_lon, ne_lat, ne_lon — только Москва
        resp = Client().get(self.URL, {'bbox': '55,37,56,38'})
        titles = self._titles(resp)
        self.assertIn('МоскваОбъявление', titles)
        self.assertNotIn('НовосибОбъявление', titles)

    def test_invalid_bbox_is_ignored(self):
        resp = Client().get(self.URL, {'bbox': 'not,a,bbox'})
        self.assertEqual(resp.status_code, 200)
        self.assertIn('МоскваОбъявление', self._titles(resp))

    def test_catalog_filter(self):
        resp = Client().get(self.URL, {'catalog': str(self.other_catalog.pk)})
        titles = self._titles(resp)
        self.assertEqual(titles, ['НовосибОбъявление'])

    def test_category_filter(self):
        resp = Client().get(self.URL, {'category': str(self.category.pk)})
        titles = self._titles(resp)
        self.assertIn('МоскваОбъявление', titles)
        self.assertNotIn('НовосибОбъявление', titles)

    def test_type_filter(self):
        _make_advert(self.user, self.category, 'СпросОбъявление', type=1)
        titles = self._titles(Client().get(self.URL, {'type': 'demand'}))
        self.assertEqual(titles, ['СпросОбъявление'])

    def test_opt_and_delivery_filters(self):
        _make_advert(
            self.user, self.category, 'ОптДоставка',
            wholesale_price=90, delivery=True,
        )
        titles = self._titles(Client().get(self.URL, {'opt': '1'}))
        self.assertEqual(titles, ['ОптДоставка'])
        titles = self._titles(Client().get(self.URL, {'delivery': '1'}))
        self.assertEqual(titles, ['ОптДоставка'])

    def test_limit_is_clamped(self):
        resp = Client().get(self.URL, {'limit': '999999'})
        self.assertEqual(resp.status_code, 200)
        resp = Client().get(self.URL, {'limit': 'garbage'})
        self.assertEqual(resp.status_code, 200)

    def test_text_short_truncated(self):
        _make_advert(
            self.user, self.category, 'ДлинныйТекст', text='х' * 500,
        )
        advert = next(
            a for a in Client().get(self.URL).json()['adverts']
            if a['title'] == 'ДлинныйТекст'
        )
        self.assertLessEqual(len(advert['text_short']), 161)
        self.assertTrue(advert['text_short'].endswith('…'))


@override_settings(CACHES=_DUMMY_CACHE)
class MapCategoriesApiTests(TestCase):
    def test_returns_active_categories(self):
        catalog = Catalog.objects.create(title='Зерно', sort=0, active=1)
        active = Categories.objects.create(catalog=catalog, title='Пшеница', active=1)
        Categories.objects.create(catalog=catalog, title='Неактивная', active=0)

        resp = Client().get('/api/map/categories/')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        ids = [c['id'] for c in data['items']]
        self.assertIn(active.pk, ids)
        self.assertEqual(len(ids), 1)
