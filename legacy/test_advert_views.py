"""
Тесты каталога и редактирования объявлений
(``legacy/views/adverts.py``) — страховочная сетка перед рефакторингом
``advert_list`` (C=28) и ``advert_edit`` (C=13).

Покрывается: видимость по статусам (anon vs admin), slug-URL и 301-редиректы
со старых GET-параметров, фильтры type/opt/delivery, валидация sort/page_size,
права доступа к редактированию, префилл формы, обновление полей и удаление фото.
"""
from django.contrib.gis.geos import Point
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from .constants import (
    ADVERT_STATUS_DELETED, ADVERT_STATUS_HIDDEN, ADVERT_STATUS_MODERATION,
    ADVERT_STATUS_PUBLISHED, USER_STATUS_ACTIVE,
)
from .models import Advert, AdvertPhoto, Catalog, Categories, LegacyUser
from .slug_utils import slugify_ru

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
class AdvertListTests(TestCase):
    URL = '/adverts/'

    @classmethod
    def setUpTestData(cls):
        cls.user = _make_user('listseller')
        cls.catalog = Catalog.objects.create(title='Тестзерно', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=cls.catalog, title='Тестпшеница', active=1,
        )
        cls.other_catalog = Catalog.objects.create(
            title='Тесттехника', sort=1, active=1,
        )
        cls.other_category = Categories.objects.create(
            catalog=cls.other_catalog, title='Тесттрактора', active=1,
        )
        cls.catalog_slug = slugify_ru(cls.catalog.title)
        cls.category_slug = slugify_ru(cls.category.title)

        cls.published = _make_advert(cls.user, cls.category, 'Опубликовано')
        cls.other = _make_advert(
            cls.user, cls.other_category, 'ДругойКаталог',
        )
        cls.hidden = _make_advert(
            cls.user, cls.category, 'Скрыто', status=ADVERT_STATUS_HIDDEN,
        )
        cls.moderation = _make_advert(
            cls.user, cls.category, 'НаМодерации',
            status=ADVERT_STATUS_MODERATION,
        )
        cls.deleted = _make_advert(
            cls.user, cls.category, 'Удалено', status=ADVERT_STATUS_DELETED,
        )

    def _titles(self, resp):
        return [a.title for a in resp.context['adverts']]

    def test_anonymous_sees_only_published(self):
        resp = Client().get(self.URL)
        self.assertEqual(resp.status_code, 200)
        titles = self._titles(resp)
        self.assertIn('Опубликовано', titles)
        self.assertNotIn('Скрыто', titles)
        self.assertNotIn('НаМодерации', titles)
        self.assertNotIn('Удалено', titles)

    def test_admin_sees_all_but_deleted(self):
        admin = _make_user('admin')
        client = Client()
        _login(client, admin)
        titles = self._titles(client.get(self.URL))
        self.assertIn('Опубликовано', titles)
        self.assertIn('Скрыто', titles)
        self.assertIn('НаМодерации', titles)
        self.assertNotIn('Удалено', titles)

    def test_catalog_slug_filters(self):
        resp = Client().get(f'/adverts/{self.catalog_slug}/')
        self.assertEqual(resp.status_code, 200)
        titles = self._titles(resp)
        self.assertIn('Опубликовано', titles)
        self.assertNotIn('ДругойКаталог', titles)
        self.assertEqual(resp.context['catalog_id'], self.catalog.pk)

    def test_category_slug_filters(self):
        resp = Client().get(f'/adverts/{self.catalog_slug}/{self.category_slug}/')
        self.assertEqual(resp.status_code, 200)
        titles = self._titles(resp)
        self.assertIn('Опубликовано', titles)
        self.assertNotIn('ДругойКаталог', titles)
        self.assertEqual(resp.context['category_id'], self.category.pk)

    def test_unknown_catalog_slug_404(self):
        self.assertEqual(
            Client().get('/adverts/no-such-catalog/').status_code, 404,
        )

    def test_unknown_category_slug_404(self):
        resp = Client().get(f'/adverts/{self.catalog_slug}/no-such-category/')
        self.assertEqual(resp.status_code, 404)

    def test_legacy_catalog_param_redirects_to_slug(self):
        resp = Client().get(self.URL, {'catalog': str(self.catalog.pk), 'sort': 'price'})
        self.assertEqual(resp.status_code, 301)
        self.assertEqual(resp['Location'], f'/adverts/{self.catalog_slug}/?sort=price')

    def test_legacy_category_param_redirects_to_slug(self):
        resp = Client().get(
            self.URL,
            {'catalog': str(self.catalog.pk), 'category': str(self.category.pk)},
        )
        self.assertEqual(resp.status_code, 301)
        self.assertEqual(
            resp['Location'],
            f'/adverts/{self.catalog_slug}/{self.category_slug}/',
        )

    def test_unknown_catalog_id_filters_without_redirect(self):
        resp = Client().get(self.URL, {'catalog': '999999'})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self._titles(resp), [])

    def test_garbage_catalog_param_ignored(self):
        resp = Client().get(self.URL, {'catalog': 'мусор'})
        self.assertEqual(resp.status_code, 200)
        self.assertIn('Опубликовано', self._titles(resp))

    def test_type_filter(self):
        _make_advert(self.user, self.category, 'Спрос', type=1)
        titles = self._titles(Client().get(self.URL, {'type': 'demand'}))
        self.assertEqual(titles, ['Спрос'])
        titles = self._titles(Client().get(self.URL, {'type': 'offer'}))
        self.assertNotIn('Спрос', titles)
        self.assertIn('Опубликовано', titles)

    def test_opt_and_delivery_filters(self):
        _make_advert(
            self.user, self.category, 'ОптДоставка',
            wholesale_price=90, delivery=True,
        )
        titles = self._titles(Client().get(self.URL, {'opt': '1'}))
        self.assertEqual(titles, ['ОптДоставка'])
        titles = self._titles(Client().get(self.URL, {'delivery': '1'}))
        self.assertEqual(titles, ['ОптДоставка'])

    def test_invalid_sort_falls_back_to_id(self):
        resp = Client().get(self.URL, {'sort': 'bogus'})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context['sort'], 'id')

    def test_sort_price_accepted(self):
        resp = Client().get(self.URL, {'sort': 'price'})
        self.assertEqual(resp.context['sort'], 'price')

    def test_page_size_validation(self):
        resp = Client().get(self.URL, {'page_size': '24'})
        self.assertEqual(resp.context['page_size'], 24)
        resp = Client().get(self.URL, {'page_size': '999'})
        self.assertEqual(resp.context['page_size'], 12)
        resp = Client().get(self.URL, {'page_size': 'garbage'})
        self.assertEqual(resp.context['page_size'], 12)

    def test_garbage_page_param_is_safe(self):
        resp = Client().get(self.URL, {'page': 'garbage'})
        self.assertEqual(resp.status_code, 200)

    def test_search_smoke(self):
        resp = Client().get(self.URL, {'q': 'пшеница'})
        self.assertEqual(resp.status_code, 200)


@override_settings(CACHES=_DUMMY_CACHE)
class AdvertEditTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.author = _make_user('editauthor')
        cls.stranger = _make_user('editstranger')
        cls.catalog = Catalog.objects.create(title='Тестовощи', sort=0, active=1)
        cls.category = Categories.objects.create(
            catalog=cls.catalog, title='Тестогурцы', active=1,
        )
        cls.new_category = Categories.objects.create(
            catalog=cls.catalog, title='Тестпомидоры', active=1,
        )

    def setUp(self):
        self.advert = _make_advert(
            self.author, self.category, 'Исходный заголовок', price=100,
        )
        self.url = f'/adverts/{self.advert.pk}/edit/'

    def _valid_post(self, **overrides):
        data = {
            'type': '0',
            'category': str(self.new_category.pk),
            'title': 'Новый заголовок',
            'text': 'Новое описание',
            'contacts': '+7 911 111-11-11',
            'address': 'Санкт-Петербург',
            'price': '250,5',
            'price_unit': 'т',
            'volume': '10',
            'min_volume': '1',
            'wholesale_volume': '5',
            'opt': '1',
            'delivery': '1',
            'lat': '59.93',
            'lon': '30.31',
        }
        data.update(overrides)
        return data

    def test_anonymous_redirected_to_login(self):
        resp = Client().get(self.url)
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp['Location'].startswith('/login/?next='))

    def test_stranger_redirected_to_detail(self):
        client = Client()
        _login(client, self.stranger)
        resp = client.get(self.url)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], f'/adverts/{self.advert.pk}/')

    def test_author_get_form_prefilled(self):
        client = Client()
        _login(client, self.author)
        resp = client.get(self.url)
        self.assertEqual(resp.status_code, 200)
        form = resp.context['form']
        self.assertEqual(form['title'], 'Исходный заголовок')
        self.assertEqual(form['category'], self.category.pk)
        self.assertAlmostEqual(form['lat'], 55.7, places=4)
        self.assertAlmostEqual(form['lon'], 37.6, places=4)
        self.assertFalse(form['opt'])

    def test_admin_can_open_form(self):
        admin = _make_user('admin')
        client = Client()
        _login(client, admin)
        self.assertEqual(client.get(self.url).status_code, 200)

    def test_author_post_updates_advert(self):
        client = Client()
        _login(client, self.author)
        resp = client.post(self.url, self._valid_post())
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp['Location'], f'/adverts/{self.advert.pk}/')
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.title, 'Новый заголовок')
        self.assertEqual(self.advert.category_id, self.new_category.pk)
        self.assertEqual(float(self.advert.price), 250.5)
        # opt='1' → wholesale_price копирует price
        self.assertEqual(float(self.advert.wholesale_price), 250.5)
        self.assertTrue(self.advert.delivery)
        self.assertAlmostEqual(self.advert.location.y, 59.93, places=4)
        self.assertAlmostEqual(self.advert.location.x, 30.31, places=4)

    def test_post_without_opt_zeroes_wholesale_price(self):
        client = Client()
        _login(client, self.author)
        data = self._valid_post()
        data.pop('opt')
        client.post(self.url, data)
        self.advert.refresh_from_db()
        self.assertEqual(float(self.advert.wholesale_price), 0.0)

    def test_invalid_post_shows_errors_and_keeps_advert(self):
        client = Client()
        _login(client, self.author)
        resp = client.post(self.url, self._valid_post(title=''))
        self.assertEqual(resp.status_code, 200)
        self.assertIn('title', resp.context['errors'])
        self.advert.refresh_from_db()
        self.assertEqual(self.advert.title, 'Исходный заголовок')

    def test_missing_coords_is_error(self):
        client = Client()
        _login(client, self.author)
        resp = client.post(self.url, self._valid_post(lat='', lon=''))
        self.assertEqual(resp.status_code, 200)
        self.assertIn('lat', resp.context['errors'])

    def test_delete_photo(self):
        photo = AdvertPhoto.objects.create(
            advert=self.advert, sort=0, image='adverts/test.jpg',
        )
        keep = AdvertPhoto.objects.create(
            advert=self.advert, sort=1, image='adverts/keep.jpg',
        )
        client = Client()
        _login(client, self.author)
        data = self._valid_post()
        data['delete_photo'] = [str(photo.pk), 'garbage']
        resp = client.post(self.url, data)
        self.assertEqual(resp.status_code, 302)
        ids = set(
            AdvertPhoto.objects.filter(advert=self.advert).values_list('id', flat=True),
        )
        self.assertEqual(ids, {keep.pk})
