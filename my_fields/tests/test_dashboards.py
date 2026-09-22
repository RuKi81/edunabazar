"""Тесты страницы «Дашборды» и справочника районов для её селектов.

Фиксируем:

* гейт страницы ``/me/gis/dashboards/`` совпадает с ``/me/gis`` —
  аноним уходит на логин, пользователь без грантов получает 404,
  админ и обладатель ГИС-гранта — 200;
* страница содержит три селекта (регион/район/слой) и server-rendered
  список регионов;
* ``/me/gis/api/districts/`` отдаёт только ``id``/``name`` районов
  запрошенного региона, пустой список без параметра и 401/403 без
  доступа.
"""
import json

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase
from django.utils import timezone

from access.models import ResourceGrant
from agrocosmos.models import District, Region
from legacy.models import LegacyUser
from my_fields.models import GisLayer

User = get_user_model()

PAGE = '/me/gis/dashboards/'
DISTRICTS = '/me/gis/api/districts/'


def _mk_legacy(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RUB', name=username,
        address='', phone='', inn='', status=10,
        created_at=now, updated_at=now, contacts='',
    )


def _square(x, y):
    return MultiPolygon(Polygon((
        (x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1), (x, y),
    )))


class DashboardsPageTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.roles = {}
        for name in ('admin', 'viewer', 'nobody'):
            dj = User.objects.create_user(name, password='x')
            cls.roles[name] = (dj, _mk_legacy(name))

        _, viewer_lu = cls.roles['viewer']
        ResourceGrant.objects.create(
            legacy_user=viewer_lu,
            resource_type=ResourceGrant.ResourceType.GIS_LAYER,
            resource_id=None, level='view',
        )

        cls.region = Region.objects.create(
            name='Тульская область', code='RU-TUL', geom=_square(37, 54),
        )
        cls.other_region = Region.objects.create(
            name='Рязанская область', code='RU-RYA', geom=_square(39, 54),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Богородицкий район', geom=_square(37, 54),
        )
        District.objects.create(
            region=cls.region, name='Алексинский район', geom=_square(37, 54),
        )
        District.objects.create(
            region=cls.other_region, name='Касимовский район', geom=_square(39, 54),
        )

        cls.layer = GisLayer.objects.create(
            title='Поля хозяйства', table_name='gis_up_dash', original_filename='d.shp',
            geom_kind='polygon', feature_count=7, color='#333333', sort_order=0,
        )

    def _login(self, role):
        dj, lu = self.roles[role]
        self.client.force_login(dj)
        session = self.client.session
        session['legacy_user_id'] = lu.pk
        session.save()

    # ── Гейт страницы ──
    def test_anonymous_redirected_to_login(self):
        resp = self.client.get(PAGE)
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/login/', resp['Location'])

    def test_user_without_grants_404(self):
        self._login('nobody')
        self.assertEqual(self.client.get(PAGE).status_code, 404)

    def test_admin_opens_page(self):
        self._login('admin')
        resp = self.client.get(PAGE)
        self.assertEqual(resp.status_code, 200)

    def test_gis_grant_opens_page(self):
        self._login('viewer')
        self.assertEqual(self.client.get(PAGE).status_code, 200)

    # ── Содержимое страницы ──
    def test_page_has_three_selects_and_regions(self):
        self._login('admin')
        html = self.client.get(PAGE).content.decode()
        for sel_id in ('dash-region', 'dash-district', 'dash-layer'):
            self.assertIn(f'id="{sel_id}"', html)
        # Регионы рендерятся на сервере, оба — в алфавитном порядке.
        self.assertIn('Тульская область', html)
        self.assertIn('Рязанская область', html)
        self.assertLess(html.index('Рязанская область'), html.index('Тульская область'))

    def test_gis_page_links_to_dashboards(self):
        self._login('admin')
        html = self.client.get('/me/gis/').content.decode()
        self.assertIn(PAGE, html)
        self.assertIn('Дашборды', html)

    # ── Справочник районов ──
    def test_districts_anonymous_401(self):
        self.assertEqual(self.client.get(DISTRICTS).status_code, 401)

    def test_districts_without_grants_403(self):
        self._login('nobody')
        self.assertEqual(self.client.get(DISTRICTS).status_code, 403)

    def test_districts_of_region_sorted_by_name(self):
        self._login('viewer')
        resp = self.client.get(DISTRICTS, {'region': self.region.pk})
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.content)
        self.assertTrue(data['ok'])
        self.assertEqual(
            [d['name'] for d in data['results']],
            ['Алексинский район', 'Богородицкий район'],
        )
        # Никакой геометрии в ответе — только id/name.
        self.assertEqual(set(data['results'][0].keys()), {'id', 'name'})

    def test_districts_empty_without_region(self):
        self._login('viewer')
        data = json.loads(self.client.get(DISTRICTS).content)
        self.assertEqual(data['results'], [])

    def test_districts_garbage_region_is_empty(self):
        self._login('viewer')
        data = json.loads(self.client.get(DISTRICTS, {'region': 'abc'}).content)
        self.assertEqual(data['results'], [])

    def test_districts_post_not_allowed(self):
        self._login('viewer')
        self.assertEqual(self.client.post(DISTRICTS).status_code, 405)

    # ── Слои для третьего селекта ──
    def test_layers_list_feeds_layer_select(self):
        """Третий селект наполняется из существующего реестра слоёв —
        встроенных «ЗСН» и «Мои поля» там нет по построению."""
        self._login('viewer')
        data = json.loads(self.client.get('/me/gis/api/layers/').content)
        titles = [row['title'] for row in data['results']]
        self.assertIn('Поля хозяйства', titles)
        self.assertNotIn('ЗСН (Росреестр)', titles)
        self.assertNotIn('Мои поля', titles)
