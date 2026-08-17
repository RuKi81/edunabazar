"""Тесты пер-ресурсного доступа к ГИС-данным (app ``access``).

Фиксируем гейты ``my_fields.api._require_gis_access`` и страницы
``my_fields.views.gis_page`` на основе грантов ``ResourceGrant``:

* аноним → 401 на API, 404 на странице;
* пользователь без грантов → 403 на API, 404 на странице;
* view (весь класс) → видит все слои, тайлы OK, но upload/rename/delete/
  reorder запрещены;
* view на конкретный слой → в списке только он; тайлы своего слоя OK,
  чужого — 403;
* edit (весь класс) → rename + reorder разрешены, delete/upload — нет;
* manage (весь класс) → upload + delete разрешены;
* админ (username ∈ ADMIN_USERNAMES) → полный доступ.

Гранты вешаются на ``LegacyUser`` (каноничная личность портала); в
сессию кладём ``legacy_user_id`` — так же, как делает боевой логин.
Слои создаём напрямую в реестре (``GisLayer``) + пустые физические
PostGIS-таблицы, чтобы MVT-запрос разрешённого слоя выполнял валидный
SQL (вернёт пустой protobuf 200).
"""
import json

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.utils import timezone

from access.models import ResourceGrant
from legacy.models import LegacyUser
from my_fields.models import GisLayer

User = get_user_model()


def _mk_legacy(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RUB', name=username,
        address='', phone='', inn='', status=10,
        created_at=now, updated_at=now, contacts='',
    )


class GisAccessTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        # username 'admin' ∈ дефолтный ADMIN_USERNAMES → админ-гейт.
        cls.roles = {}
        for name in ('admin', 'viewer', 'perlayer', 'editor', 'manager', 'nobody'):
            dj = User.objects.create_user(name, password='x')
            lu = _mk_legacy(name)
            cls.roles[name] = (dj, lu)

        cls.layer_a = GisLayer.objects.create(
            title='Слой A', table_name='gis_up_a', original_filename='a.shp',
            geom_kind='polygon', feature_count=1, color='#111111', sort_order=0,
        )
        cls.layer_b = GisLayer.objects.create(
            title='Слой B', table_name='gis_up_b', original_filename='b.shp',
            geom_kind='polygon', feature_count=1, color='#222222', sort_order=1,
        )

        # Физические PostGIS-таблицы слоёв: без них MVT-запрос к
        # несуществующей таблице роняет тест-транзакцию (в бою каждый
        # запрос — отдельный autocommit, ошибка там безвредна).
        with connection.cursor() as cur:
            for table in ('gis_up_a', 'gis_up_b'):
                cur.execute(
                    f'CREATE TABLE {table} '
                    f'(id serial PRIMARY KEY, geom geometry(Geometry, 4326))'
                )

        GL = ResourceGrant.ResourceType.GIS_LAYER
        _, viewer_lu = cls.roles['viewer']
        _, perlayer_lu = cls.roles['perlayer']
        _, editor_lu = cls.roles['editor']
        _, manager_lu = cls.roles['manager']
        ResourceGrant.objects.create(legacy_user=viewer_lu, resource_type=GL, resource_id=None, level='view')
        ResourceGrant.objects.create(legacy_user=perlayer_lu, resource_type=GL, resource_id=cls.layer_a.pk, level='view')
        ResourceGrant.objects.create(legacy_user=editor_lu, resource_type=GL, resource_id=None, level='edit')
        ResourceGrant.objects.create(legacy_user=manager_lu, resource_type=GL, resource_id=None, level='manage')

    def _login(self, role):
        dj, lu = self.roles[role]
        self.client.force_login(dj)
        session = self.client.session
        session['legacy_user_id'] = lu.pk
        session.save()

    # ── URLs ──
    LIST = '/me/gis/api/layers/'
    REORDER = '/me/gis/api/layers/reorder/'
    PAGE = '/me/gis/'

    def _detail(self, pk):
        return f'/me/gis/api/layers/{pk}/'

    def _tiles(self, pk):
        return f'/me/gis/api/layers/{pk}/tiles/0/0/0.pbf'

    # ── Аноним / без грантов ──
    def test_anonymous_list_401(self):
        self.assertEqual(self.client.get(self.LIST).status_code, 401)

    def test_nobody_list_403_and_page_404(self):
        self._login('nobody')
        self.assertEqual(self.client.get(self.LIST).status_code, 403)
        self.assertEqual(self.client.get(self.PAGE).status_code, 404)

    # ── view: весь класс ──
    def test_viewer_sees_all_and_page_ok(self):
        self._login('viewer')
        resp = self.client.get(self.LIST)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['count'], 2)
        self.assertEqual(self.client.get(self.PAGE).status_code, 200)

    def test_viewer_cannot_mutate(self):
        self._login('viewer')
        # upload (manage)
        self.assertEqual(self.client.post(self.LIST, {}).status_code, 403)
        # rename (edit)
        r = self.client.patch(self._detail(self.layer_a.pk),
                              data=json.dumps({'title': 'x'}),
                              content_type='application/json')
        self.assertEqual(r.status_code, 403)
        # reorder (edit)
        r = self.client.post(self.REORDER,
                             data=json.dumps({'order': [self.layer_b.pk, self.layer_a.pk]}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 403)
        # delete (manage)
        self.assertEqual(self.client.delete(self._detail(self.layer_a.pk)).status_code, 403)

    # ── view: конкретный слой ──
    def test_perlayer_scoped_list_and_tiles(self):
        self._login('perlayer')
        resp = self.client.get(self.LIST)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body['count'], 1)
        self.assertEqual(body['results'][0]['id'], self.layer_a.pk)
        # тайлы своего слоя — гейт пройден (пустой protobuf 200, таблицы нет)
        ta = self.client.get(self._tiles(self.layer_a.pk))
        self.assertEqual(ta.status_code, 200)
        self.assertEqual(ta['Content-Type'], 'application/x-protobuf')
        # тайлы чужого слоя — 403 (тоже protobuf)
        tb = self.client.get(self._tiles(self.layer_b.pk))
        self.assertEqual(tb.status_code, 403)
        self.assertEqual(tb['Content-Type'], 'application/x-protobuf')

    def test_perlayer_page_ok(self):
        self._login('perlayer')
        self.assertEqual(self.client.get(self.PAGE).status_code, 200)

    # ── edit: весь класс ──
    def test_editor_can_rename_and_reorder_not_delete_or_upload(self):
        self._login('editor')
        r = self.client.patch(self._detail(self.layer_a.pk),
                              data=json.dumps({'title': 'Переименован'}),
                              content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.layer_a.refresh_from_db()
        self.assertEqual(self.layer_a.title, 'Переименован')

        r = self.client.post(self.REORDER,
                             data=json.dumps({'order': [self.layer_b.pk, self.layer_a.pk]}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)

        # delete/upload требуют manage → 403
        self.assertEqual(self.client.delete(self._detail(self.layer_a.pk)).status_code, 403)
        self.assertEqual(self.client.post(self.LIST, {}).status_code, 403)

    # ── manage: весь класс ──
    def test_manager_can_upload_gate_and_delete(self):
        self._login('manager')
        # upload без файлов: гейт пройден → 400 (no_files), не 403
        self.assertEqual(self.client.post(self.LIST, {}).status_code, 400)
        # delete конкретного слоя разрешён
        r = self.client.delete(self._detail(self.layer_b.pk))
        self.assertEqual(r.status_code, 200, r.content)
        self.assertFalse(GisLayer.objects.filter(pk=self.layer_b.pk).exists())

    # ── admin ──
    def test_admin_full_access(self):
        self._login('admin')
        self.assertEqual(self.client.get(self.LIST).json()['count'], 2)
        self.assertEqual(self.client.post(self.LIST, {}).status_code, 400)
        self.assertEqual(self.client.get(self.PAGE).status_code, 200)
        self.assertEqual(self.client.delete(self._detail(self.layer_a.pk)).status_code, 200)
