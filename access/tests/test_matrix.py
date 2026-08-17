"""Тесты матрицы прав к ГИС-слоям (access.admin.ResourceGrantAdmin).

Матрица: `/admin/access/resourcegrant/matrix/`. Строки — пользователи,
колонки — «Все ГИС-слои» + каждый слой, под каждым чекбоксы view/edit/manage.
Уровни кумулятивны, хранится один ``ResourceGrant`` на пару с максимальным
отмеченным уровнем. Пустой ряд = отзыв доступа.
"""
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from access.models import ResourceGrant
from legacy.models import LegacyUser
from my_fields.models import GisLayer

User = get_user_model()
GL = ResourceGrant.ResourceType.GIS_LAYER


def _mk_legacy(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RUB', name=username,
        address='', phone='', inn='', status=10,
        created_at=now, updated_at=now, contacts='',
    )


class MatrixTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin_dj = User.objects.create_superuser('root', 'r@t.com', 'x')
        cls.admin_lu = _mk_legacy('root')
        cls.target = _mk_legacy('grantee')
        cls.layer_a = GisLayer.objects.create(
            title='Слой A', table_name='gis_up_a', original_filename='a.shp',
            geom_kind='polygon', feature_count=1, color='#111', sort_order=0)
        cls.layer_b = GisLayer.objects.create(
            title='Слой B', table_name='gis_up_b', original_filename='b.shp',
            geom_kind='polygon', feature_count=1, color='#222', sort_order=1)

    def setUp(self):
        self.url = reverse('admin:access_resourcegrant_matrix')

    def _login_admin(self):
        self.client.force_login(self.admin_dj)
        session = self.client.session
        session['legacy_user_id'] = self.admin_lu.pk
        session.save()

    def _post(self, **checkboxes):
        data = {'user_ids': [self.target.pk]}
        data.update({k: 'on' for k in checkboxes})
        return self.client.post(self.url, data)

    # ── доступ к странице ──────────────────────────────────────────────
    def test_anonymous_redirected_to_login(self):
        resp = self.client.get(self.url)
        self.assertIn(resp.status_code, (302, 403))

    def test_non_staff_denied(self):
        dj = User.objects.create_user('joe', password='x')
        self.client.force_login(dj)
        resp = self.client.get(self.url)
        self.assertIn(resp.status_code, (302, 403))

    def test_admin_get_renders_layers_and_levels(self):
        self._login_admin()
        resp = self.client.get(self.url)
        self.assertEqual(resp.status_code, 200)
        body = resp.content.decode()
        self.assertIn('Слой A', body)
        self.assertIn('Слой B', body)
        self.assertIn('Все ГИС-слои', body)

    def test_changelist_has_matrix_link(self):
        self._login_admin()
        resp = self.client.get(reverse('admin:access_resourcegrant_changelist'))
        self.assertContains(resp, self.url)

    # ── создание грантов ───────────────────────────────────────────────
    def test_grant_view_on_specific_layer(self):
        self._login_admin()
        self._post(**{f'g_{self.target.pk}_{self.layer_a.pk}_view': 1})
        g = ResourceGrant.objects.get(
            legacy_user=self.target, resource_type=GL, resource_id=self.layer_a.pk)
        self.assertEqual(g.level, 'view')
        self.assertEqual(g.granted_by_id, self.admin_lu.pk)

    def test_grant_whole_class(self):
        self._login_admin()
        self._post(**{f'g_{self.target.pk}_all_edit': 1})
        g = ResourceGrant.objects.get(
            legacy_user=self.target, resource_type=GL, resource_id__isnull=True)
        self.assertEqual(g.level, 'edit')

    def test_highest_checked_level_wins(self):
        # Отмечены view+edit+manage → хранится один грант manage.
        self._login_admin()
        p = self.layer_a.pk
        self._post(**{
            f'g_{self.target.pk}_{p}_view': 1,
            f'g_{self.target.pk}_{p}_edit': 1,
            f'g_{self.target.pk}_{p}_manage': 1,
        })
        grants = ResourceGrant.objects.filter(
            legacy_user=self.target, resource_type=GL, resource_id=p)
        self.assertEqual(grants.count(), 1)
        self.assertEqual(grants.first().level, 'manage')

    def test_manage_only_checkbox_stores_manage(self):
        self._login_admin()
        p = self.layer_b.pk
        self._post(**{f'g_{self.target.pk}_{p}_manage': 1})
        self.assertEqual(
            ResourceGrant.objects.get(
                legacy_user=self.target, resource_id=p).level, 'manage')

    # ── обновление и отзыв ─────────────────────────────────────────────
    def test_upgrade_existing_grant(self):
        ResourceGrant.objects.create(
            legacy_user=self.target, resource_type=GL,
            resource_id=self.layer_a.pk, level='view')
        self._login_admin()
        p = self.layer_a.pk
        self._post(**{
            f'g_{self.target.pk}_{p}_view': 1,
            f'g_{self.target.pk}_{p}_edit': 1,
        })
        self.assertEqual(
            ResourceGrant.objects.get(
                legacy_user=self.target, resource_id=p).level, 'edit')

    def test_empty_row_revokes_grant(self):
        ResourceGrant.objects.create(
            legacy_user=self.target, resource_type=GL,
            resource_id=self.layer_a.pk, level='manage')
        self._login_admin()
        # POST без единого чекбокса для этого пользователя → грант удалён.
        self._post()
        self.assertFalse(
            ResourceGrant.objects.filter(
                legacy_user=self.target, resource_id=self.layer_a.pk).exists())

    def test_user_not_in_post_untouched(self):
        # Грант другого пользователя, которого нет в user_ids, не трогаем.
        other = _mk_legacy('other')
        ResourceGrant.objects.create(
            legacy_user=other, resource_type=GL,
            resource_id=self.layer_a.pk, level='view')
        self._login_admin()
        self._post(**{f'g_{self.target.pk}_all_view': 1})
        self.assertTrue(
            ResourceGrant.objects.filter(
                legacy_user=other, resource_id=self.layer_a.pk).exists())

    # ── поиск пользователей ────────────────────────────────────────────
    def test_search_adds_user_row(self):
        self._login_admin()
        resp = self.client.get(self.url, {'u': 'grantee'})
        self.assertContains(resp, 'grantee')
