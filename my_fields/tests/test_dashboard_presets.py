"""Тесты сохранённых дашбордов и экспорта сводки (пресеты + email).

Фиксируем:
* нормализацию параметров отчёта (поля только из атрибутов слоя);
* CRUD пресетов: сохранение, перезапись по имени, переименование, удаление;
* изоляцию по владельцу (чужой пресет не виден и не удаляется);
* отправку сводки на email: письмо со таблицей и ссылкой, валидацию адресов;
* кнопки экспорта на странице и отсутствие прежней подписи «Диаграммы».

Требуют PostGIS. Локально: $env:PROJ_LIB='' (конфликт PROJ/GDAL).
"""
from django.core import mail
from django.db import connection
from django.test import override_settings
from psycopg import sql

from my_fields.models import GisDashboard
from my_fields.services.dashboard_presets import (
    DashboardParamsError, clean_recipients, dashboard_url, normalize_params,
)
from my_fields.services.shp_import import create_empty_layer

from .test_gis_layers import GisLayersTestCase


class _DashboardTestCase(GisLayersTestCase):
    """Слой с одним полигоном и двумя текстовыми атрибутами."""

    def setUp(self):
        self._login_admin()
        self.layer = create_empty_layer(
            'Почвы', 'polygon',
            attributes=[
                {'name': 'soil', 'type': 'text'},
                {'name': 'zone', 'type': 'text'},
            ],
            owner=self.admin_user,
        )
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'INSERT INTO {t} (soil, zone, geom) VALUES (%s, %s, '
                'ST_SetSRID(ST_MakeEnvelope(34.1, 45.1, 34.15, 45.15), 4326))'
            ).format(t=sql.Identifier(self.layer.table_name)),
                ['Чернозём', 'A'])
        self.layer.feature_count = 1
        self.layer.save(update_fields=['feature_count'])

    def _params(self, **over):
        params = {'group': 'soil', 'split': None, 'bysplit': True}
        params.update(over)
        return params

    def _save(self, name='Отчёт', **over):
        return self.client.post(
            '/me/gis/api/dashboards/',
            data={'name': name, 'layer': self.layer.pk,
                  'params': self._params(**over)},
            content_type='application/json',
        )


class NormalizeParamsTests(_DashboardTestCase):
    """Чистая нормализация параметров пресета."""

    def test_keeps_known_keys_and_drops_others(self):
        out = normalize_params(self.layer, {
            'group': 'soil', 'split': 'zone', 'region': '71',
            'district': '5', 'bysplit': False, 'evil': 'DROP TABLE',
        })
        self.assertEqual(out, {
            'region': 71, 'district': 5, 'group': 'soil',
            'split': 'zone', 'bysplit': False,
        })

    def test_split_equal_to_group_is_dropped(self):
        out = normalize_params(self.layer, {'group': 'soil', 'split': 'soil'})
        self.assertIsNone(out['split'])
        # Разумные значения по умолчанию.
        self.assertTrue(out['bysplit'])
        self.assertIsNone(out['region'])

    def test_unknown_field_rejected(self):
        with self.assertRaises(DashboardParamsError):
            normalize_params(self.layer, {'group': 'nope'})
        with self.assertRaises(DashboardParamsError):
            normalize_params(self.layer, {'group': 'soil', 'split': 'nope'})

    def test_group_required(self):
        with self.assertRaises(DashboardParamsError):
            normalize_params(self.layer, {})

    def test_bad_ints_become_none(self):
        out = normalize_params(
            self.layer, {'group': 'soil', 'region': 'abc', 'district': -3})
        self.assertIsNone(out['region'])
        self.assertIsNone(out['district'])

    def test_url_carries_state(self):
        url = dashboard_url(self.layer, normalize_params(self.layer, {
            'group': 'soil', 'split': 'zone', 'region': 71, 'bysplit': False,
        }))
        self.assertIn('/me/gis/dashboards/?', url)
        self.assertIn(f'layer={self.layer.pk}', url)
        self.assertIn('group=soil', url)
        self.assertIn('split=zone', url)
        self.assertIn('bysplit=0', url)


class RecipientsTests(GisLayersTestCase):
    """Разбор списка адресов получателей."""

    def test_splits_and_dedups(self):
        self.assertEqual(
            clean_recipients('a@b.ru, a@b.ru; c@d.ru'), ['a@b.ru', 'c@d.ru'])

    def test_accepts_list(self):
        self.assertEqual(clean_recipients(['a@b.ru']), ['a@b.ru'])

    def test_empty_rejected(self):
        with self.assertRaises(DashboardParamsError):
            clean_recipients('   ')

    def test_invalid_rejected(self):
        with self.assertRaises(DashboardParamsError):
            clean_recipients('a@b.ru, not-an-email')

    def test_too_many_rejected(self):
        with self.assertRaises(DashboardParamsError):
            clean_recipients(','.join(f'a{i}@b.ru' for i in range(6)))


class DashboardPresetApiTests(_DashboardTestCase):
    """CRUD сохранённых дашбордов."""

    def test_save_and_list(self):
        resp = self._save('Почвы по подтипам')
        self.assertEqual(resp.status_code, 201, resp.content)
        item = resp.json()['dashboard']
        self.assertEqual(item['name'], 'Почвы по подтипам')
        self.assertEqual(item['layer'], self.layer.pk)
        self.assertEqual(item['params']['group'], 'soil')

        listed = self.client.get('/me/gis/api/dashboards/').json()
        self.assertEqual(listed['count'], 1)
        self.assertEqual(listed['results'][0]['layer_title'], 'Почвы')

    def test_same_name_overwrites(self):
        self._save('Отчёт')
        resp = self._save('Отчёт', split='zone')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(GisDashboard.objects.count(), 1)
        self.assertEqual(
            GisDashboard.objects.get().params['split'], 'zone')

    def test_empty_name_rejected(self):
        resp = self._save('   ')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'empty_name')

    def test_bad_group_rejected(self):
        resp = self._save('Отчёт', group='nope')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_params')

    def test_missing_layer_rejected(self):
        resp = self.client.post(
            '/me/gis/api/dashboards/',
            data={'name': 'x', 'params': self._params()},
            content_type='application/json')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'no_layer')

    def test_rename_and_update_params(self):
        pk = self._save('Старое').json()['dashboard']['id']
        resp = self.client.patch(
            f'/me/gis/api/dashboards/{pk}/',
            data={'name': 'Новое', 'params': self._params(split='zone')},
            content_type='application/json')
        self.assertEqual(resp.status_code, 200, resp.content)
        item = GisDashboard.objects.get(pk=pk)
        self.assertEqual(item.name, 'Новое')
        self.assertEqual(item.params['split'], 'zone')

    def test_patch_bad_params_rejected(self):
        pk = self._save('Отчёт').json()['dashboard']['id']
        resp = self.client.patch(
            f'/me/gis/api/dashboards/{pk}/',
            data={'params': {'group': 'nope'}},
            content_type='application/json')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_params')

    def test_delete(self):
        pk = self._save('Отчёт').json()['dashboard']['id']
        resp = self.client.delete(f'/me/gis/api/dashboards/{pk}/')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertFalse(GisDashboard.objects.exists())

    def test_foreign_preset_hidden(self):
        other = GisDashboard.objects.create(
            owner=self.plain_user, name='Чужой', layer=self.layer,
            params=self._params())
        listed = self.client.get('/me/gis/api/dashboards/').json()
        self.assertEqual(listed['count'], 0)
        resp = self.client.delete(f'/me/gis/api/dashboards/{other.pk}/')
        self.assertEqual(resp.status_code, 404)
        self.assertTrue(GisDashboard.objects.filter(pk=other.pk).exists())

    def test_anonymous_denied(self):
        self.client.logout()
        self.assertEqual(
            self.client.get('/me/gis/api/dashboards/').status_code, 401)

    def test_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        self.assertEqual(
            self.client.get('/me/gis/api/dashboards/').status_code, 403)

    def test_layer_delete_removes_preset(self):
        self._save('Отчёт')
        self.layer.delete()
        self.assertFalse(GisDashboard.objects.exists())


@override_settings(
    EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend',
    DEFAULT_FROM_EMAIL='robot@edunabazar.ru')
class DashboardSendTests(_DashboardTestCase):
    """POST /me/gis/api/dashboards/send/ — письмо со сводкой."""

    def _send(self, **over):
        body = {'layer': self.layer.pk, 'params': self._params(),
                'to': 'agronom@example.ru'}
        body.update(over)
        return self.client.post(
            '/me/gis/api/dashboards/send/', data=body,
            content_type='application/json')

    def test_sends_summary(self):
        mail.outbox = []
        resp = self._send(note='Смотрите сводку')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(resp.json()['delivered'], 1)
        self.assertEqual(len(mail.outbox), 1)
        msg = mail.outbox[0]
        self.assertEqual(msg.to, ['agronom@example.ru'])
        self.assertIn('Почвы', msg.subject)
        self.assertIn('Чернозём', msg.body)
        self.assertIn('Смотрите сводку', msg.body)
        self.assertIn('/me/gis/dashboards/?', msg.body)
        html = msg.alternatives[0][0]
        self.assertIn('<table', html)
        self.assertIn('Чернозём', html)

    def test_each_recipient_gets_own_message(self):
        mail.outbox = []
        resp = self._send(to='a@b.ru, c@d.ru')
        self.assertEqual(resp.json()['delivered'], 2)
        self.assertEqual([m.to for m in mail.outbox], [['a@b.ru'], ['c@d.ru']])

    def test_invalid_address_rejected(self):
        mail.outbox = []
        resp = self._send(to='oops')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_params')
        self.assertEqual(mail.outbox, [])

    def test_bad_group_rejected(self):
        resp = self._send(params={'group': 'nope'})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_params')

    def test_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        self.assertEqual(self._send().status_code, 403)


class DashboardsPageControlsTests(GisLayersTestCase):
    """Страница: панель действий и почищенная подпись чекбокса."""

    def test_page_has_export_controls(self):
        self._login_admin()
        resp = self.client.get('/me/gis/dashboards/')
        self.assertEqual(resp.status_code, 200)
        for marker in ('dash-presets', 'dash-save', 'dash-pdf',
                       'dash-email', 'dash-link'):
            self.assertContains(resp, marker)
        self.assertContains(resp, 'Сохранённые дашборды')

    def test_chart_checkbox_has_no_group_label(self):
        self._login_admin()
        html = self.client.get('/me/gis/dashboards/').content.decode()
        # Подпись-заголовок «Диаграммы» над чекбоксом убрана (ломала вёрстку).
        self.assertNotIn('<label>Диаграммы</label>', html)
        self.assertIn('dash-bysplit', html)
