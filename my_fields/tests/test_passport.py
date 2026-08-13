"""Тесты паспорта пользовательского поля (NDVI-снимки + зоны).

Фиксируем:
* права: 401 без логина, 403 для чужого пользователя, 404 для несуществующего;
* scope растров поля — ``f<id>``;
* frames-эндпоинт возвращает кадры (свежие первыми) и пустой список без данных;
* zones-эндпоинт возвращает ``zones: null`` без данных и словарь при наличии;
* preview без данных — 204; KML/SHP без данных — 404;
* KML/SHP отдают файл-вложение при наличии зон;
* HTML-страница паспорта рендерится владельцу и 404 для чужого.

Растровые функции ``agrocosmos.services.raster_tiles`` мокаются — реальные
GeoTIFF в юнит-тестах не нужны, проверяем связку (scope/bbox/outline и склейку
композитов S2+L8).
"""
from unittest import mock

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase
from django.utils import timezone

from agrocosmos.models import District, Region
from legacy.models import LegacyUser
from my_fields.models import UserField
from my_fields.services import passport

User = get_user_model()

RT = 'agrocosmos.services.raster_tiles'


def _mpoly(x0, y0, x1, y1):
    return MultiPolygon(Polygon((
        (x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0),
    )))


class PassportTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.owner = User.objects.create_user('owner', password='x')
        cls.stranger = User.objects.create_user('stranger', password='x')
        cls.region = Region.objects.create(
            name='Тест-регион', code='test-region', geom=_mpoly(34, 45, 35, 46),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тест-район', geom=_mpoly(34, 45, 35, 46),
        )
        cls.field = UserField.objects.create(
            owner=cls.owner, name='Поле у оврага',
            geom=_mpoly(34.10, 45.10, 34.12, 45.12),
            area_ha=250.0, region=cls.region, district=cls.district,
            cadastral_number='90:01:000:1',
        )
        now = timezone.now()
        cls.legacy_user = LegacyUser.objects.create(
            type=0, username='owner', auth_key='', password_hash='',
            email='owner@test.com', currency='RUB', name='Owner',
            address='', phone='', inn='', status=10,
            created_at=now, updated_at=now, contacts='',
        )

    def base(self, suffix=''):
        return f'/api/my/fields/{self.field.pk}/passport/{suffix}'


# ── Service layer ──────────────────────────────────────────────────────

class ServiceTests(PassportTestCase):
    def test_scope_id(self):
        self.assertEqual(passport.scope_id(self.field), f'f{self.field.pk}')

    def test_field_bbox_padded(self):
        bbox = passport.field_bbox(self.field)
        xmin, ymin, xmax, ymax = bbox
        self.assertLess(xmin, 34.10)
        self.assertGreater(xmax, 34.12)
        self.assertLess(ymin, 45.10)
        self.assertGreater(ymax, 45.12)

    def test_field_outline_rings(self):
        rings = passport.field_outline(self.field)
        self.assertTrue(rings)
        self.assertIsInstance(rings[0][0], list)  # [lon, lat]

    def test_resolve_composites_merges_s2_over_l8(self):
        def fake_list(sensor, scope, year):
            self.assertEqual(scope, f'f{self.field.pk}')
            common = {'date_from': '2025-05-01', 'date_to': '2025-05-10'}
            if sensor == 'l8':
                return [common, {'date_from': '2025-04-01', 'date_to': '2025-04-10'}]
            return [common]  # s2 covers the same period

        with mock.patch(f'{RT}.list_available_composites', side_effect=fake_list):
            comps = passport.resolve_composites(self.field, '2025')

        # Два уникальных периода; общий — за Sentinel-2.
        self.assertEqual(len(comps), 2)
        may = [c for c in comps if c['date_from'] == '2025-05-01'][0]
        self.assertEqual(may['sensor'], 's2')
        # Отсортировано по возрастанию даты.
        self.assertEqual(comps[0]['date_from'], '2025-04-01')

    def test_resolve_composites_empty(self):
        with mock.patch(f'{RT}.list_available_composites', return_value=[]):
            self.assertEqual(passport.resolve_composites(self.field, '2025'), [])


# ── Permissions ────────────────────────────────────────────────────────

class PermissionTests(PassportTestCase):
    def test_frames_requires_auth(self):
        resp = self.client.get(self.base('frames/'))
        self.assertEqual(resp.status_code, 401)

    def test_frames_forbidden_for_stranger(self):
        self.client.force_login(self.stranger)
        resp = self.client.get(self.base('frames/'))
        self.assertEqual(resp.status_code, 403)

    def test_zones_404_for_unknown_field(self):
        self.client.force_login(self.owner)
        resp = self.client.get('/api/my/fields/999999/passport/zones/')
        self.assertEqual(resp.status_code, 404)

    def test_preview_204_for_stranger(self):
        # Для <img> отдаём 204, а не JSON-403.
        self.client.force_login(self.stranger)
        resp = self.client.get(self.base('preview/') + '?sensor=s2&date=2025-05-01_2025-05-10')
        self.assertEqual(resp.status_code, 204)


# ── Frames endpoint ────────────────────────────────────────────────────

class FramesEndpointTests(PassportTestCase):
    def setUp(self):
        self.client.force_login(self.owner)

    def test_empty_when_no_rasters(self):
        with mock.patch(f'{RT}.list_available_composites', return_value=[]):
            resp = self.client.get(self.base('frames/') + '?year=2025')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['frames'], [])
        self.assertEqual(data['scope'], f'f{self.field.pk}')

    def test_frames_latest_first(self):
        def fake_list(sensor, scope, year):
            if sensor == 's2':
                return [
                    {'date_from': '2025-04-01', 'date_to': '2025-04-10'},
                    {'date_from': '2025-05-01', 'date_to': '2025-05-10'},
                ]
            return []

        with mock.patch(f'{RT}.list_available_composites', side_effect=fake_list):
            resp = self.client.get(self.base('frames/') + '?year=2025')
        frames = resp.json()['frames']
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0]['date_from'], '2025-05-01')  # свежий первым
        self.assertEqual(frames[0]['sensor'], 's2')


# ── Zones endpoint ─────────────────────────────────────────────────────

class ZonesEndpointTests(PassportTestCase):
    def setUp(self):
        self.client.force_login(self.owner)

    def test_zones_null_without_data(self):
        with mock.patch(f'{RT}.list_available_composites', return_value=[]):
            resp = self.client.get(self.base('zones/') + '?year=2025')
        self.assertEqual(resp.status_code, 200)
        self.assertIsNone(resp.json()['zones'])

    def test_zones_rendered(self):
        comp = {'date_from': '2025-05-01', 'date_to': '2025-05-10'}

        def fake_list(sensor, scope, year):
            return [comp] if sensor == 's2' else []

        stats = {'problem_pct': 20.0, 'warn_pct': 10.0, 'median': 0.55}
        with mock.patch(f'{RT}.list_available_composites', side_effect=fake_list), \
             mock.patch(f'{RT}.find_raster_path', return_value='/fake/s2.tif'), \
             mock.patch(f'{RT}.render_zones', return_value=(b'\x89PNG', stats)):
            resp = self.client.get(self.base('zones/') + '?year=2025')

        z = resp.json()['zones']
        self.assertIsNotNone(z)
        self.assertEqual(z['sensor'], 's2')
        self.assertEqual(z['date_from'], '2025-05-01')
        self.assertEqual(z['stats']['problem_pct'], 20.0)
        self.assertTrue(z['image'].startswith('data:image/png;base64,'))
        self.assertIsNone(z['dynamics'])  # единственный композит


# ── Export endpoints (KML / SHP) ───────────────────────────────────────

class ExportEndpointTests(PassportTestCase):
    def setUp(self):
        self.client.force_login(self.owner)

    def test_kml_404_without_data(self):
        with mock.patch(f'{RT}.list_available_composites', return_value=[]):
            resp = self.client.get(self.base('zones/kml/') + '?year=2025')
        self.assertEqual(resp.status_code, 404)

    def test_kml_attachment(self):
        comp = {'date_from': '2025-05-01', 'date_to': '2025-05-10'}
        feats = [{
            'zone': 'problem', 'area_ha': 1.5,
            'geometry': {'type': 'Polygon', 'coordinates': [[
                [34.10, 45.10], [34.11, 45.10], [34.11, 45.11],
                [34.10, 45.11], [34.10, 45.10],
            ]]},
        }]
        with mock.patch(f'{RT}.list_available_composites',
                        side_effect=lambda s, sc, y: [comp] if s == 's2' else []), \
             mock.patch(f'{RT}.find_raster_path', return_value='/fake/s2.tif'), \
             mock.patch(f'{RT}.zones_to_features', return_value=feats):
            resp = self.client.get(self.base('zones/kml/') + '?year=2025')
        self.assertEqual(resp.status_code, 200)
        self.assertIn('application/vnd.google-earth.kml', resp['Content-Type'])
        self.assertIn(f'zones_f{self.field.pk}', resp['Content-Disposition'])
        self.assertIn(b'<kml', resp.content)

    def test_shp_invalid_rate(self):
        resp = self.client.get(self.base('zones/shp/') + '?year=2025&rate_problem=abc')
        self.assertEqual(resp.status_code, 400)

    def test_shp_attachment(self):
        comp = {'date_from': '2025-05-01', 'date_to': '2025-05-10'}
        feats = [{'zone': 'problem', 'area_ha': 1.5,
                  'geometry': {'type': 'Polygon', 'coordinates': [[
                      [34.10, 45.10], [34.11, 45.10], [34.11, 45.11],
                      [34.10, 45.10]]]}}]
        with mock.patch(f'{RT}.list_available_composites',
                        side_effect=lambda s, sc, y: [comp] if s == 's2' else []), \
             mock.patch(f'{RT}.find_raster_path', return_value='/fake/s2.tif'), \
             mock.patch(f'{RT}.zones_to_features', return_value=feats), \
             mock.patch(f'{RT}.zones_to_agras_shp_zip', return_value=b'PK\x03\x04'):
            resp = self.client.get(
                self.base('zones/shp/') + '?year=2025&rate_problem=12&rate_warn=10&rate_ok=8')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp['Content-Type'], 'application/zip')
        self.assertIn(f'prescription_f{self.field.pk}', resp['Content-Disposition'])


# ── HTML page ──────────────────────────────────────────────────────────

class PageTests(PassportTestCase):
    @property
    def page_url(self):
        return f'/me/fields/{self.field.pk}/passport/'

    def test_owner_gets_page(self):
        self.client.force_login(self.owner)
        session = self.client.session
        session['legacy_user_id'] = self.legacy_user.pk
        session.save()
        resp = self.client.get(self.page_url)
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'Паспорт поля')
        self.assertTemplateUsed(resp, 'my_fields/field_passport.html')

    def test_stranger_gets_404(self):
        self.client.force_login(self.stranger)
        resp = self.client.get(self.page_url)
        self.assertEqual(resp.status_code, 404)

    def test_anonymous_redirected_to_login(self):
        resp = self.client.get(self.page_url)
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/login/', resp['Location'])
