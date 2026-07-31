"""Тесты REST API ``my_fields`` (/api/my/fields/...).

Критические свойства, которые фиксируем:
* аутентификация (401 без логина) и изоляция владельцев (403 на чужие
  поля, 404 не «утекает» — см. UI-тесты);
* создание поля: коэрция Polygon → MultiPolygon, пересчёт площади,
  авто-резолв региона/района по centroid'у;
* валидация геометрии и обязательных полей (400, а не 500);
* сезоны: upsert по (field, year, crop) вместо дубликата;
* журнал событий: обязательность event_type/event_date.
"""
import json

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase

from agrocosmos.models import District, Region
from my_fields.models import FieldEvent, FieldSeason, UserField

User = get_user_model()

# Квадрат ~0.02°x0.02° около Крыма — внутри справочного региона ниже.
SQUARE_COORDS = [[
    [34.10, 45.10], [34.12, 45.10], [34.12, 45.12], [34.10, 45.12], [34.10, 45.10],
]]
SQUARE_GEOJSON = {'type': 'Polygon', 'coordinates': SQUARE_COORDS}


def _mpoly(x0, y0, x1, y1):
    return MultiPolygon(Polygon((
        (x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0),
    )))


class MyFieldsApiTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.owner = User.objects.create_user('owner', password='x')
        cls.stranger = User.objects.create_user('stranger', password='x')
        cls.staff = User.objects.create_user('staff', password='x', is_staff=True)
        cls.region = Region.objects.create(
            name='Тест-регион', code='test-region', geom=_mpoly(34, 45, 35, 46),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Тест-район', geom=_mpoly(34, 45, 35, 46),
        )

    def _post_json(self, url, payload):
        return self.client.post(
            url, data=json.dumps(payload), content_type='application/json',
        )

    def _patch_json(self, url, payload):
        return self.client.patch(
            url, data=json.dumps(payload), content_type='application/json',
        )

    def _create_field(self, **extra):
        payload = {'name': 'Поле у лесополосы', 'geometry': SQUARE_GEOJSON}
        payload.update(extra)
        return self._post_json('/api/my/fields/', payload)


class AuthTests(MyFieldsApiTestCase):
    def test_anonymous_gets_401(self):
        self.assertEqual(self.client.get('/api/my/fields/').status_code, 401)
        self.assertEqual(self._create_field().status_code, 401)


class FieldCreateTests(MyFieldsApiTestCase):
    def setUp(self):
        self.client.force_login(self.owner)

    def test_create_field_full_cycle(self):
        resp = self._create_field()
        self.assertEqual(resp.status_code, 201)
        field = UserField.objects.get(owner=self.owner)
        # Polygon коэрцирован в MultiPolygon
        self.assertEqual(field.geom.geom_type, 'MultiPolygon')
        # Площадь пересчитана (квадрат ~0.02° в этих широтах — сотни га)
        self.assertGreater(field.area_ha, 0)
        # Регион/район отрезолвлены по centroid'у
        self.assertEqual(field.region_id, self.region.pk)
        self.assertEqual(field.district_id, self.district.pk)
        # Ответ — GeoJSON Feature с теми же свойствами
        data = resp.json()
        self.assertEqual(data['type'], 'Feature')
        self.assertEqual(data['properties']['district_name'], 'Тест-район')

    def test_missing_name_or_geometry_is_400(self):
        self.assertEqual(
            self._post_json('/api/my/fields/', {'name': 'x'}).status_code, 400,
        )
        self.assertEqual(
            self._post_json(
                '/api/my/fields/', {'geometry': SQUARE_GEOJSON},
            ).status_code, 400,
        )

    def test_non_polygon_geometry_is_400(self):
        resp = self._create_field(
            geometry={'type': 'Point', 'coordinates': [34.1, 45.1]},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_geometry')

    def test_broken_json_is_400(self):
        resp = self.client.post(
            '/api/my/fields/', data='{not json', content_type='application/json',
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_json')

    def test_create_with_season(self):
        resp = self._create_field(season={'year': 2026, 'crop': 'wheat'})
        self.assertEqual(resp.status_code, 201)
        field = UserField.objects.get(owner=self.owner)
        season = field.seasons.get()
        self.assertEqual((season.year, season.crop), (2026, 'wheat'))

    def test_bad_season_does_not_block_field_creation(self):
        resp = self._create_field(season={'year': 'не-год', 'crop': 'wheat'})
        self.assertEqual(resp.status_code, 201)
        self.assertIn('season_warning', resp.json()['properties'])
        field = UserField.objects.get(owner=self.owner)
        self.assertEqual(field.seasons.count(), 0)


class FieldListDetailTests(MyFieldsApiTestCase):
    def setUp(self):
        self.client.force_login(self.owner)
        self._create_field()
        self.field = UserField.objects.get(owner=self.owner)
        self.url = f'/api/my/fields/{self.field.pk}/'

    def test_list_hides_archived_by_default(self):
        self.field.is_archived = True
        self.field.save(update_fields=['is_archived'])
        self.assertEqual(len(self.client.get('/api/my/fields/').json()['features']), 0)
        self.assertEqual(
            len(self.client.get('/api/my/fields/?archived=1').json()['features']), 1,
        )

    def test_list_shows_only_own_fields(self):
        self.client.force_login(self.stranger)
        self.assertEqual(len(self.client.get('/api/my/fields/').json()['features']), 0)

    def test_stranger_is_forbidden(self):
        self.client.force_login(self.stranger)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(self._patch_json(self.url, {}).status_code, 403)
        self.assertEqual(self.client.delete(self.url).status_code, 403)
        # Поле не пострадало
        self.assertTrue(UserField.objects.filter(pk=self.field.pk).exists())

    def test_staff_can_view(self):
        self.client.force_login(self.staff)
        self.assertEqual(self.client.get(self.url).status_code, 200)

    def test_patch_properties(self):
        resp = self._patch_json(self.url, {
            'properties': {'name': 'Новое имя', 'is_archived': True},
        })
        self.assertEqual(resp.status_code, 200)
        self.field.refresh_from_db()
        self.assertEqual(self.field.name, 'Новое имя')
        self.assertTrue(self.field.is_archived)

    def test_patch_invalid_geometry_is_400(self):
        resp = self._patch_json(self.url, {
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        })
        self.assertEqual(resp.status_code, 400)

    def test_delete(self):
        self.assertEqual(self.client.delete(self.url).status_code, 200)
        self.assertFalse(UserField.objects.filter(pk=self.field.pk).exists())


class EventsApiTests(MyFieldsApiTestCase):
    def setUp(self):
        self.client.force_login(self.owner)
        self._create_field()
        self.field = UserField.objects.get(owner=self.owner)
        self.url = f'/api/my/fields/{self.field.pk}/events/'

    def test_create_and_list(self):
        resp = self._post_json(self.url, {
            'event_type': 'sowing', 'event_date': '2026-05-02',
            'title': 'Сев яровой пшеницы',
        })
        self.assertEqual(resp.status_code, 201)
        listing = self.client.get(self.url).json()
        self.assertEqual(listing['count'], 1)
        self.assertEqual(listing['results'][0]['event_type'], 'sowing')

    def test_missing_required_fields_is_400(self):
        self.assertEqual(
            self._post_json(self.url, {'event_type': 'sowing'}).status_code, 400,
        )

    def test_invalid_date_is_400(self):
        resp = self._post_json(self.url, {
            'event_type': 'sowing', 'event_date': '02.05.2026',
        })
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'invalid_date')

    def test_stranger_forbidden(self):
        self.client.force_login(self.stranger)
        self.assertEqual(self.client.get(self.url).status_code, 403)

    def test_delete_event(self):
        self._post_json(self.url, {'event_type': 'scout', 'event_date': '2026-06-01'})
        event = FieldEvent.objects.get(field=self.field)
        resp = self.client.delete(f'{self.url}{event.pk}/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.field.events.count(), 0)


class SeasonsApiTests(MyFieldsApiTestCase):
    def setUp(self):
        self.client.force_login(self.owner)
        self._create_field()
        self.field = UserField.objects.get(owner=self.owner)
        self.url = f'/api/my/fields/{self.field.pk}/seasons/'

    def test_create_season(self):
        resp = self._post_json(self.url, {'year': 2026, 'crop': 'barley'})
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(self.field.seasons.count(), 1)

    def test_same_year_crop_is_upsert_not_duplicate(self):
        self._post_json(self.url, {'year': 2026, 'crop': 'barley'})
        resp = self._post_json(self.url, {
            'year': 2026, 'crop': 'barley', 'variety': 'Маргрет',
        })
        # Повтор — обновление существующего сезона, 200 вместо 201
        self.assertEqual(resp.status_code, 200)
        season = self.field.seasons.get()
        self.assertEqual(season.variety, 'Маргрет')

    def test_missing_year_or_crop_is_400(self):
        self.assertEqual(self._post_json(self.url, {'year': 2026}).status_code, 400)
        self.assertEqual(self._post_json(self.url, {'crop': 'oats'}).status_code, 400)

    def test_patch_and_delete(self):
        self._post_json(self.url, {'year': 2026, 'crop': 'oats'})
        season = FieldSeason.objects.get(field=self.field)
        resp = self._patch_json(
            f'{self.url}{season.pk}/',
            {'actual_yield_t_per_ha': 3.4, 'actual_harvest_date': '2026-09-01'},
        )
        self.assertEqual(resp.status_code, 200)
        season.refresh_from_db()
        self.assertEqual(season.actual_yield_t_per_ha, 3.4)
        self.assertEqual(str(season.actual_harvest_date), '2026-09-01')
        self.assertEqual(
            self.client.delete(f'{self.url}{season.pk}/').status_code, 200,
        )
        self.assertEqual(self.field.seasons.count(), 0)


class UiPagesTests(MyFieldsApiTestCase):
    def test_fields_list_requires_login(self):
        resp = self.client.get('/me/fields/')
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/login/', resp['Location'])

    def test_field_detail_of_stranger_is_404_not_403(self):
        self.client.force_login(self.owner)
        self._create_field()
        field = UserField.objects.get(owner=self.owner)
        self.client.force_login(self.stranger)
        # 404, а не 403 — наличие чужого поля не должно «утекать»
        self.assertEqual(self.client.get(f'/me/fields/{field.pk}/').status_code, 404)
