"""Тесты сводки по слою (services/layer_summary.py + эндпоинт summary/).

Фиксируем:
* группировку по атрибуту: количество объектов, геодезическая площадь (га),
  доли и порядок строк (по убыванию площади);
* разрез вторым атрибутом (кросс-таб) и итоги по значениям разреза;
* ВТОРОЙ УРОВЕНЬ группировки (``group2``): категория = пара значений,
  свой перечень значений, снятие повторов с group/split;
* не-полигональные слои: площади нет, доли считаются по числу объектов;
* NULL как отдельная категория (``value = None``), а не выпадение строки;
* фильтр по району: остаются только пересекающие объекты, площадь ПОЛНАЯ;
* фильтр по перечню значений (чекбоксы у группировки и разреза),
  включая токен NULL и повторяющиеся параметры ``gv``/``sv``;
* валидацию (чужое поле → LayerSummaryError/400) и доступ к эндпоинту.

Требуют PostGIS. Локально: $env:PROJ_LIB='' (конфликт PROJ/GDAL).
"""
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.db import connection
from psycopg import sql

from agrocosmos.models import District, Region
from my_fields.services.layer_summary import (
    MAX_FILTER_VALUES, NULL_TOKEN, LayerSummaryError, summary_by_field,
)
from my_fields.services.shp_import import create_empty_layer

from .test_gis_layers import GisLayersTestCase

# (soil, zone, x, y) — квадраты 0.05° рядом друг с другом.
_ENV = (
    ('Чернозём', 'A', 34.10, 45.10),
    ('Чернозём', 'B', 34.20, 45.10),
    ('Серая', 'A', 34.30, 45.10),
    (None, 'B', 34.40, 45.10),
)
# Далеко от остальных — попадает за границу тестового района.
_OUTSIDE = ('Каштановая', 'C', 40.00, 50.00)


def _rows(summary):
    """``{категория: (count, area_ha)}`` — удобно для ассертов."""
    return {r['value']: (r['count'], r['area_ha']) for r in summary['rows']}


class _LayerFactoryMixin:
    """Слой с двумя текстовыми атрибутами и геометрией из ``_ENV``."""

    def _make_layer(self, kind, env=_ENV):
        layer = create_empty_layer(
            'Почвы', kind,
            attributes=[
                {'name': 'soil', 'type': 'text'},
                {'name': 'zone', 'type': 'text'},
            ],
            owner=self.admin_user,
        )
        t = sql.Identifier(layer.table_name)
        with connection.cursor() as cur:
            for soil, zone, x, y in env:
                if kind == 'polygon':
                    geom = sql.SQL(
                        'ST_SetSRID(ST_MakeEnvelope(%s, %s, %s, %s), 4326)')
                    params = [soil, zone, x, y, x + 0.05, y + 0.05]
                else:
                    geom = sql.SQL('ST_SetSRID(ST_MakePoint(%s, %s), 4326)')
                    params = [soil, zone, x, y]
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (soil, zone, geom) VALUES (%s, %s, {g})'
                ).format(t=t, g=geom), params)
        layer.feature_count = len(env)
        layer.save(update_fields=['feature_count'])
        return layer


class LayerSummaryServiceTests(_LayerFactoryMixin, GisLayersTestCase):
    """Чистый сервис: группировка, площади, разрез, валидация."""

    def setUp(self):
        self.layer = self._make_layer('polygon')

    def test_groups_count_and_area(self):
        s = summary_by_field(self.layer, 'soil')
        self.assertTrue(s['polygonal'])
        self.assertIsNone(s['split'])
        rows = _rows(s)
        self.assertEqual(rows['Чернозём'][0], 2)
        self.assertEqual(rows['Серая'][0], 1)
        # NULL — отдельная категория, объект не теряется.
        self.assertEqual(rows[None][0], 1)
        self.assertEqual(s['total']['count'], 4)
        # Площадь геодезическая: квадрат 0.05° на широте 45° ≈ 2000-2200 га.
        self.assertGreater(rows['Серая'][1], 1500)
        self.assertLess(rows['Серая'][1], 3000)
        # Чернозём — два квадрата, значит примерно вдвое больше одного.
        self.assertAlmostEqual(
            rows['Чернозём'][1] / rows['Серая'][1], 2.0, delta=0.1)

    def test_shares_sum_to_one_and_rows_sorted_by_area(self):
        s = summary_by_field(self.layer, 'soil')
        self.assertAlmostEqual(sum(r['share'] for r in s['rows']), 1.0, places=6)
        areas = [r['area_ha'] for r in s['rows']]
        self.assertEqual(areas, sorted(areas, reverse=True))
        self.assertAlmostEqual(
            sum(areas), s['total']['area_ha'], delta=0.01)

    def test_split_builds_crosstab(self):
        s = summary_by_field(self.layer, 'soil', split='zone')
        self.assertEqual(s['split'], 'zone')
        by_zone = {x['value']: x['count'] for x in s['splits']}
        self.assertEqual(by_zone, {'A': 2, 'B': 2})
        cher = next(r for r in s['rows'] if r['value'] == 'Чернозём')
        self.assertEqual(cher['splits']['A']['count'], 1)
        self.assertEqual(cher['splits']['B']['count'], 1)
        # Сумма по ячейкам разреза = итог строки.
        self.assertAlmostEqual(
            sum(c['area_ha'] for c in cher['splits'].values()),
            cher['area_ha'], delta=0.01)

    def test_split_equal_to_group_is_ignored(self):
        s = summary_by_field(self.layer, 'soil', split='soil')
        self.assertIsNone(s['split'])
        self.assertEqual(s['splits'], [])

    def test_null_split_key_is_empty_string(self):
        # Группируем по zone, разрез по soil, где есть NULL: ключ ячейки ''.
        s = summary_by_field(self.layer, 'zone', split='soil')
        row_b = next(r for r in s['rows'] if r['value'] == 'B')
        self.assertIn('', row_b['splits'])
        self.assertEqual(row_b['splits']['']['count'], 1)

    def test_point_layer_has_no_area(self):
        layer = self._make_layer('point')
        s = summary_by_field(layer, 'soil')
        self.assertFalse(s['polygonal'])
        self.assertEqual(s['total']['area_ha'], 0)
        # Доли считаются по числу объектов: 2 из 4.
        cher = next(r for r in s['rows'] if r['value'] == 'Чернозём')
        self.assertAlmostEqual(cher['share'], 0.5, places=6)

    def test_unknown_group_field_raises(self):
        with self.assertRaises(LayerSummaryError):
            summary_by_field(self.layer, 'geom')
        with self.assertRaises(LayerSummaryError):
            summary_by_field(self.layer, 'soil"; DROP TABLE x; --')

    def test_unknown_split_field_raises(self):
        with self.assertRaises(LayerSummaryError):
            summary_by_field(self.layer, 'soil', split='nope')

    def test_too_many_features_raises(self):
        self.layer.feature_count = 10 ** 9
        self.layer.save(update_fields=['feature_count'])
        with self.assertRaises(LayerSummaryError):
            summary_by_field(self.layer, 'soil')


class LayerSummaryValueFilterTests(_LayerFactoryMixin, GisLayersTestCase):
    """Выборка по отмеченным значениям атрибутов."""

    def setUp(self):
        self.layer = self._make_layer('polygon')

    def test_group_values_limit_rows(self):
        s = summary_by_field(self.layer, 'soil', group_values=['Серая'])
        self.assertEqual(list(_rows(s)), ['Серая'])
        self.assertEqual(s['total']['count'], 1)
        # Доли пересчитаны по ВЫБОРКЕ, а не по всему слою.
        self.assertAlmostEqual(s['rows'][0]['share'], 1.0, places=6)
        self.assertEqual(s['group_values'], ['Серая'])

    def test_null_token_selects_empty_cells(self):
        s = summary_by_field(self.layer, 'soil', group_values=[NULL_TOKEN])
        self.assertEqual(list(_rows(s)), [None])
        self.assertEqual(s['total']['count'], 1)

    def test_empty_string_is_not_null(self):
        """Пустая строка — не NULL: токены не должны их смешивать."""
        s = summary_by_field(self.layer, 'soil', group_values=[''])
        self.assertEqual(s['total']['count'], 0)

    def test_split_values_limit_columns(self):
        s = summary_by_field(self.layer, 'soil', split='zone',
                             split_values=['A'])
        self.assertEqual([x['value'] for x in s['splits']], ['A'])
        self.assertEqual(s['total']['count'], 2)
        self.assertEqual(s['split_values'], ['A'])

    def test_both_filters_combine(self):
        s = summary_by_field(self.layer, 'soil', split='zone',
                             group_values=['Чернозём'], split_values=['B'])
        self.assertEqual(s['total']['count'], 1)
        self.assertEqual(list(_rows(s)), ['Чернозём'])

    def test_split_values_ignored_without_split(self):
        s = summary_by_field(self.layer, 'soil', split_values=['A'])
        self.assertIsNone(s['split_values'])
        self.assertEqual(s['total']['count'], 4)

    def test_empty_list_means_no_filter(self):
        s = summary_by_field(self.layer, 'soil', group_values=[])
        self.assertIsNone(s['group_values'])
        self.assertEqual(s['total']['count'], 4)

    def test_duplicates_dropped(self):
        s = summary_by_field(self.layer, 'soil',
                             group_values=['Серая', 'Серая'])
        self.assertEqual(s['group_values'], ['Серая'])

    def test_unknown_value_gives_empty_summary(self):
        s = summary_by_field(self.layer, 'soil', group_values=['нету'])
        self.assertEqual(s['rows'], [])
        self.assertEqual(s['total']['count'], 0)

    def test_numeric_values_compare_as_text(self):
        layer = create_empty_layer(
            'Зоны', 'polygon',
            attributes=[{'name': 'zone_2', 'type': 'integer'}],
            owner=self.admin_user,
        )
        t = sql.Identifier(layer.table_name)
        with connection.cursor() as cur:
            for code in (3, 3, 7):
                cur.execute(sql.SQL(
                    'INSERT INTO {t} (zone_2, geom) VALUES (%s, ST_SetSRID('
                    'ST_MakeEnvelope(34.1, 45.1, 34.15, 45.15), 4326))'
                ).format(t=t), [code])
        layer.feature_count = 3
        layer.save(update_fields=['feature_count'])

        s = summary_by_field(layer, 'zone_2', group_values=['3'])
        self.assertEqual(list(_rows(s)), ['3'])
        self.assertEqual(s['total']['count'], 2)

    def test_too_many_values_raises(self):
        with self.assertRaises(LayerSummaryError):
            summary_by_field(
                self.layer, 'soil',
                group_values=[str(i) for i in range(MAX_FILTER_VALUES + 1)])

    def test_string_instead_of_list_raises(self):
        with self.assertRaises(LayerSummaryError):
            summary_by_field(self.layer, 'soil', group_values='Серая')

    def test_sql_injection_in_value_is_just_a_value(self):
        s = summary_by_field(
            self.layer, 'soil', group_values=["'; DROP TABLE x; --"])
        self.assertEqual(s['rows'], [])
        # Таблица жива: значения идут параметрами, а не в тело SQL.
        self.assertEqual(summary_by_field(self.layer, 'soil')['total']['count'], 4)


class LayerSummaryTwoLevelTests(_LayerFactoryMixin, GisLayersTestCase):
    """Второй уровень группировки: строка = пара (group, group2)."""

    def setUp(self):
        self.layer = self._make_layer('polygon')

    def _pairs(self, s):
        return {(r['value'], r['value2']): r['count'] for r in s['rows']}

    def test_rows_are_pairs(self):
        s = summary_by_field(self.layer, 'soil', group2='zone')
        self.assertEqual(s['group2'], 'zone')
        self.assertEqual(self._pairs(s), {
            ('Чернозём', 'A'): 1,
            ('Чернозём', 'B'): 1,
            ('Серая', 'A'): 1,
            (None, 'B'): 1,
        })
        self.assertEqual(s['total']['count'], 4)
        self.assertAlmostEqual(sum(r['share'] for r in s['rows']), 1.0, places=6)

    def test_without_group2_value2_is_none(self):
        s = summary_by_field(self.layer, 'soil')
        self.assertIsNone(s['group2'])
        self.assertTrue(all(r['value2'] is None for r in s['rows']))

    def test_same_field_as_group_is_dropped(self):
        s = summary_by_field(self.layer, 'soil', group2='soil')
        self.assertIsNone(s['group2'])
        self.assertEqual(len(s['rows']), 3)

    def test_split_equal_to_group2_is_dropped(self):
        s = summary_by_field(self.layer, 'soil', group2='zone', split='zone')
        self.assertEqual(s['group2'], 'zone')
        self.assertIsNone(s['split'])
        self.assertEqual(s['splits'], [])

    def test_group2_values_filter_rows(self):
        s = summary_by_field(self.layer, 'soil', group2='zone',
                             group2_values=['A'])
        self.assertEqual(s['group2_values'], ['A'])
        self.assertEqual(set(self._pairs(s)), {('Чернозём', 'A'),
                                              ('Серая', 'A')})
        self.assertEqual(s['total']['count'], 2)

    def test_group2_values_ignored_without_group2(self):
        s = summary_by_field(self.layer, 'soil', group2_values=['A'])
        self.assertIsNone(s['group2_values'])
        self.assertEqual(s['total']['count'], 4)

    def test_bad_group2_raises(self):
        with self.assertRaises(LayerSummaryError):
            summary_by_field(self.layer, 'soil', group2='nope')

    def test_cross_tab_kept_with_two_levels(self):
        layer = self._make_layer('polygon', env=(
            ('Чернозём', 'A', 34.10, 45.10),
            ('Чернозём', 'A', 34.20, 45.10),
        ))
        s = summary_by_field(layer, 'soil', group2='zone', split='soil')
        # split совпал с group → снят, осталась чистая двухуровневая сводка.
        self.assertIsNone(s['split'])
        self.assertEqual(self._pairs(s), {('Чернозём', 'A'): 2})


class LayerSummaryDistrictTests(_LayerFactoryMixin, GisLayersTestCase):
    """Фильтр по району: отбор по пересечению, площадь без обрезки."""

    def setUp(self):
        self.layer = self._make_layer('polygon', env=_ENV + (_OUTSIDE,))
        box = Polygon.from_bbox((34.0, 45.0, 34.5, 45.5))
        box.srid = 4326
        region = Region.objects.create(
            name='Тестовый регион', code='test-r',
            geom=MultiPolygon(box.clone(), srid=4326))
        self.district = District.objects.create(
            region=region, name='Тестовый район', code='test-d',
            geom=MultiPolygon(box, srid=4326))

    def test_district_filters_features(self):
        full = summary_by_field(self.layer, 'soil')
        self.assertEqual(full['total']['count'], 5)

        s = summary_by_field(self.layer, 'soil', district_id=self.district.pk)
        self.assertEqual(s['district_id'], self.district.pk)
        self.assertEqual(s['total']['count'], 4)
        self.assertNotIn('Каштановая', _rows(s))

    def test_district_area_is_not_clipped(self):
        # Полигон «Серая» целиком внутри района — площадь та же, что без фильтра.
        full = _rows(summary_by_field(self.layer, 'soil'))
        inside = _rows(summary_by_field(
            self.layer, 'soil', district_id=self.district.pk))
        self.assertAlmostEqual(
            full['Серая'][1], inside['Серая'][1], delta=0.01)


class LayerSummaryEndpointTests(GisLayersTestCase):
    """GET /me/gis/api/layers/<pk>/summary/ — доступ и валидация."""

    def setUp(self):
        self._login_admin()
        self.layer = create_empty_layer(
            'Почвы', 'polygon',
            attributes=[{'name': 'soil', 'type': 'text'}],
            owner=self.admin_user,
        )
        t = sql.Identifier(self.layer.table_name)
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'INSERT INTO {t} (soil, geom) VALUES (%s, ST_SetSRID('
                'ST_MakeEnvelope(34.1, 45.1, 34.15, 45.15), 4326))'
            ).format(t=t), ['Чернозём'])

    def _get(self, query=''):
        return self.client.get(
            f'/me/gis/api/layers/{self.layer.pk}/summary/{query}')

    def test_summary_ok(self):
        resp = self._get('?group=soil')
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['layer']['id'], self.layer.pk)
        self.assertEqual(body['summary']['group'], 'soil')
        self.assertEqual(body['summary']['total']['count'], 1)

    def test_missing_group_is_400(self):
        resp = self._get()
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'no_group')

    def test_bad_group_is_400(self):
        resp = self._get('?group=nope')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_summary')

    def test_gv_param_filters(self):
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'INSERT INTO {t} (soil, geom) VALUES (%s, ST_SetSRID('
                'ST_MakeEnvelope(34.2, 45.1, 34.25, 45.15), 4326))'
            ).format(t=sql.Identifier(self.layer.table_name)), ['Серая'])

        full = self._get('?group=soil').json()['summary']
        self.assertEqual(full['total']['count'], 2)
        self.assertIsNone(full['group_values'])

        s = self._get('?group=soil&gv=Серая').json()['summary']
        self.assertEqual(s['total']['count'], 1)
        self.assertEqual(s['group_values'], ['Серая'])

    def test_repeated_gv_params_accumulate(self):
        s = self._get('?group=soil&gv=Чернозём&gv=Серая').json()['summary']
        self.assertEqual(s['group_values'], ['Чернозём', 'Серая'])

    def test_too_many_gv_is_400(self):
        query = '?group=soil' + ''.join(
            f'&gv={i}' for i in range(MAX_FILTER_VALUES + 1))
        resp = self._get(query)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_summary')

    def test_group2_and_g2v_params(self):
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'ALTER TABLE {t} ADD COLUMN zone text'
            ).format(t=sql.Identifier(self.layer.table_name)))
        self.layer.attributes = (self.layer.attributes or []) + [
            {'name': 'zone', 'db': 'zone', 'type': 'text'}]
        self.layer.save(update_fields=['attributes'])
        with connection.cursor() as cur:
            cur.execute(sql.SQL(
                'UPDATE {t} SET zone = %s'
            ).format(t=sql.Identifier(self.layer.table_name)), ['A'])

        s = self._get('?group=soil&group2=zone').json()['summary']
        self.assertEqual(s['group2'], 'zone')
        self.assertEqual(s['rows'][0]['value2'], 'A')

        s = self._get('?group=soil&group2=zone&g2v=B').json()['summary']
        self.assertEqual(s['group2_values'], ['B'])
        self.assertEqual(s['rows'], [])

    def test_bad_group2_is_400(self):
        resp = self._get('?group=soil&group2=nope')
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()['error'], 'bad_summary')

    def test_bad_district_is_ignored(self):
        resp = self._get('?group=soil&district=abc')
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertIsNone(resp.json()['summary']['district_id'])

    def test_unknown_layer_is_404(self):
        resp = self.client.get('/me/gis/api/layers/999999/summary/?group=soil')
        self.assertEqual(resp.status_code, 404)

    def test_anonymous_denied(self):
        self.client.logout()
        self.assertEqual(self._get('?group=soil').status_code, 401)

    def test_non_admin_denied(self):
        self.client.logout()
        self._login_plain()
        self.assertEqual(self._get('?group=soil').status_code, 403)


class DashboardsPageTests(GisLayersTestCase):
    """Страница /me/gis/dashboards/ — тот же гейт, что у /me/gis."""

    def test_admin_opens(self):
        self._login_admin()
        resp = self.client.get('/me/gis/dashboards/')
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'dash-build')
        # Переключатель малых диаграмм по значениям разреза.
        self.assertContains(resp, 'dash-bysplit')
        # Второй уровень группировки — по кнопке, блок скрыт.
        self.assertContains(resp, 'dash-group2-add')

    def test_non_admin_gets_404(self):
        self._login_plain()
        self.assertEqual(self.client.get('/me/gis/dashboards/').status_code, 404)
