"""Тесты ``services/osm_overpass``: запасной источник полигонов (Nominatim)
при недоступном polygons.openstreetmap.fr и перебор зеркал Overpass.
Сеть мокается."""
from unittest import mock

import requests
from django.test import SimpleTestCase

from agrocosmos.services import osm_overpass as osm

POLY = {'type': 'Polygon', 'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 0]]]}


def _resp(status=200, text='', payload=None):
    resp = mock.Mock(status_code=status, text=text, reason='x')
    resp.json.return_value = payload
    resp.raise_for_status.side_effect = (
        requests.HTTPError(str(status)) if status >= 400 else None
    )
    return resp


def _nominatim(*geometries):
    return _resp(payload={'type': 'FeatureCollection', 'features': [
        {'type': 'Feature', 'geometry': g} for g in geometries
    ]})


class FetchPolygonTests(SimpleTestCase):
    def setUp(self):
        osm._polygons_failures = 0
        self.addCleanup(setattr, osm, '_polygons_failures', 0)
        patcher = mock.patch.object(osm.time, 'sleep')
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def _fetch(self, *responses, osm_id=215743):
        with mock.patch.object(osm.requests, 'get',
                               side_effect=list(responses)) as get:
            result = osm.fetch_polygon_geojson(osm_id)
        return result, [c.args[0] for c in get.call_args_list]

    def test_primary_source_used_when_healthy(self):
        result, urls = self._fetch(_resp(text='{"type": "MultiPolygon", "coordinates": []}'))
        self.assertEqual(result['type'], 'MultiPolygon')
        self.assertEqual(urls, [osm.POLYGONS_URL])

    def test_server_error_falls_back_to_nominatim_without_retries(self):
        result, urls = self._fetch(_resp(500, 'Internal Server Error'),
                                   _nominatim(POLY))
        self.assertEqual(result, POLY)
        self.assertEqual(urls, [osm.POLYGONS_URL, osm.NOMINATIM_LOOKUP_URL])
        self.sleep.assert_not_called()

    def test_network_error_falls_back_to_nominatim(self):
        result, urls = self._fetch(requests.ConnectionError('reset'),
                                   _nominatim(POLY))
        self.assertEqual(result, POLY)
        self.assertEqual(urls, [osm.POLYGONS_URL, osm.NOMINATIM_LOOKUP_URL])

    def test_polygon_still_building_is_retried_then_falls_back(self):
        result, urls = self._fetch(_resp(text='None'), _resp(text='None'),
                                   _resp(text='None'), _nominatim(POLY))
        self.assertEqual(result, POLY)
        self.assertEqual(urls, [osm.POLYGONS_URL] * 3 + [osm.NOMINATIM_LOOKUP_URL])
        self.assertEqual(self.sleep.call_count, 3)

    def test_nominatim_lookup_params(self):
        with mock.patch.object(osm.requests, 'get', side_effect=[
            _resp(500), _nominatim(POLY),
        ]) as get:
            osm.fetch_polygon_geojson(59195)
        self.assertEqual(get.call_args.kwargs['params'], {
            'osm_ids': 'R59195', 'format': 'geojson', 'polygon_geojson': 1,
        })

    def test_nominatim_point_is_not_a_polygon(self):
        result, _ = self._fetch(
            _resp(500), _nominatim({'type': 'Point', 'coordinates': [0, 0]}))
        self.assertIsNone(result)

    def test_both_sources_down(self):
        result, _ = self._fetch(_resp(500), _resp(503))
        self.assertIsNone(result)
        result, _ = self._fetch(_resp(500), requests.Timeout('slow'))
        self.assertIsNone(result)

    def test_breaker_skips_dead_primary_source(self):
        """После трёх отказов подряд polygons.osm.fr больше не опрашивается."""
        for _ in range(osm._POLYGONS_DOWN_AFTER):
            self._fetch(_resp(500), _nominatim(POLY))
        result, urls = self._fetch(_nominatim(POLY))
        self.assertEqual(result, POLY)
        self.assertEqual(urls, [osm.NOMINATIM_LOOKUP_URL])

    def test_success_resets_breaker(self):
        self._fetch(_resp(500), _nominatim(POLY))
        self._fetch(_resp(500), _nominatim(POLY))
        self._fetch(_resp(text='{"type": "Polygon", "coordinates": []}'))
        self.assertEqual(osm._polygons_failures, 0)


class OverpassMirrorTests(SimpleTestCase):
    def setUp(self):
        patcher = mock.patch.object(osm.time, 'sleep')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _post(self, *responses):
        with mock.patch.object(osm.requests, 'post',
                               side_effect=list(responses)) as post:
            try:
                result = osm._overpass_post('query')
            except Exception as exc:
                result = exc
        return result, [c.args[0] for c in post.call_args_list]

    def test_first_endpoint_is_configured_url(self):
        result, urls = self._post(_resp(payload={'elements': []}))
        self.assertEqual(result, {'elements': []})
        self.assertEqual(urls, [osm.OVERPASS_URL])

    def test_rotates_to_next_mirror_on_failure(self):
        result, urls = self._post(
            _resp(502), requests.Timeout('hung'),
            _resp(payload={'elements': [1]}),
        )
        self.assertEqual(result, {'elements': [1]})
        self.assertEqual(urls, osm._overpass_endpoints()[:3])
        self.assertEqual(len(set(urls)), 3)

    def test_gives_up_after_two_passes_over_endpoints(self):
        result, urls = self._post(*[_resp(502)] * 8)
        self.assertIsInstance(result, requests.HTTPError)
        self.assertEqual(urls, osm._overpass_endpoints() * 2)

    def test_rejected_by_one_endpoint_tries_the_next(self):
        """403/429 — отказ конкретного зеркала, а не ошибка запроса."""
        result, urls = self._post(_resp(403), _resp(payload={'elements': []}))
        self.assertEqual(result, {'elements': []})
        self.assertEqual(urls, osm._overpass_endpoints()[:2])


class OverpassBadAnswerTests(SimpleTestCase):
    """HTTP 200, которому нельзя верить: ``remark`` и неразрешённая область."""

    def setUp(self):
        patcher = mock.patch.object(osm.time, 'sleep')
        patcher.start()
        self.addCleanup(patcher.stop)

    AREA = {'type': 'area', 'id': 3600223474}
    REL = {'type': 'relation', 'id': 301,
           'tags': {'name': 'Kadıköy', 'name:ru': 'Кадыкёй'}}

    def _districts(self, *responses):
        with mock.patch.object(osm.requests, 'post',
                               side_effect=list(responses)) as post:
            try:
                result = osm.fetch_admin_relations_in(223474, 6)
            except Exception as exc:
                result = exc
        return result, post.call_count

    def test_timeout_remark_is_retried(self):
        """Запрос, оборванный по времени, — не «ничего не найдено»."""
        result, calls = self._districts(
            _resp(payload={'elements': [], 'remark':
                           'runtime error: Query timed out in "query" at line 3'}),
            _resp(payload={'elements': [self.AREA, self.REL]}),
        )
        self.assertEqual([r['name'] for r in result], ['Кадыкёй'])
        self.assertEqual(calls, 2)

    def test_unresolved_area_is_retried(self):
        """Зеркало без базы областей отвечает пустым списком."""
        result, calls = self._districts(
            _resp(payload={'elements': []}),
            _resp(payload={'elements': [self.AREA, self.REL]}),
        )
        self.assertEqual([r['osm_id'] for r in result], [301])
        self.assertEqual(calls, 2)

    def test_empty_answer_needs_a_second_endpoint(self):
        """Область найдена, районов нет — верим, только когда это
        подтвердил второй, другой сервер."""
        empty = _resp(payload={'elements': [self.AREA]})
        with mock.patch.object(osm.requests, 'post',
                               side_effect=[empty, empty]) as post:
            result = osm.fetch_admin_relations_in(223474, 6)
        self.assertEqual(result, [])
        urls = [c.args[0] for c in post.call_args_list]
        self.assertEqual(len(set(urls)), 2)

    def test_stale_mirror_empty_answer_is_overruled(self):
        """Зеркало с неполной базой: область есть, районов «нет». Ответ
        другого сервера с районами побеждает сразу."""
        result, calls = self._districts(
            _resp(payload={'elements': [self.AREA]}),
            _resp(payload={'elements': [self.AREA, self.REL]}),
        )
        self.assertEqual([r['osm_id'] for r in result], [301])
        self.assertEqual(calls, 2)

    def test_unconfirmed_empty_answer_raises(self):
        """Один сервер говорит «пусто», остальные лежат — это «не знаю»."""
        empty = _resp(payload={'elements': [self.AREA]})
        n = len(osm._overpass_endpoints())
        # Пустой ответ только от первого сервера, на обоих проходах.
        responses = [empty if i % n == 0 else _resp(502) for i in range(8)]
        result, calls = self._districts(*responses)
        # Падает последней ошибкой перебора — главное, что не «пусто».
        self.assertIsInstance(result, requests.RequestException)
        self.assertEqual(calls, 8)

    def test_never_resolved_area_raises(self):
        """Ни одно зеркало не знает область — ошибка, а не «районов нет»."""
        result, calls = self._districts(*[_resp(payload={'elements': []})] * 8)
        self.assertIsInstance(result, osm.OverpassBadAnswer)
        self.assertEqual(calls, 8)
