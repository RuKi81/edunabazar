"""Тесты инструмента разметки обучающей выборки (``views/labeling.py``).

Покрывается:
1. Гейт админа на странице ``label/`` и всех трёх API.
2. Отбор кандидатов: обязательный субъект, режим «сомнительные»
   (близость пика к порогу / низкая уверенность), скрытие размеченных,
   отдача геометрии и фич.
3. Сохранение/обновление/снятие метки + валидация входа.
4. Счётчики меток по классам.
5. Использование ручных меток командой ``classify_winter_spring
   --reference-labels`` как эталонов.
"""
import json
from datetime import date, timedelta
from io import StringIO

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from legacy.constants import USER_STATUS_ACTIVE
from legacy.models import LegacyUser

from agrocosmos.models import (
    District, Farmland, FarmlandCropSeason, FarmlandTrainingLabel, Region,
    SatelliteScene, VegetationIndex,
)

YEAR = 2026
PAGE_URL = '/agrocosmos/label/'
CANDIDATES_URL = '/agrocosmos/api/label/candidates/'
SAVE_URL = '/agrocosmos/api/label/'
STATS_URL = '/agrocosmos/api/label/stats/'

_DUMMY_CACHE = {
    'default': {'BACKEND': 'django.core.cache.backends.dummy.DummyCache'},
}


def _square(x, y, size=0.05):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _make_user(username):
    now = timezone.now()
    return LegacyUser.objects.create(
        type=0, username=username, auth_key='', password_hash='',
        email=f'{username}@test.com', currency='RU', name='', address='',
        phone='', inn='', status=USER_STATUS_ACTIVE,
        created_at=now, updated_at=now, contacts='',
    )


def _login(client, user):
    client.get('/')
    session = client.session
    session['legacy_user_id'] = user.pk
    session.save()
    from django.conf import settings as _s
    client.cookies[_s.SESSION_COOKIE_NAME] = session.session_key
    return client


@override_settings(CACHES=_DUMMY_CACHE, ADMIN_USERNAMES={'admin'})
class LabelingApiTests(TestCase):
    """Кандидаты, сохранение меток и счётчики."""

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион Л', code='rl-1', geom=_square(37, 54, 1),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район Л', geom=_square(37, 54, 1),
        )
        # Субъект без кандидатов — проверяем изоляцию выборки.
        cls.other_region = Region.objects.create(
            name='Регион О', code='ro-1', geom=_square(40, 54, 1),
        )

        def farmland(x, crop=Farmland.CropType.ARABLE, area=100):
            return Farmland.objects.create(
                region=cls.region, district=cls.district,
                crop_type=crop, area_ha=area, geom=_square(x, 54.1),
            )

        def season(fl, **kwargs):
            data = dict(
                farmland=fl, year=YEAR,
                source=FarmlandCropSeason.Source.FUSED,
                season_class=FarmlandCropSeason.SeasonClass.WINTER,
                confidence=0.9, peak_doy=185, peak_doy_threshold=185,
            )
            data.update(kwargs)
            return FarmlandCropSeason.objects.create(**data)

        # Сомнительное: пик ровно на пороге.
        cls.fl_ambig_peak = farmland(37.1)
        season(cls.fl_ambig_peak, peak_doy=188, confidence=0.95)
        # Сомнительное: низкая уверенность, пик далеко от порога.
        cls.fl_ambig_conf = farmland(37.2)
        season(cls.fl_ambig_conf, peak_doy=240, confidence=0.30,
               season_class=FarmlandCropSeason.SeasonClass.SPRING)
        # Уверенное: и пик далеко, и уверенность высокая.
        cls.fl_confident = farmland(37.3)
        season(cls.fl_confident, peak_doy=245, confidence=0.92,
               season_class=FarmlandCropSeason.SeasonClass.SPRING)
        # Не участвует: пастбище (не пашня/сенокос).
        cls.fl_pasture = farmland(37.4, crop=Farmland.CropType.PASTURE)
        season(cls.fl_pasture, peak_doy=186, confidence=0.4)
        # Не участвует: класс unknown.
        cls.fl_unknown = farmland(37.5)
        season(cls.fl_unknown, peak_doy=186, confidence=0.4,
               season_class=FarmlandCropSeason.SeasonClass.UNKNOWN)

    def setUp(self):
        self.admin = _make_user('admin')
        self.plain = _make_user('plainuser')
        self.client = _login(Client(), self.admin)

    # ── доступ ───────────────────────────────────────────────────────
    def test_page_requires_admin(self):
        anon = Client()
        self.assertEqual(anon.get(PAGE_URL).status_code, 403)
        user_client = _login(Client(), self.plain)
        self.assertEqual(user_client.get(PAGE_URL).status_code, 403)

    def test_page_ok_for_admin(self):
        resp = self.client.get(PAGE_URL)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context['year'], str(date.today().year))
        self.assertIn('Обучающая выборка', resp.content.decode())

    def test_apis_require_admin(self):
        c = _login(Client(), self.plain)
        self.assertEqual(
            c.get(CANDIDATES_URL, {'region': self.region.pk,
                                   'year': YEAR}).status_code, 403)
        self.assertEqual(
            c.post(SAVE_URL, data='{}',
                   content_type='application/json').status_code, 403)
        self.assertEqual(c.get(STATS_URL, {'year': YEAR}).status_code, 403)

    # ── кандидаты ────────────────────────────────────────────────────
    def _candidates(self, **params):
        params.setdefault('region', self.region.pk)
        params.setdefault('year', YEAR)
        params.setdefault('source', 'fused')
        return self.client.get(CANDIDATES_URL, params).json()

    def test_candidates_region_required(self):
        resp = self.client.get(CANDIDATES_URL, {'year': YEAR})
        self.assertEqual(resp.status_code, 400)

    def test_candidates_ambiguous_mode_filters_confident(self):
        data = self._candidates(mode='ambiguous')
        self.assertTrue(data['ok'])
        ids = {c['farmland_id'] for c in data['candidates']}
        self.assertEqual(ids, {self.fl_ambig_peak.pk, self.fl_ambig_conf.pk})

    def test_candidates_all_mode_adds_confident(self):
        ids = {c['farmland_id'] for c in self._candidates(mode='all')['candidates']}
        self.assertEqual(ids, {self.fl_ambig_peak.pk, self.fl_ambig_conf.pk,
                               self.fl_confident.pk})

    def test_candidates_exclude_pasture_and_unknown(self):
        ids = {c['farmland_id'] for c in self._candidates(mode='all')['candidates']}
        self.assertNotIn(self.fl_pasture.pk, ids)
        self.assertNotIn(self.fl_unknown.pk, ids)

    def test_candidates_other_region_empty(self):
        data = self._candidates(region=self.other_region.pk, mode='all')
        self.assertEqual(data['count'], 0)

    def test_candidates_payload_shape(self):
        data = self._candidates(mode='ambiguous')
        by_id = {c['farmland_id']: c for c in data['candidates']}
        c = by_id[self.fl_ambig_conf.pk]
        self.assertEqual(c['predicted_class'], 'spring')
        self.assertEqual(c['peak_doy'], 240)
        self.assertEqual(c['district'], 'Район Л')
        self.assertEqual(c['crop_type'], 'arable')
        self.assertAlmostEqual(c['confidence'], 0.3, places=3)
        self.assertIsNone(c['label'])
        self.assertIn(c['geometry']['type'], ('Polygon', 'MultiPolygon'))

    def test_candidates_missing_features_stay_none(self):
        """``sos_doy``/``early_spring_ndvi`` пусты → в ответе ``None``."""
        c = self._candidates(mode='ambiguous')['candidates'][0]
        self.assertIsNone(c['sos_doy'])
        self.assertIsNone(c['early_spring_ndvi'])

    def test_candidates_unlabeled_hides_labeled(self):
        FarmlandTrainingLabel.objects.create(
            farmland=self.fl_ambig_peak, year=YEAR, true_class='winter',
        )
        ids = {c['farmland_id']
               for c in self._candidates(mode='ambiguous',
                                         unlabeled='1')['candidates']}
        self.assertEqual(ids, {self.fl_ambig_conf.pk})
        labeled = {c['farmland_id']: c['label']
                   for c in self._candidates(mode='ambiguous',
                                             unlabeled='0')['candidates']}
        self.assertEqual(labeled[self.fl_ambig_peak.pk], 'winter')

    def test_candidates_limit(self):
        data = self._candidates(mode='all', limit=1)
        self.assertEqual(data['count'], 1)

    # ── сохранение метки ─────────────────────────────────────────────
    def _save(self, payload, method='post'):
        fn = getattr(self.client, method)
        return fn(SAVE_URL, data=json.dumps(payload),
                  content_type='application/json')

    def test_save_creates_and_updates(self):
        resp = self._save({'farmland_id': self.fl_ambig_peak.pk,
                           'year': YEAR, 'true_class': 'winter',
                           'note': 'озимая пшеница'})
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()['created'])
        label = FarmlandTrainingLabel.objects.get(
            farmland=self.fl_ambig_peak, year=YEAR)
        self.assertEqual(label.true_class, 'winter')
        self.assertEqual(label.note, 'озимая пшеница')
        self.assertEqual(label.labeled_by, 'admin')

        resp = self._save({'farmland_id': self.fl_ambig_peak.pk,
                           'year': YEAR, 'true_class': 'spring'})
        self.assertFalse(resp.json()['created'])
        label.refresh_from_db()
        self.assertEqual(label.true_class, 'spring')
        self.assertEqual(
            FarmlandTrainingLabel.objects.filter(year=YEAR).count(), 1)

    def test_save_validation(self):
        self.assertEqual(
            self._save({'year': YEAR, 'true_class': 'winter'}).status_code, 400)
        self.assertEqual(
            self._save({'farmland_id': self.fl_ambig_peak.pk, 'year': YEAR,
                        'true_class': 'wheat'}).status_code, 400)
        self.assertEqual(
            self._save({'farmland_id': 10 ** 9, 'year': YEAR,
                        'true_class': 'winter'}).status_code, 404)
        bad = self.client.post(SAVE_URL, data='{not json',
                               content_type='application/json')
        self.assertEqual(bad.status_code, 400)
        self.assertFalse(FarmlandTrainingLabel.objects.exists())

    def test_delete_removes_label(self):
        FarmlandTrainingLabel.objects.create(
            farmland=self.fl_ambig_peak, year=YEAR, true_class='winter')
        resp = self._save({'farmland_id': self.fl_ambig_peak.pk,
                           'year': YEAR}, method='delete')
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()['deleted'])
        self.assertFalse(FarmlandTrainingLabel.objects.exists())

    def test_save_rejects_get(self):
        self.assertEqual(self.client.get(SAVE_URL).status_code, 405)

    # ── счётчики ─────────────────────────────────────────────────────
    def test_stats_by_class_and_region(self):
        FarmlandTrainingLabel.objects.create(
            farmland=self.fl_ambig_peak, year=YEAR, true_class='winter')
        FarmlandTrainingLabel.objects.create(
            farmland=self.fl_ambig_conf, year=YEAR, true_class='spring')
        FarmlandTrainingLabel.objects.create(
            farmland=self.fl_confident, year=YEAR - 1, true_class='unused')

        stats = self.client.get(STATS_URL, {'year': YEAR}).json()['stats']
        self.assertEqual(stats['winter'], 1)
        self.assertEqual(stats['spring'], 1)
        self.assertEqual(stats['unused'], 0)
        self.assertEqual(stats['total'], 2)

        scoped = self.client.get(
            STATS_URL, {'year': YEAR, 'region': self.other_region.pk},
        ).json()['stats']
        self.assertEqual(scoped['total'], 0)


def _winter_ndvi(doy):
    """Озимые: ранний пик NDVI (конец мая–июнь)."""
    if doy < 60:
        return 0.32
    if doy < 150:
        return 0.32 + (doy - 60) / 90 * 0.53
    if doy < 200:
        return 0.85 - (doy - 150) / 50 * 0.55
    return 0.20


def _spring_ndvi(doy):
    """Яровые: голая почва весной, поздний пик."""
    if doy < 140:
        return 0.16
    if doy < 220:
        return 0.16 + (doy - 140) / 80 * 0.69
    if doy < 270:
        return 0.85 - (doy - 220) / 50 * 0.60
    return 0.20


@override_settings(CACHES=_DUMMY_CACHE)
class ClassifyWithManualLabelsTests(TestCase):
    """``classify_winter_spring --reference-labels`` берёт ручные метки."""

    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион М', code='rm-1', geom=_square(37, 54, 1),
        )
        cls.district = District.objects.create(
            region=cls.region, name='Район М', geom=_square(37, 54, 1),
        )
        cls.fl_winter = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(37.1, 54.1),
        )
        cls.fl_spring = Farmland.objects.create(
            region=cls.region, district=cls.district,
            crop_type=Farmland.CropType.ARABLE, area_ha=100,
            geom=_square(37.2, 54.1),
        )

        start = date(YEAR, 1, 1)
        for step in range(0, 46):
            acq = start + timedelta(days=step * 8)
            doy = acq.timetuple().tm_yday
            scene = SatelliteScene.objects.create(
                satellite='sentinel2', scene_id=f'S2_{acq}', acquired_date=acq,
            )
            for fl, profile in ((cls.fl_winter, _winter_ndvi),
                                (cls.fl_spring, _spring_ndvi)):
                VegetationIndex.objects.create(
                    farmland=fl, scene=scene, index_type='ndvi',
                    acquired_date=acq, mean=profile(doy),
                    mean_smooth=profile(doy),
                )

        FarmlandTrainingLabel.objects.create(
            farmland=cls.fl_winter, year=YEAR, true_class='winter')
        FarmlandTrainingLabel.objects.create(
            farmland=cls.fl_spring, year=YEAR, true_class='spring')
        # Сенокос в калибровке пик-порога не участвует.
        FarmlandTrainingLabel.objects.create(
            farmland=cls.fl_spring, year=YEAR - 1, true_class='hayfield')

    def _run(self, **kwargs):
        out = StringIO()
        call_command(
            'classify_winter_spring', region_id=self.region.pk, year=YEAR,
            source='raster', stdout=out, **kwargs,
        )
        return out.getvalue()

    def test_labels_used_as_references(self):
        out = self._run(reference_labels=True, dry_run=True)
        self.assertIn('Ручные метки', out)
        self.assertIn('озимые=1', out)
        self.assertIn('яровые=1', out)

    def test_labels_not_used_without_flag(self):
        self.assertNotIn('Ручные метки', self._run(dry_run=True))

    def test_hayfield_label_of_other_year_ignored(self):
        out = self._run(reference_labels=True, dry_run=True)
        self.assertNotIn('пропущено(сенокос/сад)', out)

    def test_labels_marked_as_reference_in_db(self):
        self._run(reference_labels=True)
        winter = FarmlandCropSeason.objects.get(
            farmland=self.fl_winter, year=YEAR, source='raster')
        self.assertTrue(winter.is_reference)
        self.assertEqual(winter.reference_crop, 'label:winter')
