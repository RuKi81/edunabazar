"""Код субъекта РФ у региона: справочник миграции и ``--subject-code``."""
from importlib import import_module
from io import StringIO

from django.apps import apps
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase

from agrocosmos.models import Region
from agrocosmos.services.regions import apply_subject_code

migration = import_module('agrocosmos.migrations.0046_region_subject_code')


def _mpoly():
    return MultiPolygon(Polygon(
        ((34, 45), (35, 45), (35, 46), (34, 46), (34, 45)),
    ))


class SubjectCodeDirectoryTests(SimpleTestCase):

    def test_codes_are_unique_two_digit(self):
        codes = list(migration.SUBJECT_CODES.values())
        self.assertEqual(len(codes), 85)
        self.assertEqual(len(set(codes)), len(codes))
        self.assertTrue(all(len(c) == 2 and c.isdigit() for c in codes))

    def test_known_regions(self):
        codes = migration.SUBJECT_CODES
        self.assertEqual(codes['tulskaya_obl'], '71')
        self.assertEqual(codes['moskovskaya_obl'], '50')
        self.assertEqual(codes['krim_resp'], '91')
        self.assertEqual(codes['kurganskaya_obl'], '45')


class SubjectCodeTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.tula = Region.objects.create(
            name='Тульская область', code='tulskaya_obl', geom=_mpoly())
        cls.other = Region.objects.create(
            name='Неизвестный', code='no_such_key', geom=_mpoly())

    def test_fill_by_key_leaves_unknown_empty(self):
        migration.fill_subject_codes(apps, None)
        self.tula.refresh_from_db()
        self.other.refresh_from_db()
        self.assertEqual(self.tula.subject_code, '71')
        self.assertIsNone(self.other.subject_code)

    def test_apply_subject_code_sets_region_id(self):
        Region.objects.filter(pk=self.tula.pk).update(subject_code='71')
        options = {'subject_code': '71', 'region_id': None}
        apply_subject_code(options)
        self.assertEqual(options['region_id'], self.tula.pk)

    def test_apply_subject_code_pads_single_digit(self):
        Region.objects.filter(pk=self.other.pk).update(subject_code='01')
        options = {'subject_code': '1', 'region_id': None}
        apply_subject_code(options)
        self.assertEqual(options['region_id'], self.other.pk)

    def test_apply_subject_code_noop_without_code(self):
        options = {'subject_code': None, 'region_id': 5}
        apply_subject_code(options)
        self.assertEqual(options['region_id'], 5)

    def test_unknown_subject_code_fails(self):
        with self.assertRaises(CommandError):
            apply_subject_code({'subject_code': '99', 'region_id': None})

    def test_subject_code_with_region_id_fails(self):
        Region.objects.filter(pk=self.tula.pk).update(subject_code='71')
        with self.assertRaises(CommandError):
            apply_subject_code(
                {'subject_code': '71', 'region_id': self.other.pk})

    def test_classify_command_accepts_subject_code(self):
        Region.objects.filter(pk=self.tula.pk).update(subject_code='71')
        out = StringIO()
        call_command('classify_winter_spring', subject_code='71',
                     year=2026, stdout=out, stderr=out)
        self.assertIn('No data', out.getvalue())
