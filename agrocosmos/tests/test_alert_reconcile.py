"""Тесты ``_reconcile`` в ``detect_vegetation_alerts`` (per-farmland) и
``detect_district_ndvi_alerts`` (per-district) — страховочная сетка перед
рефакторингом (C=16 / C=15). Проверяем create / update / resolve,
эскалацию severity с email-нотификацией и dry-run.
"""
from datetime import date
from unittest import mock

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase

from agrocosmos.management.commands.detect_district_ndvi_alerts import (
    Command as DistrictCommand,
)
from agrocosmos.management.commands.detect_vegetation_alerts import (
    Command as FarmlandCommand,
)
from agrocosmos.models import District, Farmland, Region, VegetationAlert

EMAIL = 'agrocosmos.services.notifications.send_anomaly_email'


def _square(x, y, size=0.5):
    return MultiPolygon(Polygon((
        (x, y), (x + size, y), (x + size, y + size), (x, y + size), (x, y),
    )))


def _detection(severity='warning', detected_on=None, ndvi=0.3):
    return {
        'detected_on': detected_on or date(2025, 6, 10),
        'severity': severity,
        'context': {'ndvi': ndvi},
        'message': f'NDVI {ndvi}',
    }


class _BaseReconcileTests:
    """Общие сценарии; наследники задают _reconcile и _make_existing."""

    def test_no_detection_no_existing(self):
        with mock.patch(EMAIL) as email:
            result = self._reconcile(None, dry=False)
        self.assertEqual(result, (0, 0, 0))
        email.assert_not_called()

    def test_no_detection_resolves_existing(self):
        alert = self._make_existing()
        with mock.patch(EMAIL):
            result = self._reconcile(None, dry=False)
        self.assertEqual(result, (0, 0, 1))
        alert.refresh_from_db()
        self.assertEqual(alert.status, VegetationAlert.Status.RESOLVED)
        self.assertIsNotNone(alert.resolved_at)

    def test_detection_creates_alert_and_notifies(self):
        with mock.patch(EMAIL) as email:
            result = self._reconcile(_detection(), dry=False)
        self.assertEqual(result, (1, 0, 0))
        alert = VegetationAlert.objects.get()
        self.assertEqual(alert.status, VegetationAlert.Status.ACTIVE)
        self.assertEqual(alert.severity, 'warning')
        self.assertEqual(alert.detected_on, date(2025, 6, 10))
        email.assert_called_once()

    def test_dry_run_creates_nothing(self):
        with mock.patch(EMAIL) as email:
            result = self._reconcile(_detection(), dry=True)
        self.assertEqual(result, (1, 0, 0))
        self.assertEqual(VegetationAlert.objects.count(), 0)
        email.assert_not_called()

    def test_same_detection_no_update(self):
        with mock.patch(EMAIL):
            self._reconcile(_detection(), dry=False)
            result = self._reconcile(_detection(), dry=False)
        self.assertEqual(result, (0, 0, 0))

    def test_escalation_updates_and_notifies(self):
        with mock.patch(EMAIL):
            self._reconcile(_detection('warning'), dry=False)
        with mock.patch(EMAIL) as email:
            result = self._reconcile(_detection('critical'), dry=False)
        self.assertEqual(result, (0, 1, 0))
        alert = VegetationAlert.objects.get()
        self.assertEqual(alert.severity, 'critical')
        email.assert_called_once()

    def test_non_escalation_update_without_email(self):
        with mock.patch(EMAIL):
            self._reconcile(_detection(), dry=False)
        with mock.patch(EMAIL) as email:
            result = self._reconcile(
                _detection(detected_on=date(2025, 6, 12)), dry=False)
        self.assertEqual(result, (0, 1, 0))
        alert = VegetationAlert.objects.get()
        self.assertEqual(alert.detected_on, date(2025, 6, 12))
        email.assert_not_called()

    def test_email_failure_is_swallowed(self):
        with mock.patch(EMAIL, side_effect=RuntimeError('smtp down')):
            result = self._reconcile(_detection(), dry=False)
        self.assertEqual(result, (1, 0, 0))
        self.assertEqual(VegetationAlert.objects.count(), 1)


class FarmlandReconcileTests(_BaseReconcileTests, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50))
        cls.farmland = Farmland.objects.create(
            region=cls.region, district=cls.district,
            geom=_square(30.1, 50.1))

    def _reconcile(self, detection, dry):
        return FarmlandCommand()._reconcile(
            farmland=self.farmland,
            alert_type=VegetationAlert.AlertType.RAPID_DROP,
            detection=detection,
            dry=dry,
        )

    def _make_existing(self):
        return VegetationAlert.objects.create(
            farmland=self.farmland,
            alert_type=VegetationAlert.AlertType.RAPID_DROP,
            severity='warning',
            status=VegetationAlert.Status.ACTIVE,
            detected_on=date(2025, 6, 1),
        )


class DistrictReconcileTests(_BaseReconcileTests, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.region = Region.objects.create(
            name='Регион', code='r1', geom=_square(30, 50))
        cls.district = District.objects.create(
            region=cls.region, name='Район', geom=_square(30, 50))

    def _reconcile(self, detection, dry):
        return DistrictCommand()._reconcile(
            district=self.district,
            crop_type='arable',
            alert_type=VegetationAlert.AlertType.RAPID_DROP,
            detection=detection,
            dry=dry,
        )

    def _make_existing(self):
        return VegetationAlert.objects.create(
            farmland=None,
            district=self.district,
            crop_type='arable',
            source=VegetationAlert.Source.MODIS,
            alert_type=VegetationAlert.AlertType.RAPID_DROP,
            severity='warning',
            status=VegetationAlert.Status.ACTIVE,
            detected_on=date(2025, 6, 1),
        )

    def test_created_alert_is_district_level(self):
        with mock.patch(EMAIL):
            self._reconcile(_detection(), dry=False)
        alert = VegetationAlert.objects.get()
        self.assertIsNone(alert.farmland)
        self.assertEqual(alert.district, self.district)
        self.assertEqual(alert.crop_type, 'arable')
        self.assertEqual(alert.source, VegetationAlert.Source.MODIS)
