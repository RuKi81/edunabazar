"""
Tests for `_ndvi_assessment` helper in agrocosmos.views.

Covers every branch of the assessment logic so any change to thresholds
produces an obvious test failure.
"""
from django.test import SimpleTestCase

from agrocosmos.views import _ndvi_assessment


class NdviAssessmentTests(SimpleTestCase):
    # ── None / missing data ────────────────────────────────────────

    def test_none_ndvi_returns_no_data(self):
        self.assertEqual(_ndvi_assessment(None), 'Нет данных')
        self.assertEqual(_ndvi_assessment(None, z_score=-3), 'Нет данных')

    # ── z-score branches dominate NDVI thresholds ──────────────────

    def test_severe_drought_when_z_below_minus_2(self):
        # z is the primary signal: even high NDVI flips to critical
        self.assertEqual(
            _ndvi_assessment(0.7, z_score=-2.5),
            'Критическое снижение вегетации',
        )

    def test_below_normal_when_z_between_minus_2_and_minus_1(self):
        self.assertEqual(
            _ndvi_assessment(0.5, z_score=-1.5),
            'Вегетация ниже нормы',
        )

    def test_significantly_above_when_z_over_2(self):
        self.assertEqual(
            _ndvi_assessment(0.5, z_score=2.5),
            'Вегетация значительно выше нормы',
        )

    def test_above_normal_when_z_between_1_and_2(self):
        self.assertEqual(
            _ndvi_assessment(0.5, z_score=1.5),
            'Вегетация выше нормы',
        )

    # ── Fallback to NDVI thresholds when z is None or "normal" ─────

    def test_active_vegetation_high_ndvi(self):
        self.assertEqual(_ndvi_assessment(0.75), 'Активная вегетация')
        self.assertEqual(_ndvi_assessment(0.6), 'Активная вегетация')

    def test_moderate_vegetation(self):
        self.assertEqual(_ndvi_assessment(0.5), 'Умеренная вегетация')
        self.assertEqual(_ndvi_assessment(0.4), 'Умеренная вегетация')

    def test_weak_vegetation(self):
        self.assertEqual(_ndvi_assessment(0.3), 'Слабая вегетация')
        self.assertEqual(_ndvi_assessment(0.2), 'Слабая вегетация')

    def test_no_vegetation_low_ndvi(self):
        self.assertEqual(
            _ndvi_assessment(0.15),
            'Вегетация практически отсутствует',
        )
        self.assertEqual(
            _ndvi_assessment(0.0),
            'Вегетация практически отсутствует',
        )

    # ── z in "normal" range falls through to NDVI branch ───────────

    def test_normal_z_uses_ndvi_threshold(self):
        """|z| <= 1 → z is ignored, NDVI decides."""
        self.assertEqual(
            _ndvi_assessment(0.7, z_score=0.5),
            'Активная вегетация',
        )
        self.assertEqual(
            _ndvi_assessment(0.1, z_score=-0.5),
            'Вегетация практически отсутствует',
        )
