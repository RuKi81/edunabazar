"""
Tests for the HLS-style S2+Landsat fusion logic in ``compute_fused_ndvi``.

The fusion rules we verify (see command docstring):

1. S2 and Landsat observations within ±8 days are merged by weighted
   mean with weights = ``valid_pixel_count``.
2. S2 observations without a Landsat pair keep their original value.
3. Landsat observations that were not paired with any S2 are added as
   standalone fused points (gap-fill).
4. Output is sorted chronologically.
5. Repeat runs must not drift — fusion is a pure function of inputs.
"""
from datetime import date

from django.test import SimpleTestCase

from agrocosmos.management.commands.compute_fused_ndvi import Command


class FuseOneTests(SimpleTestCase):
    """Pure-function tests for ``Command._fuse_one``."""

    fuse = staticmethod(Command._fuse_one)

    # ── Weighted mean ──────────────────────────────────────────────

    def test_s2_and_l_same_day_weighted_by_n_valid(self):
        """S2 (400 pix, 0.7) + L (10 pix, 0.5) → ≈0.6951."""
        d = date(2025, 7, 10)
        result = self.fuse(
            s2_obs=[(d, 0.7, 400)],
            l_obs=[(d, 0.5, 10)],
        )
        self.assertEqual(len(result), 1)
        fused_date, fused_mean, fused_n = result[0]
        self.assertEqual(fused_date, d)
        # (0.7*400 + 0.5*10) / 410 = 285 / 410 = 0.6951...
        self.assertAlmostEqual(fused_mean, 0.6951, places=3)
        self.assertEqual(fused_n, 410)

    def test_l_within_window_is_paired(self):
        """L 5 days away from S2 is within ±8d window → paired."""
        s2_date = date(2025, 7, 10)
        l_date = date(2025, 7, 15)  # +5 days
        result = self.fuse(
            s2_obs=[(s2_date, 0.6, 100)],
            l_obs=[(l_date, 0.4, 100)],
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], s2_date)  # fused kept on S2 date
        self.assertAlmostEqual(result[0][1], 0.5, places=4)

    def test_l_outside_window_not_paired_orphan_added(self):
        """L 10 days away → not paired; added as standalone orphan."""
        s2_date = date(2025, 7, 10)
        l_date = date(2025, 7, 25)  # +15 days, outside ±8d
        result = self.fuse(
            s2_obs=[(s2_date, 0.6, 100)],
            l_obs=[(l_date, 0.4, 50)],
        )
        self.assertEqual(len(result), 2)
        # S2 untouched
        self.assertEqual(result[0], (s2_date, 0.6, 100))
        # Orphan L kept as-is
        self.assertEqual(result[1], (l_date, 0.4, 50))

    # ── S2 only / Landsat only ─────────────────────────────────────

    def test_s2_only_keeps_all_points(self):
        obs = [
            (date(2025, 6, 3), 0.4, 200),
            (date(2025, 6, 8), 0.45, 200),
            (date(2025, 6, 13), 0.5, 200),
        ]
        result = self.fuse(s2_obs=obs, l_obs=[])
        self.assertEqual(result, obs)

    def test_landsat_only_all_are_orphans(self):
        l = [
            (date(2025, 6, 8), 0.55, 50),
            (date(2025, 6, 24), 0.60, 55),
        ]
        result = self.fuse(s2_obs=[], l_obs=l)
        self.assertEqual(result, l)

    def test_empty_inputs_return_empty(self):
        self.assertEqual(self.fuse([], []), [])

    # ── Nearest-pair selection ─────────────────────────────────────

    def test_nearest_l_is_chosen_when_multiple_in_window(self):
        """Given 2 Landsats within ±8d, the closest one wins."""
        s2_date = date(2025, 7, 10)
        l_near = date(2025, 7, 8)    # 2 days
        l_far = date(2025, 7, 15)    # 5 days
        result = self.fuse(
            s2_obs=[(s2_date, 0.6, 100)],
            l_obs=[(l_far, 0.9, 100), (l_near, 0.2, 100)],
        )
        self.assertEqual(len(result), 2)  # 1 fused + 1 orphan
        fused = next(r for r in result if r[0] == s2_date)
        # Paired with l_near (0.2) → (0.6*100 + 0.2*100)/200 = 0.4
        self.assertAlmostEqual(fused[1], 0.4, places=4)
        # l_far becomes orphan (was NOT used for pairing)
        orphan = next(r for r in result if r[0] == l_far)
        self.assertEqual(orphan, (l_far, 0.9, 100))

    def test_same_l_used_by_only_one_s2_further_s2_is_untouched(self):
        """If one L is the nearest for two S2 points, it's only marked as
        "used" once — the second S2 still gets paired, but the L is not
        duplicated as an orphan. This matches the current semantics:
        "used" = L participated in at least one fusion."""
        l_date = date(2025, 7, 10)
        s2a = date(2025, 7, 9)
        s2b = date(2025, 7, 11)
        result = self.fuse(
            s2_obs=[(s2a, 0.5, 100), (s2b, 0.7, 100)],
            l_obs=[(l_date, 0.3, 100)],
        )
        # Both S2 get paired with the same L; L is NOT emitted as orphan.
        self.assertEqual(len(result), 2)
        dates = {r[0] for r in result}
        self.assertEqual(dates, {s2a, s2b})

    # ── Sorting ────────────────────────────────────────────────────

    def test_output_is_chronologically_sorted(self):
        result = self.fuse(
            s2_obs=[
                (date(2025, 8, 1), 0.7, 100),
                (date(2025, 6, 1), 0.4, 100),
            ],
            l_obs=[(date(2025, 7, 15), 0.6, 100)],
        )
        dates = [r[0] for r in result]
        self.assertEqual(dates, sorted(dates))

    # ── Edge: exact ±8d boundary is inclusive ──────────────────────

    def test_boundary_8_days_inclusive(self):
        s2_date = date(2025, 7, 10)
        l_date = date(2025, 7, 18)  # exactly 8 days
        result = self.fuse(
            s2_obs=[(s2_date, 0.6, 100)],
            l_obs=[(l_date, 0.4, 100)],
        )
        # Paired, not orphaned
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0][1], 0.5, places=4)

    def test_boundary_9_days_exclusive(self):
        s2_date = date(2025, 7, 10)
        l_date = date(2025, 7, 19)  # 9 days
        result = self.fuse(
            s2_obs=[(s2_date, 0.6, 100)],
            l_obs=[(l_date, 0.4, 100)],
        )
        # Not paired → 2 points
        self.assertEqual(len(result), 2)

    # ── Weighting favours the source with more valid pixels ────────

    def test_high_weight_s2_dominates(self):
        """S2 at 10m has ~9× more pixels per polygon than L at 30m —
        the fused value is strongly pulled towards S2."""
        d = date(2025, 7, 10)
        result = self.fuse(
            s2_obs=[(d, 0.70, 900)],
            l_obs=[(d, 0.30, 100)],
        )
        fused_mean = result[0][1]
        # (0.70*900 + 0.30*100) / 1000 = 0.66
        self.assertAlmostEqual(fused_mean, 0.66, places=3)
        # Much closer to S2 than to L
        self.assertGreater(fused_mean, 0.6)
