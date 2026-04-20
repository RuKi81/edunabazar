"""
Tests for `_next_aligned_period` used by `check_monitoring`.

CRITICAL regression guard: the NDVI "Mar 29 shown as Apr 2" bug was caused
by check_monitoring advancing `last_date_to` using a fixed ``timedelta``
instead of snapping to the Jan-1-anchored 16-day grid. These tests ensure
the helper stays aligned with `_biweekly_chunks`.
"""
from datetime import date

from django.test import SimpleTestCase

from agrocosmos.management.commands.check_monitoring import (
    _next_aligned_period,
)
from agrocosmos.services.satellite_modis_raster import _biweekly_chunks


class NextAlignedPeriodTests(SimpleTestCase):
    # ── Cold-start: no prior processing ────────────────────────────

    def test_no_last_date_starts_at_jan_1(self):
        nf, nt = _next_aligned_period(None, 2026)
        self.assertEqual(nf, date(2026, 1, 1))
        self.assertEqual(nt, date(2026, 1, 16))

    # ── Advance to next chunk ──────────────────────────────────────

    def test_advance_after_completed_chunk(self):
        """last_date_to = Jan 16 → next window is Jan 17..Feb 1."""
        nf, nt = _next_aligned_period(date(2026, 1, 16), 2026)
        self.assertEqual(nf, date(2026, 1, 17))
        self.assertEqual(nt, date(2026, 2, 1))

    def test_advance_after_mid_chunk_still_snaps(self):
        """
        Even if last_date_to is in the middle of a chunk (misconfigured
        manually), the next period must start at the *next* chunk boundary.
        """
        # Mar 29 lies inside chunk idx 5 (Mar 22..Apr 6).
        # Next chunk must begin Apr 7, not Apr 13.
        nf, nt = _next_aligned_period(date(2026, 3, 29), 2026)
        self.assertEqual(nf, date(2026, 4, 7))
        self.assertEqual(nt, date(2026, 4, 22))

    # ── Regression for the production bug ──────────────────────────

    def test_regression_apr_6_advances_to_apr_7(self):
        """
        After processing the Mar 22..Apr 6 composite, the next window
        MUST be Apr 7..Apr 22 (mid Apr 14). The pre-fix code produced
        Apr 2..Apr 17, shifting the graph label by 4 days.
        """
        nf, nt = _next_aligned_period(date(2026, 4, 6), 2026)
        self.assertEqual(nf, date(2026, 4, 7))
        self.assertEqual(nt, date(2026, 4, 22))

    # ── Year-end clamping ──────────────────────────────────────────

    def test_last_chunk_end_is_clamped_to_dec_31(self):
        # Chunk idx 22 starts Dec 19 (Jan 1 + 352d) and would end Jan 3 (2027);
        # it must clamp to Dec 31.
        nf, nt = _next_aligned_period(date(2026, 12, 18), 2026)
        self.assertEqual(nf, date(2026, 12, 19))
        self.assertEqual(nt, date(2026, 12, 31))

    # ── Consistency with the compositor's grid ─────────────────────

    def test_aligned_with_biweekly_chunks_grid(self):
        """
        Every (next_from, next_to) returned by _next_aligned_period for
        the full year must also be a valid chunk in _biweekly_chunks.
        This guarantees check_monitoring and the compositor share one grid.
        """
        grid_starts = {cf for cf, _ in _biweekly_chunks(date(2026, 1, 1), date(2026, 12, 31))}

        last = None
        seen = set()
        # Iterate the full year the same way check_monitoring does
        for _ in range(30):  # safety cap
            nf, nt = _next_aligned_period(last, 2026)
            if nf in seen:
                break
            seen.add(nf)
            self.assertIn(nf, grid_starts,
                          f'{nf} is not on the compositor grid')
            if nt >= date(2026, 12, 31):
                break
            last = nt
