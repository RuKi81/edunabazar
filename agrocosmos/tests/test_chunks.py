"""
Tests for `_biweekly_chunks` in satellite_modis_raster.

This function drives the MODIS 16-day compositing grid and MUST stay
anchored to January 1st of the target year, otherwise downstream
records get misaligned dates (regression test for the Mar 29 / Apr 2 bug).
"""
from datetime import date

from django.test import SimpleTestCase

from agrocosmos.services.satellite_modis_raster import _biweekly_chunks


class BiweeklyChunksTests(SimpleTestCase):
    # ── Grid anchoring ──────────────────────────────────────────────

    def test_anchor_to_jan_1(self):
        """First chunk always starts at Jan 1 regardless of date_from."""
        # date_from much later than Jan 1 — still aligned to grid
        chunks = _biweekly_chunks(date(2026, 3, 10), date(2026, 3, 25))
        self.assertGreater(len(chunks), 0)
        # Expected grid near March: chunk idx 4 = Mar 2..Mar 17, idx 5 = Mar 18..Apr 2
        # All returned chunks must have starts that are (Jan 1 + k*16)
        year_start = date(2026, 1, 1)
        for cf, _ct in chunks:
            # Either cf == grid_start OR cf == date_from (clamped to date_from)
            if cf != date(2026, 3, 10):
                days = (cf - year_start).days
                self.assertEqual(days % 16, 0,
                                 f'chunk start {cf} is not aligned to 16-day grid')

    def test_full_year_produces_23_chunks(self):
        """2026 Jan 1..Dec 31 → 23 full 16-day chunks (last one clamped)."""
        chunks = _biweekly_chunks(date(2026, 1, 1), date(2026, 12, 31))
        # ceil(365 / 16) = 23
        self.assertEqual(len(chunks), 23)
        # First chunk: Jan 1..Jan 16
        self.assertEqual(chunks[0], (date(2026, 1, 1), date(2026, 1, 16)))

    def test_chunk_size_is_16_days(self):
        """Each full chunk spans exactly 16 days (end-start = 15)."""
        chunks = _biweekly_chunks(date(2026, 1, 1), date(2026, 6, 30))
        # All except possibly last one (clamped) must be 16 days long
        for cf, ct in chunks[:-1]:
            self.assertEqual((ct - cf).days, 15,
                             f'chunk {cf}..{ct} is not 16 days long')

    # ── Overlap semantics ──────────────────────────────────────────

    def test_date_from_clamps_first_chunk_start(self):
        """Chunk start is max(grid_start, date_from)."""
        # Mar 10 falls inside chunk idx 4 (Mar 2..Mar 17)
        chunks = _biweekly_chunks(date(2026, 3, 10), date(2026, 3, 17))
        self.assertEqual(len(chunks), 1)
        cf, ct = chunks[0]
        self.assertEqual(cf, date(2026, 3, 10))  # clamped to date_from
        self.assertEqual(ct, date(2026, 3, 17))  # grid end of chunk idx 4

    def test_date_to_clamps_last_chunk_end(self):
        """Chunk end is min(grid_end, date_to)."""
        chunks = _biweekly_chunks(date(2026, 1, 1), date(2026, 1, 5))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], (date(2026, 1, 1), date(2026, 1, 5)))

    def test_chunks_do_not_overlap_and_are_ordered(self):
        """Adjacent chunks touch without gaps or overlaps."""
        chunks = _biweekly_chunks(date(2026, 1, 1), date(2026, 12, 31))
        for i in range(len(chunks) - 1):
            _, prev_end = chunks[i]
            cur_start, _ = chunks[i + 1]
            self.assertEqual((cur_start - prev_end).days, 1,
                             f'gap between {prev_end} and {cur_start}')

    # ── Regression: Mar 29 / Apr 2 bug ─────────────────────────────

    def test_march_chunk_contains_mar_22_to_apr_6(self):
        """
        Regression test: chunk idx 5 of 2026 must be Mar 18..Apr 2, and
        chunk idx 6 must be Apr 3..Apr 18 (NOT Apr 2..Apr 17).

        Historical bug: a non-anchored chunker produced chunks starting
        at arbitrary date_from values, causing ``acquired_date`` mid-points
        to drift off the MOD13Q1 reference grid.
        """
        chunks = _biweekly_chunks(date(2026, 1, 1), date(2026, 4, 30))
        starts = [cf for cf, _ in chunks]
        # Jan 1, Jan 17, Feb 2, Feb 18, Mar 6, Mar 22, Apr 7, Apr 23
        # (Jan 1 + 16k for k=0..7)
        expected = [date(2026, 1, 1), date(2026, 1, 17), date(2026, 2, 2),
                    date(2026, 2, 18), date(2026, 3, 6), date(2026, 3, 22),
                    date(2026, 4, 7), date(2026, 4, 23)]
        self.assertEqual(starts, expected)

    # ── Leap-year boundary ─────────────────────────────────────────

    def test_leap_year_2024(self):
        """2024 has 366 days — last chunk still clamps cleanly."""
        chunks = _biweekly_chunks(date(2024, 1, 1), date(2024, 12, 31))
        self.assertEqual(chunks[0], (date(2024, 1, 1), date(2024, 1, 16)))
        # Last chunk ends on Dec 31 (clamped from the 24th chunk that would
        # overshoot into Jan 2025).
        self.assertEqual(chunks[-1][1], date(2024, 12, 31))

    def test_single_day_range_returns_one_chunk(self):
        """A 1-day range produces exactly one (clamped) chunk."""
        chunks = _biweekly_chunks(date(2026, 5, 15), date(2026, 5, 15))
        self.assertEqual(chunks, [(date(2026, 5, 15), date(2026, 5, 15))])
