"""Тесты ``_compute_phenology`` (compute_phenology, C=14) — страховочная
сетка перед декомпозицией. Функция чистая: синтетические сезонные ряды
NDVI без БД.
"""
import math
from datetime import date, timedelta

import numpy as np
from django.test import SimpleTestCase

from agrocosmos.management.commands.compute_phenology import (
    _compute_phenology,
)


def _series(peak=0.8, base=0.15, peak_doy=170, width=55.0, step=10):
    """Колоколообразный годовой ряд NDVI (шаг 10 дней)."""
    dates, vals = [], []
    d = date(2025, 1, 5)
    while d.year == 2025:
        doy = d.timetuple().tm_yday
        v = base + (peak - base) * math.exp(-((doy - peak_doy) ** 2) / (2 * width ** 2))
        dates.append(d)
        vals.append(v)
        d += timedelta(days=step)
    return dates, np.array(vals)


class ComputePhenologyTests(SimpleTestCase):

    def test_normal_season(self):
        dates, vals = _series()
        pheno = _compute_phenology(dates, vals)
        self.assertIsNotNone(pheno)
        self.assertLess(pheno['sos'], pheno['pos'])
        self.assertLess(pheno['pos'], pheno['eos'])
        self.assertEqual(pheno['los'], (pheno['eos'] - pheno['sos']).days)
        self.assertAlmostEqual(pheno['max_ndvi'], 0.8, delta=0.01)
        self.assertGreater(pheno['ti'], 0)
        self.assertGreater(pheno['mean_ndvi'], 0.2)

    def test_pos_restricted_to_spring_window(self):
        # осенний пик выше весеннего, но POS должен остаться весной
        dates, vals = _series(peak=0.7, peak_doy=170)
        for i, d in enumerate(dates):
            if d.timetuple().tm_yday > 280:
                vals[i] = 0.9  # ложный осенний пик
        pheno = _compute_phenology(dates, vals)
        self.assertIsNotNone(pheno)
        self.assertLessEqual(pheno['pos'].timetuple().tm_yday, 244)
        self.assertAlmostEqual(pheno['max_ndvi'], 0.7, delta=0.01)

    def test_flat_signal_returns_none(self):
        dates, _ = _series()
        vals = np.full(len(dates), 0.22)  # амплитуда < 0.10
        self.assertIsNone(_compute_phenology(dates, vals))

    def test_no_observations_in_pos_window(self):
        # только зимние точки (DOY < 60)
        dates = [date(2025, 1, 5), date(2025, 1, 15), date(2025, 1, 25),
                 date(2025, 2, 5), date(2025, 2, 15)]
        vals = np.array([0.5, 0.6, 0.7, 0.6, 0.5])
        self.assertIsNone(_compute_phenology(dates, vals))

    def test_too_short_season_returns_none(self):
        # резкий узкий пик — LOS < 30 дней
        dates, vals = _series(width=8.0)
        self.assertIsNone(_compute_phenology(dates, vals))

    def test_rounding(self):
        dates, vals = _series()
        pheno = _compute_phenology(dates, vals)
        self.assertEqual(pheno['max_ndvi'], round(pheno['max_ndvi'], 4))
        self.assertEqual(pheno['ti'], round(pheno['ti'], 2))
