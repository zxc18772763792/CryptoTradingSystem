from __future__ import annotations

import pandas as pd
import pytest

from core.research.vesting_spec import growth, schedule_from_spec, usable

DAYS = pd.date_range("2025-01-01", "2028-12-31", freq="D", tz="UTC")


def test_cliff_then_monthly_vesting_and_discretionary_sections():
    spec = {"tge_date": "2025-01-01", "confidence": 0.8, "allocations": [
        {"pct_of_total": 20, "tge_unlock_pct": 100, "cliff_months": 0, "vesting_months": 0, "frequency": "once"},
        {"pct_of_total": 30, "tge_unlock_pct": 0, "cliff_months": 12, "vesting_months": 24, "frequency": "monthly"},
        {"pct_of_total": 50, "schedule_known": False},  # treasury: never counted as unlocked
    ]}
    curve, known = schedule_from_spec(spec, None, DAYS)
    assert known == pytest.approx(0.5)
    assert curve[pd.Timestamp("2025-06-01", tz="UTC")] == pytest.approx(0.20)  # inside the cliff
    assert curve[pd.Timestamp("2028-06-01", tz="UTC")] == pytest.approx(0.50)  # fully vested
    mid = curve[pd.Timestamp("2027-01-15", tz="UTC")]
    assert 0.20 + 0.30 * 11 / 24 <= mid <= 0.20 + 0.30 * 13 / 24
    assert growth(curve, pd.Timestamp("2025-06-01", tz="UTC")) == pytest.approx(0.0)
    assert growth(curve, pd.Timestamp("2026-06-01", tz="UTC")) > 0
    assert usable(spec, known) and not usable({**spec, "confidence": 0.3}, known) and not usable(spec, 0.2)


def test_daily_vesting_and_fallback_start():
    spec = {"allocations": [{"pct_of_total": 100, "tge_unlock_pct": 10, "cliff_months": 0, "vesting_months": 12, "frequency": "daily"}]}
    curve, _ = schedule_from_spec(spec, "2025-01-01", DAYS)
    assert curve[pd.Timestamp("2025-01-01", tz="UTC")] == pytest.approx(0.10)
    assert curve[pd.Timestamp("2025-07-02", tz="UTC")] == pytest.approx(0.10 + 0.90 * 182 / (12 * 30.4375), rel=1e-6)
