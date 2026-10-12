from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import audit_fill_klines as audit  # noqa: E402

START = datetime(2024, 1, 1)


def _hours(first: int, last: int) -> pd.DatetimeIndex:
    return pd.date_range(START + timedelta(hours=first), START + timedelta(hours=last), freq="1h")


def test_interior_gaps_count_and_far_apart_runs_stay_separate_windows():
    have = _hours(0, 99).append(_hours(110, 2999)).append(_hours(3600, 3999))  # gaps 100..109 and 3000..3599
    missing, windows = audit.missing_windows(have, START, START + timedelta(hours=3999), "1h")
    assert missing == 10 + 600
    assert windows == [(START + timedelta(hours=100), START + timedelta(hours=109)),
                       (START + timedelta(hours=3000), START + timedelta(hours=3599))]


def test_nearby_runs_are_fetched_as_one_window_and_a_complete_series_needs_nothing():
    have = _hours(0, 9).append(_hours(11, 19)).append(_hours(21, 29))  # single bars missing at 10 and 20
    missing, windows = audit.missing_windows(have, START, START + timedelta(hours=29), "1h")
    assert missing == 2 and windows == [(START + timedelta(hours=10), START + timedelta(hours=20))]
    assert audit.missing_windows(_hours(0, 29), START, START + timedelta(hours=29), "1h") == (0, [])


def test_an_empty_series_is_one_window_from_start_to_end():
    missing, windows = audit.missing_windows(pd.DatetimeIndex([]), START, START + timedelta(days=9), "1d")
    assert missing == 10 and windows == [(START, START + timedelta(days=9))]
