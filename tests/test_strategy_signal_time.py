from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from core.strategies.strategy_base import bar_time


def test_bar_time_corrects_naive_local_future_timestamp_to_utc():
    data = pd.DataFrame(
        {"close": [1.0]},
        index=[pd.Timestamp("2026-05-19 13:15:00")],
    )
    fallback = datetime(2026, 5, 19, 5, 29, 16, tzinfo=timezone.utc)

    assert bar_time(data, fallback=fallback).isoformat() == "2026-05-19T05:15:00+00:00"


def test_bar_time_preserves_aware_utc_timestamp():
    data = pd.DataFrame(
        {"close": [1.0]},
        index=[pd.Timestamp("2026-05-19T05:15:00+00:00")],
    )

    assert bar_time(data).isoformat() == "2026-05-19T05:15:00+00:00"
