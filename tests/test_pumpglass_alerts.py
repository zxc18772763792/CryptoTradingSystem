from __future__ import annotations

import numpy as np
import pandas as pd

from scripts.research_pumpglass_alerts import parse_rows, week_block_ci


def test_alert_log_rows_parse_with_blank_time_to_peak_and_utc_timestamps(tmp_path):
    path = tmp_path / "rows.txt"
    path.write_text("# header\nBTRUSDT|1792690180|0.054240|+32.7%|8h|-5.5%|TRACKING\n"
                    "FRAXUSDT|1791622017|0.376200|+0.0%||0.0%|CLOSED\n", encoding="utf-8")
    rows = parse_rows(path)
    assert rows["symbol"].tolist() == ["BTRUSDT", "FRAXUSDT"]
    assert rows["alert_at"].iloc[0] == pd.Timestamp("2026-10-22T17:29:40Z")
    assert rows["claimed_peak_pct"].tolist() == [32.7, 0.0] and rows["time_to_peak"].iloc[1] is None


def test_week_block_interval_groups_by_calendar_week():
    stamps = pd.Series(pd.date_range("2026-09-01", periods=28, freq="D", tz="UTC"))
    ci = week_block_ci(np.full(28, .01), stamps)
    assert ci == [0.01, 0.01] or np.allclose(ci, [.01, .01])
    assert week_block_ci(np.ones(5), stamps[:5]) is None  # fewer than three weeks
