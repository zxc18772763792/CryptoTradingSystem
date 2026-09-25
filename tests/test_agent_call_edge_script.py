from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SPEC = importlib.util.spec_from_file_location(
    "agent_call_edge", Path(__file__).resolve().parents[1] / "scripts" / "agent_call_edge.py"
)
ace = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ace)


def _write_klines(root: Path, closes: np.ndarray, start="2026-09-01") -> pd.DatetimeIndex:
    idx = pd.date_range(start, periods=len(closes), freq="5min")
    part = root / "binance" / "AAA_USDT" / "5m_parts"
    part.mkdir(parents=True)
    pd.DataFrame({"close": closes}, index=idx).to_parquet(part / "p0.parquet")
    return idx


def _journal(path: Path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _row(ts, action, symbol="AAA/USDT"):
    return {
        "timestamp": ts,
        "decision": {"action": action, "confidence": 0.7},
        "execution": {"submitted": False, "signal": {"symbol": symbol}},
        "config": {"exchange": "binance", "model": "m", "timeframe": "15m"},
        "context": {"aggregated_signal": {"direction": "LONG"}},
    }


def test_entry_uses_last_closed_bar_and_short_sign_flips(tmp_path):
    ace._KLINES.clear()
    closes = np.full(2000, 100.0)
    idx = _write_klines(tmp_path, closes)
    # A jump that starts in the bar containing the call must not leak into entry.
    call_at = idx[1000] + pd.Timedelta("2min")
    closes[1000:] = 110.0
    ace._KLINES.clear()
    (tmp_path / "binance" / "AAA_USDT" / "5m_parts" / "p0.parquet").unlink()
    pd.DataFrame({"close": closes}, index=idx).to_parquet(tmp_path / "binance" / "AAA_USDT" / "5m_parts" / "p0.parquet")

    journal = tmp_path / "j.jsonl"
    _journal(journal, [_row(call_at.isoformat() + "+00:00", "buy"), _row((call_at + pd.Timedelta("5h")).isoformat() + "+00:00", "sell")])
    calls = ace.extract_calls([journal])
    scored = ace.score_calls(calls, {"1h": 12}, baseline_days=0.5, root=tmp_path).set_index("side")

    assert scored.loc[1, "ret_1h"] == pytest.approx(0.10)       # bought at the pre-jump close
    assert scored.loc[-1, "ret_1h"] == pytest.approx(0.0)       # flat afterwards: short earns nothing
    # Random timing in the +/-0.5d window catches the jump, so a flat short beats it.
    assert scored.loc[-1, "edge_1h"] > 0


def test_dedupe_counts_repeated_calls_once():
    ts = pd.to_datetime(["2026-09-01T00:00Z", "2026-09-01T00:02Z", "2026-09-01T03:00Z", "2026-09-01T04:01Z"], utc=True)
    df = pd.DataFrame({"ts": ts, "symbol": "AAA/USDT", "side": 1})
    kept = ace.dedupe(df, hours=4)
    assert list(kept["ts"]) == [ts[0], ts[3]]
