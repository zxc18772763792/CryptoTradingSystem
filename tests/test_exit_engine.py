from __future__ import annotations

import pandas as pd
import pytest

from core.backtest.exit_engine import ExitEngineConfig, run_exit_engine


def _ohlc_frame(rows: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=len(rows), freq="1h")
    return pd.DataFrame(
        {
            "open": [row[0] for row in rows],
            "high": [row[1] for row in rows],
            "low": [row[2] for row in rows],
            "close": [row[3] for row in rows],
        },
        index=index,
    )


def test_exit_engine_reversal_only_preserves_close_then_reverse_behavior():
    frame = _ohlc_frame(
        [
            (100.0, 100.0, 100.0, 100.0),
            (100.0, 101.0, 99.5, 101.0),
            (101.0, 101.5, 98.5, 99.0),
        ]
    )
    signal_position = pd.Series([0.0, 1.0, -1.0], index=frame.index)
    result = run_exit_engine(
        df=frame,
        signal_position=signal_position,
        config=ExitEngineConfig(template_name="ReversalOnly"),
    )

    assert list(result.effective_position.astype(float)) == [0.0, 1.0, -1.0]
    assert result.trade_points["entries"] == 2
    assert result.trade_points["exits"] == 1
    assert result.trade_points["close_points"][0]["reason"] == "reverse"
    assert result.exit_reason_breakdown["reversal"] == 1


def test_exit_engine_time_stop_prevents_reentry_until_new_signal_epoch():
    frame = _ohlc_frame(
        [
            (100.0, 100.0, 100.0, 100.0),
            (100.0, 101.0, 99.0, 100.0),
            (100.0, 102.0, 99.5, 101.0),
            (101.0, 103.0, 100.5, 102.0),
            (102.0, 104.0, 101.5, 103.0),
        ]
    )
    signal_position = pd.Series([0.0, 1.0, 1.0, 1.0, 1.0], index=frame.index)
    result = run_exit_engine(
        df=frame,
        signal_position=signal_position,
        config=ExitEngineConfig(
            signal_reversal_exit=False,
            time_stop_enabled=True,
            max_bars_in_trade=2,
        ),
    )

    assert list(result.effective_position.astype(float)) == [0.0, 1.0, 0.0, 0.0, 0.0]
    assert result.trade_points["entries"] == 1
    assert result.trade_points["exits"] == 1
    assert result.trade_points["close_points"][0]["reason"] == "time_stop"
    assert result.exit_reason_breakdown["time_stop"] == 1


def test_exit_engine_partial_take_profit_reduces_position_and_tracks_remaining_state():
    frame = _ohlc_frame(
        [
            (100.0, 100.0, 100.0, 100.0),
            (100.0, 101.0, 99.0, 100.0),
            (100.0, 106.0, 100.0, 104.0),
            (104.0, 107.0, 103.0, 106.0),
        ]
    )
    signal_position = pd.Series([0.0, 1.0, 1.0, 0.0], index=frame.index)
    result = run_exit_engine(
        df=frame,
        signal_position=signal_position,
        config=ExitEngineConfig(
            signal_reversal_exit=True,
            fixed_stop_loss_pct=0.05,
            partial_take_profit_enabled=True,
            partial_take_profit_r=1.0,
            partial_take_profit_ratio=0.5,
        ),
    )

    assert result.trade_points["entries"] == 1
    assert result.trade_points["exits"] == 2
    assert result.exit_reason_breakdown["partial"] == 1
    assert result.exit_reason_breakdown["reversal"] == 1
    assert result.effective_position.iloc[2] == pytest.approx(0.5)
    assert result.effective_position.iloc[3] == pytest.approx(0.0)

    partial_event = result.exit_events[0]
    final_event = result.exit_events[1]
    assert partial_event["reason"] == "partial"
    assert partial_event["size_fraction"] == pytest.approx(0.5)
    assert partial_event["remaining_fraction"] == pytest.approx(0.5)
    assert final_event["reason"] == "reversal"

    completed_trade = result.completed_trades[0]
    assert completed_trade["partial_exit_count"] == 1
    assert completed_trade["final_exit_reason"] == "reversal"
