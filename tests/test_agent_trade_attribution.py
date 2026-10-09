from __future__ import annotations

import json

import pandas as pd
import pytest

from scripts import agent_trade_attribution as attr


def _pos(symbol, opened, closed, entry, exit_, qty, pnl):
    return {"symbol": symbol, "side": "long", "strategy": attr.STRATEGY, "entry_price": entry, "current_price": exit_,
            "quantity": qty, "realized_pnl": pnl, "opened_at": opened, "updated_at": closed, "metadata": {}}


def _fill(ts, action, qty, fee, reason=None, symbol="FIL/USDT"):
    return {"symbol": symbol, "strategy": attr.STRATEGY, "timestamp": ts, "action": action, "quantity": qty,
            "fee_usd": fee, "close_reason": reason}


@pytest.fixture
def state(tmp_path):
    positions = [
        # partial take-profit at 10:30 left 5 of the original 10 for the final exit
        _pos("FIL/USDT", "2026-10-10T10:00:00+00:00", "2026-10-10T11:00:00+00:00", 3.0, 3.06, 5.0, 0.45),
        # re-entered 10 seconds after that close: its window must not take the previous exit's fee
        _pos("FIL/USDT", "2026-10-10T11:00:10+00:00", "2026-10-10T12:00:00+00:00", 3.07, 3.0, 8.0, -0.56),
    ]
    history = [
        _fill("2026-10-10T10:00:00.5+00:00", "open_or_add", 10.0, 0.030),
        _fill("2026-10-10T10:30:00+00:00", "manual_order", 5.0, 0.015, "partial_take_profit"),
        _fill("2026-10-10T11:00:00.2+00:00", "manual_order", 5.0, 0.015, "trailing_stop"),
        _fill("2026-10-10T11:00:10.3+00:00", "open_or_add", 8.0, 0.025),
        _fill("2026-10-10T12:00:00.1+00:00", "manual_order", 8.0, 0.024, "stop_loss"),
        {**_fill("2026-10-10T10:00:00+00:00", "open_or_add", 99.0, 9.9), "strategy": "Grid"},
    ]
    (tmp_path / "positions_paper.json").write_text(json.dumps({"closed_positions": positions}), encoding="utf-8")
    (tmp_path / "risk_trade_history_paper.json").write_text(json.dumps({"trade_history": history}), encoding="utf-8")
    return tmp_path


def test_trades_take_their_own_fills_only(state):
    trades = attr.load_trades(state)

    first, second = trades.iloc[0], trades.iloc[1]
    assert first["qty"] == 10.0 and first["notional"] == pytest.approx(30.0)
    assert first["fees"] == pytest.approx(0.060) and first["partial_tp"] and first["exit_reason"] == "trailing_stop"
    assert first["ret_net"] == pytest.approx((0.45 - 0.060) / 30.0)
    assert second["fees"] == pytest.approx(0.049) and not second["partial_tp"] and second["exit_reason"] == "stop_loss"
    assert second["reentry_gap_h"] == pytest.approx(10 / 3600)


def test_verdict_waits_for_the_registered_sample_then_reads_the_ci(state, tmp_path, capsys):
    trades = attr.add_market_context(attr.load_trades(state), root=tmp_path / "no_bars")
    split = pd.Timestamp("2026-10-09T00:00:00Z")
    assert attr.report(trades, split, None)["label"].startswith("collecting (2/40")

    many = pd.concat([trades.iloc[[1]]] * 40, ignore_index=True)
    many["opened"] = [pd.Timestamp("2026-10-10T11:00:10Z") + pd.Timedelta(days=i % 20, hours=i) for i in range(40)]
    many["closed"] = many["opened"] + pd.Timedelta(hours=1)
    many["ret_net"] = [-0.01 - 0.001 * (i % 3) for i in range(40)]
    assert attr.report(many, split, None)["label"].startswith("NOT rescued")
    many["ret_net"] = -many["ret_net"]
    assert attr.report(many, split, None)["label"].startswith("working")
    capsys.readouterr()
