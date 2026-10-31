import pandas as pd
import pytest

import scripts.research_kol4_backtest as B
from scripts.research_kol_execution import COST

T0 = pd.Timestamp("2026-01-01 00:00")


def trade(lv, side=1, kind="signal", updates=(), horizon_h=24):
    return {"channel": "wise", "id": 1, "symbol": "BTC", "side": side, "kind": kind, "lv": lv,
            "activation": str(T0.tz_localize("UTC")), "horizon_h": horizon_h, "trigger": None,
            "updates": list(updates)}


def test_activation_uses_edit_time_only_after_ten_minutes():
    pub = "2026-01-01T00:00:00+00:00"
    act, pit = B.activation_of({"published_at": pub, "edited_at": "2026-01-01T00:09:00+00:00"})
    assert pit and act == pd.Timestamp(pub) + pd.Timedelta(seconds=60)
    act, pit = B.activation_of({"published_at": pub, "edited_at": "2026-01-01T00:11:00+00:00"})
    assert not pit and act == pd.Timestamp("2026-01-01T00:12:00+00:00")


def test_r_multiple_targets_from_absolute_stop_distance():
    plan, why = B.levels_to_plan(trade({"e": "m", "sl_abs": 700, "tp_r": [1, 2]}, side=-1), 64700.0, 1.0)
    assert why == "ok" and plan.stop == 65400.0
    assert plan.targets == [64000.0, 63300.0]


def test_margin_percent_targets_use_stated_leverage_or_ten():
    plan, _ = B.levels_to_plan(trade({"e": "m", "tp_pct": [50, 100], "lev": 5}), 1.0, 1.0)
    assert plan.targets == pytest.approx([1.10, 1.20])
    plan, _ = B.levels_to_plan(trade({"e": "m", "tp_pct": [50]}), 1.0, 1.0)
    assert plan.targets == pytest.approx([1.05])


def test_implied_stop_from_target_and_reward_ratio():
    plan, _ = B.levels_to_plan(trade({"e": [76000], "tp": [73000], "sl_r_implied": {"tp": 73000, "r": 3.5}},
                                     side=-1, kind="conditional_numeric"), 76000.0, 1.0)
    assert plan.entry_market and plan.stop == pytest.approx(76000 + 3000 / 3.5)


def test_passed_target_marks_a_market_call_stale():
    _, why = B.levels_to_plan(trade({"e": "m", "tp": [99.0]}), 100.0, 1.0)
    assert why == "stale_target_passed"


def test_no_levels_is_not_a_bracket():
    plan, why = B.levels_to_plan(trade({"e": "m"}), 100.0, 1.0)
    assert plan is None and why == "no_levels"


def _bars(rows, freq="1min", start=T0):
    return pd.DataFrame(rows, columns=["open", "high", "low", "close"],
                        index=pd.date_range(start, periods=len(rows), freq=freq))


@pytest.fixture
def market(monkeypatch):
    rows = [[100, 100.2, 99.9, 100]] * 5 + [[100, 101.6, 99.9, 101.5]] + [[101.5, 101.6, 101.4, 101.5]] * 114
    m1 = _bars(rows)
    m5 = _bars([[101.5, 101.6, 101.4, 101.5]] * 300, "5min", T0 + pd.Timedelta(hours=2))

    def klines(native, interval, start, end):
        f = m1 if interval == "1m" else m5
        return f[(f.index >= start) & (f.index < end)]

    monkeypatch.setattr(B.E, "klines", klines)
    monkeypatch.setattr(B.E, "native_symbol", lambda s: ("BTCUSDT", 1.0))
    monkeypatch.setattr(B.E, "funding", lambda *a, **k: pd.Series(dtype=float))
    monkeypatch.setattr(B.E, "DATA_END", T0 + pd.Timedelta(days=30))


def test_policy_b_author_close_exits_before_target(market):
    close_at = str((T0 + pd.Timedelta(minutes=3)).tz_localize("UTC"))
    t = trade({"e": "m", "tp": [101.5], "sl": 98.0}, updates=[{"kind": "close", "at": close_at}])
    a = B.simulate(t, "A")
    b = B.simulate(t, "B")
    assert a["exit_reason"] == "tp1" and a["ret_on_filled"] == pytest.approx(0.015 - 2 * COST)
    assert b["exit_reason"] == "author_close" and b["updates_applied"] == ["close"]
    assert b["ret_on_filled"] == pytest.approx(-2 * COST)


def test_direction_returns_measure_from_activation_open(market):
    out = B.direction_returns(trade({"e": "m"}))
    assert out["status"] == "ok" and out["ref"] == 100
    assert out["r24h"] == pytest.approx(0.015 - 2 * COST)


def test_text_condition_is_never_executed(market):
    assert B.simulate(trade({"e": "m"}, kind="conditional_text"), "A")["status"] == "condition_not_executable"
    assert B.direction_returns(trade({"e": "m"}, kind="conditional_text"))["status"] == "conditional_excluded"


def test_marketable_limit_with_stop_already_passed_is_stale():
    # "long at 68,050 support" posted while the market trades 66,756: fills at the market, below its own stop
    plan, why = B.levels_to_plan(trade({"e": [68050], "sl_abs": 500, "tp_abs": [500]}), 66756.0, 1.0)
    assert plan is None and why == "stale_stop_passed"


def test_tp1_only_variant_closes_everything_at_the_first_target():
    plan, _ = B.levels_to_plan(trade({"e": "m", "tp": [101, 102, 103], "sl": 98, "tp1_only": True}), 100.0, 1.0)
    assert plan.targets == [101] and plan.weights == [1.0]
