from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_standalone(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


entry = load_standalone(
    "binance_entry_exit_validation_tested",
    ROOT / "core" / "research" / "binance_entry_exit_validation.py",
)
exit_study = load_standalone(
    "binance_exit_study_risk_sizing_tested",
    ROOT / "scripts" / "analyze_binance_exit_strategies.py",
)
breakout_add = load_standalone(
    "binance_utility_breakout_add_tested",
    ROOT / "scripts" / "analyze_binance_utility_breakout_add.py",
)
taker_flow = load_standalone(
    "binance_utility_taker_flow_filter_tested",
    ROOT / "scripts" / "analyze_binance_utility_taker_flow_filter.py",
)
taker_robustness = load_standalone(
    "binance_utility_taker_flow_robustness_tested",
    ROOT / "scripts" / "analyze_binance_utility_taker_flow_robustness.py",
)
breakout_runner = load_standalone(
    "binance_utility_breakout_runner_tested",
    ROOT / "scripts" / "analyze_binance_utility_breakout_runner.py",
)
partial_fraction = load_standalone(
    "binance_utility_breakout_partial_fraction_tested",
    ROOT / "scripts" / "analyze_binance_utility_breakout_partial_fraction.py",
)
regime_partial_router = load_standalone(
    "binance_utility_regime_partial_router_tested",
    ROOT / "scripts" / "analyze_binance_utility_regime_partial_router.py",
)


def bars_frame(count: int = 40) -> pd.DataFrame:
    times = pd.date_range("2026-01-01", periods=count, freq="4h", tz="UTC")
    return pd.DataFrame(
        {
            "open_time": times,
            "open": np.full(count, 100.0),
            "high": np.full(count, 101.0),
            "low": np.full(count, 99.0),
            "close": np.full(count, 100.0),
            "quote_volume": np.full(count, 10.0),
            "taker_buy_quote": np.full(count, 5.0),
        }
    )


def test_past_only_volume_reference_excludes_current_bar():
    bars = bars_frame(20)
    bars.loc[19, "quote_volume"] = 100.0
    featured = entry.add_past_only_4h_features(bars)
    assert featured.loc[19, "volume_ratio_18"] == pytest.approx(10.0)


def test_one_bar_wait_observes_close_then_enters_next_open():
    bars = bars_frame(24)
    bars.loc[1, "open"] = 105.0
    bars.loc[2, "high"] = 315.0
    featured = entry.add_past_only_4h_features(bars)
    signal = {
        "entry_time": bars.loc[0, "open_time"],
        "close": 100.0,
    }
    located = entry.locate_entry(signal, featured, rule_name="one_bar_wait")
    assert located is not None
    assert located["trigger_time"] == bars.loc[0, "open_time"] + pd.Timedelta(hours=4)
    assert located["entry_time"] == bars.loc[1, "open_time"]
    assert located["entry_open"] == 105.0
    assert located["entry_delay_hours"] == 4.0
    assert located["target200_14d_from_entry"] is True


def test_breakout_confirmation_always_enters_following_bar():
    bars = bars_frame(32)
    bars.loc[20, ["open", "high", "low", "close", "quote_volume", "taker_buy_quote"]] = [
        100.0, 106.0, 100.0, 105.0, 20.0, 12.0,
    ]
    bars.loc[21, "open"] = 106.0
    featured = entry.add_past_only_4h_features(bars)
    signal = {"entry_time": bars.loc[20, "open_time"], "close": 100.0}
    located = entry.locate_entry(signal, featured, rule_name="breakout_balanced")
    assert located is not None
    assert located["trigger_time"] == bars.loc[20, "open_time"] + pd.Timedelta(hours=4)
    assert located["entry_time"] == bars.loc[21, "open_time"]
    assert located["entry_open"] == 106.0


def test_failure_to_launch_exits_at_next_open_not_trigger_close():
    bars = bars_frame(30)
    bars.loc[:, "high"] = 105.0
    bars.loc[:, "low"] = 98.0
    bars.loc[:, "close"] = 101.0
    bars.loc[19, "open"] = 97.0
    featured = entry.add_past_only_4h_features(bars)
    policy = entry.build_exit_policy_catalog()["launch3_mfe10"]
    outcome = entry.simulate_stateful_trade(
        featured,
        entry_time=bars.loc[0, "open_time"],
        policy=policy,
        slippage_bps=0.0,
        fee_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "failure_to_launch"
    assert outcome["exit_time"] == bars.loc[18, "open_time"]
    assert outcome["exit_price"] == 100.0


def test_breakeven_stop_only_activates_on_next_bar():
    bars = bars_frame(12)
    bars.loc[0, ["open", "high", "low", "close"]] = [100.0, 135.0, 99.0, 130.0]
    bars.loc[1, ["open", "high", "low", "close"]] = [130.0, 131.0, 95.0, 98.0]
    featured = entry.add_past_only_4h_features(bars)
    policy = entry.build_exit_policy_catalog()["breakeven30"]
    outcome = entry.simulate_stateful_trade(
        featured,
        entry_time=bars.loc[0, "open_time"],
        policy=policy,
        slippage_bps=0.0,
        fee_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "break_even_stop"
    assert outcome["exit_time"] == bars.loc[1, "open_time"]
    assert outcome["exit_price"] == 100.0


def test_policy_switch_waits_for_decision_open_and_preserves_prior_state():
    bars = bars_frame(12)
    bars.loc[0, ["open", "high", "low", "close"]] = [100.0, 220.0, 99.0, 210.0]
    bars.loc[1, ["open", "high", "low", "close"]] = [210.0, 220.0, 160.0, 180.0]
    featured = entry.add_past_only_4h_features(bars)
    baseline = entry.build_exit_policy_catalog()["frozen_half100_trail25"]
    routed = dict(baseline)
    routed.update({"partial_target": 1.50, "trail_activation": 1.50, "trail_distance": 0.30})
    outcome = entry.simulate_stateful_trade(
        featured,
        entry_time=bars.loc[0, "open_time"],
        policy=baseline,
        slippage_bps=0.0,
        fee_bps=0.0,
        policy_switch_time=bars.loc[1, "open_time"],
        post_switch_policy=routed,
    )
    assert outcome is not None
    assert outcome["policy_switch_applied"] is True
    assert outcome["partial_done_before_switch"] is True
    assert outcome["trail_active_before_switch"] is True
    assert outcome["exit_time"] != bars.loc[1, "open_time"]


def test_policy_switch_requires_time_and_policy_together():
    bars = entry.add_past_only_4h_features(bars_frame(12))
    policy = entry.build_exit_policy_catalog()["frozen_half100_trail25"]
    with pytest.raises(ValueError, match="provided together"):
        entry.simulate_stateful_trade(
            bars,
            entry_time=bars.loc[0, "open_time"],
            policy=policy,
            slippage_bps=0.0,
            fee_bps=0.0,
            policy_switch_time=bars.loc[1, "open_time"],
        )


def test_breakout_add_uses_fixed_planned_position_and_cash_reserve():
    entry_time = pd.Timestamp("2026-01-01", tz="UTC")
    initial = {
        "entry_time": entry_time,
        "exit_time": entry_time + pd.Timedelta(days=1),
        "exit_reason": "time_stop",
        "net_return": 0.20,
        "funding_return": -0.001,
        "fee_return": 0.001,
        "mark_path": [
            {"time": entry_time, "mark_return": 0.0, "remaining_fraction": 1.0},
            {"time": entry_time + pd.Timedelta(days=1), "mark_return": 0.20, "remaining_fraction": 0.0},
        ],
        "partial_take_profit": False,
        "funding_observed": True,
    }
    reserved = breakout_add.combine_tranches(initial, None, add_fraction=0.25)
    assert reserved["net_return"] == pytest.approx(0.15)
    assert reserved["planned_deployed_fraction"] == pytest.approx(0.75)
    assert reserved["breakout_add_executed"] is False


def test_breakout_add_starts_only_at_add_open_and_weights_both_legs():
    entry_time = pd.Timestamp("2026-01-01", tz="UTC")
    add_time = entry_time + pd.Timedelta(hours=8)
    initial = {
        "entry_time": entry_time,
        "exit_time": entry_time + pd.Timedelta(days=1),
        "exit_reason": "time_stop",
        "net_return": 0.20,
        "funding_return": 0.0,
        "fee_return": 0.001,
        "mark_path": [
            {"time": entry_time, "mark_return": 0.0, "remaining_fraction": 1.0},
            {"time": add_time - pd.Timedelta(hours=4), "mark_return": 0.08, "remaining_fraction": 1.0},
            {"time": entry_time + pd.Timedelta(days=1), "mark_return": 0.20, "remaining_fraction": 0.0},
        ],
        "partial_take_profit": False,
        "funding_observed": True,
    }
    add = {
        "entry_time": add_time,
        "entry_price": 108.0,
        "exit_time": entry_time + pd.Timedelta(days=1),
        "exit_reason": "time_stop",
        "net_return": 0.10,
        "funding_return": 0.0,
        "fee_return": 0.001,
        "mark_path": [
            {"time": add_time, "mark_return": 0.0, "remaining_fraction": 1.0},
            {"time": entry_time + pd.Timedelta(days=1), "mark_return": 0.10, "remaining_fraction": 0.0},
        ],
        "partial_take_profit": False,
        "funding_observed": True,
    }
    combined = breakout_add.combine_tranches(initial, add, add_fraction=0.25)
    assert combined["net_return"] == pytest.approx(0.175)
    before_add = next(item for item in combined["mark_path"] if item["time"] == add_time - pd.Timedelta(hours=4))
    assert before_add["mark_return"] == pytest.approx(0.75 * 0.08)
    assert before_add["remaining_fraction"] == pytest.approx(0.75)
    at_add = next(item for item in combined["mark_path"] if item["time"] == add_time)
    assert at_add["remaining_fraction"] == pytest.approx(1.0)


def test_taker_flow_filter_is_inclusive_and_missing_is_fail_closed():
    rows = pd.DataFrame(
        {
            "early_taker_buy_share": [0.499999, 0.50, 0.75, np.nan],
        }
    )
    filtered = taker_flow.add_filter(rows)
    assert filtered["taker_flow_supportive"].tolist() == [False, True, True, False]
    assert taker_flow.TAKER_BUY_SHARE_THRESHOLD == pytest.approx(0.50)


def test_taker_flow_nested_precision_bootstrap_is_deterministic():
    rows = pd.DataFrame(
        {
            "symbol": ["A", "A", "B", "B", "C", "C"],
            "decision_time": pd.date_range("2026-01-01", periods=6, freq="7D", tz="UTC"),
            "late_target200": [1, 0, 1, 0, 0, 0],
            "taker_flow_supportive": [True, False, True, False, True, False],
        }
    )
    first = taker_flow.nested_precision_bootstrap(rows, cluster="symbol", samples=200, seed=7)
    second = taker_flow.nested_precision_bootstrap(rows, cluster="symbol", samples=200, seed=7)
    assert first == second
    assert first["samples"] == 200
    assert first["precision_delta_median"] > 0


def test_taker_flow_bar_reconstruction_and_nearby_flags_are_past_only():
    rows = pd.DataFrame(
        {
            "early_taker_buy_share": [0.50, 0.49],
            "early_taker_acceleration": [0.10, -0.02],
        }
    )
    reconstructed = taker_robustness.add_bar_shares(rows)
    assert reconstructed["first_bar_taker_buy_share"].tolist() == pytest.approx([0.45, 0.50])
    assert reconstructed["last_bar_taker_buy_share"].tolist() == pytest.approx([0.55, 0.48])
    flags = taker_robustness.flag_catalog(reconstructed)
    assert flags["mean_ge_50pct"].tolist() == [True, False]
    assert flags["both_bars_ge_50pct"].tolist() == [False, False]
    assert flags["last_bar_ge_50pct"].tolist() == [True, False]


def test_breakout_full_runner_removes_only_partial_sale_controls():
    policies = breakout_runner.policy_catalog(30)
    current = policies[breakout_runner.CURRENT_ROUTER]
    runner = policies["breakout6_full_runner_a150_t30_wick"]
    assert current["partial_target"] == pytest.approx(1.50)
    assert current["partial_fraction"] == pytest.approx(0.50)
    assert runner["partial_target"] is None
    assert runner["partial_fraction"] == pytest.approx(0.0)
    for key in [
        "hard_stop",
        "trail_activation",
        "trail_distance",
        "exhaustion_activation",
        "exhaustion_upper_wick_min",
        "exhaustion_close_location_max",
        "exhaustion_volume_ratio_min",
    ]:
        assert runner[key] == current[key]


def test_breakout_partial_fraction_catalog_changes_only_fraction():
    policies = partial_fraction.policy_catalog(30)
    assert sorted(policy["partial_fraction"] for policy in policies.values()) == [0.25, 0.50, 0.75]
    baseline = policies["breakout6_sell50_at150_trail30_wick"]
    for policy in policies.values():
        for key in baseline:
            if key not in {"family", "partial_fraction"}:
                assert policy[key] == baseline[key]


def test_regime_partial_routes_are_frozen_and_use_only_available_state():
    routes = regime_partial_router.route_catalog({"bear_highvol_broad": 0.75})
    highvol = {
        "btc_vol_regime": "highvol",
        "btc_trend_regime": "bear",
        "breadth_regime": "broad",
        "market_state": "bear_highvol_broad",
    }
    lowvol_bull = {
        "btc_vol_regime": "lowvol",
        "btc_trend_regime": "bull",
        "breadth_regime": "broad",
        "market_state": "bull_lowvol_broad",
    }
    bear_narrow = {
        "btc_vol_regime": "lowvol",
        "btc_trend_regime": "bear",
        "breadth_regime": "narrow",
        "market_state": "bear_lowvol_narrow",
    }
    assert routes["baseline_sell50"](highvol) == pytest.approx(0.50)
    assert routes["semantic_vol_high25_low75"](highvol) == pytest.approx(0.25)
    assert routes["semantic_vol_high25_low75"](lowvol_bull) == pytest.approx(0.75)
    assert routes["calibration_vol_high75_low50"](highvol) == pytest.approx(0.75)
    assert routes["calibration_vol_high75_low50"](lowvol_bull) == pytest.approx(0.50)
    assert routes["semantic_risk_bull25_bear_narrow75"](lowvol_bull) == pytest.approx(0.25)
    assert routes["semantic_risk_bull25_bear_narrow75"](bear_narrow) == pytest.approx(0.75)
    assert routes["calibration_market_state_min5"](highvol) == pytest.approx(0.75)
    assert routes["calibration_market_state_min5"](lowvol_bull) == pytest.approx(0.50)


def test_regime_state_learning_falls_back_when_fewer_than_five_trades_are_affected():
    base_time = pd.Timestamp("2026-01-01", tz="UTC")
    catalog: dict[float, list[dict[str, object]]] = {0.25: [], 0.50: [], 0.75: []}
    for signal_key in range(4):
        common = {
            "signal_key": signal_key,
            "validation_split": "cal_1",
            "market_state": "bear_highvol_broad",
            "entry_time": base_time + pd.Timedelta(days=signal_key),
        }
        catalog[0.25].append({**common, "net_return": -0.10})
        catalog[0.50].append({**common, "net_return": 0.00})
        catalog[0.75].append({**common, "net_return": 0.10})
    mapping, support = regime_partial_router.calibration_state_map(catalog)
    assert int(support["affected"].sum()) == 4
    assert mapping == {"bear_highvol_broad": pytest.approx(0.50)}


def test_portfolio_notional_scales_inverse_to_stop_width():
    outcome = {
        "entry_time": pd.Timestamp("2026-01-01", tz="UTC"),
        "exit_time": pd.Timestamp("2026-01-02", tz="UTC"),
        "symbol": "TESTUSDT",
        "score": 1.0,
        "net_return": 0.10,
        "mark_path": [],
    }
    trades20, _ = exit_study.apply_portfolio_constraints_fast(
        [outcome], initial_equity=100_000.0, risk_stop_pct=0.20
    )
    trades30, _ = exit_study.apply_portfolio_constraints_fast(
        [outcome], initial_equity=100_000.0, risk_stop_pct=0.30
    )
    assert trades20[0]["notional_usd"] == pytest.approx(2_500.0)
    assert trades30[0]["notional_usd"] == pytest.approx(1_666.6666667)
