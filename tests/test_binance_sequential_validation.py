from __future__ import annotations

import numpy as np
import pandas as pd
import importlib.util
from pathlib import Path
import sys
import pytest

from core.research.binance_entry_exit_validation import (
    add_past_only_4h_features,
    build_exit_policy_catalog,
    simulate_stateful_trade,
)
from core.research.binance_sequential_validation import (
    PATH_FEATURES,
    checkpoint_row,
    combine_weighted_outcomes,
    fit_sequence_score,
)
from core.research.binance_postlaunch_validation import locate_postlaunch_entry
from core.research.binance_hybrid_stop_validation import (
    excursion_before_target,
    simulate_hybrid_stop_trade,
)
from core.research.binance_forward_microstructure import (
    latest_and_changes,
    orderbook_metrics,
    records_at_or_before,
    taker_flow,
    validate_capture_clock,
)
from core.research.binance_continuation_utility import (
    FrozenUtilityModel,
    fit_frozen_utility_model,
)


def load_sequence_monitor():
    path = Path(__file__).resolve().parents[1] / "scripts" / "binance_sequence_forward_monitor.py"
    spec = importlib.util.spec_from_file_location("binance_sequence_forward_monitor_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_microstructure_monitor():
    path = Path(__file__).resolve().parents[1] / "scripts" / "binance_launch_microstructure_forward_monitor.py"
    spec = importlib.util.spec_from_file_location("binance_launch_microstructure_forward_monitor_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_forward_strategy_labeler():
    path = Path(__file__).resolve().parents[1] / "scripts" / "binance_forward_strategy_labeler.py"
    spec = importlib.util.spec_from_file_location("binance_forward_strategy_labeler_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_continuation_utility_monitor():
    path = Path(__file__).resolve().parents[1] / "scripts" / "binance_continuation_utility_forward_monitor.py"
    spec = importlib.util.spec_from_file_location("binance_continuation_utility_forward_monitor_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def make_bars(count: int = 96) -> pd.DataFrame:
    times = pd.date_range("2026-01-01", periods=count, freq="4h", tz="UTC")
    close = 100.0 + np.arange(count, dtype=float)
    return pd.DataFrame(
        {
            "open_time": times,
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "quote_volume": 1_000.0 + 10.0 * np.arange(count),
            "taker_buy_quote": 520.0 + 5.0 * np.arange(count),
        }
    )


def make_signal() -> dict[str, object]:
    return {
        "symbol": "TESTUSDT",
        "date": pd.Timestamp("2025-12-31", tz="UTC"),
        "fold": 1,
        "signal_key": 7,
        "entry_time": pd.Timestamp("2026-01-01", tz="UTC"),
        "entry_open": 99.5,
        "close": 99.0,
        "price_model_score": 0.8,
        "price_model_pctile": 0.99,
        "target200_14d_from_entry": True,
    }


def test_checkpoint_features_exclude_decision_bar() -> None:
    bars = make_bars()
    altered = bars.copy()
    altered.loc[6, ["high", "low", "close", "quote_volume", "taker_buy_quote"]] = [
        10_000.0,
        1.0,
        9_000.0,
        1_000_000_000.0,
        999_000_000.0,
    ]
    left = checkpoint_row(make_signal(), bars, horizon_hours=24)
    right = checkpoint_row(make_signal(), altered, horizon_hours=24)
    assert left is not None and right is not None
    assert left["decision_time"] == pd.Timestamp("2026-01-02", tz="UTC")
    for feature in PATH_FEATURES:
        assert np.isclose(float(left[feature]), float(right[feature]), equal_nan=True)


def test_checkpoint_late_label_uses_decision_open() -> None:
    bars = make_bars()
    bars.loc[7, "high"] = 400.0
    row = checkpoint_row(make_signal(), bars, horizon_hours=24)
    assert row is not None
    assert row["decision_open"] == bars.loc[6, "open"]
    assert row["late_target200"] is True


def test_sequence_reject_exits_at_checkpoint_open() -> None:
    bars = add_past_only_4h_features(make_bars(20))
    policy = dict(build_exit_policy_catalog()["frozen_half100_trail25"])
    policy.update(
        {
            "launch_deadline_days": 1.0,
            "launch_mfe_required": 999.0,
            "family": "sequence_reject",
        }
    )
    outcome = simulate_stateful_trade(
        bars,
        entry_time=pd.Timestamp("2026-01-01", tz="UTC"),
        policy=policy,
        slippage_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "failure_to_launch"
    assert outcome["exit_time"] == pd.Timestamp("2026-01-02", tz="UTC")
    assert outcome["exit_price"] == bars.loc[6, "open"]


def test_model_routed_exit_fills_at_decision_open_before_same_bar_extremes() -> None:
    bars = make_bars(12)
    bars.loc[3, "high"] = 1_000.0
    bars.loc[3, "low"] = 1.0
    featured = add_past_only_4h_features(bars)
    policy = dict(build_exit_policy_catalog()["frozen_half100_trail25"])
    policy.update(
        {
            "hard_stop": 0.25,
            "forced_exit_after_hours": 12,
            "forced_exit_reason": "intensity_reject_exit",
        }
    )
    outcome = simulate_stateful_trade(
        featured,
        entry_time=pd.Timestamp("2026-01-01", tz="UTC"),
        policy=policy,
        slippage_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "intensity_reject_exit"
    assert outcome["exit_time"] == pd.Timestamp("2026-01-01 12:00", tz="UTC")
    assert outcome["exit_price"] == bars.loc[3, "open"]


def test_blowoff_exhaustion_waits_for_reversal_bar_close_then_next_open() -> None:
    bars = make_bars(12)
    bars.loc[4, ["open", "high", "low", "close", "quote_volume"]] = [180.0, 230.0, 170.0, 180.0, 5_000_000.0]
    bars.loc[5, "low"] = 1.0
    featured = add_past_only_4h_features(bars)
    featured.loc[4, "volume_ratio_18"] = 2.0
    policy = dict(build_exit_policy_catalog()["frozen_half100_trail25"])
    policy.update(
        {
            "hard_stop": 0.25,
            "exhaustion_activation": 1.0,
            "exhaustion_upper_wick_min": 0.35,
            "exhaustion_close_location_max": 0.35,
            "exhaustion_volume_ratio_min": 1.5,
        }
    )
    outcome = simulate_stateful_trade(
        featured,
        entry_time=pd.Timestamp("2026-01-01", tz="UTC"),
        policy=policy,
        slippage_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "blowoff_exhaustion"
    assert outcome["exit_time"] == bars.loc[5, "open_time"]
    assert outcome["exit_price"] == bars.loc[5, "open"]


def test_weighted_outcomes_keep_unallocated_cash_flat() -> None:
    first = {
        "entry_time": pd.Timestamp("2026-01-01", tz="UTC"),
        "exit_time": pd.Timestamp("2026-01-02", tz="UTC"),
        "net_return": -0.10,
        "funding_return": -0.002,
        "fee_return": 0.001,
        "mfe": 0.10,
        "mae": -0.15,
        "mark_path": [{"time": pd.Timestamp("2026-01-02", tz="UTC"), "mark_return": -0.10}],
    }
    rejected = combine_weighted_outcomes(first, None, first_weight=0.50)
    assert np.isclose(rejected["net_return"], -0.05)
    assert rejected["exit_reason"] == "staged_reject"

    second = {
        "entry_time": pd.Timestamp("2026-01-02", tz="UTC"),
        "exit_time": pd.Timestamp("2026-01-04", tz="UTC"),
        "net_return": 0.30,
        "funding_return": -0.004,
        "fee_return": 0.001,
        "mfe": 0.40,
        "mae": -0.05,
        "mark_path": [{"time": pd.Timestamp("2026-01-04", tz="UTC"), "mark_return": 0.30}],
    }
    added = combine_weighted_outcomes(first, second, first_weight=0.50)
    assert np.isclose(added["net_return"], 0.10)
    assert added["exit_time"] == pd.Timestamp("2026-01-04", tz="UTC")


def test_sequence_model_is_train_only_and_bounded() -> None:
    rng = np.random.default_rng(9)
    train = pd.DataFrame({feature: rng.normal(size=80) for feature in ["price_model_score", "price_model_pctile", *PATH_FEATURES]})
    train["original_target200"] = ([0] * 70) + ([1] * 10)
    test = train.iloc[:12].copy()
    train_scores, test_scores = fit_sequence_score(train, test, family="logistic")
    assert len(train_scores) == len(train)
    assert len(test_scores) == len(test)
    assert ((train_scores >= 0) & (train_scores <= 1)).all()
    assert ((test_scores >= 0) & (test_scores <= 1)).all()


def test_forward_observer_uses_six_completed_bars_only() -> None:
    monitor = load_sequence_monitor()
    bars = make_bars(20)
    # Snapshot cutoff at 00:20 means the reference entry is the 04:00 bar.
    signal = {
        "signal_id": "abc",
        "snapshot_date": "2026-01-01",
        "symbol": "TESTUSDT",
        "data_cutoff_utc": "2026-01-01T00:20:00+00:00",
        "score_pctile": 0.99,
        "stage": "price_watch",
    }
    # Make the six completed bars satisfy the rule and the decision bar absurd;
    # the latter must not alter the observation.
    bars.loc[1:6, "high"] = 130.0
    bars.loc[6, "close"] = 110.0
    bars.loc[7, ["high", "low", "close"]] = [10_000.0, 1.0, 9_000.0]
    observed = monitor.observe(
        signal,
        bars,
        as_of=pd.Timestamp("2026-01-02 04:00", tz="UTC"),
    )
    assert observed is not None
    assert observed["entry_time_utc"] == pd.Timestamp("2026-01-01 04:00", tz="UTC")
    assert observed["decision_time_utc"] == pd.Timestamp("2026-01-02 04:00", tz="UTC")
    assert observed["decision_reference_price"] == bars.loc[7, "open"]
    assert observed["launch_status"] == "launch_confirmed"
    assert observed["paper_eligible_after_original_stage"] is True


def test_forward_observer_preserves_original_rejection() -> None:
    monitor = load_sequence_monitor()
    bars = make_bars(20)
    bars.loc[1:6, "high"] = 130.0
    bars.loc[6, "close"] = 110.0
    signal = {
        "signal_id": "abc",
        "snapshot_date": "2026-01-01",
        "symbol": "TESTUSDT",
        "data_cutoff_utc": "2026-01-01T00:20:00+00:00",
        "score_pctile": 0.99,
        "stage": "reject_chase",
    }
    observed = monitor.observe(signal, bars, as_of=pd.Timestamp("2026-01-02 04:00", tz="UTC"))
    assert observed is not None
    assert observed["launch_status"] == "launch_confirmed"
    assert observed["paper_eligible_after_original_stage"] is False
    assert observed["ranking_changed"] is False


def test_forward_observation_is_immutable(tmp_path: Path) -> None:
    monitor = load_sequence_monitor()
    path = tmp_path / "signal.json"
    assert monitor.immutable_write(path, {"signal_id": "x", "value": 1}) == "created"
    assert monitor.immutable_write(path, {"signal_id": "x", "value": 1}) == "idempotent_existing"
    with pytest.raises(RuntimeError):
        monitor.immutable_write(path, {"signal_id": "x", "value": 2})


def make_postlaunch_signal(bars: pd.DataFrame) -> dict[str, object]:
    return {
        "decision_time": bars.loc[6, "open_time"],
        "decision_open": float(bars.loc[6, "open"]),
        "baseline_entry_open": float(bars.loc[0, "open"]),
        "early_mfe": 0.20,
    }


def test_postlaunch_immediate_enters_at_decision_open() -> None:
    bars = make_bars()
    signal = make_postlaunch_signal(bars)
    located = locate_postlaunch_entry(signal, bars, rule_name="launch_open")
    assert located is not None
    assert located["entry_time"] == bars.loc[6, "open_time"]
    assert located["entry_open"] == bars.loc[6, "open"]
    assert located["entry_delay_hours_after_launch"] == 0.0


def test_postlaunch_discount_touch_enters_next_bar_open() -> None:
    bars = make_bars()
    signal = make_postlaunch_signal(bars)
    decision_open = float(signal["decision_open"])
    bars.loc[8, "low"] = decision_open * 0.94
    located = locate_postlaunch_entry(signal, bars, rule_name="discount5_touch")
    assert located is not None
    assert located["trigger_time"] == bars.loc[9, "open_time"]
    assert located["entry_time"] == bars.loc[9, "open_time"]
    assert located["entry_open"] == bars.loc[9, "open"]
    assert located["entry_open"] != bars.loc[8, "open"]


def test_postlaunch_reclaim_requires_completed_reclaim_bar() -> None:
    bars = make_bars()
    signal = make_postlaunch_signal(bars)
    decision_open = float(signal["decision_open"])
    bars.loc[7, ["open", "high", "low", "close"]] = [104.0, 105.0, decision_open * 0.94, 100.0]
    bars.loc[8, ["open", "high", "low", "close"]] = [100.0, 105.0, 99.0, 104.0]
    located = locate_postlaunch_entry(signal, bars, rule_name="discount5_reclaim")
    assert located is not None
    assert located["trigger_time"] == bars.loc[9, "open_time"]
    assert located["entry_time"] == bars.loc[9, "open_time"]
    # A future price spike changes the actual label, but never the chosen entry.
    bars.loc[10, "high"] = float(located["entry_open"]) * 3.10
    relabeled = locate_postlaunch_entry(signal, bars, rule_name="discount5_reclaim")
    assert relabeled is not None
    assert relabeled["entry_time"] == located["entry_time"]
    assert relabeled["target200_14d_from_entry"] is True


def test_utility_entry_reclaim_respects_24h_trigger_window() -> None:
    bars = make_bars(110)
    signal = make_postlaunch_signal(bars)
    decision_open = float(signal["decision_open"])
    bars.loc[13, ["open", "high", "low", "close"]] = [104.0, 105.0, decision_open * 0.94, 100.0]
    bars.loc[14, ["open", "high", "low", "close"]] = [100.0, 105.0, 99.0, 104.0]
    assert locate_postlaunch_entry(
        signal, bars, rule_name="discount5_reclaim", trigger_window_hours=24
    ) is None
    located = locate_postlaunch_entry(
        signal, bars, rule_name="discount5_reclaim", trigger_window_hours=48
    )
    assert located is not None
    assert located["entry_time"] == bars.loc[15, "open_time"]


def hybrid_policy(**updates: object) -> dict[str, object]:
    policy: dict[str, object] = {
        "time_days": 1,
        "catastrophic_stop": 0.40,
        "close_stop": 0.15,
        "close_stop_bars": 1,
        "breakeven_activation": 0.30,
        "partial_target": 1.0,
        "partial_fraction": 0.50,
        "trail_activation": 1.0,
        "trail_distance": 0.25,
        "family": "test_hybrid",
    }
    policy.update(updates)
    return policy


def test_hybrid_stop_survives_intrabar_wick_above_catastrophe() -> None:
    bars = make_bars(12)
    bars.loc[:, ["open", "high", "low", "close"]] = [100.0, 102.0, 98.0, 100.0]
    bars.loc[0, ["high", "low", "close"]] = [102.0, 74.0, 90.0]
    outcome = simulate_hybrid_stop_trade(
        bars,
        entry_time=bars.loc[0, "open_time"],
        policy=hybrid_policy(),
        slippage_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "time_stop"
    assert outcome["mae"] == -0.26


def test_hybrid_close_stop_exits_only_at_next_open() -> None:
    bars = make_bars(12)
    bars.loc[:, ["open", "high", "low", "close"]] = [100.0, 102.0, 98.0, 100.0]
    bars.loc[0, ["high", "low", "close"]] = [102.0, 75.0, 80.0]
    bars.loc[1, "open"] = 82.0
    outcome = simulate_hybrid_stop_trade(
        bars,
        entry_time=bars.loc[0, "open_time"],
        policy=hybrid_policy(),
        slippage_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "close_confirmed_stop"
    assert outcome["exit_time"] == bars.loc[1, "open_time"]
    assert outcome["exit_price"] == 82.0


def test_hybrid_catastrophic_stop_precedes_same_bar_profit() -> None:
    bars = make_bars(12)
    bars.loc[:, ["open", "high", "low", "close"]] = [100.0, 102.0, 98.0, 100.0]
    bars.loc[0, ["high", "low", "close"]] = [250.0, 50.0, 200.0]
    outcome = simulate_hybrid_stop_trade(
        bars,
        entry_time=bars.loc[0, "open_time"],
        policy=hybrid_policy(),
        slippage_bps=0.0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "catastrophic_stop"
    assert outcome["partial_take_profit"] is False


def test_excursion_reports_prior_and_ambiguous_target_bar_separately() -> None:
    bars = make_bars(20)
    bars.loc[:, ["open", "high", "low", "close"]] = [100.0, 110.0, 95.0, 100.0]
    bars.loc[2, ["high", "low", "close"]] = [160.0, 70.0, 150.0]
    result = excursion_before_target(
        bars,
        entry_time=bars.loc[0, "open_time"],
        target_return=0.50,
    )
    assert result is not None and result["target_hit"] is True
    assert np.isclose(result["prior_bar_intrabar_mae"], -0.05)
    assert np.isclose(result["inclusive_intrabar_mae"], -0.30)


def test_forward_orderbook_metrics_use_quote_depth_and_vwap() -> None:
    payload = {
        "lastUpdateId": 7,
        "E": 1_767_225_600_000,
        "T": 1_767_225_600_000,
        "bids": [["99", "10"], ["98", "20"]],
        "asks": [["101", "10"], ["102", "20"]],
    }
    result = orderbook_metrics(payload, depth_bands=(0.01,), impact_notionals=(500.0, 2_000.0, 5_000.0))
    assert result["mid_price"] == 100.0
    assert np.isclose(result["bid_depth_usd_100bp"], 990.0)
    assert np.isclose(result["ask_depth_usd_100bp"], 1_010.0)
    assert np.isclose(result["depth_imbalance_100bp"], -0.01)
    assert result["buy_impact_bps_500"] < result["buy_impact_bps_2000"]
    assert result["buy_impact_bps_5000"] is None


def test_forward_history_excludes_records_after_decision() -> None:
    cutoff = pd.Timestamp("2026-01-02 04:00", tz="UTC")
    records = [
        {"timestamp": int((cutoff - pd.Timedelta(hours=4)).timestamp() * 1000), "value": "100"},
        {"timestamp": int((cutoff - pd.Timedelta(hours=1)).timestamp() * 1000), "value": "120"},
        {"timestamp": int((cutoff + pd.Timedelta(minutes=5)).timestamp() * 1000), "value": "999"},
    ]
    filtered = records_at_or_before(records, cutoff=cutoff)
    assert len(filtered) == 2
    changes = latest_and_changes(records, cutoff=cutoff, value_key="value", lookbacks_hours=(3,))
    assert changes["latest"] == 120.0
    assert np.isclose(changes["change_3h"], 0.20)


def test_forward_taker_flow_uses_only_requested_past_window() -> None:
    cutoff = pd.Timestamp("2026-01-02 04:00", tz="UTC")
    records = [
        {"timestamp": int((cutoff - pd.Timedelta(minutes=30)).timestamp() * 1000), "buyVol": "60", "sellVol": "40"},
        {"timestamp": int((cutoff - pd.Timedelta(hours=2)).timestamp() * 1000), "buyVol": "10", "sellVol": "90"},
        {"timestamp": int((cutoff + pd.Timedelta(minutes=5)).timestamp() * 1000), "buyVol": "999", "sellVol": "1"},
    ]
    result = taker_flow(records, cutoff=cutoff, windows_hours=(1, 4))
    assert result["observations_1h"] == 1
    assert result["taker_buy_share_1h"] == 0.60
    assert result["observations_4h"] == 2
    assert result["taker_buy_share_4h"] == 0.35


def test_forward_capture_clock_fails_closed_when_stale() -> None:
    decision = pd.Timestamp("2026-01-02 04:00", tz="UTC")
    fresh = validate_capture_clock(decision_time=decision, capture_time=decision + pd.Timedelta(minutes=5))
    stale = validate_capture_clock(decision_time=decision, capture_time=decision + pd.Timedelta(minutes=16))
    early = validate_capture_clock(decision_time=decision, capture_time=decision - pd.Timedelta(seconds=1))
    assert fresh["passed"] is True
    assert stale["passed"] is False
    assert early["passed"] is False


def make_microstructure_bundle(decision: pd.Timestamp, capture: pd.Timestamp) -> dict[str, object]:
    bid_levels = [[str(99.99 - index * 0.01), "100"] for index in range(120)]
    ask_levels = [[str(100.01 + index * 0.01), "100"] for index in range(120)]
    history = [
        {
            "timestamp": int((decision - pd.Timedelta(minutes=5 * index)).timestamp() * 1000),
            "sumOpenInterestValue": str(1_000_000 + (288 - index) * 1_000),
        }
        for index in range(289)
    ]
    taker = [
        {
            "timestamp": int((decision - pd.Timedelta(minutes=5 * index)).timestamp() * 1000),
            "buyVol": "60",
            "sellVol": "40",
        }
        for index in range(289)
    ]
    return {
        "depth": {
            "lastUpdateId": 1,
            "E": int(capture.timestamp() * 1000),
            "T": int(capture.timestamp() * 1000),
            "bids": bid_levels,
            "asks": ask_levels,
        },
        "premium": {
            "symbol": "TESTUSDT",
            "markPrice": "100",
            "indexPrice": "99.9",
            "lastFundingRate": "0.0001",
            "nextFundingTime": int((capture + pd.Timedelta(hours=4)).timestamp() * 1000),
            "time": int(capture.timestamp() * 1000),
        },
        "current_oi": {
            "symbol": "TESTUSDT",
            "openInterest": "12000",
            "time": int(capture.timestamp() * 1000),
        },
        "oi_history": history,
        "taker": taker,
        "source_errors": {},
    }


def test_microstructure_snapshot_is_rank_neutral_and_past_only() -> None:
    monitor = load_microstructure_monitor()
    decision = pd.Timestamp("2026-01-02 04:00", tz="UTC")
    capture = decision + pd.Timedelta(minutes=5)
    observation = {
        "signal_id": "abc123",
        "snapshot_date": "2026-01-01",
        "symbol": "TESTUSDT",
        "original_stage": "price_watch",
        "original_score_pctile": 0.99,
        "paper_eligible_after_original_stage": True,
        "decision_time_utc": decision,
    }
    bundle = make_microstructure_bundle(decision, capture)
    bundle["oi_history"].append(
        {
            "timestamp": int((decision + pd.Timedelta(minutes=5)).timestamp() * 1000),
            "sumOpenInterestValue": "999999999",
        }
    )
    snapshot, audit = monitor.assemble_snapshot(observation, bundle, capture_time=capture)
    assert audit["data_quality"]["passed"] is True
    assert snapshot is not None
    assert snapshot["ranking_changed"] is False
    assert snapshot["annotation_only"] is True
    assert snapshot["automatic_trading_allowed"] is False
    assert snapshot["features"]["open_interest_history"]["latest"] != 999999999.0


def test_microstructure_snapshot_fails_closed_when_capture_is_late() -> None:
    monitor = load_microstructure_monitor()
    decision = pd.Timestamp("2026-01-02 04:00", tz="UTC")
    capture = decision + pd.Timedelta(minutes=16)
    observation = {
        "signal_id": "abc123",
        "snapshot_date": "2026-01-01",
        "symbol": "TESTUSDT",
        "original_stage": "price_watch",
        "original_score_pctile": 0.99,
        "paper_eligible_after_original_stage": True,
        "decision_time_utc": decision,
    }
    snapshot, audit = monitor.assemble_snapshot(
        observation, make_microstructure_bundle(decision, capture), capture_time=capture
    )
    assert snapshot is None
    assert "capture_clock_outside_allowed_window" in audit["data_quality"]["failures"]


def test_forward_strategy_label_requires_complete_path_and_freezes_counterfactuals() -> None:
    labeler = load_forward_strategy_labeler()
    decision = pd.Timestamp("2026-01-02 04:00", tz="UTC")
    times = pd.date_range(decision, periods=181, freq="4h", tz="UTC")
    close = np.linspace(100.0, 340.0, len(times))
    bars = pd.DataFrame(
        {
            "open_time": times,
            "open": close,
            "high": close * 1.03,
            "low": close * 0.97,
            "close": close,
            "quote_volume": 1_000_000.0,
            "taker_buy_quote": 550_000.0,
        }
    )
    funding = [
        {
            "fundingTime": int(when.timestamp() * 1000),
            "fundingRate": "0.0001",
        }
        for when in pd.date_range(decision, decision + pd.Timedelta(days=30), freq="8h", tz="UTC")
    ]
    item = {
        "signal_id": "abc123",
        "snapshot_date": "2026-01-01",
        "symbol": "TESTUSDT",
        "original_stage": "price_watch",
        "paper_eligible_after_original_stage": True,
        "decision_time_utc": decision,
    }
    label, audit = labeler.build_label(
        item,
        bars,
        funding,
        labeled_at=decision + pd.Timedelta(days=30, hours=4),
    )
    assert audit["data_quality"]["passed"] is True
    assert label is not None
    assert label["path_labels"]["target200_14d_from_decision"] is False
    assert set(label["exit_outcomes"]) == {
        "baseline_hard25_be30_half100_trail25",
        "cat40_close25x1_be30_half100_trail25",
        "hard25_be30_half150_trail30",
        "hard25_be30_half150_trail35_wick",
        "hard25_be30_half150_trail30_wick_posthoc",
    }
    assert label["entry_counterfactuals"] == {}
    assert label["hold_counterfactuals"] == {}
    utility_label, utility_audit = labeler.build_label(
        item,
        bars,
        funding,
        labeled_at=decision + pd.Timedelta(days=30, hours=4),
        include_utility_entry_counterfactual=True,
        include_utility_hold_counterfactual=True,
    )
    assert utility_audit["data_quality"]["passed"] is True
    assert utility_label is not None
    assert set(utility_label["entry_counterfactuals"]) == {"discount5_reclaim_24h_next_open"}
    utility_counterfactual = utility_label["entry_counterfactuals"]["discount5_reclaim_24h_next_open"]
    assert utility_counterfactual["triggered"] is False
    assert utility_counterfactual["historically_promoted"] is False
    assert utility_counterfactual["ranking_changed"] is False
    assert utility_counterfactual["automatic_trading_allowed"] is False
    assert set(utility_label["hold_counterfactuals"]) == {"breakout6_half150_trail30_wick_24h_next_open"}
    hold_counterfactual = utility_label["hold_counterfactuals"]["breakout6_half150_trail30_wick_24h_next_open"]
    assert hold_counterfactual["historically_promoted"] is False
    assert hold_counterfactual["primary_exit_changed"] is False
    assert hold_counterfactual["ranking_changed"] is False
    assert hold_counterfactual["automatic_trading_allowed"] is False
    assert label["ranking_changed"] is False
    assert label["automatic_trading_allowed"] is False
    incomplete, bad_audit = labeler.build_label(
        item,
        bars.iloc[:-1],
        funding,
        labeled_at=decision + pd.Timedelta(days=30, hours=4),
    )
    assert incomplete is None
    assert "incomplete_or_noncontiguous_30d_4h_path" in bad_audit["data_quality"]["failures"]


def test_checkpoint_can_build_forward_features_without_future_labels() -> None:
    bars = make_bars(10)
    signal = make_signal()
    signal["entry_time"] = bars.iloc[0]["open_time"]
    signal["entry_open"] = bars.iloc[0]["open"]
    forward = checkpoint_row(signal, bars, horizon_hours=8, require_future_labels=False)
    historical = checkpoint_row(signal, bars, horizon_hours=8)
    assert forward is not None
    assert historical is None
    assert forward["decision_time"] == bars.iloc[2]["open_time"]
    assert forward["late_target200"] is None
    altered = bars.copy()
    altered.loc[3:, ["high", "low", "close", "quote_volume", "taker_buy_quote"]] *= 100
    repeated = checkpoint_row(signal, altered, horizon_hours=8, require_future_labels=False)
    assert repeated is not None
    for feature in PATH_FEATURES:
        assert np.isclose(float(forward[feature]), float(repeated[feature]), equal_nan=True)


def test_frozen_utility_model_roundtrip_preserves_scores() -> None:
    rows = pd.DataFrame(
        {
            "decision_time": pd.date_range("2026-01-01", periods=60, freq="D", tz="UTC"),
            "x1": np.linspace(-2.0, 2.0, 60),
            "x2": np.sin(np.linspace(0.0, 8.0, 60)),
        }
    )
    rows["target"] = (rows["x1"] + 0.25 * rows["x2"] > 0).astype(int)
    model = fit_frozen_utility_model(
        rows,
        features=["x1", "x2"],
        label_column="target",
        version="test-v1",
        checkpoint_hours=8,
        selection_quantile=0.70,
    )
    restored = FrozenUtilityModel.from_dict(model.to_dict())
    assert np.allclose(model.predict_score(rows), restored.predict_score(rows))
    assert model.training_rows == 60
    assert 0 < model.selection_threshold < 1


def test_continuation_utility_daily_limit_keeps_highest_three() -> None:
    monitor = load_continuation_utility_monitor()
    rows = pd.DataFrame(
        {
            "decision_time": [pd.Timestamp("2026-01-02 12:00", tz="UTC")] * 5,
            "raw": [True] * 5,
            "score": [0.1, 0.9, 0.7, 0.8, 0.2],
        }
    )
    selected = monitor.apply_daily_limit(rows, "raw", "score")
    assert selected.sum() == 3
    assert set(rows.index[selected]) == {1, 2, 3}


def test_taker_flow_annotation_never_changes_rank_stage_or_paper_gate() -> None:
    monitor = load_continuation_utility_monitor()
    annotation = monitor.taker_flow_annotation({"early_taker_buy_share": 0.50})
    assert annotation["supportive"] is True
    assert annotation["name"] == "taker_buy_share_ge_50pct_at_8h"
    assert annotation["changes_primary_rank"] is False
    assert annotation["changes_stage_veto"] is False
    assert annotation["changes_paper_eligibility"] is False
