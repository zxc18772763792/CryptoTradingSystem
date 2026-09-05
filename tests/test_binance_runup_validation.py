from __future__ import annotations

import importlib.util
import json
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


validation = load_standalone(
    "binance_runup_validation_tested",
    ROOT / "core" / "research" / "binance_runup_validation.py",
)
monitor = load_standalone(
    "binance_runup_forward_monitor_tested",
    ROOT / "scripts" / "binance_runup_forward_monitor.py",
)
exit_study = load_standalone(
    "binance_exit_strategy_study_tested",
    ROOT / "scripts" / "analyze_binance_exit_strategies.py",
)


def make_bars(prices: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=len(prices), freq="4h", tz="UTC")
    return pd.DataFrame(
        {
            "symbol": "TESTUSDT",
            "open_time": dates,
            "open": [row[0] for row in prices],
            "high": [row[1] for row in prices],
            "low": [row[2] for row in prices],
            "close": [row[3] for row in prices],
        }
    )


def test_runup_is_any_ordered_subwindow_not_first_to_last_return():
    bars = make_bars([(10, 10, 9, 10), (9, 9, 8, 8.5), (9, 27, 8, 9)])
    result = validation.scan_30d_runups(bars)
    assert result.iloc[0]["classification"] == "close-confirmed"
    assert result.iloc[0]["close_return"] >= 2.0
    assert bars.iloc[-1]["close"] / bars.iloc[0]["close"] - 1 < 0


def test_wick_only_is_separate_from_close_confirmed():
    bars = make_bars([(10, 10, 5, 10), (11, 16, 10, 11)])
    result = validation.scan_30d_runups(bars)
    assert result.iloc[0]["classification"] == "wick-only"


def test_forward_target_enters_at_next_day_open():
    dates = pd.date_range("2026-01-01", periods=5, freq="1D", tz="UTC")
    panel = pd.DataFrame(
        {
            "symbol": "TESTUSDT", "date": dates,
            "open": [10, 20, 30, 40, 50], "high": [11, 22, 33, 44, 55],
            "low": [9, 18, 27, 36, 45], "close": [10, 21, 31, 41, 51],
        }
    )
    result = validation.add_forward_targets(panel, horizons=(3,))
    assert result.loc[0, "entry_open_3d"] == 20
    assert result.loc[0, "future_high_3d"] == 44
    assert bool(result.loc[0, "target_complete_3d"])


def test_monitor_label_uses_0400_next_4h_open_and_excludes_0000_bar():
    daily_dates = pd.date_range("2026-01-01", periods=5, freq="1D", tz="UTC")
    daily = pd.DataFrame(
        {
            "symbol": "TESTUSDT", "date": daily_dates,
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
        }
    )
    times = pd.date_range("2026-01-01", periods=5 * 6, freq="4h", tz="UTC")
    bars = pd.DataFrame(
        {
            "symbol": "TESTUSDT", "open_time": times,
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
        }
    )
    bars.loc[bars["open_time"] == pd.Timestamp("2026-01-02 00:00", tz="UTC"), "high"] = 100.0
    bars.loc[bars["open_time"] == pd.Timestamp("2026-01-02 04:00", tz="UTC"), "open"] = 20.0
    result = validation.add_forward_targets_from_4h(daily, bars, horizons=(3,))
    assert result.loc[0, "entry_time_3d"] == pd.Timestamp("2026-01-02 04:00", tz="UTC")
    assert result.loc[0, "entry_open_3d"] == 20.0
    assert result.loc[0, "future_high_3d"] == 11.0


def test_walk_forward_has_full_14_day_purge():
    dates = pd.date_range("2025-01-01", periods=260, freq="1D", tz="UTC")
    rows = pd.DataFrame({"date": dates, "symbol": "TESTUSDT"})
    folds = validation.expanding_walk_forward_splits(rows)
    assert folds
    for fold in folds:
        assert fold["test_start"] - fold["purge_start"] == pd.Timedelta(days=14)
        assert fold["train_end"] < fold["purge_start"]


def test_overlapping_positive_rows_are_one_independent_event():
    dates = pd.date_range("2026-01-01", periods=4, freq="1D", tz="UTC")
    scored = pd.DataFrame(
        {
            "symbol": "TESTUSDT", "date": dates,
            "target_complete_14d": True, "target_max_return_14d": [2.1, 3.0, 2.2, 2.4],
            "price_model_pctile": [0.5, 0.99, 0.8, 0.7],
        }
    )
    events = validation.independent_event_episodes(scored)
    assert len(events) == 1
    assert bool(events.iloc[0]["captured_top_2pct"])


def test_same_bar_stop_is_conservative_and_precedes_profit_target():
    bars = make_bars([(100, 220, 70, 150)])
    outcome = validation.simulate_trade_path(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy_name="stop20_half_at100_trail25_time30",
        fee_bps=0,
        slippage_bps=0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "hard_stop"
    assert not outcome["partial_take_profit"]
    assert outcome["net_return"] == pytest.approx(-0.20)


def test_fixed_take_profit_fills_at_target_not_bar_high():
    bars = make_bars([(100, 180, 95, 170)])
    outcome = validation.simulate_trade_path_with_policy(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy={
            "family": "fixed_take_profit",
            "hard_stop": 0.20,
            "time_days": 14,
            "full_take_profit": 0.50,
        },
        fee_bps=0,
        slippage_bps=0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "take_profit"
    assert outcome["exit_price"] == pytest.approx(150.0)
    assert outcome["net_return"] == pytest.approx(0.50)


def test_same_bar_stop_precedes_new_fixed_take_profit():
    bars = make_bars([(100, 180, 70, 150)])
    outcome = validation.simulate_trade_path_with_policy(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy={
            "family": "fixed_take_profit",
            "hard_stop": 0.20,
            "time_days": 14,
            "full_take_profit": 0.50,
        },
        fee_bps=0,
        slippage_bps=0,
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "hard_stop"
    assert outcome["net_return"] == pytest.approx(-0.20)


def test_two_stage_take_profit_accounts_for_both_halves():
    bars = make_bars([(100, 210, 90, 200)])
    outcome = validation.simulate_trade_path_with_policy(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy={
            "family": "two_stage_fixed_take_profit",
            "hard_stop": 0.20,
            "time_days": 30,
            "partial_target": 0.50,
            "partial_fraction": 0.50,
            "full_take_profit": 1.00,
        },
        fee_bps=0,
        slippage_bps=0,
    )
    assert outcome is not None
    assert outcome["partial_take_profit"]
    assert outcome["full_take_profit"]
    assert outcome["net_return"] == pytest.approx(0.75)


def test_exit_policy_catalog_covers_fixed_partial_and_trailing_families():
    catalog = validation.build_exit_policy_catalog()
    families = {item["family"] for item in catalog.values()}
    assert len(catalog) == 52
    assert {
        "time_only",
        "fixed_take_profit",
        "trailing_take_profit",
        "partial_then_trailing",
        "two_stage_fixed_take_profit",
    }.issubset(families)


def test_cached_exit_simulator_matches_reference_path_logic():
    bars = make_bars([(100, 160, 90, 150), (150, 210, 140, 200)])
    policy = {
        "family": "two_stage_fixed_take_profit",
        "hard_stop": 0.20,
        "time_days": 30,
        "partial_target": 0.50,
        "partial_fraction": 0.50,
        "full_take_profit": 1.00,
    }
    times = bars["open_time"].dt.as_unit("ns").astype("int64").to_numpy()
    cached = {
        "times": times,
        "open": bars["open"].to_numpy(float),
        "high": bars["high"].to_numpy(float),
        "low": bars["low"].to_numpy(float),
        "close": bars["close"].to_numpy(float),
        "funding_times": np.asarray([], dtype=np.int64),
        "funding_cumulative": np.asarray([0.0]),
        "funding_observed": False,
        "entry_ns": int(times[0]),
    }
    fast = exit_study.simulate_cached_path(
        cached, policy=policy, slippage_bps=5, fee_bps=5, include_marks=True
    )
    reference = validation.simulate_trade_path_with_policy(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy=policy,
        fee_bps=5,
        slippage_bps=5,
    )
    assert fast is not None and reference is not None
    for field in ["net_return", "funding_return", "fee_return", "exit_price", "mfe", "mae"]:
        assert fast[field] == pytest.approx(reference[field])
    assert fast["exit_reason"] == reference["exit_reason"]
    assert fast["exit_time"] == reference["exit_time"]


def test_vectorized_portfolio_curve_matches_frozen_reference():
    start = pd.Timestamp("2026-01-01 04:00", tz="UTC")
    outcomes = []
    for index, net_return in enumerate([0.50, -0.20]):
        entry = start + pd.Timedelta(days=index)
        exit_time = entry + pd.Timedelta(days=2)
        outcomes.append(
            {
                "symbol": f"TEST{index}USDT",
                "score": 1.0 - 0.1 * index,
                "entry_time": entry,
                "exit_time": exit_time,
                "net_return": net_return,
                "mark_path": [
                    {"time": entry, "mark_return": 0.0, "remaining_fraction": 1.0},
                    {
                        "time": entry + pd.Timedelta(days=1),
                        "mark_return": net_return / 2,
                        "remaining_fraction": 1.0,
                    },
                    {"time": exit_time, "mark_return": net_return, "remaining_fraction": 0.0},
                ],
            }
        )
    reference_trades, reference_curve = validation._apply_portfolio_constraints(
        outcomes, initial_equity=100_000
    )
    fast_trades, fast_curve = exit_study.apply_portfolio_constraints_fast(
        outcomes, initial_equity=100_000
    )
    assert [row["notional_usd"] for row in fast_trades] == pytest.approx(
        [row["notional_usd"] for row in reference_trades]
    )
    assert fast_curve["equity"].to_numpy() == pytest.approx(reference_curve["equity"].to_numpy())
    assert fast_curve["drawdown"].to_numpy() == pytest.approx(reference_curve["drawdown"].to_numpy())


def test_positive_funding_is_a_cost_for_long_position():
    bars = make_bars([(100, 101, 99, 100), (100, 101, 99, 100)])
    funding = pd.Series([0.001], index=[bars.iloc[1]["open_time"]])
    outcome = validation.simulate_trade_path(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy_name="hold_14d",
        funding=funding,
        fee_bps=0,
        slippage_bps=0,
    )
    assert outcome is not None
    assert outcome["funding_return"] == pytest.approx(-0.001)
    assert outcome["net_return"] == pytest.approx(-0.001)


def test_round_trip_fee_and_slippage_are_both_charged():
    bars = make_bars([(100, 100, 100, 100)])
    outcome = validation.simulate_trade_path(
        bars,
        entry_time=bars.iloc[0]["open_time"],
        policy_name="hold_14d",
        fee_bps=5,
        slippage_bps=5,
    )
    assert outcome is not None
    expected_price_return = (100 * 0.9995) / (100 * 1.0005) - 1
    assert outcome["fee_return"] == pytest.approx(0.001)
    assert outcome["net_return"] == pytest.approx(expected_price_return - 0.001)


def test_market_cap_and_oi_availability_is_shifted_one_day(tmp_path: Path):
    root = tmp_path
    for folder in ["oi_1d", "mcap_1d", "funding"]:
        (root / folder).mkdir()
    (root / "manifest.json").write_text(
        json.dumps({"symbols": {"TEST": {"symbol": "TESTUSDT", "cg_id": "test"}}}),
        encoding="utf-8",
    )
    date = pd.Timestamp("2026-01-01", tz="UTC")
    pd.DataFrame({"oi_usd": [100.0]}, index=[date]).to_parquet(root / "oi_1d" / "TEST.parquet")
    pd.DataFrame({"mcap_usd": [1000.0]}, index=[date]).to_parquet(root / "mcap_1d" / "TEST.parquet")
    panel, _ = validation.load_long_oi_panel(root)
    assert panel.iloc[0]["date"] == date + pd.Timedelta(days=1)


def test_snapshot_write_is_idempotent_but_never_overwrites(tmp_path: Path):
    path = tmp_path / "snapshot.json"
    assert monitor.immutable_write_json(path, {"a": 1}) == "created"
    assert monitor.immutable_write_json(path, {"a": 1}) == "idempotent_existing"
    with pytest.raises(RuntimeError):
        monitor.immutable_write_json(path, {"a": 2})


def test_fail_closed_quality_thresholds():
    rows = pd.DataFrame([{factor: 1.0 for factor in validation.PRICE_FACTORS}] * 89)
    audit = monitor.assess_quality(
        rows=rows,
        universe_count=100,
        previous_count=100,
        feature_date=pd.Timestamp("2026-01-01", tz="UTC"),
        latest_bar=pd.Timestamp("2026-01-01 20:00", tz="UTC"),
        source_errors=[],
    )
    assert not audit["passed"]
    assert "valid_price_features_below_95pct" in audit["failure_reasons"]


def test_live_monitor_feature_formula_matches_frozen_training_panel():
    times = pd.date_range("2026-01-01", periods=12, freq="4h", tz="UTC")
    panel = pd.DataFrame(
        {
            "symbol": "TESTUSDT", "base_asset": "TEST", "open_time": times,
            "open": 10.0, "high": 12.0, "low": 8.0, "close": [10.0] * 6 + [11.0] * 6,
            "quote_volume": 1_000_000.0, "trades": 100, "taker_buy_quote": 500_000.0,
        }
    )
    features = monitor.engineer_daily_features(panel).sort_values("date")
    assert features.iloc[-1]["intraday_range_1d"] == pytest.approx(12 / 8 - 1)
    assert features.iloc[-1]["return_1d"] == pytest.approx(0.10)


@pytest.mark.integration
def test_ake_btw_and_17_plus_2_baseline_event_regression():
    baseline = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
    path = baseline / "futures_4h_panel.csv.gz"
    if not path.exists():
        pytest.skip("Frozen baseline is not available")
    panel = pd.read_csv(path, compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    events = validation.scan_30d_runups(panel)
    assert int((events["classification"] == "close-confirmed").sum()) == 17
    assert int((events["classification"] == "wick-only").sum()) == 2
    assert {"AKEUSDT", "BTWUSDT"}.issubset(set(events["symbol"]))
