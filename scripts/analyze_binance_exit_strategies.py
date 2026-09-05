"""Research fixed, staged, and trailing exits for the frozen Binance run-up entry signal.

This script deliberately does not refit the price model or change the top-2%
entry rule.  Exit policies are selected on matured trades before fold 3 and
then frozen for folds 3-6.  The output is historical validation evidence only;
there is no order-routing dependency in this module.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
VALIDATION_PATH = ROOT / "core" / "research" / "binance_runup_validation.py"
SPEC = importlib.util.spec_from_file_location("binance_exit_validation_standalone", VALIDATION_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load {VALIDATION_PATH}")
VALIDATION = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = VALIDATION
SPEC.loader.exec_module(VALIDATION)

DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"

BLUE = "#3B6FB6"
GOLD = "#C7922B"
ORANGE = "#D97732"
INK = "#20262E"
GREY = "#AAB2BD"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean_trade(trade: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in trade.items() if key != "mark_path"}


def attach_signal_fields(outcome: dict[str, Any], signal: pd.Series, policy_name: str, slippage: float) -> None:
    outcome.update(
        {
            "symbol": str(signal["symbol"]),
            "signal_date": pd.Timestamp(signal["date"]),
            "fold": int(signal["fold"]),
            "score": float(signal["price_model_score"]),
            "score_pctile": float(signal["price_model_pctile"]),
            "breadth_regime": signal.get("breadth_regime"),
            "btc_trend_regime": signal.get("btc_trend_regime"),
            "btc_vol_regime": signal.get("btc_vol_regime"),
            "market_state": signal.get("market_state"),
            "policy": policy_name,
            "slippage_bps_each_side": float(slippage),
            "signal_key": int(signal["signal_key"]),
        }
    )


def build_signal_path_cache(
    signals: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
    funding_history: Mapping[str, pd.Series],
) -> dict[int, dict[str, Any]]:
    cache: dict[int, dict[str, Any]] = {}
    symbol_arrays: dict[str, dict[str, Any]] = {}
    for symbol, bars in bars_by_symbol.items():
        times = (
            pd.to_datetime(bars["open_time"], utc=True)
            .dt.as_unit("ns")
            .astype("int64")
            .to_numpy()
        )
        funding = funding_history.get(symbol)
        if funding is None or funding.empty:
            funding_times = np.asarray([], dtype=np.int64)
            funding_cumulative = np.asarray([0.0], dtype=float)
            funding_observed = False
        else:
            ordered = funding.sort_index()
            funding_times = pd.to_datetime(ordered.index, utc=True).as_unit("ns").asi8
            rates = pd.to_numeric(ordered, errors="coerce").fillna(0.0).to_numpy(float)
            funding_cumulative = np.concatenate([[0.0], np.cumsum(rates)])
            funding_observed = True
        symbol_arrays[symbol] = {
            "times": times,
            "open": bars["open"].to_numpy(float),
            "high": bars["high"].to_numpy(float),
            "low": bars["low"].to_numpy(float),
            "close": bars["close"].to_numpy(float),
            "funding_times": funding_times,
            "funding_cumulative": funding_cumulative,
            "funding_observed": funding_observed,
        }
    four_hours_ns = int(pd.Timedelta(hours=4).value)
    thirty_days_ns = int(pd.Timedelta(days=30).value)
    for _, signal in signals.iterrows():
        key = int(signal["signal_key"])
        symbol = str(signal["symbol"])
        arrays = symbol_arrays.get(symbol)
        if arrays is None:
            continue
        entry_ns = int(pd.Timestamp(signal["entry_time"]).value)
        start = int(np.searchsorted(arrays["times"], entry_ns, side="left"))
        if start >= len(arrays["times"]) or arrays["times"][start] > entry_ns + four_hours_ns:
            continue
        end = int(np.searchsorted(arrays["times"], entry_ns + thirty_days_ns, side="right"))
        cache[key] = {
            name: value[start:end] if isinstance(value, np.ndarray) and name in {
                "times", "open", "high", "low", "close"
            } else value
            for name, value in arrays.items()
        }
        cache[key]["entry_ns"] = entry_ns
    return cache


def simulate_cached_path(
    cached: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
    slippage_bps: float,
    fee_bps: float = 5.0,
    include_marks: bool = False,
) -> dict[str, Any] | None:
    times = cached["times"]
    if len(times) == 0:
        return None
    horizon_ns = int(cached["entry_ns"] + pd.Timedelta(days=int(policy["time_days"])).value)
    length = int(np.searchsorted(times, horizon_ns, side="right"))
    if length <= 0:
        return None
    opens = cached["open"][:length]
    highs = cached["high"][:length]
    lows = cached["low"][:length]
    closes = cached["close"][:length]
    times = times[:length]
    slip = float(slippage_bps) / 10_000.0
    fee = float(fee_bps) / 10_000.0
    entry_price = float(opens[0]) * (1.0 + slip)
    hard_stop_price = (
        None if policy.get("hard_stop") is None
        else entry_price * (1.0 - float(policy["hard_stop"]))
    )
    remaining = 1.0
    cumulative_return = -fee
    exit_fee_return = 0.0
    funding_return = 0.0
    peak_price = entry_price
    trail_active = False
    partial_done = False
    full_target_done = False
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason = "time_stop"
    mfe = -np.inf
    mae = np.inf
    mark_path: list[dict[str, Any]] = []
    funding_times = cached["funding_times"]
    funding_cumulative = cached["funding_cumulative"]
    previous_time = int(times[0] - 1)

    for index in range(length):
        bar_time_ns = int(times[index])
        bar_open = float(opens[index])
        bar_high = float(highs[index])
        bar_low = float(lows[index])
        bar_close = float(closes[index])
        mfe = max(mfe, bar_high / entry_price - 1.0)
        mae = min(mae, bar_low / entry_price - 1.0)
        if len(funding_times):
            left = int(np.searchsorted(funding_times, previous_time, side="right"))
            right = int(np.searchsorted(funding_times, bar_time_ns, side="right"))
            if right > left:
                funding_return -= float(funding_cumulative[right] - funding_cumulative[left]) * remaining

        stop_fill: float | None = None
        stop_reason: str | None = None
        if hard_stop_price is not None and bar_low <= hard_stop_price:
            raw_fill = bar_open if bar_open < hard_stop_price else hard_stop_price
            stop_fill = raw_fill * (1.0 - slip)
            stop_reason = "hard_stop"
        elif trail_active and policy.get("trail_distance") is not None:
            trailing_price = peak_price * (1.0 - float(policy["trail_distance"]))
            if bar_low <= trailing_price:
                raw_fill = bar_open if bar_open < trailing_price else trailing_price
                stop_fill = raw_fill * (1.0 - slip)
                stop_reason = "trailing_stop"

        if stop_fill is not None:
            cumulative_return += remaining * (stop_fill / entry_price - 1.0)
            exit_fee_return += remaining * fee
            remaining = 0.0
            exit_time = pd.Timestamp(bar_time_ns, tz="UTC")
            exit_price = stop_fill
            exit_reason = str(stop_reason)
        else:
            partial_target = policy.get("partial_target")
            if (
                partial_target is not None
                and not partial_done
                and bar_high >= entry_price * (1.0 + float(partial_target))
            ):
                target_fill = entry_price * (1.0 + float(partial_target)) * (1.0 - slip)
                fraction = min(float(policy.get("partial_fraction", 0.5)), remaining)
                cumulative_return += fraction * (target_fill / entry_price - 1.0)
                exit_fee_return += fraction * fee
                remaining -= fraction
                partial_done = True
            full_target = policy.get("full_take_profit")
            if (
                remaining > 0
                and full_target is not None
                and bar_high >= entry_price * (1.0 + float(full_target))
            ):
                target_fill = entry_price * (1.0 + float(full_target)) * (1.0 - slip)
                cumulative_return += remaining * (target_fill / entry_price - 1.0)
                exit_fee_return += remaining * fee
                remaining = 0.0
                full_target_done = True
                exit_time = pd.Timestamp(bar_time_ns, tz="UTC")
                exit_price = target_fill
                exit_reason = "take_profit"
            peak_price = max(peak_price, bar_high)
            activation = policy.get("trail_activation")
            if activation is not None and peak_price >= entry_price * (1.0 + float(activation)):
                trail_active = True

        if include_marks:
            marked = cumulative_return + funding_return - exit_fee_return
            if remaining > 0:
                marked += remaining * (bar_close / entry_price - 1.0)
            mark_path.append(
                {
                    "time": pd.Timestamp(bar_time_ns, tz="UTC"),
                    "mark_return": float(marked),
                    "remaining_fraction": float(remaining),
                }
            )
        previous_time = bar_time_ns
        if remaining <= 0:
            break

    if remaining > 0:
        final_index = length - 1
        exit_time = pd.Timestamp(int(times[final_index]), tz="UTC") + pd.Timedelta(hours=4)
        exit_price = float(closes[final_index]) * (1.0 - slip)
        cumulative_return += remaining * (exit_price / entry_price - 1.0)
        exit_fee_return += remaining * fee
        remaining = 0.0
        if include_marks:
            mark_path.append(
                {
                    "time": exit_time,
                    "mark_return": float(cumulative_return + funding_return - exit_fee_return),
                    "remaining_fraction": 0.0,
                }
            )
    return {
        "entry_time": pd.Timestamp(int(times[0]), tz="UTC"),
        "entry_price": entry_price,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "net_return": float(cumulative_return + funding_return - exit_fee_return),
        "mfe": float(mfe),
        "mae": float(mae),
        "funding_return": float(funding_return),
        "fee_return": float(fee + exit_fee_return),
        "partial_take_profit": bool(partial_done),
        "partial_fraction": float(policy.get("partial_fraction", 0.5)) if partial_done else 0.0,
        "full_take_profit": bool(full_target_done),
        "policy_family": str(policy.get("family") or "custom"),
        "funding_observed": bool(cached["funding_observed"]),
        "mark_path": mark_path,
    }


def simulate_policy(
    signals: pd.DataFrame,
    path_cache: Mapping[int, Mapping[str, Any]],
    *,
    policy_name: str,
    policy: Mapping[str, Any],
    slippage_bps: float,
    include_marks: bool = False,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    for _, signal in signals.iterrows():
        cached = path_cache.get(int(signal["signal_key"]))
        if cached is None:
            continue
        outcome = simulate_cached_path(
            cached,
            policy=policy,
            slippage_bps=slippage_bps,
            include_marks=include_marks,
        )
        if outcome is None:
            continue
        attach_signal_fields(outcome, signal, policy_name, slippage_bps)
        outcomes.append(outcome)
    return outcomes


def policy_metadata(policy_name: str, policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy": policy_name,
        "family": policy.get("family"),
        "hard_stop_pct": None if policy.get("hard_stop") is None else 100 * float(policy["hard_stop"]),
        "time_days": int(policy["time_days"]),
        "fixed_take_profit_pct": (
            None if policy.get("full_take_profit") is None else 100 * float(policy["full_take_profit"])
        ),
        "partial_take_profit_pct": (
            None if policy.get("partial_target") is None else 100 * float(policy["partial_target"])
        ),
        "partial_fraction": float(policy.get("partial_fraction", 0.5)),
        "trail_activation_pct": (
            None if policy.get("trail_activation") is None else 100 * float(policy["trail_activation"])
        ),
        "trail_distance_pct": (
            None if policy.get("trail_distance") is None else 100 * float(policy["trail_distance"])
        ),
    }


def apply_portfolio_constraints_fast(
    outcomes: Sequence[dict[str, Any]],
    *,
    initial_equity: float,
    risk_stop_pct: float = 0.20,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    """Apply the frozen portfolio rules and vectorise daily mark-to-market.

    Position acceptance and sizing are line-for-line equivalent to the frozen
    implementation.  Only daily mark lookup is vectorised with ``searchsorted``
    so a large exit-policy grid does not repeatedly scan Python mark lists.
    """
    ordered = sorted(outcomes, key=lambda row: (pd.Timestamp(row["entry_time"]), -float(row["score"])))
    active: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    last_exit: dict[str, pd.Timestamp] = {}
    realized_equity = float(initial_equity)
    for raw in ordered:
        entry_time = pd.Timestamp(raw["entry_time"])
        still_active: list[dict[str, Any]] = []
        for trade in active:
            if pd.Timestamp(trade["exit_time"]) <= entry_time:
                realized_equity += float(trade["pnl_usd"])
                last_exit[str(trade["symbol"])] = pd.Timestamp(trade["exit_time"])
            else:
                still_active.append(trade)
        active = still_active
        symbol = str(raw["symbol"])
        if symbol in {str(item["symbol"]) for item in active}:
            continue
        if symbol in last_exit and entry_time < last_exit[symbol] + pd.Timedelta(days=14):
            continue
        if len(active) >= 10:
            continue
        gross_active = sum(float(item["notional_usd"]) for item in active)
        if risk_stop_pct <= 0:
            raise ValueError("risk_stop_pct must be positive")
        risk_notional = realized_equity * 0.005 / float(risk_stop_pct)
        available = max(0.0, realized_equity * 0.50 - gross_active)
        notional = min(risk_notional, available)
        if notional <= 1.0:
            continue
        trade = dict(raw)
        trade["notional_usd"] = float(notional)
        trade["pnl_usd"] = float(notional * float(trade["net_return"]))
        active.append(trade)
        accepted.append(trade)

    if not accepted:
        return [], pd.DataFrame()
    start = min(pd.Timestamp(item["entry_time"]).floor("D") for item in accepted)
    end = max(pd.Timestamp(item["exit_time"]).ceil("D") for item in accepted)
    dates = pd.date_range(start, end, freq="1D", tz="UTC")
    day_ns = dates.as_unit("ns").asi8
    next_day_ns = day_ns + int(pd.Timedelta(days=1).value)
    values = np.full(len(dates), float(initial_equity), dtype=float)
    for trade in accepted:
        entry_floor = int(pd.Timestamp(trade["entry_time"]).floor("D").value)
        exit_ceil = int(pd.Timestamp(trade["exit_time"]).ceil("D").value)
        after_exit = day_ns >= exit_ceil
        values[after_exit] += float(trade["pnl_usd"])
        active_mask = (day_ns >= entry_floor) & (day_ns < exit_ceil)
        if not active_mask.any():
            continue
        marks = trade.get("mark_path") or []
        if not marks:
            continue
        mark_times = np.asarray([pd.Timestamp(mark["time"]).value for mark in marks], dtype=np.int64)
        mark_returns = np.asarray([float(mark["mark_return"]) for mark in marks], dtype=float)
        positions = np.searchsorted(mark_times, next_day_ns[active_mask], side="right") - 1
        valid = positions >= 0
        active_indices = np.flatnonzero(active_mask)
        values[active_indices[valid]] += float(trade["notional_usd"]) * mark_returns[positions[valid]]
    curve = pd.DataFrame({"date": dates, "equity": values})
    curve["peak_equity"] = curve["equity"].cummax()
    curve["drawdown"] = curve["equity"] / curve["peak_equity"] - 1.0
    return accepted, curve


def summarize_portfolio(
    outcomes: Sequence[dict[str, Any]],
    *,
    initial_equity: float = 100_000.0,
    risk_stop_pct: float = 0.20,
) -> tuple[list[dict[str, Any]], pd.DataFrame, dict[str, Any]]:
    accepted, equity = apply_portfolio_constraints_fast(
        outcomes,
        initial_equity=initial_equity,
        risk_stop_pct=risk_stop_pct,
    )
    if not accepted:
        return [], equity, {}
    frame = pd.DataFrame([clean_trade(row) for row in accepted])
    summary = VALIDATION.summarize_trade_frame(frame, equity, initial_equity=initial_equity)
    summary["take_profit_exit_rate"] = float((frame["exit_reason"] == "take_profit").mean())
    summary["trailing_exit_rate"] = float((frame["exit_reason"] == "trailing_stop").mean())
    summary["hard_stop_exit_rate"] = float((frame["exit_reason"] == "hard_stop").mean())
    summary["median_holding_days"] = float(
        (pd.to_datetime(frame["exit_time"], utc=True) - pd.to_datetime(frame["entry_time"], utc=True))
        .dt.total_seconds()
        .div(86_400)
        .median()
    )
    return accepted, equity, summary


def calibration_rank(
    trades: pd.DataFrame,
    catalog: Mapping[str, Mapping[str, Any]],
    *,
    cutoff: pd.Timestamp,
    minimum_trades: int = 40,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    default = trades[trades["slippage_bps_each_side"] == 5.0].copy()
    default["signal_date"] = pd.to_datetime(default["signal_date"], utc=True)
    default["exit_time"] = pd.to_datetime(default["exit_time"], utc=True)
    default = default[(default["signal_date"] < cutoff) & (default["exit_time"] < cutoff)]
    for policy_name, group in default.groupby("policy"):
        returns = pd.to_numeric(group["net_return"], errors="coerce").dropna()
        if returns.empty:
            continue
        gains = float(returns[returns > 0].sum())
        losses = float(-returns[returns < 0].sum())
        profit_factor = gains / losses if losses > 0 else np.inf
        fold_expectancy = group.groupby("fold")["net_return"].mean().astype(float)
        selection_score = float(fold_expectancy.mean() - 0.5 * fold_expectancy.std(ddof=0))
        leave_largest = float(returns.drop(returns.idxmax()).mean()) if len(returns) > 1 else np.nan
        positive = pd.to_numeric(group.loc[group["pnl_usd"] > 0, "pnl_usd"], errors="coerce")
        concentration = float(positive.max() / positive.sum()) if positive.sum() > 0 else 1.0
        eligible = bool(
            len(returns) >= minimum_trades
            and fold_expectancy.size >= 2
            and returns.mean() > 0
            and profit_factor > 1.0
            and leave_largest > 0
            and concentration <= 0.25
        )
        row = policy_metadata(policy_name, catalog[policy_name])
        row.update(
            {
                "matured_trades": int(len(returns)),
                "matured_folds": int(fold_expectancy.size),
                "expectancy": float(returns.mean()),
                "median_return": float(returns.median()),
                "profit_factor": float(profit_factor),
                "fold_expectancy_mean": float(fold_expectancy.mean()),
                "fold_expectancy_std": float(fold_expectancy.std(ddof=0)),
                "selection_score": selection_score,
                "leave_largest_winner_out_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_eligible": eligible,
                "information_cutoff_utc": cutoff,
            }
        )
        rows.append(row)
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(
        ["selection_eligible", "selection_score", "profit_factor"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def bootstrap_returns(
    trades: pd.DataFrame,
    *,
    samples: int,
    cluster: str,
    seed: int,
) -> dict[str, Any]:
    data = trades.copy()
    if cluster == "week":
        data["cluster"] = pd.to_datetime(data["signal_date"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        data["cluster"] = data["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    grouped_returns = [
        pd.to_numeric(group["net_return"], errors="coerce").dropna().to_numpy(float)
        for _, group in data.groupby("cluster", sort=False)
    ]
    grouped_returns = [values for values in grouped_returns if len(values)]
    rng = np.random.default_rng(seed)
    expectancy: list[float] = []
    profit_factor: list[float] = []
    for _ in range(samples):
        sampled_indices = rng.integers(0, len(grouped_returns), size=len(grouped_returns))
        returns = np.concatenate([grouped_returns[index] for index in sampled_indices])
        if not len(returns):
            continue
        gains = float(returns[returns > 0].sum())
        losses = float(-returns[returns < 0].sum())
        expectancy.append(float(np.mean(returns)))
        profit_factor.append(gains / losses if losses > 0 else np.inf)
    exp = pd.Series(expectancy, dtype=float)
    pf = pd.Series(profit_factor, dtype=float).replace([np.inf, -np.inf], np.nan).dropna()
    return {
        "cluster": cluster,
        "samples": int(min(len(exp), len(pf))),
        "expectancy_median": float(exp.median()),
        "expectancy_lower_95pct": float(exp.quantile(0.025)),
        "expectancy_upper_95pct": float(exp.quantile(0.975)),
        "profit_factor_median": float(pf.median()),
        "profit_factor_lower_95pct": float(pf.quantile(0.025)),
        "profit_factor_upper_95pct": float(pf.quantile(0.975)),
    }


def plot_exit_results(
    report: Path,
    confirmation_sensitivity: pd.DataFrame,
    confirmation_summary: pd.DataFrame,
) -> list[str]:
    charts = report / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    generated: list[str] = []

    fixed = confirmation_sensitivity[
        confirmation_sensitivity["family"] == "fixed_take_profit"
    ].copy()
    if not fixed.empty:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
        for days, group in fixed.groupby("time_days"):
            group = group.sort_values("fixed_take_profit_pct")
            axes[0].plot(
                group["fixed_take_profit_pct"], 100 * group["expectancy"],
                marker="o", label=f"Max {int(days)}d",
            )
            axes[1].plot(
                group["fixed_take_profit_pct"], group["profit_factor"],
                marker="o", label=f"Max {int(days)}d",
            )
        axes[0].axhline(0, color=INK, linewidth=1)
        axes[1].axhline(1, color=INK, linewidth=1)
        axes[0].set(title="Fixed take-profit: confirmation expectancy", xlabel="Take-profit threshold (%)", ylabel="Expectancy (%)")
        axes[1].set(title="Fixed take-profit: confirmation profit factor", xlabel="Take-profit threshold (%)", ylabel="Profit factor")
        for axis in axes:
            axis.grid(axis="y", color="#E5E7EB")
            axis.legend(frameon=False)
            axis.spines[["top", "right"]].set_visible(False)
        fig.suptitle("Fixed -20% stop; confirmation on walk-forward folds 3–6")
        fig.tight_layout()
        path = charts / "exit_fixed_tp_confirmation.png"
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        generated.append(str(path.relative_to(report)))

    if not confirmation_summary.empty:
        costs = confirmation_summary.sort_values("slippage_bps_each_side")
        fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8))
        axes[0].bar(costs["slippage_bps_each_side"].astype(str), 100 * costs["expectancy"], color=BLUE)
        axes[1].bar(costs["slippage_bps_each_side"].astype(str), costs["profit_factor"], color=GOLD)
        axes[0].axhline(0, color=INK, linewidth=1)
        axes[1].axhline(1, color=INK, linewidth=1)
        axes[0].set(title="Confirmation expectancy after costs", xlabel="Slippage each side (bps)", ylabel="Expectancy (%)")
        axes[1].set(title="Confirmation profit factor", xlabel="Slippage each side (bps)", ylabel="Profit factor")
        for axis in axes:
            axis.grid(axis="y", color="#E5E7EB")
            axis.spines[["top", "right"]].set_visible(False)
        fig.suptitle("Calibration-selected frozen exit; fee is 5 bps each side")
        fig.tight_layout()
        path = charts / "exit_cost_stress_confirmation.png"
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        generated.append(str(path.relative_to(report)))
    return generated


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    report.mkdir(parents=True, exist_ok=True)

    scored_path = report / "historical_walk_forward_scored.csv.gz"
    panel_path = baseline / "futures_4h_panel.csv.gz"
    folds_path = report / "walk_forward_fold_metrics.csv"
    required = [scored_path, panel_path, folds_path]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing exit-study inputs: {missing}")

    print("Loading frozen entry scores and 4h paths", flush=True)
    scored = pd.read_csv(scored_path)
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    panel = pd.read_csv(panel_path)
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    panel = panel.sort_values(["symbol", "open_time"])
    folds = pd.read_csv(folds_path)
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    confirmation_start = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])

    signals = VALIDATION.build_first_crossing_signals(scored)
    signals = signals.reset_index(drop=True)
    signals["signal_key"] = np.arange(len(signals), dtype=int)
    bars_by_symbol = {
        symbol: group.sort_values("open_time")
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding_history = VALIDATION.load_funding_history(AMBUSH_ROOT)
    path_cache = build_signal_path_cache(signals, bars_by_symbol, funding_history)
    catalog = VALIDATION.build_exit_policy_catalog()
    catalog_frame = pd.DataFrame(
        [policy_metadata(name, policy) for name, policy in catalog.items()]
    ).sort_values(["family", "time_days", "fixed_take_profit_pct", "policy"])

    grid_summaries: list[dict[str, Any]] = []
    accepted_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    confirmation_summaries: list[dict[str, Any]] = []
    confirmation_trade_records: list[dict[str, Any]] = []
    confirmation_equity_records: list[dict[str, Any]] = []

    # The full policy grid is compared at the single pre-registered default
    # cost.  Stress costs are applied only after the policy has been selected,
    # which avoids spending compute on information that is not allowed to
    # influence policy selection.
    total = len(catalog)
    completed = 0
    for policy_name, policy in catalog.items():
        metadata = policy_metadata(policy_name, policy)
        for slippage in (5.0,):
            outcomes = simulate_policy(
                signals,
                path_cache,
                policy_name=policy_name,
                policy=policy,
                slippage_bps=slippage,
            )
            accepted, equity, summary = summarize_portfolio(outcomes)
            if summary:
                summary.update(metadata)
                summary["slippage_bps_each_side"] = slippage
                summary["sample_scope"] = "all_historical_folds"
                grid_summaries.append(summary)
                for trade in accepted:
                    row = clean_trade(trade)
                    row.update(metadata)
                    row["sample_scope"] = "all_historical_folds"
                    accepted_records.append(row)
                if not equity.empty:
                    curve = equity.copy()
                    curve["policy"] = policy_name
                    curve["family"] = metadata["family"]
                    curve["slippage_bps_each_side"] = slippage
                    curve["sample_scope"] = "all_historical_folds"
                    equity_records.extend(curve.to_dict(orient="records"))

            if slippage == 5.0:
                confirmation_outcomes = [
                    row for row in outcomes if pd.Timestamp(row["signal_date"]) >= confirmation_start
                ]
                conf_accepted, conf_equity, conf_summary = summarize_portfolio(confirmation_outcomes)
                if conf_summary:
                    conf_summary.update(metadata)
                    conf_summary["slippage_bps_each_side"] = slippage
                    conf_summary["sample_scope"] = "folds_3_to_6_confirmation_sensitivity"
                    confirmation_summaries.append(conf_summary)
                    for trade in conf_accepted:
                        row = clean_trade(trade)
                        row.update(metadata)
                        row["sample_scope"] = "folds_3_to_6_confirmation_sensitivity"
                        confirmation_trade_records.append(row)
                    if not conf_equity.empty:
                        curve = conf_equity.copy()
                        curve["policy"] = policy_name
                        curve["family"] = metadata["family"]
                        curve["slippage_bps_each_side"] = slippage
                        curve["sample_scope"] = "folds_3_to_6_confirmation_sensitivity"
                        confirmation_equity_records.extend(curve.to_dict(orient="records"))
            completed += 1
            if completed % 10 == 0 or completed == total:
                print(f"Exit simulations {completed}/{total}", flush=True)

    grid = pd.DataFrame(grid_summaries)
    accepted_frame = pd.DataFrame(accepted_records)
    confirmation_sensitivity = pd.DataFrame(confirmation_summaries)
    confirmation_trade_frame = pd.DataFrame(confirmation_trade_records)

    ranking = calibration_rank(
        accepted_frame,
        catalog,
        cutoff=confirmation_start,
        minimum_trades=40,
    )
    if ranking.empty:
        raise RuntimeError("No matured calibration trades were available")
    eligible = ranking[ranking["selection_eligible"]]
    selected_row = (eligible.iloc[0] if not eligible.empty else ranking.iloc[0])
    selected_policy = str(selected_row["policy"])
    print(f"Calibration-selected policy: {selected_policy}", flush=True)

    fixed_confirmation_summaries: list[dict[str, Any]] = []
    fixed_confirmation_trades: list[dict[str, Any]] = []
    fixed_confirmation_equity: list[dict[str, Any]] = []
    confirmation_signals = signals[signals["date"] >= confirmation_start].copy()
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate_policy(
            confirmation_signals,
            path_cache,
            policy_name=selected_policy,
            policy=catalog[selected_policy],
            slippage_bps=slippage,
            include_marks=True,
        )
        accepted, equity, summary = summarize_portfolio(outcomes)
        if not summary:
            continue
        summary.update(policy_metadata(selected_policy, catalog[selected_policy]))
        summary["slippage_bps_each_side"] = slippage
        summary["sample_scope"] = "frozen_policy_folds_3_to_6"
        fixed_confirmation_summaries.append(summary)
        for trade in accepted:
            row = clean_trade(trade)
            row.update(policy_metadata(selected_policy, catalog[selected_policy]))
            row["sample_scope"] = "frozen_policy_folds_3_to_6"
            fixed_confirmation_trades.append(row)
        if not equity.empty:
            curve = equity.copy()
            curve["policy"] = selected_policy
            curve["family"] = catalog[selected_policy]["family"]
            curve["slippage_bps_each_side"] = slippage
            curve["sample_scope"] = "frozen_policy_folds_3_to_6"
            fixed_confirmation_equity.extend(curve.to_dict(orient="records"))
    fixed_summary = pd.DataFrame(fixed_confirmation_summaries)
    fixed_trades = pd.DataFrame(fixed_confirmation_trades)

    choices: list[dict[str, Any]] = []
    adaptive_raw: list[dict[str, Any]] = []
    for fold in sorted(int(value) for value in folds["fold"].unique() if int(value) >= 2):
        cutoff = pd.Timestamp(folds.loc[folds["fold"] == fold, "test_start"].iloc[0])
        fold_rank = calibration_rank(accepted_frame, catalog, cutoff=cutoff, minimum_trades=30)
        fold_eligible = fold_rank[fold_rank["selection_eligible"]]
        if fold_rank.empty:
            continue
        choice = fold_eligible.iloc[0] if not fold_eligible.empty else fold_rank.iloc[0]
        policy_name = str(choice["policy"])
        choices.append(
            {
                "fold": fold,
                "test_start": cutoff,
                "selected_policy": policy_name,
                "selection_eligible": bool(choice["selection_eligible"]),
                "matured_trades": int(choice["matured_trades"]),
                "selection_score": float(choice["selection_score"]),
                "calibration_expectancy": float(choice["expectancy"]),
                "calibration_profit_factor": float(choice["profit_factor"]),
            }
        )
        fold_signals = signals[signals["fold"] == fold]
        adaptive_raw.extend(
            simulate_policy(
                fold_signals,
                path_cache,
                policy_name=policy_name,
                policy=catalog[policy_name],
                slippage_bps=5.0,
                include_marks=True,
            )
        )
    adaptive_accepted, adaptive_equity, adaptive_summary = summarize_portfolio(adaptive_raw)
    adaptive_trades = pd.DataFrame([clean_trade(row) for row in adaptive_accepted])

    adaptive_summaries: list[dict[str, Any]] = []
    adaptive_trade_records: list[dict[str, Any]] = []
    adaptive_equity_records: list[dict[str, Any]] = []
    choice_frame = pd.DataFrame(choices)
    for slippage in (5.0, 15.0, 30.0):
        if slippage == 5.0:
            cost_raw = adaptive_raw
        else:
            cost_raw = []
            for _, choice in choice_frame.iterrows():
                fold = int(choice["fold"])
                policy_name = str(choice["selected_policy"])
                cost_raw.extend(
                    simulate_policy(
                        signals[signals["fold"] == fold],
                        path_cache,
                        policy_name=policy_name,
                        policy=catalog[policy_name],
                        slippage_bps=slippage,
                        include_marks=True,
                    )
                )
        cost_accepted, cost_equity, cost_summary = summarize_portfolio(cost_raw)
        if not cost_summary:
            continue
        cost_summary["slippage_bps_each_side"] = slippage
        cost_summary["sample_scope"] = "time_forward_exit_selection_folds_2_to_6"
        adaptive_summaries.append(cost_summary)
        for trade in cost_accepted:
            row = clean_trade(trade)
            row["sample_scope"] = "time_forward_exit_selection_folds_2_to_6"
            adaptive_trade_records.append(row)
        if not cost_equity.empty:
            curve = cost_equity.copy()
            curve["slippage_bps_each_side"] = slippage
            curve["sample_scope"] = "time_forward_exit_selection_folds_2_to_6"
            adaptive_equity_records.extend(curve.to_dict(orient="records"))
    adaptive_summary_frame = pd.DataFrame(adaptive_summaries)
    adaptive_trade_frame = pd.DataFrame(adaptive_trade_records)

    default_fixed = fixed_trades[fixed_trades["slippage_bps_each_side"] == 5.0].copy()
    if default_fixed.empty:
        raise RuntimeError("Selected policy has no confirmation trades")
    week_bootstrap = bootstrap_returns(
        default_fixed,
        samples=args.bootstrap_samples,
        cluster="week",
        seed=20260719,
    )
    symbol_bootstrap = bootstrap_returns(
        default_fixed,
        samples=args.bootstrap_samples,
        cluster="symbol",
        seed=20260720,
    )
    adaptive_default_trades = adaptive_trade_frame[
        adaptive_trade_frame["slippage_bps_each_side"] == 5.0
    ].copy()
    adaptive_week_bootstrap = bootstrap_returns(
        adaptive_default_trades,
        samples=args.bootstrap_samples,
        cluster="week",
        seed=20260721,
    )
    adaptive_symbol_bootstrap = bootstrap_returns(
        adaptive_default_trades,
        samples=args.bootstrap_samples,
        cluster="symbol",
        seed=20260722,
    )
    default_summary = fixed_summary[fixed_summary["slippage_bps_each_side"] == 5.0].iloc[0]
    stress = fixed_summary.set_index("slippage_bps_each_side")
    stress_ok = bool(
        all(
            value in stress.index
            and float(stress.loc[value, "expectancy"]) > 0
            and float(stress.loc[value, "profit_factor"]) > 1.0
            and float(stress.loc[value, "max_drawdown"]) >= -0.30
            for value in (15.0, 30.0)
        )
    )
    point_gate = bool(default_summary["gate_pass"] and stress_ok)
    uncertainty_gate = bool(
        week_bootstrap["expectancy_lower_95pct"] > 0
        and symbol_bootstrap["expectancy_lower_95pct"] > 0
    )
    if point_gate and uncertainty_gate:
        classification = "exit_strategy_candidate"
    elif point_gate:
        classification = "promising_but_unconfirmed"
    else:
        classification = "watchlist_only"

    adaptive_stress = adaptive_summary_frame.set_index("slippage_bps_each_side")
    adaptive_stress_ok = bool(
        all(
            value in adaptive_stress.index
            and float(adaptive_stress.loc[value, "expectancy"]) > 0
            and float(adaptive_stress.loc[value, "profit_factor"]) > 1.0
            and float(adaptive_stress.loc[value, "max_drawdown"]) >= -0.30
            for value in (15.0, 30.0)
        )
    )
    adaptive_point_gate = bool(adaptive_summary.get("gate_pass", False) and adaptive_stress_ok)
    adaptive_uncertainty_gate = bool(
        adaptive_week_bootstrap["expectancy_lower_95pct"] > 0
        and adaptive_symbol_bootstrap["expectancy_lower_95pct"] > 0
    )
    if adaptive_point_gate and adaptive_uncertainty_gate:
        adaptive_classification = "adaptive_exit_strategy_candidate"
    elif adaptive_point_gate:
        adaptive_classification = "adaptive_promising_but_unconfirmed"
    else:
        adaptive_classification = "adaptive_watchlist_only"

    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "entry_model_changed": False,
        "selection_information_cutoff_utc": confirmation_start,
        "calibration_scope": "matured accepted trades strictly exited before fold 3",
        "confirmation_scope": "walk-forward folds 3-6; selected exit policy frozen",
        "catalog_policies": int(len(catalog)),
        "selected_policy": selected_policy,
        "selected_policy_family": catalog[selected_policy]["family"],
        "selection_was_eligible": bool(selected_row["selection_eligible"]),
        "default_cost_confirmation": {
            key: default_summary[key]
            for key in [
                "trades", "expectancy", "median_return", "win_rate", "profit_factor",
                "cvar_5pct", "mean_mfe", "mean_mae", "total_return_on_initial_equity",
                "max_drawdown", "funding_coverage", "fold_majority_profit_factor_gt_1",
                "market_state_majority_profit_factor_gt_1", "largest_positive_pnl_share",
                "leave_largest_winner_out_expectancy", "median_holding_days",
                "take_profit_exit_rate", "trailing_exit_rate", "hard_stop_exit_rate",
            ]
        },
        "stress_costs": fixed_summary[
            ["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]
        ].to_dict(orient="records"),
        "week_block_bootstrap": week_bootstrap,
        "symbol_cluster_bootstrap": symbol_bootstrap,
        "point_gate_pass": point_gate,
        "uncertainty_gate_pass": uncertainty_gate,
        "final_classification": classification,
        "adaptive_time_forward_summary": adaptive_summary,
        "adaptive_stress_costs": adaptive_summary_frame[
            ["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]
        ].to_dict(orient="records"),
        "adaptive_week_block_bootstrap": adaptive_week_bootstrap,
        "adaptive_symbol_cluster_bootstrap": adaptive_symbol_bootstrap,
        "adaptive_point_gate_pass": adaptive_point_gate,
        "adaptive_uncertainty_gate_pass": adaptive_uncertainty_gate,
        "adaptive_classification": adaptive_classification,
        "catalog_hash": VALIDATION.stable_hash(catalog),
        "source_hashes": {
            scored_path.name: file_sha256(scored_path),
            panel_path.name: file_sha256(panel_path),
            folds_path.name: file_sha256(folds_path),
        },
    }

    catalog_frame.to_csv(report / "exit_policy_catalog.csv", index=False)
    grid.to_csv(report / "exit_policy_grid_summary.csv", index=False)
    accepted_frame.to_csv(report / "exit_policy_grid_trades.csv.gz", index=False, compression="gzip")
    pd.DataFrame(equity_records).to_csv(
        report / "exit_policy_grid_equity.csv.gz", index=False, compression="gzip"
    )
    ranking.to_csv(report / "exit_policy_calibration_ranking.csv", index=False)
    confirmation_sensitivity.to_csv(report / "exit_policy_confirmation_sensitivity.csv", index=False)
    confirmation_trade_frame.to_csv(
        report / "exit_policy_confirmation_sensitivity_trades.csv.gz", index=False, compression="gzip"
    )
    pd.DataFrame(confirmation_equity_records).to_csv(
        report / "exit_policy_confirmation_sensitivity_equity.csv.gz", index=False, compression="gzip"
    )
    fixed_summary.to_csv(report / "exit_policy_frozen_confirmation_summary.csv", index=False)
    fixed_trades.to_csv(report / "exit_policy_frozen_confirmation_trades.csv.gz", index=False, compression="gzip")
    pd.DataFrame(fixed_confirmation_equity).to_csv(
        report / "exit_policy_frozen_confirmation_equity.csv.gz", index=False, compression="gzip"
    )
    choice_frame.to_csv(report / "exit_policy_time_forward_choices.csv", index=False)
    adaptive_summary_frame.to_csv(report / "exit_policy_time_forward_summary.csv", index=False)
    adaptive_trade_frame.to_csv(
        report / "exit_policy_time_forward_trades.csv.gz", index=False, compression="gzip"
    )
    pd.DataFrame(adaptive_equity_records).to_csv(
        report / "exit_policy_time_forward_equity.csv.gz", index=False, compression="gzip"
    )
    (report / "exit_strategy_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    generated_charts = plot_exit_results(report, confirmation_sensitivity, fixed_summary)
    print(
        json.dumps(
            VALIDATION.json_ready(
                {
                    "decision": decision,
                    "generated_charts": generated_charts,
                    "signals": int(len(signals)),
                    "confirmation_start": confirmation_start,
                }
            ),
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
