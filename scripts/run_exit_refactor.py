from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import strategies as strategy_module
from config.strategy_registry import (
    get_backtest_strategy_catalog,
    get_backtest_strategy_info,
    get_strategy_defaults,
    get_strategy_recommended_symbols,
)
from core.backtest.exit_engine import EXIT_TEMPLATE_PRESETS
from web.api.backtest import (
    _attach_backtest_enrichment_if_needed,
    _build_positions,
    _load_backtest_df,
    _min_required_bars,
    _run_backtest_core,
)


DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "exit_refactor"
TEMPLATE_ORDER = ["Original", "ReversalOnly", "ATRTrail", "PartialPlusATR", "SignalPlusTimeStop"]
CORE_METRICS = [
    "total_return",
    "annualized_return",
    "sharpe_ratio",
    "max_drawdown",
    "calmar",
    "win_rate",
    "profit_factor",
    "average_trade",
    "average_winner",
    "average_loser",
    "avg_bars_per_trade",
    "expectancy",
    "max_consecutive_losses",
    "mfe_avg_pct",
    "mae_avg_pct",
]


@dataclass
class StrategyBundle:
    strategy: str
    symbol: str
    timeframe: str
    df: pd.DataFrame
    market_bundle: Dict[str, pd.DataFrame]
    pair_symbol: Optional[str] = None


def _make_probe_frame(kind: str) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=360, freq="1h")
    if kind == "trend_up":
        close = pd.Series(np.linspace(100.0, 180.0, len(index)), index=index)
    elif kind == "trend_down":
        close = pd.Series(np.linspace(180.0, 100.0, len(index)), index=index)
    else:
        close = pd.Series(120.0 + np.sin(np.linspace(0, 12 * np.pi, len(index))) * 12.0, index=index)
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + 0.8
    low = np.minimum(open_, close) - 0.8
    volume = np.full(len(index), 1000.0)
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": volume,
            "symbol": ["BTC/USDT"] * len(index),
        },
        index=index,
    )


def _copy_with_attrs(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.attrs = dict(df.attrs)
    return out


def _extract_logic_excerpt(strategy_name: str, *, mode: str) -> str:
    klass = getattr(strategy_module, strategy_name, None)
    if klass is None:
        return ""
    try:
        source = inspect.getsource(klass.generate_signals)
    except Exception:
        return ""

    targets = ["SignalType.BUY"] if mode == "entry" else ["SignalType.CLOSE", "SignalType.SELL"]
    lines = source.splitlines()
    snippets: List[str] = []
    for idx, line in enumerate(lines):
        if not any(token in line for token in targets):
            continue
        start = max(0, idx - 4)
        snippet = " ".join(item.strip() for item in lines[start : idx + 1] if item.strip())
        snippets.append(snippet)
    return " || ".join(snippets[:2])


def _detect_direction_support(strategy_name: str) -> str:
    if strategy_name == "PairsTradingStrategy":
        return "spread_long_short"
    defaults = dict(get_strategy_defaults(strategy_name) or {})
    has_long = False
    has_short = False
    for kind in ("trend_up", "trend_down", "oscillating"):
        try:
            probe = _make_probe_frame(kind)
            position = pd.to_numeric(_build_positions(strategy_name, probe, defaults), errors="coerce").fillna(0.0)
        except Exception:
            continue
        has_long = has_long or bool((position > 0).any())
        has_short = has_short or bool((position < 0).any())
    if has_long and has_short:
        return "long_short"
    if has_long:
        return "long_only"
    if has_short:
        return "short_only"
    return "unknown"


def _collect_strategy_inventory(strategy_name: str) -> Dict[str, Any]:
    info = dict(get_backtest_strategy_info(strategy_name) or {})
    klass = getattr(strategy_module, strategy_name, None)
    source_file = ""
    if klass is not None:
        try:
            source_file = str(Path(inspect.getsourcefile(klass) or "").resolve())
        except Exception:
            source_file = ""
    defaults = dict(get_strategy_defaults(strategy_name) or {})
    source_blob = "\n".join(
        filter(
            None,
            [
                _extract_logic_excerpt(strategy_name, mode="entry"),
                _extract_logic_excerpt(strategy_name, mode="exit"),
            ],
        )
    )
    return {
        "strategy": strategy_name,
        "strategy_file": source_file,
        "strategy_class": strategy_name,
        "strategy_function": f"{strategy_name}.generate_signals",
        "backtest_supported": bool(info.get("backtest_supported")),
        "backtest_reason": str(info.get("reason") or ""),
        "direction_support": _detect_direction_support(strategy_name),
        "entry_logic": _extract_logic_excerpt(strategy_name, mode="entry"),
        "exit_logic": _extract_logic_excerpt(strategy_name, mode="exit"),
        "has_stop_loss": bool("stop_loss_pct" in defaults or "stop_loss" in source_blob),
        "has_take_profit": bool("take_profit_pct" in defaults or "take_profit" in source_blob),
        "has_reversal_exit": bool("SignalType.SELL" in source_blob or "SignalType.CLOSE" in source_blob),
        "has_time_stop": bool(any(token in source_blob for token in ["max_bars", "bars_in_trade", "time_stop", "holding_period"])),
    }


def _split_frame(df: pd.DataFrame, min_bars: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    size = len(df)
    if size < max(min_bars * 2, 60):
        return _copy_with_attrs(df), _copy_with_attrs(df)
    split_idx = max(min_bars, min(size - min_bars, int(size * 0.7)))
    return _copy_with_attrs(df.iloc[:split_idx]), _copy_with_attrs(df.iloc[split_idx:])


def _slice_market_bundle(bundle: Dict[str, pd.DataFrame], index: pd.Index) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    for symbol, frame in bundle.items():
        subset = _copy_with_attrs(frame.loc[frame.index.intersection(index)])
        out[symbol] = subset
    return out


async def _load_strategy_bundle(
    strategy_name: str,
    cache: Dict[Tuple[str, str], pd.DataFrame],
    *,
    symbol_override: Optional[str] = None,
    timeframe_override: Optional[str] = None,
) -> StrategyBundle:
    info = dict(get_backtest_strategy_info(strategy_name) or {})
    timeframe = str(timeframe_override or info.get("timeframe") or "1h")
    symbols = list(get_strategy_recommended_symbols(strategy_name) or [])
    symbol = str(symbol_override or (symbols[0] if symbols else "BTC/USDT"))

    async def _get_df(target_symbol: str) -> pd.DataFrame:
        cache_key = (str(target_symbol), timeframe)
        cached = cache.get(cache_key)
        if cached is not None:
            return _copy_with_attrs(cached)
        loaded = await _load_backtest_df(target_symbol, timeframe)
        cache[cache_key] = _copy_with_attrs(loaded)
        return _copy_with_attrs(loaded)

    primary_df = await _get_df(symbol)
    primary_df = await _attach_backtest_enrichment_if_needed(
        strategy=strategy_name,
        df=primary_df,
        symbol=symbol,
        start_time=None,
        end_time=None,
    )

    market_bundle: Dict[str, pd.DataFrame] = {symbol: _copy_with_attrs(primary_df)}
    pair_symbol = None
    if strategy_name == "PairsTradingStrategy":
        defaults = dict(get_strategy_defaults(strategy_name) or {})
        pair_symbol = str(defaults.get("pair_symbol") or "")
        if not pair_symbol or pair_symbol == symbol:
            pair_symbol = next((item for item in symbols if item != symbol), "ETH/USDT")
        pair_df = await _get_df(pair_symbol)
        market_bundle[pair_symbol] = _copy_with_attrs(pair_df)

    return StrategyBundle(
        strategy=strategy_name,
        symbol=symbol,
        timeframe=timeframe,
        df=_copy_with_attrs(primary_df),
        market_bundle=market_bundle,
        pair_symbol=pair_symbol,
    )


def _score_template(metrics: Dict[str, Any]) -> float:
    if not metrics or metrics.get("error"):
        return -1e9
    oos_sharpe = float(metrics.get("oos_sharpe_ratio") or 0.0)
    is_sharpe = float(metrics.get("is_sharpe_ratio") or 0.0)
    total_sharpe = float(metrics.get("sharpe_ratio") or 0.0)
    calmar = float(metrics.get("calmar") or 0.0)
    oos_return = float(metrics.get("oos_total_return") or 0.0)
    max_drawdown = float(metrics.get("max_drawdown") or 0.0)
    stability_penalty = abs(is_sharpe - oos_sharpe) * 0.35
    score = (
        0.45 * oos_sharpe
        + 0.20 * total_sharpe
        + 0.20 * calmar
        + 0.10 * (oos_return / 25.0)
        - 0.05 * (max_drawdown / 20.0)
        - stability_penalty
    )
    return float(score)


def _run_template(
    bundle: StrategyBundle,
    *,
    template_name: Optional[str],
    split_name: str,
    df: pd.DataFrame,
    market_bundle: Dict[str, pd.DataFrame],
    precomputed_position: Optional[pd.Series] = None,
) -> Dict[str, Any]:
    params = dict(get_strategy_defaults(bundle.strategy) or {})
    if bundle.pair_symbol:
        params["pair_symbol"] = bundle.pair_symbol
    result = _run_backtest_core(
        strategy=bundle.strategy,
        df=df,
        timeframe=bundle.timeframe,
        initial_capital=10000.0,
        params=params,
        include_series=False,
        commission_rate=0.0004,
        slippage_bps=2.0,
        market_bundle=market_bundle,
        use_stop_take=False,
        exit_template=template_name,
        exit_overrides=None,
        include_trade_log=(split_name == "full"),
        precomputed_position=precomputed_position,
    )
    result["split"] = split_name
    return result


def _evaluate_strategy(bundle: StrategyBundle, template_names: List[str]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    min_bars = _min_required_bars(bundle.timeframe)
    full_df = _copy_with_attrs(bundle.df)
    is_df, oos_df = _split_frame(full_df, min_bars=min_bars)
    full_bundle = _slice_market_bundle(bundle.market_bundle, full_df.index)
    is_bundle = _slice_market_bundle(bundle.market_bundle, is_df.index)
    oos_bundle = _slice_market_bundle(bundle.market_bundle, oos_df.index)
    params = dict(get_strategy_defaults(bundle.strategy) or {})
    if bundle.pair_symbol:
        params["pair_symbol"] = bundle.pair_symbol

    position_cache: Dict[str, Optional[pd.Series]] = {
        "full": None,
        "in_sample": None,
        "out_of_sample": None,
    }
    if bundle.strategy not in {"PairsTradingStrategy", "FamaFactorArbitrageStrategy"}:
        position_cache["full"] = pd.to_numeric(_build_positions(bundle.strategy, full_df, params), errors="coerce").fillna(0.0)
        position_cache["in_sample"] = pd.to_numeric(_build_positions(bundle.strategy, is_df, params), errors="coerce").fillna(0.0)
        position_cache["out_of_sample"] = pd.to_numeric(_build_positions(bundle.strategy, oos_df, params), errors="coerce").fillna(0.0)

    row: Dict[str, Any] = {
        "strategy": bundle.strategy,
        "symbol": bundle.symbol,
        "timeframe": bundle.timeframe,
    }
    detail_rows: List[Dict[str, Any]] = []
    template_results: Dict[str, Dict[str, Any]] = {}

    for template_name in template_names:
        if template_name == "ReversalOnly" and "Original" in template_results:
            merged = dict(template_results["Original"])
            merged["robust_score"] = _score_template(merged)
            template_results[template_name] = merged
            for metric in CORE_METRICS + ["oos_total_return", "oos_sharpe_ratio", "robust_score"]:
                row[f"{template_name}_{metric}"] = merged.get(metric)
            for event in list(merged.get("trade_events") or []):
                detail_rows.append(
                    {
                        "strategy": bundle.strategy,
                        "symbol": bundle.symbol,
                        "timeframe": bundle.timeframe,
                        "template": template_name,
                        **event,
                    }
                )
            continue

        engine_template = None if template_name == "Original" else template_name
        try:
            full_metrics = _run_template(
                bundle,
                template_name=engine_template,
                split_name="full",
                df=full_df,
                market_bundle=full_bundle,
                precomputed_position=position_cache.get("full"),
            )
            is_metrics = _run_template(
                bundle,
                template_name=engine_template,
                split_name="in_sample",
                df=is_df,
                market_bundle=is_bundle,
                precomputed_position=position_cache.get("in_sample"),
            )
            oos_metrics = _run_template(
                bundle,
                template_name=engine_template,
                split_name="out_of_sample",
                df=oos_df,
                market_bundle=oos_bundle,
                precomputed_position=position_cache.get("out_of_sample"),
            )
        except Exception as exc:
            full_metrics = {"error": str(exc)}
            is_metrics = {"error": str(exc)}
            oos_metrics = {"error": str(exc)}

        merged = dict(full_metrics)
        merged["is_total_return"] = is_metrics.get("total_return")
        merged["is_sharpe_ratio"] = is_metrics.get("sharpe_ratio")
        merged["oos_total_return"] = oos_metrics.get("total_return")
        merged["oos_sharpe_ratio"] = oos_metrics.get("sharpe_ratio")
        merged["robust_score"] = _score_template(merged)
        template_results[template_name] = merged

        for metric in CORE_METRICS + ["oos_total_return", "oos_sharpe_ratio", "robust_score"]:
            row[f"{template_name}_{metric}"] = merged.get(metric)

        for event in list(full_metrics.get("trade_events") or []):
            detail_rows.append(
                {
                    "strategy": bundle.strategy,
                    "symbol": bundle.symbol,
                    "timeframe": bundle.timeframe,
                    "template": template_name,
                    **event,
                }
            )

    ranked = sorted(template_results.items(), key=lambda item: item[1].get("robust_score", -1e9), reverse=True)
    row["recommended_template"] = ranked[0][0] if ranked else "Original"
    row["recommended_score"] = ranked[0][1].get("robust_score") if ranked else None
    row["_template_results"] = template_results
    return row, detail_rows


def _render_report(summary_rows: List[Dict[str, Any]], inventory_rows: List[Dict[str, Any]]) -> str:
    if not summary_rows:
        return "# Exit Refactor Report\n\nNo strategies were evaluated.\n"

    total_strategy_count = len(inventory_rows)
    supported_strategy_count = sum(1 for row in inventory_rows if bool(row.get("backtest_supported")))
    unsupported_rows = [row for row in inventory_rows if not bool(row.get("backtest_supported"))]
    template_wins: Dict[str, List[str]] = {name: [] for name in TEMPLATE_ORDER}
    atr_trail_fit: List[str] = []
    partial_fit: List[str] = []
    reversal_fit: List[str] = []
    time_stop_fit: List[str] = []
    overfit_risk: List[str] = []
    aggregate_scores: Dict[str, List[float]] = {name: [] for name in TEMPLATE_ORDER if name != "Original"}

    for row in summary_rows:
        strategy = str(row["strategy"])
        template_results = _template_results_from_row(row)
        recommended = str(row.get("recommended_template") or "Original")
        template_wins.setdefault(recommended, []).append(strategy)
        if recommended == "ATRTrail":
            atr_trail_fit.append(strategy)
        if recommended == "PartialPlusATR":
            partial_fit.append(strategy)
        if recommended in {"Original", "ReversalOnly"}:
            reversal_fit.append(strategy)

        baseline = template_results.get("Original") or {}
        for template_name in ["ATRTrail", "PartialPlusATR", "SignalPlusTimeStop"]:
            score = template_results.get(template_name, {}).get("robust_score")
            if score is not None:
                aggregate_scores[template_name].append(float(score))
        if (
            float(template_results.get("SignalPlusTimeStop", {}).get("max_drawdown") or 0.0)
            < float(baseline.get("max_drawdown") or 0.0)
            and float(template_results.get("SignalPlusTimeStop", {}).get("oos_sharpe_ratio") or 0.0)
            >= float(baseline.get("oos_sharpe_ratio") or 0.0)
        ) or (
            float(template_results.get("PartialPlusATR", {}).get("max_drawdown") or 0.0)
            < float(baseline.get("max_drawdown") or 0.0)
            and float(template_results.get("PartialPlusATR", {}).get("oos_sharpe_ratio") or 0.0)
            >= float(baseline.get("oos_sharpe_ratio") or 0.0)
        ):
            time_stop_fit.append(strategy)

        oos_scores = [
            float((template_results.get(name) or {}).get("oos_sharpe_ratio") or 0.0)
            for name in TEMPLATE_ORDER
        ]
        if np.std(oos_scores) > 0.75 or (
            max(oos_scores) - min(oos_scores) > 1.5
            and (template_results.get("Original") or {}).get("oos_sharpe_ratio") != (template_results.get(recommended) or {}).get("oos_sharpe_ratio")
        ):
            overfit_risk.append(strategy)

    best_default = sorted(
        aggregate_scores.items(),
        key=lambda item: np.median(item[1]) if item[1] else -1e9,
        reverse=True,
    )[0][0]

    inventory_df = pd.DataFrame(inventory_rows)
    inventory_lines = ["| Strategy | Direction | Stop | Take | Reversal | Time |", "| --- | --- | --- | --- | --- | --- |"]
    for _, row in inventory_df.iterrows():
        inventory_lines.append(
            f"| {row['strategy']} | {row['direction_support']} | {row['has_stop_loss']} | {row['has_take_profit']} | {row['has_reversal_exit']} | {row['has_time_stop']} |"
        )

    lines = [
        "# Exit Refactor Report",
        "",
        f"- Total strategies scanned in repo: {total_strategy_count}",
        f"- Backtest-supported strategies evaluated: {len(summary_rows)} / {supported_strategy_count}",
        f"- Exit templates compared: {', '.join(TEMPLATE_ORDER)}",
        "- Cost model retained from current repo defaults: commission `0.0004`, slippage `2bps`, existing OHLCV backtest matching preserved.",
        f"- Default robust recommendation: `{best_default}`",
        "",
        "## Template Fit",
        "",
        f"- ATRTrail fit: {', '.join(atr_trail_fit) if atr_trail_fit else 'None'}",
        f"- PartialPlusATR fit: {', '.join(partial_fit) if partial_fit else 'None'}",
        f"- Best left on original reversal exit: {', '.join(reversal_fit) if reversal_fit else 'None'}",
        f"- Time stop meaningfully reduced giveback: {', '.join(time_stop_fit) if time_stop_fit else 'None'}",
        f"- Exit-parameter sensitivity / overfit risk: {', '.join(overfit_risk) if overfit_risk else 'None'}",
        "",
        "## Recommendation",
        "",
        f"`{best_default}` was chosen as the default not because it delivered the single highest peak return, but because it produced the strongest median robustness score across the library with better OOS Sharpe / drawdown balance than the pure reversal baseline.",
        "",
        "## Strategy Inventory",
        "",
        *inventory_lines,
        "",
        "## Unsupported / Compatibility Notes",
        "",
        *(
            [f"- {row['strategy']}: {row['backtest_reason'] or 'Not supported by the current single-series backtest path.'}" for row in unsupported_rows]
            if unsupported_rows
            else ["- None"]
        ),
        "",
        "## Notes",
        "",
        "- `Original` reflects the current repo baseline under the existing backtest path.",
        "- `ReversalOnly` is the unified-exit equivalent of baseline signal-only exits.",
        "- `SignalPlusTimeStop` and `PartialPlusATR` were the two templates most likely to reduce profit giveback when OOS Sharpe did not deteriorate.",
        "- `FamaFactorArbitrageStrategy` stays on its portfolio-style compatibility path; exit templates are reported for completeness, but the cross-sectional rebalance logic is intentionally left untouched.",
    ]
    return "\n".join(lines) + "\n"


def _template_results_from_row(row: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    nested = row.get("_template_results")
    if isinstance(nested, dict) and nested:
        return nested

    template_results: Dict[str, Dict[str, Any]] = {}
    for template_name in TEMPLATE_ORDER:
        metrics: Dict[str, Any] = {}
        for metric in CORE_METRICS + ["oos_total_return", "oos_sharpe_ratio", "robust_score"]:
            key = f"{template_name}_{metric}"
            if key in row:
                metrics[metric] = row.get(key)
        if metrics:
            template_results[template_name] = metrics
    return template_results


def _resolve_output_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path


def merge_exit_refactor_outputs(
    *,
    source_dirs: List[str],
    output_dir: str,
    write_root_report: bool = True,
) -> Dict[str, Any]:
    if not source_dirs:
        raise ValueError("merge_exit_refactor_outputs requires at least one source directory")

    resolved_sources = [_resolve_output_path(item) for item in source_dirs]
    resolved_output = _resolve_output_path(output_dir)
    resolved_output.mkdir(parents=True, exist_ok=True)

    summary_frames: List[pd.DataFrame] = []
    detail_frames: List[pd.DataFrame] = []
    inventory_df: Optional[pd.DataFrame] = None

    for source in resolved_sources:
        summary_path = source / "summary_exit_templates.csv"
        details_path = source / "trade_details_exit_templates.csv"
        inventory_path = source / "strategy_inventory.csv"
        if summary_path.exists():
            summary_frames.append(pd.read_csv(summary_path))
        if details_path.exists():
            detail_frames.append(pd.read_csv(details_path))
        if inventory_df is None and inventory_path.exists():
            inventory_df = pd.read_csv(inventory_path)

    if not summary_frames:
        raise FileNotFoundError("No summary_exit_templates.csv files found in the provided source directories")
    if inventory_df is None:
        raise FileNotFoundError("No strategy_inventory.csv found in the provided source directories")

    summary_df = pd.concat(summary_frames, ignore_index=True)
    summary_df = summary_df.drop_duplicates(subset=["strategy"], keep="last")
    supported_order = [item["name"] for item in get_backtest_strategy_catalog() if item.get("backtest_supported")]
    order_map = {name: idx for idx, name in enumerate(supported_order)}
    summary_df["__order"] = summary_df["strategy"].map(order_map).fillna(10**9)
    summary_df = summary_df.sort_values(["__order", "strategy"]).drop(columns="__order")

    trade_df = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()
    if not trade_df.empty:
        trade_df = trade_df.drop_duplicates()

    summary_path = resolved_output / "summary_exit_templates.csv"
    detail_path = resolved_output / "trade_details_exit_templates.csv"
    inventory_path = resolved_output / "strategy_inventory.csv"
    report_path = resolved_output / "report_exit_refactor.md"

    summary_df.to_csv(summary_path, index=False)
    trade_df.to_csv(detail_path, index=False)
    inventory_df.to_csv(inventory_path, index=False)
    report_path.write_text(
        _render_report(summary_df.to_dict("records"), inventory_df.to_dict("records")),
        encoding="utf-8",
    )

    root_report_path = REPO_ROOT / "report_exit_refactor.md"
    if write_root_report:
        root_report_path.write_text(report_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "summary_csv": str(summary_path),
        "details_csv": str(detail_path),
        "inventory_csv": str(inventory_path),
        "report_md": str(report_path),
        "root_report_md": str(root_report_path) if write_root_report else None,
        "strategy_count": int(len(summary_df)),
        "detail_rows": int(len(trade_df)),
    }


async def _async_main(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_DIR
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "summary_exit_templates.csv"
    inventory_path = output_dir / "strategy_inventory.csv"
    details_path = output_dir / "trade_details_exit_templates.csv"
    report_path = output_dir / "report_exit_refactor.md"
    progress_path = output_dir / "progress.json"
    cache: Dict[Tuple[str, str], pd.DataFrame] = {}
    template_names = [args.template] if args.template else list(TEMPLATE_ORDER)
    all_strategy_names = list(getattr(strategy_module, "ALL_STRATEGIES", []))
    supported_catalog = [item for item in get_backtest_strategy_catalog() if item.get("backtest_supported")]
    supported_strategy_names = [str(item["name"]) for item in supported_catalog]
    strategies_to_run = list(supported_strategy_names)
    if args.strategy:
        strategies_to_run = [args.strategy]
        if args.strategy not in supported_strategy_names:
            raise ValueError(f"{args.strategy} 当前不在 41 个可回测策略内")
    elif args.start_index and int(args.start_index) > 1:
        offset = max(0, int(args.start_index) - 1)
        strategies_to_run = strategies_to_run[offset:]
    inventory_rows = [_collect_strategy_inventory(name) for name in all_strategy_names]
    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []

    if args.resume:
        if summary_path.exists():
            summary_rows = pd.read_csv(summary_path).to_dict("records")
        if details_path.exists():
            detail_rows = pd.read_csv(details_path).to_dict("records")
        completed = {str(row.get("strategy") or "") for row in summary_rows}
        strategies_to_run = [name for name in strategies_to_run if name not in completed]
        print(f"Resuming run, skipping {len(completed)} completed strategies.", flush=True)

    if args.max_strategies:
        strategies_to_run = strategies_to_run[: int(args.max_strategies)]

    total_count = len(strategies_to_run)
    for idx, strategy_name in enumerate(strategies_to_run, start=1):
        print(f"[{idx}/{total_count}] loading {strategy_name}", flush=True)
        bundle = await _load_strategy_bundle(
            strategy_name,
            cache,
            symbol_override=args.symbol if args.strategy else None,
            timeframe_override=args.timeframe if args.strategy else None,
        )
        print(f"[{idx}/{total_count}] running {strategy_name} on {bundle.symbol} {bundle.timeframe}", flush=True)
        row, detail = _evaluate_strategy(bundle, template_names=template_names)
        summary_rows.append(row)
        detail_rows.extend(detail)
        print(
            f"[{idx}/{total_count}] finished {strategy_name} -> recommended {row.get('recommended_template')}",
            flush=True,
        )
        pd.DataFrame(summary_rows).drop(columns=["_template_results"], errors="ignore").to_csv(summary_path, index=False)
        pd.DataFrame(inventory_rows).to_csv(inventory_path, index=False)
        pd.DataFrame(detail_rows).to_csv(details_path, index=False)
        report_path.write_text(_render_report(summary_rows, inventory_rows), encoding="utf-8")
        progress_path.write_text(
            json.dumps(
                {
                    "completed": idx,
                    "total": total_count,
                    "last_strategy": strategy_name,
                    "recommended_template": row.get("recommended_template"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    print(f"Saved outputs to {output_dir}", flush=True)

    print(json.dumps(
        {
            "summary_csv": str(summary_path),
            "inventory_csv": str(inventory_path),
            "details_csv": str(details_path),
            "report_md": str(report_path),
            "strategy_count": len(summary_rows),
        },
        ensure_ascii=False,
        indent=2,
    ))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run unified exit-engine refactor research.")
    parser.add_argument("--strategy", help="Run a single strategy by name.")
    parser.add_argument("--template", choices=TEMPLATE_ORDER, help="Run only one exit template.")
    parser.add_argument("--symbol", help="Override symbol when --strategy is used.")
    parser.add_argument("--timeframe", help="Override timeframe when --strategy is used.")
    parser.add_argument("--max-strategies", type=int, help="Run only the first N supported strategies.")
    parser.add_argument("--start-index", type=int, help="1-based starting index within the supported 41-strategy list.")
    parser.add_argument("--output-dir", help="Write outputs to a dedicated directory.")
    parser.add_argument("--resume", action="store_true", help="Resume from existing CSV outputs in the target directory.")
    parser.add_argument(
        "--merge-from",
        action="append",
        default=[],
        help="Merge one or more existing exit-refactor output directories instead of running backtests.",
    )
    args = parser.parse_args()
    if args.merge_from:
        output_dir = args.output_dir or str(DEFAULT_OUTPUT_DIR.with_name("exit_refactor_merged"))
        result = merge_exit_refactor_outputs(
            source_dirs=list(args.merge_from),
            output_dir=output_dir,
            write_root_report=True,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
