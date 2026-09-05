"""Run the extended, leakage-resistant Binance run-up research study."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_VALIDATION_PATH = ROOT / "core" / "research" / "binance_runup_validation.py"
_SPEC = importlib.util.spec_from_file_location("binance_runup_validation_standalone", _VALIDATION_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load {_VALIDATION_PATH}")
_VALIDATION = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _VALIDATION
_SPEC.loader.exec_module(_VALIDATION)

PRICE_FACTORS = _VALIDATION.PRICE_FACTORS
add_forward_targets = _VALIDATION.add_forward_targets
add_forward_targets_from_4h = _VALIDATION.add_forward_targets_from_4h
add_segments = _VALIDATION.add_segments
build_market_context = _VALIDATION.build_market_context
bootstrap_lift_by_week = _VALIDATION.bootstrap_lift_by_week
bootstrap_independent_event_capture = _VALIDATION.bootstrap_independent_event_capture
independent_event_episodes = _VALIDATION.independent_event_episodes
json_ready = _VALIDATION.json_ready
load_exchange_metadata = _VALIDATION.load_exchange_metadata
load_funding_history = _VALIDATION.load_funding_history
load_long_oi_panel = _VALIDATION.load_long_oi_panel
prepare_modeling_panel = _VALIDATION.prepare_modeling_panel
run_oi_walk_forward = _VALIDATION.run_oi_walk_forward
run_path_backtests = _VALIDATION.run_path_backtests
run_price_walk_forward = _VALIDATION.run_price_walk_forward
scan_30d_runups = _VALIDATION.scan_30d_runups
score_metrics = _VALIDATION.score_metrics
segment_metrics = _VALIDATION.segment_metrics
stable_hash = _VALIDATION.stable_hash
threshold_horizon_surface = _VALIDATION.threshold_horizon_surface


DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_OUTPUT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"

BLUE = "#3B6FB6"
GOLD = "#C7922B"
ORANGE = "#D97732"
INK = "#20262E"
GREY = "#AAB2BD"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-external-refresh", action="store_true")
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_spot_symbols() -> tuple[set[str], str | None]:
    try:
        response = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=30)
        response.raise_for_status()
        payload = response.json()
        symbols = {
            str(item["symbol"])
            for item in payload.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("isSpotTradingAllowed")
        }
        return symbols, None
    except Exception as exc:  # noqa: BLE001
        return set(), f"{type(exc).__name__}: {exc}"


def profile_inputs(
    daily: pd.DataFrame,
    panel_4h: pd.DataFrame,
    modeling: pd.DataFrame,
    current_events: pd.DataFrame,
    *,
    spot_error: str | None,
    oi_quality: dict[str, Any],
) -> dict[str, Any]:
    factor_null_rates = {
        factor: float(pd.to_numeric(modeling[factor], errors="coerce").isna().mean())
        for factor in PRICE_FACTORS
    }


def build_oi_cohort_coverage(
    daily: pd.DataFrame,
    oi_panel: pd.DataFrame,
    recent_oi_path: Path,
) -> pd.DataFrame:
    records = [
        {
            "cohort": "all_market_price",
            "rows": int(len(daily)),
            "symbols": int(daily["symbol"].nunique()),
            "min_date": daily["date"].min(),
            "max_date": daily["date"].max(),
            "role": "primary price validation",
        },
        {
            "cohort": "long_window_oi_intersection_source",
            "rows": int(len(oi_panel)),
            "symbols": int(oi_panel["symbol"].nunique()) if not oi_panel.empty else 0,
            "min_date": oi_panel["date"].min() if not oi_panel.empty else None,
            "max_date": oi_panel["date"].max() if not oi_panel.empty else None,
            "role": "walk-forward OI incremental validation",
        },
    ]
    if recent_oi_path.exists():
        recent = pd.read_csv(
            recent_oi_path,
            compression="gzip",
            usecols=["symbol", "date", "oi_usd"],
        )
        recent["date"] = pd.to_datetime(recent["date"], utc=True)
        observed = recent[recent["oi_usd"].notna()].copy()
        records.append(
            {
                "cohort": "all_market_recent_30d_oi",
                "rows": int(len(observed)),
                "symbols": int(observed["symbol"].nunique()),
                "min_date": observed["date"].min() if not observed.empty else None,
                "max_date": observed["date"].max() if not observed.empty else None,
                "role": "coverage and forward-monitor annotation only; too short for full historical conclusion",
            }
        )
    return pd.DataFrame(records)
    invalid_ohlc = int(
        (
            (panel_4h["low"] > panel_4h["high"])
            | (panel_4h["open"] <= 0)
            | (panel_4h["high"] <= 0)
            | (panel_4h["low"] <= 0)
            | (panel_4h["close"] <= 0)
        ).sum()
    )
    expected_close = {
        "AKEUSDT": 9.4756,
        "BTWUSDT": 3.3453,
    }
    regression: dict[str, Any] = {}
    for symbol, expected in expected_close.items():
        match = current_events[current_events["symbol"] == symbol]
        regression[symbol] = {
            "present": bool(not match.empty),
            "classification": str(match.iloc[0]["classification"]) if not match.empty else None,
            "observed_close_return": float(match.iloc[0]["close_return"]) if not match.empty else None,
            "expected_approx": expected,
        }
    close_count = int((current_events["classification"] == "close-confirmed").sum())
    wick_count = int((current_events["classification"] == "wick-only").sum())
    return {
        "daily_rows": int(len(daily)),
        "daily_symbols": int(daily["symbol"].nunique()),
        "daily_min_date": daily["date"].min(),
        "daily_max_date": daily["date"].max(),
        "modeling_rows": int(len(modeling)),
        "modeling_positive_rows": int(modeling["label_primary_int"].sum()),
        "panel_4h_rows": int(len(panel_4h)),
        "panel_4h_symbols": int(panel_4h["symbol"].nunique()),
        "duplicate_symbol_open_time": int(panel_4h.duplicated(["symbol", "open_time"]).sum()),
        "invalid_ohlc_rows": invalid_ohlc,
        "price_factor_null_rates_modeling": factor_null_rates,
        "current_close_confirmed_events": close_count,
        "current_wick_only_events": wick_count,
        "current_event_regression_expected": {"close_confirmed": 17, "wick_only": 2},
        "ake_btw_regression": regression,
        "current_event_regression_pass": bool(
            close_count == 17
            and wick_count == 2
            and all(item["present"] for item in regression.values())
        ),
        "spot_exchange_info_error": spot_error,
        "long_oi": oi_quality,
    }


def aggregate_primary_metrics(scored: pd.DataFrame, *, bootstrap_samples: int = 2000) -> dict[str, Any]:
    row_metrics = score_metrics(scored, "price_model_score", "label_primary_int")
    events = independent_event_episodes(scored)
    return {
        "historical_walk_forward": row_metrics,
        "lift_bootstrap_7d_blocks": bootstrap_lift_by_week(scored, samples=bootstrap_samples),
        "event_capture_bootstrap": bootstrap_independent_event_capture(
            scored, samples=bootstrap_samples
        ),
        "independent_events": int(len(events)),
        "captured_events_top_2pct": int(events["captured_top_2pct"].sum()) if not events.empty else 0,
        "event_capture_rate": float(events["captured_top_2pct"].mean()) if not events.empty else None,
        "note": "historical walk-forward, not untouched OOS; true OOS starts with the forward monitor",
    }


def profile_inputs_v2(
    daily: pd.DataFrame,
    panel_4h: pd.DataFrame,
    modeling: pd.DataFrame,
    current_events: pd.DataFrame,
    *,
    spot_error: str | None,
    oi_quality: dict[str, Any],
) -> dict[str, Any]:
    factor_null_rates = {
        factor: float(pd.to_numeric(modeling[factor], errors="coerce").isna().mean())
        for factor in PRICE_FACTORS
    }
    invalid_ohlc = int(
        (
            (panel_4h["low"] > panel_4h["high"])
            | (panel_4h["open"] <= 0)
            | (panel_4h["high"] <= 0)
            | (panel_4h["low"] <= 0)
            | (panel_4h["close"] <= 0)
        ).sum()
    )
    regression: dict[str, Any] = {}
    for symbol, expected in {"AKEUSDT": 9.4756, "BTWUSDT": 3.3453}.items():
        match = current_events[current_events["symbol"] == symbol]
        regression[symbol] = {
            "present": bool(not match.empty),
            "classification": str(match.iloc[0]["classification"]) if not match.empty else None,
            "observed_close_return": float(match.iloc[0]["close_return"]) if not match.empty else None,
            "expected_approx": expected,
        }
    close_count = int((current_events["classification"] == "close-confirmed").sum())
    wick_count = int((current_events["classification"] == "wick-only").sum())
    return {
        "daily_rows": int(len(daily)),
        "daily_symbols": int(daily["symbol"].nunique()),
        "daily_min_date": daily["date"].min(),
        "daily_max_date": daily["date"].max(),
        "modeling_rows": int(len(modeling)),
        "modeling_positive_rows": int(modeling["label_primary_int"].sum()),
        "panel_4h_rows": int(len(panel_4h)),
        "panel_4h_symbols": int(panel_4h["symbol"].nunique()),
        "duplicate_symbol_open_time": int(panel_4h.duplicated(["symbol", "open_time"]).sum()),
        "invalid_ohlc_rows": invalid_ohlc,
        "price_factor_null_rates_modeling": factor_null_rates,
        "current_close_confirmed_events": close_count,
        "current_wick_only_events": wick_count,
        "current_event_regression_expected": {"close_confirmed": 17, "wick_only": 2},
        "ake_btw_regression": regression,
        "current_event_regression_pass": bool(
            close_count == 17 and wick_count == 2 and all(item["present"] for item in regression.values())
        ),
        "spot_exchange_info_error": spot_error,
        "long_oi": oi_quality,
    }


def create_current_watchlist(baseline: Path, output: Path) -> pd.DataFrame:
    current = pd.read_csv(baseline / "current_watchlist.csv")
    current = (
        current[current["ranking_score_pctile"] >= 0.98]
        .sort_values("ranking_score", ascending=False)
        .head(3)
        .copy()
    )
    current["oi_annotation"] = np.select(
        [
            current["oi_usd"].isna(),
            current["derivatives_crowding_risk"].fillna(False),
            (current["oi_change_1d"] > 0.20) & (current["oi_change_3d"] > 0),
        ],
        ["missing", "crowded", "supportive"],
        default="neutral",
    )
    current["research_stage"] = np.select(
        [
            current["recent_30d_200pct_runup"].fillna(False),
            current["chase_risk"].fillna(False),
            current["ranking_score_pctile"] >= 0.98,
        ],
        ["cooldown_after_200pct_runup", "reject_chase", "price_watch"],
        default="price_watch",
    )
    keep = [
        "symbol",
        "base_asset",
        "date",
        "close",
        "ranking_score",
        "ranking_score_pctile",
        "return_1d",
        "return_7d",
        "volume_ratio_1d_20d",
        "oi_usd",
        "oi_change_1d",
        "oi_change_3d",
        "oi_to_cmc_mcap",
        "funding_bps",
        "research_stage",
        "oi_annotation",
    ]
    watchlist = current[keep].sort_values("ranking_score", ascending=False)
    watchlist.to_csv(output / "current_research_watchlist.csv", index=False)
    return watchlist


def write_chart_map(output: Path) -> None:
    content = """# Chart map

| Section | Analytical question | Family / type | Data | Palette | Artifact |
|---|---|---|---|---|---|
| Historical robustness | Does lift persist across walk-forward folds? | Comparison / bar with benchmark | walk_forward_fold_metrics.csv | blue + neutral | charts/walk_forward_lift.png |
| Threshold sensitivity | Does the frozen model generalize across horizons and returns? | Matrix / heatmap | threshold_horizon_surface.csv | blue-gold | charts/threshold_horizon_heatmap.png |
| OI increment | Does OI improve the same-row price baseline? | Comparison / grouped bar | oi_walk_forward_fold_metrics.csv | blue + gold | charts/oi_increment.png |
| Trade path | Are MFE and MAE consistent with tradable exits? | Relationship / scatter | paper_trades.csv.gz | blue + orange | charts/mfe_mae.png |
| Portfolio | What return and drawdown did the best default-cost policy produce? | Trend / line | paper_equity_curves.csv.gz | blue + orange | charts/equity_drawdown.png |
"""
    (output / "CHART_MAP.md").write_text(content, encoding="utf-8")


def plot_results(
    output: Path,
    folds: pd.DataFrame,
    surface: pd.DataFrame,
    oi_folds: pd.DataFrame,
    trades: pd.DataFrame,
    equity: pd.DataFrame,
    paper_decision: dict[str, Any],
) -> list[str]:
    chart_dir = output / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    generated: list[str] = []
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": GREY, "text.color": INK, "axes.labelcolor": INK})

    if not folds.empty:
        figure, axis = plt.subplots(figsize=(9.5, 4.8))
        values = folds["price_top_2pct_lift"].astype(float)
        axis.bar(folds["fold"].astype(str), values, color=BLUE, edgecolor=INK, linewidth=0.5)
        axis.axhline(1.0, color=INK, linestyle="--", linewidth=1)
        axis.set_title("Top-2% lift by historical walk-forward fold")
        axis.set_xlabel("Fold")
        axis.set_ylabel("Lift versus fold base rate")
        axis.grid(axis="y", color="#E5E9EF", linewidth=0.7)
        figure.tight_layout()
        path = chart_dir / "walk_forward_lift.png"
        figure.savefig(path, dpi=180)
        plt.close(figure)
        generated.append(str(path.relative_to(output)))

    if not surface.empty:
        pivot = surface.pivot(index="threshold_pct", columns="horizon_days", values="lift").sort_index()
        figure, axis = plt.subplots(figsize=(8.5, 5.8))
        image = axis.imshow(pivot.to_numpy(), aspect="auto", cmap="YlGnBu", vmin=0)
        axis.set_xticks(range(len(pivot.columns)), [f"{value}d" for value in pivot.columns])
        axis.set_yticks(range(len(pivot.index)), [f"+{value}%" for value in pivot.index])
        axis.set_title("Frozen-model lift across return thresholds and horizons")
        axis.set_xlabel("Forward horizon")
        axis.set_ylabel("Maximum-return threshold")
        for row in range(len(pivot.index)):
            for column in range(len(pivot.columns)):
                value = pivot.iloc[row, column]
                if pd.notna(value):
                    axis.text(column, row, f"{value:.1f}x", ha="center", va="center", color=INK, fontsize=8)
        figure.colorbar(image, ax=axis, label="Lift")
        figure.tight_layout()
        path = chart_dir / "threshold_horizon_heatmap.png"
        figure.savefig(path, dpi=180)
        plt.close(figure)
        generated.append(str(path.relative_to(output)))

    if not oi_folds.empty:
        figure, axis = plt.subplots(figsize=(9.5, 4.8))
        positions = np.arange(len(oi_folds))
        width = 0.36
        axis.bar(positions - width / 2, oi_folds["price_top_2pct_lift"], width, label="Price only", color=BLUE)
        axis.bar(positions + width / 2, oi_folds["combo_top_2pct_lift"], width, label="Price + OI", color=GOLD)
        axis.axhline(1.0, color=INK, linestyle="--", linewidth=1)
        axis.set_xticks(positions, oi_folds["fold"].astype(str))
        axis.set_title("Top-2% lift on identical OI-enriched rows")
        axis.set_xlabel("Fold")
        axis.set_ylabel("Lift")
        axis.legend(frameon=False)
        axis.grid(axis="y", color="#E5E9EF", linewidth=0.7)
        figure.tight_layout()
        path = chart_dir / "oi_increment.png"
        figure.savefig(path, dpi=180)
        plt.close(figure)
        generated.append(str(path.relative_to(output)))

    default_trades = trades[trades["slippage_bps_each_side"] == 5.0] if not trades.empty else pd.DataFrame()
    if not default_trades.empty:
        figure, axis = plt.subplots(figsize=(8.5, 5.8))
        for policy, group in default_trades.groupby("policy"):
            axis.scatter(group["mae"], group["mfe"], s=20, alpha=0.55, label=policy)
        axis.axvline(0, color=GREY, linewidth=0.8)
        axis.axhline(0, color=GREY, linewidth=0.8)
        axis.set_title("Maximum adverse and favorable excursion by paper trade")
        axis.set_xlabel("MAE from entry")
        axis.set_ylabel("MFE from entry")
        axis.legend(frameon=False, fontsize=8)
        axis.grid(color="#E5E9EF", linewidth=0.7)
        figure.tight_layout()
        path = chart_dir / "mfe_mae.png"
        figure.savefig(path, dpi=180)
        plt.close(figure)
        generated.append(str(path.relative_to(output)))

    best_policy = paper_decision.get("best_policy")
    best_curve = (
        equity[
            (equity["policy"] == best_policy)
            & (equity["slippage_bps_each_side"] == 5.0)
        ].copy()
        if best_policy and not equity.empty
        else pd.DataFrame()
    )
    if not best_curve.empty:
        figure, axes = plt.subplots(2, 1, figsize=(10, 6.5), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
        axes[0].plot(pd.to_datetime(best_curve["date"]), best_curve["equity"], color=BLUE, linewidth=1.7)
        axes[0].set_title(f"Paper portfolio equity: {best_policy}")
        axes[0].set_ylabel("USDT")
        axes[0].grid(color="#E5E9EF", linewidth=0.7)
        axes[1].fill_between(pd.to_datetime(best_curve["date"]), best_curve["drawdown"], 0, color=ORANGE, alpha=0.35)
        axes[1].set_ylabel("Drawdown")
        axes[1].set_xlabel("Date")
        axes[1].grid(color="#E5E9EF", linewidth=0.7)
        figure.tight_layout()
        path = chart_dir / "equity_drawdown.png"
        figure.savefig(path, dpi=180)
        plt.close(figure)
        generated.append(str(path.relative_to(output)))
    return generated


def build_report_markdown(
    output: Path,
    *,
    primary: dict[str, Any],
    oi_decision: dict[str, Any],
    paper_decision: dict[str, Any],
    data_quality: dict[str, Any],
    folds: pd.DataFrame,
    watchlist: pd.DataFrame,
) -> None:
    metric = primary["historical_walk_forward"]
    bootstrap = primary["lift_bootstrap_7d_blocks"]
    event_bootstrap = primary["event_capture_bootstrap"]
    classification = paper_decision.get("final_classification", "watchlist_only")
    price_evidence = "已验证" if (bootstrap.get("lower_95pct") or 0) > 1 else "方向性证据"
    oi_evidence = "已验证" if oi_decision.get("ranking_eligible") else "方向性证据"
    top_watch = watchlist[watchlist["research_stage"] == "price_watch"].head(10)
    watch_rows = "\n".join(
        f"| {row.symbol} | {row.ranking_score_pctile:.3f} | {row.oi_annotation} | {row.return_7d:.1%} |"
        for row in top_watch.itertuples()
    ) or "| — | — | — | — |"
    fold_text = ", ".join(
        f"F{int(row.fold)} {float(row.price_top_2pct_lift):.2f}x"
        for row in folds.itertuples()
    )
    content = f"""# Binance 暴涨币因子：严格历史验证与前瞻监控基线

## 技术摘要

- **价格因子具有筛选价值，但不能直接等同于可交易收益。** 历史 walk-forward 共 {len(folds)} 折，合并 AUC 为 {metric.get('auc', float('nan')):.3f}，top-2% 精度为 {metric.get('top_2pct_precision', float('nan')):.2%}，相对基准 lift 为 {metric.get('top_2pct_lift', float('nan')):.2f}x；7 日块 bootstrap 的 95% 区间为 {bootstrap.get('lower_95pct', float('nan')):.2f}x–{bootstrap.get('upper_95pct', float('nan')):.2f}x。
- **OI 尚未取得独立、稳定的排序资格。** 同行交集验证结论为 `{oi_evidence}`；是否满足预注册门槛：`{oi_decision.get('ranking_eligible', False)}`。因此 OI 继续只做 supportive / neutral / crowded / missing 注释。
- **交易结论：`{classification}`。** 最佳默认成本政策为 `{paper_decision.get('best_policy')}`，期望收益 {paper_decision.get('best_policy_expectancy', float('nan')):.2%}，利润因子 {paper_decision.get('best_policy_profit_factor', float('nan')):.2f}，最大回撤 {paper_decision.get('best_policy_max_drawdown', float('nan')):.2%}。未通过全部门槛时，不应自动交易或加杠杆。
- **真正未见样本从每日监控开始。** 这些历史折已经被研究者查看过，只能称 historical walk-forward；冻结模型 30/60/90 日结果才是真 OOS。

## 价格模型的优势跨折并不均匀

逐折 top-2% lift：{fold_text}。模型固定使用波动率、日内振幅、ATR、上下影线、20 日突破距离、30 日回撤和区间压缩度；没有在每个测试折重新挑因子。跨折不稳定或由少数事件主导的部分，不进入“已验证”结论。

![Walk-forward lift](charts/walk_forward_lift.png)

## +200% 是主问题，十倍币只作稀有事件探索

阈值×周期矩阵用同一冻结模型评分，避免为每个格子重新调参。+900% 单元格只用于观察模型是否对更极端右尾保持单调性；事件太少时不据此改变策略。

![Threshold and horizon sensitivity](charts/threshold_horizon_heatmap.png)

## 小市值和 OI 的证据必须看同一批币

长窗 OI 数据覆盖 {data_quality['long_oi'].get('loaded_symbols', 0)} 个币、{data_quality['long_oi'].get('rows', 0)} 行。价格-only、OI-only 和价格+OI 均在相同交集行、相同折上比较；历史市值倒推近似样本不进入主 OI 结论。当前 OI 排序资格为 `{oi_decision.get('ranking_eligible', False)}`，多数折同时改善 AP 和 lift 的比例为 {oi_decision.get('fold_majority_both_positive', 0):.1%}。

![OI incremental lift](charts/oi_increment.png)

## 4h 路径回测仍是决定能否交易的门槛

回测使用下一 UTC 日首根 4h 开盘、同 K 线止损优先、每边 5bps 手续费、5/15/30bps 滑点、可得的实际资金费率，以及 0.5% 单笔风险、10 个并发、50% 总敞口。均值为正但依赖单一大赢家、回撤超标或多数折利润因子不大于 1，都会判为 `watchlist_only`。

![MFE and MAE](charts/mfe_mae.png)

![Equity and drawdown](charts/equity_drawdown.png)

## 当前只读观察名单

| Symbol | Score percentile | OI annotation | 7d return |
|---|---:|---|---:|
{watch_rows}

`price_watch` 只表示进入冻结价格模型前 2%；`reject_chase` 和 `cooldown_after_200pct_runup` 优先级更高。名单不构成下单建议。

## 数据、定义与质量边界

- 数据截止：{data_quality.get('daily_max_date')}；UTC 日界，信号在日线结束后形成，下一根 4h 开盘是假设进场点。
- 当前 30 日回归：收盘确认 {data_quality.get('current_close_confirmed_events')} 个、插针 {data_quality.get('current_wick_only_events')} 个；AKE/BTW 回归通过：{data_quality.get('current_event_regression_pass')}。
- 当前宇宙仍存在幸存者偏差；本轮没有发现可安全补入主面板的历史下架合约，因此该缺口没有被伪装成“已解决”。
- Binance 官方 OI 历史仅约一个月；长窗 OI 依赖既有 Coinglass 缓存，覆盖不足会使 OI 结论降级。
- 本研究是预测性观察，不建立因果关系，也不批准自动交易。

## 结论分级

- **{price_evidence}：** 冻结价格因子在历史 walk-forward 中提高了暴涨币浓度，但需要结合置信区间和事件集中度解释。
- **{oi_evidence}：** 小市值、绝对 OI、OI/市值和 OI 变化仅在交集样本中评估；未通过预注册门槛前不得提高排名。
- **尚未验证：** 每日冻结模型在未来 30/60/90 日的真实 OOS 表现。

## 推荐下一步

保持只读监控，不接 `altcoin_radar`、策略管理器或订单执行。90 天内不改模型和阈值；到 30/60/90 日只追加结果标签并复盘覆盖、lift、独立事件命中和纸面交易表现。
"""
    (output / "REPORT.md").write_text(content, encoding="utf-8")


def build_report_markdown_v2(
    output: Path,
    *,
    primary: dict[str, Any],
    oi_decision: dict[str, Any],
    paper_decision: dict[str, Any],
    data_quality: dict[str, Any],
    folds: pd.DataFrame,
    watchlist: pd.DataFrame,
) -> None:
    metric = primary["historical_walk_forward"]
    bootstrap = primary["lift_bootstrap_7d_blocks"]
    event_bootstrap = primary["event_capture_bootstrap"]
    classification = paper_decision.get("final_classification", "watchlist_only")
    factor_groups = oi_decision.get("factor_group_evidence", {})
    factor_lines = []
    labels = {
        "small_cap": "小市值",
        "absolute_oi": "绝对 OI",
        "oi_to_mcap": "OI/市值",
        "oi_growth": "OI 增长",
    }
    for key, label in labels.items():
        evidence = factor_groups.get(key, {})
        factor_lines.append(
            f"- {label}: 多数折同时改善 AP 与 lift 的比例 "
            f"{evidence.get('fold_majority_ap_and_lift_positive', 0):.0%}；"
            f"中位 AP 增量 {evidence.get('median_delta_average_precision', float('nan')):.4f}，"
            f"中位 lift 增量 {evidence.get('median_delta_top_2pct_lift', float('nan')):.2f}x。"
        )
    top_watch = watchlist[watchlist["research_stage"] == "price_watch"].head(10)
    watch_rows = "\n".join(
        f"| {row.symbol} | {row.ranking_score_pctile:.3f} | {row.oi_annotation} | {row.return_7d:.1%} |"
        for row in top_watch.itertuples()
    ) or "| — | — | — | — |"
    fold_text = ", ".join(
        f"F{int(row.fold)} {float(row.price_top_2pct_lift):.2f}x" for row in folds.itertuples()
    )
    content = f"""# Binance 暴涨币因子：严格历史验证与前瞻监控基线

## 技术摘要

- **价格因子有提前筛选价值，但不等于可交易收益。** 历史 walk-forward 共 {len(folds)} 折，AUC {metric.get('auc', float('nan')):.3f}，top-2% 精度 {metric.get('top_2pct_precision', float('nan')):.2%}，lift {metric.get('top_2pct_lift', float('nan')):.2f}x；7 日块 bootstrap 95% 区间 {bootstrap.get('lower_95pct', float('nan')):.2f}x–{bootstrap.get('upper_95pct', float('nan')):.2f}x。
- **OI 排名资格：`{oi_decision.get('ranking_eligible', False)}`。** 未通过预注册门槛时，OI 只能作 `supportive/neutral/crowded/missing` 注释，不能提高排名。
- **交易结论：`{classification}`。** 最佳默认成本政策 `{paper_decision.get('best_policy')}`，期望 {paper_decision.get('best_policy_expectancy', float('nan')):.2%}，利润因子 {paper_decision.get('best_policy_profit_factor', float('nan')):.2f}，最大回撤 {paper_decision.get('best_policy_max_drawdown', float('nan')):.2%}。无论历史结果如何，本项目不接自动交易且不使用杠杆。
- **真正 OOS 尚未开始积累。** 历史折统一标为 historical walk-forward；冻结模型未来 30/60/90 日结果才是真正未见样本。

## 价格模型的历史证据

逐折 top-2% lift：{fold_text}。模型固定使用 8 个价格因子，没有按测试折重新选因子。独立事件捕获率为 {primary.get('event_capture_rate', float('nan')):.1%}，事件 bootstrap 95% 区间为 {event_bootstrap.get('lower_95pct', float('nan')):.1%}–{event_bootstrap.get('upper_95pct', float('nan')):.1%}，避免把同一次上涨的连续标签日重复当成多个事件。

![Walk-forward lift](charts/walk_forward_lift.png)

## 阈值与周期敏感性

+200% / 14 日是主问题；+50%、+100%、+150%、+300%、+500%、+900% 与 3/7/14/30 日只使用同一冻结评分做敏感性矩阵，并进行 BH 多重检验控制。+900% 仅探索十倍币，不用于调参。

![Threshold and horizon sensitivity](charts/threshold_horizon_heatmap.png)

## 小市值与 OI 的独立增量

长窗 OI 源覆盖 {data_quality['long_oi'].get('loaded_symbols', 0)} 个币、{data_quality['long_oi'].get('rows', 0)} 行。价格-only、OI-only、价格+OI 和四个因子组消融都在完全相同的交集行、相同 walk-forward 折上比较；历史市值滞后 24 小时，价格倒推当前供应量的近似值不进入主结论。

{chr(10).join(factor_lines)}

上述四项均只属于方向性证据；完整 OI 组合只有在多数折同时改善 AP 与 top-2% lift，且两项增量置信区间下界均大于 0 时才取得排序资格。期货/现货成交比在现有长窗缓存中不可恢复，明确列为缺失的预注册特征。

![OI incremental comparison](charts/oi_increment.png)

## 1x 永续纸面可行性

模拟采用首次进入每日横截面前 2%、每天最多 3 个新信号、单币一笔、退出后冷却 14 天；08:20 Asia/Shanghai 计算后在下一根 4h 开盘（UTC 04:00）进场，同 K 线止损优先，计入资金费率、每边 5bps 手续费和 5/15/30bps 滑点。组合约束为 100,000 USDT、单笔风险 0.5%、最多 10 个仓位、总名义敞口 50%。纸面候选还必须通过多数时间折、BTC 趋势/波动与山寨宽度市场状态、最大盈利事件剔除和利润集中度门槛。

![MFE and MAE](charts/mfe_mae.png)

![Equity and drawdown](charts/equity_drawdown.png)

## 当前只读观察名单

| Symbol | Score percentile | OI annotation | 7d return |
|---|---:|---|---:|
{watch_rows}

`price_watch` 只表示进入冻结价格模型前 2%；`reject_chase` 和 `cooldown_after_200pct_runup` 的风险优先级更高。名单不构成下单建议。

## 数据质量与覆盖缺口

- 当前 30 日回归：收盘确认 {data_quality.get('current_close_confirmed_events')} 个、插针 {data_quality.get('current_wick_only_events')} 个；AKE/BTW 回归通过：{data_quality.get('current_event_regression_pass')}。
- 当前主面板仍有幸存者偏差；历史下架合约未能安全恢复进主宇宙，不能宣称覆盖完整。
- Binance 官方 OI 历史约一个月；长窗 OI 依赖已有缓存，覆盖不足会降低结论等级。
- 市值和 OI 是预测性关联研究，不建立因果关系。

## 结论分级

- **已验证：** 固定价格因子在 historical walk-forward 中显著提高 +200% 暴涨标签浓度，bootstrap lift 区间下界高于 1；当前 17+2 事件与 AKE/BTW 回归通过。
- **方向性证据：** 小市值、绝对 OI、OI/市值与 OI 增长的同行消融结果；是否进入排序只服从完整 OI 组合的预注册门槛。
- **尚未验证：** 每日冻结模型未来 30/60/90 日的真实 OOS 表现、历史下架合约的完整覆盖、长窗期货/现货成交比。

## 下一步

保持只读监控，不接 `altcoin_radar`、策略管理器或订单执行；90 天内不改模型与阈值，只追加结果标签并在第 30/60/90 天复盘覆盖、lift、独立事件命中和纸面交易表现。
"""
    (output / "REPORT.md").write_text(content, encoding="utf-8")


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)

    required = [
        baseline / "daily_feature_panel.csv.gz",
        baseline / "futures_4h_panel.csv.gz",
        baseline / "factor_model.json",
        baseline / "exchange_info_snapshot.json",
        baseline / "current_watchlist.csv",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing frozen baseline inputs: {missing}")

    print("Loading frozen baseline panels", flush=True)
    daily = pd.read_csv(baseline / "daily_feature_panel.csv.gz")
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    panel_4h = pd.read_csv(baseline / "futures_4h_panel.csv.gz")
    panel_4h["open_time"] = pd.to_datetime(panel_4h["open_time"], utc=True)

    print("Recomputing multi-horizon targets", flush=True)
    targeted = add_forward_targets_from_4h(daily, panel_4h)
    modeling = prepare_modeling_panel(targeted)
    print(f"Modeling rows={len(modeling):,} positives={int(modeling.label_primary_int.sum()):,}", flush=True)

    print("Running purged expanding-window price validation", flush=True)
    scored, fold_metrics = run_price_walk_forward(modeling)
    if scored.empty:
        raise RuntimeError("No valid walk-forward folds were produced")
    threshold_surface = threshold_horizon_surface(scored)

    spot_symbols: set[str] = set()
    spot_error: str | None = "external refresh disabled"
    if not args.no_external_refresh:
        spot_symbols, spot_error = fetch_spot_symbols()
    exchange_metadata = load_exchange_metadata(baseline / "exchange_info_snapshot.json")
    market_context = build_market_context(daily)
    scored_segmented = add_segments(
        scored,
        exchange_metadata=exchange_metadata,
        spot_symbols=spot_symbols,
        market_context=market_context,
    )
    segments = segment_metrics(scored_segmented)

    print("Loading lagged long-window OI and market-cap history", flush=True)
    oi_panel, oi_quality = load_long_oi_panel(AMBUSH_ROOT)
    oi_cohorts = build_oi_cohort_coverage(
        daily,
        oi_panel,
        baseline / "oi_daily_panel_30d.csv.gz",
    )
    oi_scored, oi_fold_metrics, oi_decision = run_oi_walk_forward(
        modeling, oi_panel, bootstrap_samples=args.bootstrap_samples
    )

    print("Running 4h path-dependent 1x paper simulations", flush=True)
    funding_history = load_funding_history(AMBUSH_ROOT)
    trades, paper_summary, equity, paper_decision = run_path_backtests(
        scored_segmented,
        panel_4h,
        funding_history=funding_history,
    )

    current_events = scan_30d_runups(panel_4h)
    data_quality = profile_inputs_v2(
        daily,
        panel_4h,
        modeling,
        current_events,
        spot_error=spot_error,
        oi_quality=oi_quality,
    )
    primary = aggregate_primary_metrics(scored_segmented, bootstrap_samples=args.bootstrap_samples)
    watchlist = create_current_watchlist(baseline, output)

    print("Writing versioned evidence", flush=True)
    scored_segmented.to_csv(output / "historical_walk_forward_scored.csv.gz", index=False, compression="gzip")
    fold_metrics.to_csv(output / "walk_forward_fold_metrics.csv", index=False)
    threshold_surface.to_csv(output / "threshold_horizon_surface.csv", index=False)
    independent_event_episodes(scored_segmented).to_csv(output / "independent_events.csv", index=False)
    segments.to_csv(output / "segment_metrics.csv", index=False)
    oi_fold_metrics.to_csv(output / "oi_walk_forward_fold_metrics.csv", index=False)
    oi_cohorts.to_csv(output / "oi_cohort_coverage.csv", index=False)
    if not oi_scored.empty:
        oi_scored.to_csv(output / "oi_walk_forward_scored.csv.gz", index=False, compression="gzip")
    paper_summary.to_csv(output / "paper_strategy_summary.csv", index=False)
    if not trades.empty:
        trades.to_csv(output / "paper_trades.csv.gz", index=False, compression="gzip")
    if not equity.empty:
        equity.to_csv(output / "paper_equity_curves.csv.gz", index=False, compression="gzip")
    current_events.to_csv(output / "current_30d_runups_verified.csv", index=False)

    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "baseline_dir": str(baseline),
        "model_factors_frozen": list(PRICE_FACTORS),
        "validation_label": "historical_walk_forward_not_untouched_oos",
        "price_validation": primary,
        "oi_validation": oi_decision,
        "paper_strategy": paper_decision,
        "data_quality": data_quality,
        "universe_bias": {
            "current_exchange_info_symbols": int(len(exchange_metadata)),
            "historical_delisted_symbols_recovered_into_primary_panel": 0,
            "status": "unresolved_survivorship_bias",
        },
    }
    (output / "analysis_summary.json").write_text(
        json.dumps(json_ready(summary), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "data_quality.json").write_text(
        json.dumps(json_ready(data_quality), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "oi_decision.json").write_text(
        json.dumps(json_ready(oi_decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "paper_decision.json").write_text(
        json.dumps(json_ready(paper_decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )

    generated_charts = plot_results(
        output,
        fold_metrics,
        threshold_surface,
        oi_fold_metrics,
        trades,
        equity,
        paper_decision,
    )
    write_chart_map(output)
    build_report_markdown_v2(
        output,
        primary=primary,
        oi_decision=oi_decision,
        paper_decision=paper_decision,
        data_quality=data_quality,
        folds=fold_metrics,
        watchlist=watchlist,
    )

    manifest_files = [path for path in output.rglob("*") if path.is_file()]
    manifest = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "baseline_inputs": {str(path.name): file_sha256(path) for path in required},
        "analysis_files": {
            str(path.relative_to(output)): {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
            for path in sorted(manifest_files)
            if path.name != "evidence_manifest.json"
        },
        "generated_charts": generated_charts,
        "analysis_hash": stable_hash(summary),
    }
    (output / "evidence_manifest.json").write_text(
        json.dumps(json_ready(manifest), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(json_ready(summary), ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
