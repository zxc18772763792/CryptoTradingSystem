from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import yaml

from core.backtest.backtest_engine import BacktestConfig
from _common import add_common_args, ensure_output_dir, load_df, run_backtest


def _slice_segments(df: pd.DataFrame, train_bars: int, test_bars: int, step_bars: int):
    n = len(df)
    start = 0
    while start + train_bars + test_bars <= n:
        yield df.iloc[start : start + train_bars], df.iloc[start + train_bars : start + train_bars + test_bars], start
        start += step_bars


def main() -> None:
    parser = add_common_args(argparse.ArgumentParser(description="Walk-forward grid for MultiFactorHF"))
    parser.add_argument("--train-bars", type=int, default=2000)
    parser.add_argument("--test-bars", type=int, default=600)
    parser.add_argument("--step-bars", type=int, default=600)
    args = parser.parse_args()
    out_dir = ensure_output_dir(args.output_dir, "hf_research")
    df = load_df(args.exchange, args.symbol, args.timeframe, args.days)

    base_cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) if Path(args.config).exists() else {}
    if not isinstance(base_cfg, dict):
        base_cfg = {}

    grid_enter = [0.65, 0.8, 1.0]
    grid_exit = [0.2, 0.3, 0.4]
    rows = []
    segment_scores = []
    for enter_th, exit_th in product(grid_enter, grid_exit):
        if exit_th >= enter_th:
            continue
        wf_returns = []
        for train_df, test_df, seg_start in _slice_segments(df, args.train_bars, args.test_bars, args.step_bars):
            # "train" stage simplified: evaluate on train and choose if positive; no parameter fitting beyond grid itself.
            cfg_obj = dict(base_cfg)
            cfg_obj["enter_th"] = float(enter_th)
            cfg_obj["exit_th"] = float(exit_th)
            tmp_cfg = out_dir / f"_tmp_cfg_{enter_th}_{exit_th}.yaml"
            tmp_cfg.write_text(yaml.safe_dump(cfg_obj, sort_keys=False, allow_unicode=True), encoding="utf-8")
            bt_cfg = BacktestConfig(enable_shorting=True, leverage=2.0, fee_model="maker_taker", maker_fee=0.0002, taker_fee=0.0005, slippage_model="dynamic")
            train_res = run_backtest(train_df, str(tmp_cfg), bt_cfg)
            if train_res.total_return_pct <= -0.25:
                wf_returns.append(None)
                continue
            test_res = run_backtest(test_df, str(tmp_cfg), bt_cfg)
            wf_returns.append(test_res.total_return_pct * 100.0)
            segment_scores.append({"segment_start": int(seg_start), "enter_th": enter_th, "exit_th": exit_th, "test_return_pct": test_res.total_return_pct * 100.0})
        vals = [v for v in wf_returns if v is not None]
        rows.append(
            {
                "enter_th": enter_th,
                "exit_th": exit_th,
                "segments": len(vals),
                "mean_test_return_pct": float(pd.Series(vals).mean()) if vals else 0.0,
                "std_test_return_pct": float(pd.Series(vals).std(ddof=0)) if len(vals) > 1 else 0.0,
                "worst_test_return_pct": float(min(vals)) if vals else 0.0,
            }
        )

    leaderboard = pd.DataFrame(rows).sort_values(["mean_test_return_pct", "worst_test_return_pct"], ascending=[False, False])
    leaderboard.to_csv(out_dir / "leaderboard.csv", index=False, encoding="utf-8-sig")

    if segment_scores:
        seg_df = pd.DataFrame(segment_scores)
        pivot = seg_df.pivot_table(index="segment_start", columns=["enter_th", "exit_th"], values="test_return_pct")
        plt.figure(figsize=(11, 5))
        for col in pivot.columns[:6]:
            plt.plot(pivot.index, pivot[col], marker="o", linewidth=1.2, label=f"{col[0]:.2f}/{col[1]:.2f}")
        plt.axhline(0, color="gray", linewidth=1)
        plt.title("Walk-forward Segment Returns")
        plt.xlabel("segment_start_bar")
        plt.ylabel("test return (%)")
        plt.grid(alpha=0.25)
        plt.legend(fontsize=8, ncol=2)
        plt.tight_layout()
        plt.savefig(out_dir / "walk_forward_stability.png", dpi=160)
        plt.close()

    print(f"[walk_forward] out={out_dir}")


if __name__ == "__main__":
    main()
