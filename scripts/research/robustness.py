from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from core.backtest.backtest_engine import BacktestConfig
from _common import add_common_args, ensure_output_dir, load_df, run_backtest


def main() -> None:
    parser = add_common_args(argparse.ArgumentParser(description="Robustness checks for HF strategy"))
    parser.add_argument("--bootstrap-runs", type=int, default=30)
    args = parser.parse_args()
    out_dir = ensure_output_dir(args.output_dir, "hf_research")
    df = load_df(args.exchange, args.symbol, args.timeframe, args.days)

    base_cfg = BacktestConfig(enable_shorting=True, leverage=2.0, fee_model="maker_taker", maker_fee=0.0002, taker_fee=0.0005, slippage_model="dynamic")
    base_res = run_backtest(df, args.config, base_cfg)

    rng = np.random.default_rng(42)
    seg_size = 240
    seg_count = max(1, len(df) // seg_size)
    returns = []
    for _ in range(max(5, args.bootstrap_runs)):
        picks = rng.integers(0, seg_count, size=seg_count)
        sample_parts = []
        for p in picks:
            part = df.iloc[p * seg_size : (p + 1) * seg_size]
            if not part.empty:
                sample_parts.append(part)
        if not sample_parts:
            continue
        boot = pd.concat(sample_parts).sort_index().copy()
        res = run_backtest(boot, args.config, base_cfg)
        returns.append({"total_return_pct": res.total_return_pct * 100.0, "max_drawdown_pct": res.max_drawdown_pct, "sharpe_ratio": res.sharpe_ratio})

    ret_df = pd.DataFrame(returns)
    if not ret_df.empty:
        ret_df.to_csv(out_dir / "robustness_bootstrap.csv", index=False, encoding="utf-8-sig")

    worst_row = ret_df.sort_values("total_return_pct").iloc[0].to_dict() if not ret_df.empty else {}
    md_lines = [
        "# Robustness Report",
        "",
        f"- Symbol: `{args.symbol}`",
        f"- Timeframe: `{args.timeframe}`",
        f"- Sample bars: `{len(df)}`",
        "",
        "## Base Run",
        f"- Return: `{base_res.total_return_pct * 100:.2f}%`",
        f"- Max Drawdown: `{base_res.max_drawdown_pct:.2f}%`",
        f"- Sharpe: `{base_res.sharpe_ratio:.2f}`",
        "",
        "## Bootstrap Summary",
    ]
    if not ret_df.empty:
        md_lines.extend(
            [
                f"- Runs: `{len(ret_df)}`",
                f"- Mean Return: `{ret_df['total_return_pct'].mean():.2f}%`",
                f"- Std Return: `{ret_df['total_return_pct'].std(ddof=0):.2f}%`",
                f"- Worst Return: `{float(worst_row.get('total_return_pct', 0.0)):.2f}%`",
                f"- 10th pct Return: `{ret_df['total_return_pct'].quantile(0.1):.2f}%`",
            ]
        )
    else:
        md_lines.append("- 无法生成 bootstrap 样本（数据不足）")

    (out_dir / "robustness_report.md").write_text("\n".join(md_lines), encoding="utf-8")
    print(f"[robustness] out={out_dir}")


if __name__ == "__main__":
    main()
