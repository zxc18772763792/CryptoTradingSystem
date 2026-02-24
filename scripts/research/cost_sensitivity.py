from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from core.backtest.backtest_engine import BacktestConfig
from _common import add_common_args, ensure_output_dir, load_df, run_backtest


def main() -> None:
    parser = add_common_args(argparse.ArgumentParser(description="Cost sensitivity for HF backtest"))
    parser.add_argument("--maker-fees", default="0.0001,0.0002")
    parser.add_argument("--taker-fees", default="0.0004,0.0006,0.0008")
    parser.add_argument("--min-slips", default="0.00003,0.00005,0.00008")
    args = parser.parse_args()
    out_dir = ensure_output_dir(args.output_dir, "hf_research")

    df = load_df(args.exchange, args.symbol, args.timeframe, args.days)
    maker_fees = [float(x) for x in str(args.maker_fees).split(",") if x.strip()]
    taker_fees = [float(x) for x in str(args.taker_fees).split(",") if x.strip()]
    min_slips = [float(x) for x in str(args.min_slips).split(",") if x.strip()]

    rows = []
    for maker_fee, taker_fee, min_slip in product(maker_fees, taker_fees, min_slips):
        cfg = BacktestConfig(
            initial_capital=10000,
            position_size_pct=0.12,
            max_positions=1,
            enable_shorting=True,
            leverage=2.0,
            fee_model="maker_taker",
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            default_execution_role="taker",
            slippage_model="dynamic",
            dynamic_slip={"min_slip": min_slip, "k_atr": 0.15, "k_rv": 0.8, "k_spread": 0.5},
            include_funding=False,
        )
        result = run_backtest(df=df, config_path=args.config, bt_cfg=cfg)
        cb = result.cost_breakdown or {}
        rows.append(
            {
                "maker_fee": maker_fee,
                "taker_fee": taker_fee,
                "min_slip": min_slip,
                "total_return_pct": result.total_return_pct * 100.0,
                "max_drawdown_pct": result.max_drawdown_pct,
                "sharpe_ratio": result.sharpe_ratio,
                "total_trades": result.total_trades,
                "fee_cost": cb.get("fee", 0.0),
                "slippage_cost": cb.get("slippage_cost", 0.0),
                "funding_pnl": cb.get("funding_pnl", 0.0),
                "turnover_notional": result.turnover_notional,
            }
        )
        print(f"[cost] taker={taker_fee:.4f} min_slip={min_slip:.5f} ret={result.total_return_pct*100:.2f}%")

    out_df = pd.DataFrame(rows).sort_values(["taker_fee", "min_slip", "maker_fee"])
    out_df.to_csv(out_dir / "cost_sensitivity_table.csv", index=False, encoding="utf-8-sig")

    plt.figure(figsize=(10, 5))
    for taker_fee, sub in out_df.groupby("taker_fee"):
        sub2 = sub.sort_values("min_slip")
        plt.plot(sub2["min_slip"], sub2["total_return_pct"], marker="o", label=f"taker={taker_fee:.4f}")
    plt.xlabel("min_slip (rate)")
    plt.ylabel("Total Return (%)")
    plt.title("Cost Sensitivity Curve (HF MultiFactor 5m)")
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "cost_curve.png", dpi=160)
    plt.close()

    print(f"[cost_sensitivity] out={out_dir}")


if __name__ == "__main__":
    main()
