from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from core.factors_ts.registry import list_factors, compute_factor
from _common import add_common_args, ensure_output_dir, load_df


def _forward_return(close: pd.Series, bars: int = 3) -> pd.Series:
    return close.shift(-bars) / close - 1.0


def main() -> None:
    parser = add_common_args(argparse.ArgumentParser(description="Single-factor edge study (5m)"))
    parser.add_argument("--horizon-bars", type=int, default=3)
    args = parser.parse_args()
    out_dir = ensure_output_dir(args.output_dir, "hf_research")
    df = load_df(args.exchange, args.symbol, args.timeframe, args.days)

    close = pd.to_numeric(df["close"], errors="coerce")
    fwd = _forward_return(close, bars=max(1, args.horizon_bars))

    factor_specs = [
        ("ret_log", {"n": 3}),
        ("ret_log", {"n": 12}),
        ("ema_slope", {"fast": 8, "slow": 21}),
        ("zscore_price", {"lookback": 30}),
        ("realized_vol", {"lookback": 60}),
        ("atr_pct", {"lookback": 30}),
        ("spread_proxy", {}),
        ("volume_z", {"lookback": 60}),
    ]
    rows = []
    factor_matrix = {}
    for name, params in factor_specs:
        s = compute_factor(name, df, params=params)
        label = f"{name}({','.join(f'{k}={v}' for k,v in sorted(params.items()))})" if params else name
        tmp = pd.DataFrame({"factor": s, "fwd": fwd}).dropna()
        if len(tmp) < 100:
            continue
        q_low = tmp["factor"].quantile(0.2)
        q_high = tmp["factor"].quantile(0.8)
        low_ret = float(tmp.loc[tmp["factor"] <= q_low, "fwd"].mean())
        high_ret = float(tmp.loc[tmp["factor"] >= q_high, "fwd"].mean())
        spread = high_ret - low_ret
        ic = float(tmp["factor"].corr(tmp["fwd"], method="spearman"))
        rows.append({"factor": label, "high_bucket_ret": high_ret, "low_bucket_ret": low_ret, "top_bottom_spread": spread, "spearman_ic": ic, "samples": len(tmp)})
        factor_matrix[label] = tmp["factor"].reindex(df.index)

    edge_df = pd.DataFrame(rows).sort_values("top_bottom_spread", ascending=False)
    edge_df.to_csv(out_dir / "factor_edge_table.csv", index=False, encoding="utf-8-sig")

    mat = pd.DataFrame(factor_matrix).corr() if factor_matrix else pd.DataFrame()
    if not mat.empty:
        plt.figure(figsize=(10, 8))
        plt.imshow(mat.values, cmap="RdBu_r", vmin=-1, vmax=1)
        plt.xticks(range(len(mat.columns)), mat.columns, rotation=45, ha="right", fontsize=8)
        plt.yticks(range(len(mat.index)), mat.index, fontsize=8)
        plt.colorbar(fraction=0.046, pad=0.04)
        plt.title("TS Factor Correlation Heatmap")
        plt.tight_layout()
        plt.savefig(out_dir / "factor_corr_heatmap.png", dpi=160)
        plt.close()

    print(f"[factor_study] factors={len(edge_df)} out={out_dir}")


if __name__ == "__main__":
    main()
