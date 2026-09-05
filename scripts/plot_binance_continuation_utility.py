"""Render the frozen 8h continuation-utility evidence figure."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BLUE = "#3B6FB6"
GOLD = "#C7922B"
ORANGE = "#D97732"
INK = "#20262E"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = args.report_dir.resolve()
    decision = json.loads((report / "continuation_entry_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(report / "continuation_utility_calibration.csv")
    folds = pd.read_csv(report / "continuation_utility_strategy_folds.csv")
    equity = pd.read_csv(report / "continuation_utility_strategy_equity.csv.gz", compression="gzip")
    equity = equity[equity["slippage_bps_each_side"] == 5.0].copy()
    equity["date"] = pd.to_datetime(equity["date"], utc=True)

    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.2))
    for quantile, group in calibration.groupby("selection_quantile"):
        group = group.sort_values("checkpoint_hours")
        axes[0].plot(
            group["checkpoint_hours"], 100 * group["selected_expectancy_14d"],
            marker="o", label=f"Top {int(round((1 - quantile) * 100))}%",
        )
    axes[0].scatter([8], [100 * float(calibration.iloc[0]["selected_expectancy_14d"])], color=ORANGE, s=90, zorder=5)
    axes[0].set(title="Calibration entry utility", xlabel="Checkpoint after P0 (hours)", ylabel="14d expectancy (%)")
    axes[0].legend(frameon=False, fontsize=8)

    colors = [BLUE if value > 1 else ORANGE for value in folds["profit_factor"]]
    axes[1].bar(folds["fold"].astype(str), folds["profit_factor"], color=colors)
    axes[1].axhline(1.0, color=INK, linewidth=1)
    axes[1].set(title="Historical confirmation by fold", xlabel="Walk-forward fold", ylabel="Profit factor")
    for _, row in folds.iterrows():
        axes[1].text(str(int(row["fold"])), float(row["profit_factor"]) + 0.08, f"{100 * row['expectancy']:.1f}%", ha="center", fontsize=8)

    axes[2].plot(equity["date"], equity["equity"], color=BLUE, linewidth=2)
    axes[2].fill_between(equity["date"], equity["equity"], 100_000, color=BLUE, alpha=0.12)
    axes[2].axhline(100_000, color=INK, linewidth=1)
    axes[2].set(title="Constrained 1x paper equity", xlabel="Date", ylabel="Equity (USDT)")
    axes[2].xaxis.set_major_locator(mdates.MonthLocator())
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%b"))

    for axis in axes:
        axis.grid(axis="y", color="#E5E7EB")
        axis.spines[["top", "right"]].set_visible(False)
    utility = decision["entry_utility"]
    fig.suptitle(
        f"Frozen 8h utility gate: PF {utility['strategy_default_summary']['profit_factor']:.2f}, "
        f"expectancy {100 * utility['strategy_default_summary']['expectancy']:.1f}% — historical evidence only",
        fontsize=14,
    )
    fig.tight_layout()
    output = report / "charts" / "continuation_utility_results.png"
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(output)


if __name__ == "__main__":
    main()
