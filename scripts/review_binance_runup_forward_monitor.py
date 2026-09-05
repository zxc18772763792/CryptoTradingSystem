"""Generate frozen-model 30/60/90-day OOS reviews when checkpoints are due."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
CHECKPOINTS = (30, 60, 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="Local Asia/Shanghai review date")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--if-due", action="store_true")
    return parser.parse_args()


def local_date(value: str | None) -> pd.Timestamp:
    if value:
        return pd.Timestamp(value).normalize()
    return pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None).normalize()


def load_snapshot_rows(snapshot_paths: list[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    for path in snapshot_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        audit = dict(payload.get("audit", {}))
        audit["snapshot_date"] = path.stem
        audits.append(audit)
        candidates = {item["signal_id"] for item in payload.get("candidates", [])}
        for item in payload.get("universe_rows", payload.get("candidates", [])):
            rows.append(
                {
                    "signal_id": item["signal_id"],
                    "symbol": item["symbol"],
                    "score": item["score"],
                    "score_pctile": item["score_pctile"],
                    "stage": item["stage"],
                    "is_daily_candidate": item["signal_id"] in candidates,
                    "data_cutoff_utc": item["data_cutoff_utc"],
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(audits)


def event_capture(rows: pd.DataFrame) -> dict[str, Any]:
    positive = rows[rows["hit_200pct"]].sort_values(["symbol", "entry_time_utc"])
    events: list[dict[str, Any]] = []
    for symbol, group in positive.groupby("symbol"):
        cluster: list[pd.Series] = []
        previous: pd.Timestamp | None = None
        for _, row in group.iterrows():
            when = pd.Timestamp(row["entry_time_utc"])
            if previous is not None and when - previous > pd.Timedelta(days=14):
                frame = pd.DataFrame(cluster)
                events.append({"symbol": symbol, "captured": bool((frame["score_pctile"] >= 0.98).any())})
                cluster = []
            cluster.append(row)
            previous = when
        if cluster:
            frame = pd.DataFrame(cluster)
            events.append({"symbol": symbol, "captured": bool((frame["score_pctile"] >= 0.98).any())})
    event_frame = pd.DataFrame(events)
    return {
        "independent_events": int(len(event_frame)),
        "captured_events": int(event_frame["captured"].sum()) if not event_frame.empty else 0,
        "event_capture_rate": float(event_frame["captured"].mean()) if not event_frame.empty else None,
    }


def build_review(checkpoint: int, rows: pd.DataFrame, audits: pd.DataFrame, outcomes: pd.DataFrame) -> dict[str, Any]:
    primary = outcomes[outcomes["horizon"].astype(int) == 14].copy()
    merged = rows.merge(primary, on="signal_id", how="inner", validate="one_to_one")
    target = merged["hit_200pct"].astype(bool)
    selected = target[merged["score_pctile"] >= 0.98]
    base_rate = float(target.mean()) if len(target) else np.nan
    precision = float(selected.mean()) if len(selected) else np.nan
    metrics = {
        "labeled_rows_14d": int(len(merged)),
        "positive_rows_14d": int(target.sum()),
        "base_rate": base_rate,
        "top_2pct_rows": int(len(selected)),
        "top_2pct_precision": precision,
        "top_2pct_lift": precision / base_rate if base_rate > 0 and np.isfinite(precision) else None,
        "auc": float(roc_auc_score(target, merged["score"])) if target.nunique() == 2 else None,
        "average_precision": float(average_precision_score(target, merged["score"])) if target.nunique() == 2 else None,
    }
    candidate_outcomes = merged[merged["is_daily_candidate"]]
    return {
        "checkpoint_days": checkpoint,
        "classification": "true_forward_oos_frozen_model",
        "price_metrics": metrics,
        "event_metrics": event_capture(merged),
        "candidate_rows": int(len(candidate_outcomes)),
        "candidate_hit_rate": float(candidate_outcomes["hit_200pct"].mean()) if len(candidate_outcomes) else None,
        "snapshot_days": int(len(audits)),
        "quality_pass_rate": float(audits["passed"].mean()) if not audits.empty else None,
        "failed_snapshot_days": audits.loc[~audits["passed"].astype(bool), "snapshot_date"].tolist() if not audits.empty else [],
        "model_refit": False,
        "threshold_changed": False,
    }


def main() -> None:
    args = parse_args()
    output = args.output_root.resolve()
    snapshots = sorted((output / "snapshots").glob("*.json"))
    if not snapshots:
        print(json.dumps({"status": "no_snapshots"}))
        return
    today = local_date(args.as_of)
    first = pd.Timestamp(snapshots[0].stem)
    elapsed = int((today - first).days)
    due = [checkpoint for checkpoint in CHECKPOINTS if elapsed >= checkpoint]
    if not due:
        print(json.dumps({"status": "not_due", "elapsed_days": elapsed, "next_checkpoint": CHECKPOINTS[0]}))
        return
    outcomes_path = output / "outcomes.csv"
    if not outcomes_path.exists():
        print(json.dumps({"status": "outcomes_not_ready", "elapsed_days": elapsed}))
        return
    rows, audits = load_snapshot_rows(snapshots)
    outcomes = pd.read_csv(outcomes_path)
    reviews_dir = output / "reviews"
    reviews_dir.mkdir(parents=True, exist_ok=True)
    generated = []
    for checkpoint in due:
        json_path = reviews_dir / f"day_{checkpoint:02d}_review.json"
        if args.if_due and json_path.exists():
            continue
        review = build_review(checkpoint, rows, audits, outcomes)
        json_path.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
        metric = review["price_metrics"]
        markdown = (
            f"# Frozen model OOS review: day {checkpoint}\n\n"
            f"- Classification: true forward OOS; no refit and no threshold change\n"
            f"- 14d labeled rows: {metric['labeled_rows_14d']}\n"
            f"- Top-2% precision: {metric['top_2pct_precision']}\n"
            f"- Top-2% lift: {metric['top_2pct_lift']}\n"
            f"- Independent event capture: {review['event_metrics']['event_capture_rate']}\n"
            f"- Snapshot quality pass rate: {review['quality_pass_rate']}\n"
        )
        (reviews_dir / f"day_{checkpoint:02d}_review.md").write_text(markdown, encoding="utf-8")
        generated.append(checkpoint)
    print(json.dumps({"status": "complete", "elapsed_days": elapsed, "generated": generated}))


if __name__ == "__main__":
    main()
