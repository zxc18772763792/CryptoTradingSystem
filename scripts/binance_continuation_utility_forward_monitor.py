"""Read-only 8h continuation-utility observer for frozen Binance candidates.

Only two completed 4h bars after P0 are used.  The record is actionable as a
paper observation only when captured within 15 minutes of the 8h boundary.
It never changes the primary price rank and has no order-routing dependency.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.research.binance_continuation_utility import FrozenUtilityModel
from core.research.binance_sequential_validation import checkpoint_row


DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
MAXIMUM_LAG_MINUTES = 15
TAKER_FLOW_FILTER_NAME = "taker_buy_share_ge_50pct_at_8h"
TAKER_BUY_SHARE_THRESHOLD = 0.50


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


FORWARD = _load(
    "binance_forward_monitor_for_continuation_utility",
    ROOT / "scripts" / "binance_runup_forward_monitor.py",
)


def utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO-8601 cutoff; a date means 20:05 Asia/Shanghai")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--maximum-lag-minutes", type=int, default=MAXIMUM_LAG_MINUTES)
    parser.add_argument("--no-external-refresh", action="store_true")
    return parser.parse_args()


def as_of_timestamp(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    if len(value.strip()) == 10:
        return pd.Timestamp(f"{value.strip()} 20:05:00", tz="Asia/Shanghai").tz_convert("UTC")
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("Asia/Shanghai")
    return stamp.tz_convert("UTC")


def load_model(report_dir: Path) -> tuple[FrozenUtilityModel, str, dict[str, Any]]:
    payload = json.loads((report_dir / "continuation_utility_forward_model.json").read_text(encoding="utf-8"))
    return FrozenUtilityModel.from_dict(payload["model"]), str(payload["model_hash"]), payload


def load_signals(output_root: Path) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    folder = output_root / "snapshots"
    for path in sorted(folder.glob("*.json")) if folder.exists() else []:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("audit", {}).get("passed") is not True:
            continue
        for item in payload.get("candidates", []):
            row = dict(item)
            row["snapshot_date"] = path.stem
            signals.append(row)
    return signals


def fetch_symbol(symbol: str, as_of: pd.Timestamp) -> tuple[str, pd.DataFrame, str | None]:
    try:
        raw = FORWARD._request_json(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "4h", "endTime": int(as_of.timestamp() * 1000), "limit": 80},
        )
        frame = FORWARD._normalize_klines(raw, symbol, symbol.removesuffix("USDT"))
        return symbol, frame, None
    except Exception as exc:  # noqa: BLE001 - recorded in read-only audit
        return symbol, pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def path_features(signal: dict[str, Any], bars: pd.DataFrame, *, checkpoint_hours: int) -> dict[str, Any] | None:
    rows = bars.sort_values("open_time").reset_index(drop=True).copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    cutoff = utc(signal["data_cutoff_utc"])
    entries = rows[rows["open_time"] >= cutoff]
    if entries.empty:
        return None
    entry = entries.iloc[0]
    entry_time = pd.Timestamp(entry["open_time"])
    prior = rows[rows["open_time"] < entry_time]
    if prior.empty:
        return None
    feature_date = signal.get("source_timestamps", {}).get("feature_date_utc") or signal["snapshot_date"]
    research_signal = {
        "symbol": str(signal["symbol"]),
        "date": utc(feature_date),
        "fold": -1,
        "signal_key": -1,
        "entry_time": entry_time,
        "entry_open": float(entry["open"]),
        "close": float(prior.iloc[-1]["close"]),
        "price_model_score": float(signal["score"]),
        "price_model_pctile": float(signal["score_pctile"]),
    }
    return checkpoint_row(
        research_signal,
        rows,
        horizon_hours=checkpoint_hours,
        require_future_labels=False,
    )


def immutable_write(path: Path, payload: dict[str, Any]) -> str:
    encoded = json.dumps(FORWARD.json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise RuntimeError(f"Immutable utility observation exists with different content: {path}")
        return "idempotent_existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return "created"


def apply_daily_limit(rows: pd.DataFrame, selected_column: str, score_column: str) -> pd.Series:
    selected = pd.Series(False, index=rows.index, dtype=bool)
    candidates = rows[rows[selected_column]].copy()
    if candidates.empty:
        return selected
    keep = candidates.sort_values(score_column, ascending=False).head(3).index
    selected.loc[keep] = True
    return selected


def taker_flow_annotation(features: dict[str, Any]) -> dict[str, Any]:
    value = features.get("early_taker_buy_share")
    observed = None if value is None or pd.isna(value) else float(value)
    return {
        "name": TAKER_FLOW_FILTER_NAME,
        "early_taker_buy_share": observed,
        "threshold": TAKER_BUY_SHARE_THRESHOLD,
        "supportive": bool(observed is not None and observed >= TAKER_BUY_SHARE_THRESHOLD),
        "evidence_role": "prospective_identification_annotation_not_promoted",
        "changes_primary_rank": False,
        "changes_stage_veto": False,
        "changes_paper_eligibility": False,
    }


def main() -> None:
    args = parse_args()
    output = args.output_root.resolve()
    report = args.report_dir.resolve()
    as_of = as_of_timestamp(args.as_of)
    model, model_hash, model_payload = load_model(report)
    existing = {path.stem for path in (output / "continuation_utility_8h").glob("*.json")} if (output / "continuation_utility_8h").exists() else set()
    terminal: set[str] = set()
    failed_folder = output / "continuation_utility_8h_failed"
    for path in sorted(failed_folder.glob("*.json")) if failed_folder.exists() else []:
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
            if item.get("terminal") is True:
                terminal.add(str(item["signal_id"]))
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    signals = [item for item in load_signals(output) if str(item["signal_id"]) not in existing and str(item["signal_id"]) not in terminal]

    pending: list[dict[str, Any]] = []
    due: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    for signal in signals:
        expected_entry = utc(signal["data_cutoff_utc"]).ceil("4h")
        decision = expected_entry + pd.Timedelta(hours=model.checkpoint_hours)
        lag = float((as_of - decision).total_seconds())
        if lag < 0:
            pending.append(signal)
        elif lag <= args.maximum_lag_minutes * 60:
            due.append(signal)
        else:
            stale.append(signal)

    frames: dict[str, pd.DataFrame] = {}
    source_errors: dict[str, str] = {}
    if not args.no_external_refresh:
        symbols = sorted({str(item["symbol"]) for item in due})
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(symbols)))) as pool:
            futures = [pool.submit(fetch_symbol, symbol, as_of) for symbol in symbols]
            for future in as_completed(futures):
                symbol, frame, error = future.result()
                if not frame.empty:
                    frames[symbol] = frame
                if error:
                    source_errors[symbol] = error

    observations: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for signal in stale:
        expected_entry = utc(signal["data_cutoff_utc"]).ceil("4h")
        decision = expected_entry + pd.Timedelta(hours=model.checkpoint_hours)
        failures.append(
            {
                "signal_id": str(signal["signal_id"]),
                "symbol": str(signal["symbol"]),
                "decision_time_utc": decision,
                "capture_time_utc": as_of,
                "capture_lag_seconds": float((as_of - decision).total_seconds()),
                "failure": "capture_clock_outside_allowed_window",
                "terminal": True,
                "fail_closed": True,
            }
        )
    feature_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for signal in due:
        if args.no_external_refresh:
            failures.append({"signal_id": str(signal["signal_id"]), "symbol": str(signal["symbol"]), "failure": "external_refresh_disabled", "terminal": False, "fail_closed": True})
            continue
        frame = frames.get(str(signal["symbol"]))
        if frame is None:
            failures.append({"signal_id": str(signal["signal_id"]), "symbol": str(signal["symbol"]), "failure": source_errors.get(str(signal["symbol"]), "missing_4h_path"), "terminal": False, "fail_closed": True})
            continue
        features = path_features(signal, frame, checkpoint_hours=model.checkpoint_hours)
        if features is None:
            failures.append({"signal_id": str(signal["signal_id"]), "symbol": str(signal["symbol"]), "failure": "incomplete_checkpoint_path", "terminal": False, "fail_closed": True})
            continue
        feature_rows.append((signal, features))

    if feature_rows:
        feature_frame = pd.DataFrame([features for _, features in feature_rows])
        scores = model.predict_score(feature_frame)
        scored: list[dict[str, Any]] = []
        for (signal, features), score in zip(feature_rows, scores, strict=True):
            scored.append({"signal": signal, "features": features, "utility_score": float(score), "selected_raw": bool(score >= model.selection_threshold)})
        rank_frame = pd.DataFrame(
            {
                "signal_id": [str(item["signal"]["signal_id"]) for item in scored],
                "selected_raw": [bool(item["selected_raw"]) for item in scored],
                "utility_score": [float(item["utility_score"]) for item in scored],
            }
        )
        selected_ids = set(rank_frame.loc[apply_daily_limit(rank_frame, "selected_raw", "utility_score"), "signal_id"])
        for item in scored:
            signal = item["signal"]
            selected = str(signal["signal_id"]) in selected_ids
            original_stage = str(signal.get("stage") or "missing")
            decision = utc(item["features"]["decision_time"])
            taker_annotation = taker_flow_annotation(item["features"])
            observations.append(
                {
                    "schema_version": "1.0",
                    "signal_id": str(signal["signal_id"]),
                    "snapshot_date": str(signal["snapshot_date"]),
                    "symbol": str(signal["symbol"]),
                    "original_stage": original_stage,
                    "original_score_pctile": float(signal["score_pctile"]),
                    "entry_reference_time_utc": utc(item["features"]["baseline_entry_time"]),
                    "entry_reference_open": float(item["features"]["baseline_entry_open"]),
                    "decision_time_utc": decision,
                    "decision_reference_open": float(item["features"]["decision_open"]),
                    "capture_time_utc": as_of,
                    "capture_lag_seconds": float((as_of - decision).total_seconds()),
                    "utility_score": item["utility_score"],
                    "utility_threshold": float(model.selection_threshold),
                    "utility_selected_raw": bool(item["selected_raw"]),
                    "utility_selected_daily_top3": selected,
                    "paper_eligible_after_original_stage": bool(selected and original_stage == "price_watch"),
                    "path_features": {name: item["features"].get(name) for name in model.features},
                    "taker_flow_annotation": taker_annotation,
                    "taker_flow_supportive": taker_annotation["supportive"],
                    "model_version": model.version,
                    "model_hash": model_hash,
                    "model_forward_role": model_payload["forward_role"],
                    "primary_ranking_changed": False,
                    "secondary_paper_gate": True,
                    "automatic_trading_allowed": False,
                    "read_only_no_order_routing": True,
                    "paper_exit_annotation": {
                        "hard_stop": 0.25,
                        "breakeven_activation": 0.30,
                        "partial_target": 1.00,
                        "partial_fraction": 0.50,
                        "trail_distance": 0.25,
                        "maximum_hold_days": 30,
                    },
                }
            )

    audit = {
        "as_of_utc": as_of,
        "model_version": model.version,
        "model_hash": model_hash,
        "signals_unobserved": len(signals),
        "terminal_previously_audited": len(terminal),
        "pending_count": len(pending),
        "due_count": len(due),
        "stale_count": len(stale),
        "observations_ready": len(observations),
        "paper_eligible_count": int(sum(item["paper_eligible_after_original_stage"] for item in observations)),
        "failed_count": len(failures),
        "failures": failures,
        "source_errors": source_errors,
        "primary_ranking_changed": False,
        "read_only_no_order_routing": True,
    }
    if args.dry_run:
        audit["observations"] = observations
        print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))
        return
    statuses = []
    for item in observations:
        statuses.append({"signal_id": item["signal_id"], "status": immutable_write(output / "continuation_utility_8h" / f"{item['signal_id']}.json", item)})
    slug = as_of.strftime("%Y%m%dT%H%M%SZ")
    for item in failures:
        statuses.append({"signal_id": item["signal_id"], "status": immutable_write(output / "continuation_utility_8h_failed" / f"{item['signal_id']}-{slug}.json", item)})
    audit["statuses"] = statuses
    print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
