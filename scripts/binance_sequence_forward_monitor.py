"""Read-only 24h launch observer for frozen forward-monitor candidates.

The observer never changes the original snapshot, score, or stage.  It writes
one immutable research observation per signal after six complete 4h bars are
available and records whether the secondary MFE20/close5 launch rule fired.
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
DEFAULT_OUTPUT = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
RULE = {
    "version": "launch-mfe20-close5-v1-frozen-2026-07-19",
    "checkpoint_hours": 24,
    "minimum_mfe": 0.20,
    "minimum_close_return": 0.05,
    "ranking_effect": False,
    "automatic_trading_allowed": False,
}


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


FORWARD = _load(
    "binance_forward_monitor_for_sequence_observer",
    ROOT / "scripts" / "binance_runup_forward_monitor.py",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="ISO-8601 cutoff; a date means 12:05 Asia/Shanghai")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--no-external-refresh", action="store_true")
    return parser.parse_args()


def as_of_timestamp(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    if len(value.strip()) == 10:
        return pd.Timestamp(f"{value.strip()} 12:05:00", tz="Asia/Shanghai").tz_convert("UTC")
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("Asia/Shanghai")
    return stamp.tz_convert("UTC")


def load_signals(output_root: Path) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    snapshots = output_root / "snapshots"
    for path in sorted(snapshots.glob("*.json")) if snapshots.exists() else []:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not payload.get("audit", {}).get("passed"):
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
            {
                "symbol": symbol,
                "interval": "4h",
                "endTime": int(as_of.timestamp() * 1000),
                "limit": 80,
            },
        )
        frame = FORWARD._normalize_klines(raw, symbol, symbol.removesuffix("USDT"))
        return symbol, frame, None
    except Exception as exc:  # noqa: BLE001 - recorded as research audit data
        return symbol, pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def load_panel(
    signals: list[dict[str, Any]],
    *,
    as_of: pd.Timestamp,
    baseline: Path,
    no_external_refresh: bool,
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    symbols = sorted({str(item["symbol"]) for item in signals})
    errors: dict[str, str] = {}
    if no_external_refresh:
        panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
        panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
        panel = panel[panel["open_time"] <= as_of]
        return {
            symbol: group.sort_values("open_time").reset_index(drop=True)
            for symbol, group in panel[panel["symbol"].isin(symbols)].groupby("symbol", sort=False)
        }, errors
    frames: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(symbols)))) as pool:
        futures = [pool.submit(fetch_symbol, symbol, as_of) for symbol in symbols]
        for future in as_completed(futures):
            symbol, frame, error = future.result()
            if not frame.empty:
                frames[symbol] = frame.sort_values("open_time").reset_index(drop=True)
            if error:
                errors[symbol] = error
    return frames, errors


def observe(signal: dict[str, Any], bars: pd.DataFrame, *, as_of: pd.Timestamp) -> dict[str, Any] | None:
    signal_time = pd.Timestamp(signal["data_cutoff_utc"])
    if signal_time.tzinfo is None:
        signal_time = signal_time.tz_localize("UTC")
    rows = bars.sort_values("open_time").copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    entry_rows = rows[rows["open_time"] >= signal_time]
    if entry_rows.empty:
        return None
    entry = entry_rows.iloc[0]
    entry_time = pd.Timestamp(entry["open_time"])
    decision_time = entry_time + pd.Timedelta(hours=24)
    if as_of < decision_time:
        return None
    observed = rows[(rows["open_time"] >= entry_time) & (rows["open_time"] < decision_time)]
    decision = rows[rows["open_time"] == decision_time]
    expected_times = pd.date_range(entry_time, periods=6, freq="4h", tz="UTC")
    if len(observed) != 6 or decision.empty:
        return None
    if list(observed["open_time"]) != list(expected_times):
        return None
    entry_price = float(entry["open"])
    mfe = float(observed["high"].max() / entry_price - 1.0)
    close_return = float(observed.iloc[-1]["close"] / entry_price - 1.0)
    launch_pass = bool(mfe >= RULE["minimum_mfe"] and close_return >= RULE["minimum_close_return"])
    base_stage = str(signal.get("stage") or "missing")
    paper_eligible = bool(launch_pass and base_stage == "price_watch")
    rule_hash = hashlib.sha256(json.dumps(RULE, sort_keys=True).encode("utf-8")).hexdigest()
    return {
        "schema_version": "1.0",
        "signal_id": str(signal["signal_id"]),
        "snapshot_date": str(signal["snapshot_date"]),
        "symbol": str(signal["symbol"]),
        "original_stage": base_stage,
        "original_score_pctile": float(signal["score_pctile"]),
        "entry_time_utc": entry_time,
        "entry_reference_price": entry_price,
        "decision_time_utc": decision_time,
        "decision_reference_price": float(decision.iloc[0]["open"]),
        "mfe_24h": mfe,
        "close_return_24h": close_return,
        "launch_status": "launch_confirmed" if launch_pass else "launch_rejected",
        "paper_eligible_after_original_stage": paper_eligible,
        "rule": RULE,
        "rule_hash": rule_hash,
        "ranking_changed": False,
        "automatic_trading_allowed": False,
        "paper_exit_annotation": {
            "hard_stop": 0.25,
            "breakeven_activation": 0.30,
            "partial_target": 1.00,
            "partial_fraction": 0.50,
            "trail_distance": 0.25,
            "maximum_hold_days": 30,
            "status": "secondary_forward_challenger_only",
        },
        "market_data_cutoff_utc": decision_time,
    }


def immutable_write(path: Path, payload: dict[str, Any]) -> str:
    encoded = json.dumps(FORWARD.json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise RuntimeError(f"Immutable observation exists with different content: {path}")
        return "idempotent_existing"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(encoded, encoding="utf-8")
    return "created"


def main() -> None:
    args = parse_args()
    output = args.output_root.resolve()
    as_of = as_of_timestamp(args.as_of)
    signals = load_signals(output)
    existing = {path.stem for path in (output / "sequence_24h").glob("*.json")} if (output / "sequence_24h").exists() else set()
    pending = [item for item in signals if str(item["signal_id"]) not in existing]
    panel, errors = load_panel(
        pending,
        as_of=as_of,
        baseline=args.baseline_dir.resolve(),
        no_external_refresh=args.no_external_refresh,
    )
    observations: list[dict[str, Any]] = []
    for signal in pending:
        bars = panel.get(str(signal["symbol"]))
        if bars is None:
            continue
        item = observe(signal, bars, as_of=as_of)
        if item is not None:
            observations.append(item)
    if args.dry_run:
        print(
            json.dumps(
                FORWARD.json_ready(
                    {
                        "as_of_utc": as_of,
                        "pending_signals": len(pending),
                        "source_errors": errors,
                        "observations": observations,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    statuses = []
    for item in observations:
        path = output / "sequence_24h" / f"{item['signal_id']}.json"
        statuses.append({"signal_id": item["signal_id"], "status": immutable_write(path, item)})
    audit = {
        "as_of_utc": as_of,
        "signals_total": len(signals),
        "pending_before_run": len(pending),
        "observations_written": len(observations),
        "source_error_count": len(errors),
        "source_errors": errors,
        "statuses": statuses,
        "read_only_no_order_routing": True,
    }
    print(json.dumps(FORWARD.json_ready(audit), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
