"""Minute-level entry timing for fading @next10xgem1 calls (companion to research_next10xgem1_fade.py).

The hourly study enters at the next full hour after a call is available (about 30 minutes later
on average), so it cannot see what happens in the first minutes. This fetches official Binance
USD-M 5m klines around each call (availability -2h to +26h, one public request per call, cached
in the restricted study folder) and compares shorts opened 1, 5, 10, 15, 30 and 60 minutes
after availability and at the hourly study's entry, all held 24h. Exploratory, same accounting as
the hourly study: (entry-exit)/entry, a high at 2x entry = -100%, 20bps + actual funding.

    python scripts/research_next10xgem1_minutes.py fetch      # cached; resumes
    python scripts/research_next10xgem1_minutes.py evaluate
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.research_next10xgem1_fade import CHANNEL, OUT, REPORT, json_ready  # noqa: E402
from scripts.research_telegram_new_kols import STUDY, load_bars, sha, signal_rows  # noqa: E402
from scripts.review_telegram_signals import BASE_COST, SymbolPanel, entry_hour, week_bootstrap  # noqa: E402

CACHE = OUT / "official_5m"
ENDPOINT = "https://fapi.binance.com/fapi/v1/klines"
STEP = pd.Timedelta(minutes=5)
BEFORE = pd.Timedelta(hours=2)
SPAN_BARS = 28 * 12           # -2h .. +26h
DELAY_MINUTES = (1, 5, 10, 15, 30, 60)  # +60m entry + 24h hold is the window edge
HOLD = pd.Timedelta(hours=24)
PATH_MINUTES = range(-120, 241, 5)


def calls(study: Path = STUDY) -> list[dict]:
    sealed = json.loads((study / "events_before_prices.json").read_text(encoding="utf-8"))
    rows = [s for s in signal_rows(sealed) if s["channel"] == CHANNEL and s["group"] == "fresh_call"
            and not s["late_edit"] and s["venue"] == "futures" and s["side"] == 1]
    for s in rows:
        s["available"] = pd.Timestamp(s["available_at"]).tz_convert("UTC")
        s["window_start"] = (s["available"] - BEFORE).floor("5min")
    return sorted(rows, key=lambda r: (r["available"], r["signal_id"]))


def cache_path(symbol: str, start: pd.Timestamp) -> Path:
    return CACHE / f"{symbol}_{int(start.timestamp())}.json"


def fetch(max_requests: int = 400) -> dict:
    import requests

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env
    ensure_proxy_env(settings)
    CACHE.mkdir(parents=True, exist_ok=True)
    done = fetched = errors = 0
    with requests.Session() as session:
        session.headers["User-Agent"] = "crypto-trading-system-public-research/1.0"
        for s in calls():
            path = cache_path(s["symbol"], s["window_start"])
            if path.exists() and json.loads(path.read_text(encoding="utf-8")).get("status") == "ok":
                done += 1
                continue
            if fetched + errors >= max_requests:
                break
            start_ms = int(s["window_start"].timestamp() * 1000)
            params = {"symbol": f"{s['symbol']}USDT", "interval": "5m", "startTime": start_ms,
                      "endTime": start_ms + (SPAN_BARS - 1) * 300_000, "limit": SPAN_BARS}
            item = {"endpoint": ENDPOINT, "params": params, "requested_at": datetime.now(timezone.utc).isoformat()}
            try:
                response = session.get(ENDPOINT, params=params, timeout=(10, 30))
                if response.status_code in (418, 429):
                    raise RuntimeError(f"rate limited ({response.status_code}); cache kept, rerun later")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("non-list kline response")
                item.update(status="ok", rows=payload, response_sha256=hashlib.sha256(response.content).hexdigest())
                fetched += 1
            except (requests.RequestException, ValueError) as exc:
                item.update(status="error", error_type=type(exc).__name__)
                errors += 1
            path.write_text(json.dumps(item), encoding="utf-8")
            time.sleep(.3)
    return {"cached_before": done, "fetched": fetched, "errors": errors}


def frame_for(s: dict) -> pd.DataFrame:
    path = cache_path(s["symbol"], s["window_start"])
    if not path.exists():
        return pd.DataFrame()
    item = json.loads(path.read_text(encoding="utf-8"))
    if item.get("status") != "ok" or not item["rows"]:
        return pd.DataFrame()
    rows = [[pd.Timestamp(x[0], unit="ms", tz="UTC"), *map(float, x[1:5])] for x in item["rows"]]
    frame = pd.DataFrame(rows, columns=["at", "open", "high", "low", "close"]).set_index("at")
    frame = frame[~frame.index.duplicated()].sort_index()
    grid = pd.date_range(s["window_start"], periods=SPAN_BARS, freq=STEP)
    return frame.reindex(grid)


def funding_paid(panel: SymbolPanel, entry: pd.Timestamp, exit_: pd.Timestamp) -> float | None:
    """Settlements stamped in [entry, exit): a position open at hh:00 pays that hour's settlement."""
    first, last = entry.ceil("h"), (exit_ - pd.Timedelta(seconds=1)).floor("h")
    a, b = panel.position(first), panel.position(last)
    if a is None or b is None:
        return None
    if b < a:
        return 0.0
    if not panel.funding_known[a:b + 1].all():
        return None
    return float(panel.fund[a:b + 1].sum())  # short receives positive funding


def short_24h(frame: pd.DataFrame, entry: pd.Timestamp, panel: SymbolPanel) -> float | None:
    exit_ = entry + HOLD
    if entry not in frame.index or exit_ not in frame.index:
        return None
    path = frame.loc[entry:exit_]
    e, x = path["open"].iloc[0], path["open"].iloc[-1]
    if not (np.isfinite(e) and np.isfinite(x)) or path["high"].iloc[:-1].isna().any():
        return None
    paid = funding_paid(panel, entry, exit_)
    if paid is None:
        return None
    gross = -1.0 if path["high"].iloc[:-1].max() >= 2 * e else max(-1.0, (e - x) / e)
    return max(-1.0, gross - BASE_COST + paid)


def evaluate(out: Path = OUT, report: Path = REPORT) -> dict:
    rows, paths, status = [], [], {"calls": 0, "no_5m": 0, "incomplete": 0}
    panels: dict[str, SymbolPanel] = {}
    busy: dict[str, pd.Timestamp] = {}
    for s in calls():
        status["calls"] += 1
        frame = frame_for(s)
        if frame.empty or frame["open"].notna().sum() < SPAN_BARS // 2:
            status["no_5m"] += 1
            continue
        if s["symbol"] not in panels:
            panels[s["symbol"]] = SymbolPanel(*load_bars(s["symbol"], "futures"))
        panel = panels[s["symbol"]]
        if s["available"] < busy.get(s["symbol"], s["available"] - STEP):
            continue  # one open position per coin, as in the hourly study
        hourly = entry_hour(s["available"])
        entries = {f"m{d}": (s["available"] + pd.Timedelta(minutes=d)).ceil("5min") for d in DELAY_MINUTES}
        entries["next_hour"] = hourly
        values = {k: short_24h(frame, t, panel) for k, t in entries.items()}
        if any(v is None for v in values.values()):
            status["incomplete"] += 1
            continue
        busy[s["symbol"]] = max(entries.values()) + HOLD
        anchor = s["available"].floor("5min")
        base = frame["open"].get(anchor)
        rows.append({"signal_id": s["signal_id"], "symbol": s["symbol"], "available": s["available"],
                     "entry_hour": hourly, "minutes_to_hour": (hourly - s["available"]).total_seconds() / 60,
                     "move_to_hour_entry": float(frame["open"].get(hourly) / base - 1) if base else None, **values})
        if base and np.isfinite(base):
            paths.append([float(frame["open"].get(anchor + pd.Timedelta(minutes=m), np.nan) / base - 1)
                          for m in PATH_MINUTES])
    table = pd.DataFrame(rows)
    stamps = [pd.Timestamp(t) for t in table["available"]]
    by_delay = {}
    for key in [f"m{d}" for d in DELAY_MINUTES] + ["next_hour"]:
        v = table[key].to_numpy(float)
        diff = v - table["next_hour"].to_numpy(float)
        by_delay[key] = {"n": int(len(v)), "mean": float(v.mean()), "median": float(np.median(v)),
                         "hit": float((v > 0).mean()), "week_ci": week_bootstrap(v, stamps),
                         "minus_next_hour": float(diff.mean()),
                         "minus_next_hour_week_ci": week_bootstrap(diff, stamps) if key != "next_hour" else None}
    p = np.array(paths)
    result = {"evaluated_at_utc": datetime.now(timezone.utc).isoformat(), "script_sha256": sha(Path(__file__).read_bytes()),
              "status": status, "independent": int(len(table)),
              "minutes_to_hour_entry": {"median": float(table["minutes_to_hour"].median()),
                                        "mean": float(table["minutes_to_hour"].mean())},
              "move_from_availability_to_hour_entry": {"mean": float(table["move_to_hour_entry"].mean()),
                                                       "median": float(table["move_to_hour_entry"].median())},
              "by_entry": by_delay,
              "path_5m": {"minutes": list(PATH_MINUTES), "n": int(len(p)), "mean": np.nanmean(p, 0).tolist(),
                          "median": np.nanmedian(p, 0).tolist()},
              "rules": {"entry": "first 5m open >= availability + d minutes; next_hour = the hourly study's entry",
                        "exit": "5m open 24h after entry", "dedup": "one open position per coin",
                        "cost": BASE_COST, "funding": "local settlements stamped in [entry, exit)"}}
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "minutes_trades.csv", index=False, encoding="utf-8-sig")
    report.mkdir(parents=True, exist_ok=True)
    (report / "minutes_summary.json").write_text(json.dumps(json_ready(result), ensure_ascii=False, indent=2,
                                                            default=str), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("fetch", "evaluate"))
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    result = fetch() if args.command == "fetch" else evaluate()
    shown = result if args.command == "fetch" else {k: result[k] for k in
                                                     ("status", "independent", "minutes_to_hour_entry",
                                                      "move_from_availability_to_hour_entry", "by_entry")}
    print(json.dumps(json_ready(shown), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
