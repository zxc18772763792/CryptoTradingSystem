"""Read-only progress since the retained 2026-11-03 snapshot; no tick or orders."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from statistics import mean
import sys
from urllib.request import urlopen

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from core.research.tracker_registry import tracker_rows
from core.research import next10xgem1_fade_tracker as fade
from core.research import telegram_forward_tracker as telegram


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


started = datetime.now(timezone.utc).isoformat()
previous_path = ROOT / "reports/tracker_readiness_20261103/metrics.json"
previous = read(previous_path)
previous_rows = {r["id"]: r for r in previous["rows"]}
current = tracker_rows()
keys = ("id", "name", "n", "min_n", "unit", "mean_pct", "ci90_pct", "ci_clusters",
        "actual_mean_pct", "verdict", "verdict_label", "open", "waiting", "late", "started_at",
        "updated_at", "metric_kind", "basis_label", "cost_scope", "tracker_status_label",
        "runner_active", "pending_review", "completed_without_primary", "error")
result = {"read_started_at": started, "read_only": True, "previous_captured_at": previous["captured_at"],
          "previous_sha256": hashlib.sha256(previous_path.read_bytes()).hexdigest(),
          "rows": [{k: row.get(k) for k in keys} for row in current],
          "verdict_counts": dict(Counter(r.get("verdict") for r in current)), "changes": []}
for row in current:
    old = previous_rows.get(row["id"], {})
    result["changes"].append({"id": row["id"], "name": row["name"],
                              "old_n": old.get("n"), "n": row.get("n"),
                              "n_added": row.get("n", 0) - old.get("n", 0),
                              "old_mean": old.get("mean_pct"), "mean": row.get("mean_pct"),
                              "old_open": old.get("open"), "open": row.get("open"),
                              "old_status": old.get("tracker_status_label"), "status": row.get("tracker_status_label")})

journal = fade.Journal()
_, events = journal._state()
done = fade.completed(events)
old_events = {e["event_id"]: e for e in previous["next10xgem1_v2"]["events"]}
new_events = []
for event in done:
    if event["event_id"] in old_events:
        continue
    book = event["settlement"]["books"]["plain"]
    new_events.append({"event_id": event["event_id"], "symbol": event["symbol"],
                       "entry_at": event["entry"]["entry_at"], "net_pct": 100 * book["net_price_only"],
                       "funded_pct": None if book.get("net_with_funding") is None else 100 * book["net_with_funding"],
                       "excess_pp": 100 * fade.excess(event)})
fade_summary = fade.summary(journal)
result["fade"] = {"old_books": previous["next10xgem1_v2"]["books"], "books": fade_summary["books"],
                  "new_events": new_events, "collector_health": fade_summary["collector_health"],
                  "runner_active": fade_summary["runner_active"], "last_error": fade_summary["last_error"]}

xs = next(r for r in current if r["id"] == "xs_reversal")
paired = xs["paired_evaluation"]
result["reversal"] = {key: paired.get(key) for key in ("n_pairs", "baseline", "shadow", "increment", "sensitivity")}
result["reversal"]["old_paired"] = {key: previous["xs_reversal"]["paired"].get(key)
                                      for key in ("n_pairs", "baseline", "shadow", "increment")}
days = read(ROOT / "data/research/xs_reversal/tracker.json")["days"]
chart_rows = []
old_dates = {r["date"] for r in previous["xs_reversal"]["daily"]}
new_days = []
for day, record in sorted(days.items()):
    if record.get("status") != "closed" or record.get("late") or record.get("missing_positions", 0):
        continue
    shadow = record.get("liq_filter") or {}
    row = {"date": day, "原版": record["net_per_position_pct"],
           "过滤版": shadow.get("net_per_position_pct") if shadow.get("status") == "closed" and not shadow.get("missing_positions", 0) else None}
    if day not in old_dates:
        new_days.append(row)
    if row["过滤版"] is not None:
        chart_rows.append(row)
result["reversal"]["new_days"] = new_days

krw = read(ROOT / "data/research/upbit_krw_listing/tracker.json")
old_krw_keys = {r["key"] for r in previous["upbit_krw"]["events"]}
result["krw_new"] = [{"key": key, "symbol": r.get("symbol"), "return_pct": r["return_pct"],
                      "entry_at": r.get("entry_at"), "exit_at": r.get("exit_at")}
                     for key, r in krw["trades"].items() if key not in old_krw_keys
                     and r.get("status") in {"closed", "stopped"} and not r.get("backfilled")
                     and not r.get("late") and r.get("symbol") and r.get("return_pct") is not None]

result["telegram"] = {}
for kind in ("cryptoquant", "binance_chat"):
    summary = telegram.summary(kind)
    result["telegram"][kind] = {key: summary.get(key) for key in
                                ("runner_active", "tracker_status_label", "source_health", "last_error", "runner", "ledgers")}
result["telegram_runner"] = read(ROOT / "runtime/telegram_forward_runner.json")
result["health"] = {}
for endpoint in ("livez", "readyz"):
    try:
        with urlopen("http://127.0.0.1:8000/" + endpoint, timeout=10) as response:
            result["health"][endpoint] = {"http": response.status, "body": json.load(response)}
    except Exception as error:
        result["health"][endpoint] = {"error_type": type(error).__name__}
result["captured_at"] = datetime.now(timezone.utc).isoformat()
result["limits"] = ["Sequential read; live writers continue between files.",
                    "Paper results and paired increments retain each original denominator and protocol.",
                    "New mature samples are identified by retained public event keys, not sums across overlapping books."]
(OUT / "progress.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
chart = {"id": "reversal-progress-20261104", "title": f"反转策略：{len(chart_rows)}个共同成熟日的纸面日收益",
         "height": 240, "description": "按多空总名义金额计，沿用账本成本与资金费；不是可成交收益或账户净值。",
         "spec": {"type": "line", "x": "date", "y": ["原版", "过滤版"], "xLabel": "UTC交易日",
                  "yLabel": "纸面日收益（%）", "showLegend": True,
                  "colors": {"原版": "#6975D9", "过滤版": "#159C84"},
                  "valueFormat": {"style": "decimal", "maximumFractionDigits": 2}}, "rows": chart_rows}
(OUT / "chart.json").write_text(json.dumps(chart, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
print(json.dumps({"captured_at": result["captured_at"], "verdicts": result["verdict_counts"],
                  "changed_sample_rows": [r for r in result["changes"] if r["n_added"]],
                  "fade_books": result["fade"]["books"], "fade_new": new_events,
                  "reversal": {key: result["reversal"][key] for key in ("n_pairs", "baseline", "shadow", "increment", "new_days")},
                  "krw_new": result["krw_new"], "telegram": result["telegram"], "health": result["health"]}, ensure_ascii=False))
