"""Read-only assessment; never ticks a tracker or uses an order/private API."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from statistics import mean, median
import sys
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
OUT = Path(__file__).resolve().parent


def read(relative):
    path = ROOT / relative
    raw = path.read_bytes()
    sources[relative] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    return json.loads(raw)


def stats(rows, field):
    values = [float(row[field]) for row in rows]
    return {"n": len(values), "mean_pct": mean(values) if values else None,
            "median_pct": median(values) if values else None,
            "min_pct": min(values) if values else None,
            "max_pct": max(values) if values else None,
            "positive_n": sum(v > 0 for v in values),
            "sum_pct_not_equity_return": sum(values) if values else None,
            "mean_without_best_pct": mean(sorted(values)[:-1]) if len(values) > 1 else None}


sources = {}
from core.research.tracker_registry import tracker_rows
from core.research import next10xgem1_fade_tracker as fade
from core.research import xs_reversal_tracker as reversal

keep = ("id", "name", "n", "min_n", "unit", "mean_pct", "ci90_pct", "ci_clusters",
        "verdict", "verdict_label", "verdict_reason", "win_rate", "open", "waiting", "late",
        "started_at", "updated_at", "metric_kind", "basis_label", "cost_scope", "is_paper",
        "tracker_status_label", "completed_without_primary", "runner_active", "error")
loaded = tracker_rows()
rows = [{k: row.get(k) for k in keep} for row in loaded]
result = {"captured_at": datetime.now(timezone.utc).isoformat(), "read_only": True,
          "rows": rows, "verdict_counts": dict(Counter(row["verdict"] for row in rows)),
          "read_errors": [row["id"] for row in rows if row["error"]]}

# Reproduce the forward daily sample exactly as the production summary does.
state = read("data/research/xs_reversal/tracker.json")
daily = []
for day, record in sorted(state["days"].items()):
    if record.get("status") != "closed" or record.get("late") or record.get("missing_positions", 0):
        continue
    daily.append({"date": day, "baseline_pct": float(record["net_per_position_pct"]),
                  "shadow_pct": (float(record["liq_filter"]["net_per_position_pct"])
                                 if (record.get("liq_filter") or {}).get("status") == "closed"
                                 and not record["liq_filter"].get("missing_positions", 0) else None)})
paired = reversal.summary(state)["paired_evaluation"]
result["xs_reversal"] = {"stats": stats(daily, "baseline_pct"), "daily": daily,
                         "paired": paired,
                         "limits": "Daily ledger marks; no executable account equity or intraday drawdown certified."}

# The journal validates its hash chain/protocol; retain public numerical fields only.
journal = fade.Journal()
records, events = journal._state()
done = fade.completed(events)
public_events = []
for event in done:
    book = event["settlement"]["books"]["plain"]
    public_events.append({"event_id": event["event_id"], "symbol": event["symbol"],
                          "entry_at": event["entry"]["entry_at"],
                          "net_pct": 100 * book["net_price_only"],
                          "funded_pct": None if book.get("net_with_funding") is None else 100 * book["net_with_funding"],
                          "stress_pct": 100 * book["net_price_only_stress"],
                          "excess_pp": 100 * fade.excess(event),
                          "exit_reason": book["exit_reason"]})
summary = fade.summary(journal)
result["next10xgem1_v2"] = {"books": summary["books"], "collector_health": summary.get("collector_health"),
                           "last_error": summary.get("last_error"), "events": public_events,
                           "price_only_stats": stats(public_events, "net_pct"),
                           "excess_stats": stats(public_events, "excess_pp"),
                           "execution_authorized": summary["execution_authorized"]}

for ident, relative, field in [
    ("unlock_t7", "data/research/unlock_short_t7/tracker.json", "hedged_return_pct"),
    ("upbit_krw", "data/research/upbit_krw_listing/tracker.json", "return_pct"),
    ("korea_hedged", "data/research/korea_caution_hedged/tracker.json", "hedged_return_pct"),
]:
    state = read(relative)
    selected = []
    for key, record in state.get("trades", {}).items():
        if record.get("status") not in {"closed", "stopped"} or record.get("late") or record.get("backfilled"):
            continue
        if ident != "unlock_t7" and not record.get("symbol"):
            continue
        if record.get(field) is None:
            continue
        selected.append({"key": key, "symbol": record.get("symbol"), "entry_at": record.get("entry_at"),
                         "entry_day": record.get("entry_day"), "day0_ms": record.get("day0_ms"),
                         field: record[field]})
    result[ident] = {"stats": stats(selected, field), "events": selected}

health = {}
for endpoint in ("livez", "readyz"):
    try:
        with urlopen("http://127.0.0.1:8000/" + endpoint, timeout=12) as response:
            health[endpoint] = {"http": response.status, "body": json.load(response)}
    except Exception as error:
        health[endpoint] = {"error_type": type(error).__name__}
result["health"] = health

# Count public transport/permission errors without exporting URLs, addresses or raw logs.
log = ROOT / "logs/uvicorn_web_20261103_083703.err.log"
clean_log = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", log.read_text(encoding="utf-8", errors="replace"))
lines = clean_log.splitlines()
failure_lines = [line for line in lines if "Health check failed for " in line]
failures = []
for line in failure_lines:
    match = re.search(r"Health check failed for (\w+): (\w+)", line)
    if match:
        failures.append({"time_local": line[:23], "exchange": match[1], "error_type": match[2]})
gate = [line for line in lines if "Request IP not in whitelist" in line]
result["transport"] = {"since_local": "2026-11-03 08:37:03", "health_check_failures": failures,
                       "counts": dict(Counter(r["exchange"] for r in failures)),
                       "gate_whitelist_error_count": len(gate),
                       "gate_whitelist_last_local": gate[-1][:23] if gate else None,
                       "limits": "Error log counts, not continuous connectivity or order-fill certification."}
result["sources"] = sources
(OUT / "metrics.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
# A standard Data chart of common days, with no account-equity inference.
chart_rows = [{"date": row["date"], "原版": row["baseline_pct"], "剔除爆仓异常版": row["shadow_pct"]}
              for row in daily if row["shadow_pct"] is not None]
chart = {"id": "reversal-common-days", "title": "反转策略：22个共同交易日的纸面日收益",
         "height": 260,
         "description": "2026-10-12至2026-11-02，按多空总名义金额计算，已扣账本成本及资金费；不是可成交收益或账户净值。",
         "spec": {"type": "line", "x": "date", "y": ["原版", "剔除爆仓异常版"],
                  "xLabel": "交易日（UTC）", "yLabel": "纸面日收益（%）", "showLegend": True,
                  "valueFormat": {"style": "decimal", "maximumFractionDigits": 2},
                  "colors": {"原版": "#6975D9", "剔除爆仓异常版": "#159C84"}},
         "rows": chart_rows}
(OUT / "chart.json").write_text(json.dumps(chart, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

source_rows = [{"tracker": r["name"], "n": r["n"], "unit": r["unit"], "mean": r["mean_pct"],
                "metric": r["metric_kind"], "min_n": r["min_n"], "verdict": r["verdict"]} for r in rows]
evidence = {"schemaVersion": 1, "items": [
    {"id": "strategy-evidence", "title": "策略收益与样本是否足够", "queries": [
        {"id": "paper-snapshot", "source": {
            "label": "本地研究账本与冻结协议",
            "files": ["metrics.json", "audit.json", "analyze_readiness.py", "core/research/tracker_registry.py", "core/research/retirement.py"],
            "executedAt": result["captured_at"],
            "filters": ["各账本只取原协议允许的成熟前向样本", "历史补采、迟到及缺失主收益不算成熟样本", "v1停收与v2数据分别保留"],
            "metricDefinitions": [
                {"label": "反转净收益", "definition": "多空总名义金额为分母的纸面日收益，扣固定往返0.15%及旧账本资金费。"},
                {"label": "next10xgem1主统计", "definition": "20bps价格净收益减入场前30天同币对照均值，单位为百分点；含资金费收益另列。"},
                {"label": "解锁与韩国对冲收益", "definition": "单腿名义金额为分母，另有等额对冲腿；不是账户收益。"}],
            "caveats": ["27行的主统计和单位不同，不能按均值直接排名。", "小样本、同日事件和重叠组合削弱独立性；描述性区间不是实盘验收。", "旧账本的日线止损、资金费率求和与不完整篮子仍有限制。", "供给因子两个已关闭月各有一个成员缺价。" ]},
         "columns": ["tracker", "n", "unit", "mean", "metric", "min_n", "verdict"], "rows": source_rows,
         "preview": {"kind": "aggregate", "note": "27条跟踪器主统计，非逐笔交易或账户净值。", "totalRows": 27}},
        {"id": "reversal-common", "source": {
            "label": "反转原版与过滤影子的同日配对",
            "files": ["data/research/xs_reversal/tracker.json", "core/research/paired_evaluation.py", "analyze_readiness.py"],
            "filters": ["原版已closed，非late，无missing_positions", "影子已closed且无missing_positions", "仅比较同一交易日"],
            "caveats": ["22天只有4个日历周组；周聚类及HAC敏感性检查仍为insufficient_data。", "实际决策比账本入场价时刻晚；没有成交盘口、深度和真实填单回放。"],
            "metricDefinitions": [{"label": "影子增益", "definition": "同日过滤版纸面收益减原版纸面收益，单位为百分点/天。"}]},
         "reportingPeriod": "2026-10-12至2026-11-02（UTC交易日）",
         "columns": ["date", "原版", "剔除爆仓异常版"], "rows": chart_rows,
         "preview": {"kind": "aggregate", "note": "全部22个共同成熟日，每日按组合聚合。", "totalRows": len(chart_rows)}}]},
    {"id": "operational-readiness", "title": "系统运行健康与实盘限制", "queries": [
        {"id": "health-and-logs", "source": {
            "label": "本地健康接口、服务状态与当日日志",
            "files": ["metrics.json", "analyze_readiness.py", "logs/uvicorn_web_20261103_083703.err.log"],
            "executedAt": result["captured_at"],
            "filters": ["只读/livez和/readyz", "日志统计自2026-11-03 08:37启动以来的Health check failed", "只保留错误类型及时间，不导出网络地址或密钥"],
            "caveats": ["健康检查成功只代表读取时刻，不能证明全天连通或订单可靠成交。", "服务当前为paper；没有进行真实下单、撤单、止损和断线恢复验收。"],
            "evidenceFlow": [{"kind": "validation", "text": "livez与readyz均HTTP 200，Binance与Gate列为已连接，行情过期标的0。"},
                             {"kind": "result", "text": "同一启动日志中仍有交易所连接失败，Gate余额请求有IP白名单拒绝。"}]}}]}
]}
(OUT / "sources.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
print(json.dumps({"verdict_counts": result["verdict_counts"], "read_errors": result["read_errors"],
                  "xs": result["xs_reversal"]["stats"], "fade": result["next10xgem1_v2"]["books"],
                  "unlock_t7": result["unlock_t7"]["stats"], "upbit_krw": result["upbit_krw"]["stats"],
                  "korea_hedged": result["korea_hedged"]["stats"],
                  "transport_counts": result["transport"]["counts"],
                  "gate_whitelist_last_local": result["transport"]["gate_whitelist_last_local"]}, ensure_ascii=False))
