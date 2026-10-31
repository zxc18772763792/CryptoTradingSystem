"""Build safe, source-backed chart inputs from a frozen event-count study."""
from __future__ import annotations

import argparse
from datetime import timedelta, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.telegram_event_sentiment import utc


def write(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data/research/binance_chat_event_time_20261031")
    parser.add_argument("--visual-output", type=Path, required=True)
    args = parser.parse_args()
    base, output = args.source.resolve(), args.visual_output.resolve()
    result = json.loads((base / "results.json").read_text(encoding="utf-8"))
    source = result["source"]
    output.mkdir(parents=True, exist_ok=True)
    labels = lambda c: f"每{c['size']}条" + ("群消息" if c["clock"] == "messages" else "有效观点")
    rows = [{"口径": labels(c), "候选窗口": c["qualified_batches"], "完整批次": c["complete_batches"],
             "可确认连续覆盖的候选": c["certified_qualified_batches"]} for c in result["configs"]]
    local = lambda at: utc(at).astimezone(timezone(timedelta(hours=8))).strftime("%m-%d %H:%M")
    chart = {"id": "binance-chat-count-sentiment", "title": "按信息条数聚合，可得到情绪候选",
             "description": "10/24—10/31存活归档回溯；至少5作者、绝对情绪分≥40。候选尚未验证收益，且未获连续覆盖确认。不同口径共享消息，结果不能相加。",
             "summary": "每10条有效BTC观点形成10个完整批次，其中3个达到情绪门槛；3个均无法由保留的覆盖记录确认连续采集。",
             "height": 320,
             "spec": {"type": "horizontalBar", "x": "候选窗口", "y": "口径", "xLabel": "达到作者数与情绪门槛的候选窗口数",
                      "showValues": True, "xDomain": [0, 5], "valueFormat": {"style": "decimal", "maximumFractionDigits": 0}},
             "rows": [{"口径": r["口径"], "候选窗口": r["候选窗口"]} for r in rows]}
    write(output / "chart.json", chart)
    caveats = ["这是存活消息归档的回溯计数；删除及修订可能改变历史批次边界，并非前向纸面交易。",
               "六种口径共享同一聊天流，候选数不能相加为独立样本，也未据收益挑选优胜参数。",
               "所有达标候选均未被保留的完整覆盖记录认证；未认证不等于已证明窗口一定有缺口。",
               "条数窗口的跨度和信息年龄仍需观察；本次未读取价格收益，也未验证交易有效性。"]
    sources = {"schemaVersion": 1, "items": [{"id": "event-count-sentiment", "title": "条数情绪候选与信息时效",
        "queries": [{"id": "archive-event-batches", "source": {
            "label": "本地 Telegram 前向归档的脱敏衍生元数据",
            "files": ["telegram_live_20261024/observations.sqlite", "binance_chat_event_time_20261031/protocol.json",
                      "binance_chat_event_time_20261031/results.json"],
            "tables": ["messages", "labels", "observations"],
            "sql": source["sql"],
            "filters": ["BinanceChinese；原激活之后的前向消息", "剔除机器人、无作者、转发、已观察修订、技术及广告内容",
                        "标签与正文哈希一致；接收及分类延迟各不超过30分钟", "方向观点仅BTCUSDT、置信度≥0.7；每批相同文本只投一票"],
            "metricDefinitions": [{"label": "观察分", "definition": "每位作者的观点方向先平均，再按作者等权平均并乘100。"},
                                  {"label": "候选窗口", "definition": "不重叠的完整N条批次中，作者数≥5且绝对观察分≥40；覆盖认证单列。"}],
            "caveats": caveats, "executedAt": result["computed_at"]},
            "reportingPeriod": f"北京时间 {local(source['activation_at'])} 至 {local(source['as_of'])}",
            "columns": list(rows[0]), "rows": rows,
            "preview": {"kind": "aggregate", "note": "六种条数口径的全部汇总；批次来自同一聊天流。", "totalRows": len(rows)},
            "methods": [{"language": "calculation", "code": "S = 100 * mean_author(mean(side for distinct_texts_by_author)); side in {-1,+1}\nBatch qualifies if complete and unique_authors >= 5 and abs(S) >= 40.\nNon-overlapping batches are counted; latest rolling windows are descriptive only."}]}]}]}
    write(output / "sources.json", sources)
    primary = next(c for c in result["configs"] if c["clock"] == "btc_views" and c["size"] == 10)
    latest = primary["latest_rolling"]
    candidates = [b for b in primary["batches"] if b["qualified"]]
    note = ["# 币安中文群：按信息条数观察情绪", "",
            f"数据快照：北京时间 {local(source['as_of'])}。仅回溯分析，不改变原小时／六小时账本。", "",
            f"原始前向消息 {source['counts']['raw_messages']} 条；清洗后 {source['counts']['clean_messages']} 条；符合原门槛的BTC方向观点 {source['counts']['btc_views']} 条，来自 {source['counts']['btc_authors']} 个账号。", "",
            "## 结果", ""]
    note += [f"- {r['口径']}：{r['完整批次']} 个完整批次，{r['候选窗口']} 个候选，{r['可确认连续覆盖的候选']} 个候选获得连续覆盖认证。" for r in rows]
    note += ["", "## 每10条观点的三个历史候选", ""]
    note += [f"- {local(b['available_at'])}：{b['authors']}作者，分数 {b['score']:+.1f}，积累 {b['span_hours']:.1f} 小时；覆盖 {b['coverage_status']}。" for b in candidates]
    note += ["", f"最新滚动10条：{latest['authors']}作者，观察分 {latest['score']:+.1f}，跨度 {latest['span_hours']:.1f} 小时，距最后观点 {latest['age_hours']:.1f} 小时。", "",
             "## 解释与限制", "",
             "用条数作为采样时钟，可以避免按整点切割稀疏观点；原始群消息仍受闲聊占比影响，因此有效BTC观点更贴近本指标的含义。",
             "这去掉了固定时间分桶，并没有消除信息老化和采集完整性问题。当前候选太少，且覆盖未认证，本轮不报告胜率或收益。", ""]
    note += [f"- {c}" for c in caveats]
    note += ["", "复现：先运行 scripts/research_telegram_event_sentiment.py；已有结果会被复用，不改写冻结快照。使用新目录才能研究新的快照或规则。", "",
             "原文保留于原授权归档；本输出没有聊天正文、引用或作者标识。"]
    (base / "readout.md").write_text("\n".join(note) + "\n", encoding="utf-8")
    print(json.dumps({"chart": str(output / "chart.json"), "sources": str(output / "sources.json"), "readout": str(base / "readout.md")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
