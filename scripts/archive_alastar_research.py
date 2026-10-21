"""Archive existing Alastar research and an inactive forward-paper protocol.

Reads local evidence only. Does not rerun market research, alter execution,
fetch messages, call an AI provider, or start a worker/scheduler.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data/research/alastar_response_20261021"
REPORT = ROOT / "reports/alastar_response_20261021"
FAMILIES = {
    "fresh_view": ("新的即时方向", "仅新观点中的即时方向，不把持续背景全部开仓", "保留为较严格的对照；后段样本较少"),
    "broad_directional": ("广义方向", "即时方向加可解释的背景方向；同币不叠加或重置持仓", "72小时列为前瞻研究候选；尚未验证稳定盈利"),
    "numeric_cross": ("等待新的价格穿越", "条件发布后等待已收盘5分钟线的新穿越，确认后至少等60秒", "暂缓作为主策略；原文的非价格条件不能由价格替代"),
    "numeric_state": ("价格条件已经成立", "允许发布后检查时条件已成立；站稳类须两根已完成收盘", "暂缓作为主策略；与新穿越分别保留，不能混为一种触发"),
    "until_opposite_update": ("随观点更新退出", "广义方向入场，较早的新反向观点或明确无条件失效可提前退出", "保留退出规则对照；没有跨周期一致改善"),
    "broad_plus_images": ("广义方向加图片首段", "另加入4张关键原图的第一段方向；不推测转折时刻", "单列观察；后段无新增图片，不能把相同结果当新增证据"),
}


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pct(value):
    return "缺失" if value is None else f"{value * 100:+.2f}%"


def main():
    now = datetime.now(timezone.utc).isoformat()
    result = read(DATA / "response_results.json")
    diagnostics = read(DATA / "response_diagnostics.json")
    taxonomy = read(DATA / "taxonomy.json")["summary"]
    protocol = read(DATA / "response_protocol.json")
    assert taxonomy["messages"] == 422 and taxonomy["information_messages"] == 300
    assert taxonomy["fresh_view_messages"] == 69
    for receipt in ("response_validation.json", "response_diagnostics_validation.json"):
        audit = read(DATA / receipt)
        assert audit["ok"] and audit["issues_count"] == 0
    grouped = defaultdict(dict)
    for row in result["summary"]:
        assert row["phase"] not in grouped[row["scenario"]]
        grouped[row["scenario"]][row["phase"]] = row
    assert len(grouped) == 108 and len(result["summary"]) == 324
    assert all(set(phases) == {"earlier", "later", "all"} for phases in grouped.values())
    assert set(result["pools"]) == set(FAMILIES)
    assert protocol["resolved_events_sha256"] == digest(DATA / "resolved_response_events.json")
    concentration = {(r["scenario"], r["phase"]): r for r in diagnostics["concentration"]}
    ledger = []
    for scenario, phases in grouped.items():
        row = phases["all"]
        family = row["family"]
        ledger.append({
            "experiment_id": scenario, "status": "completed_historical_exploration",
            "family": family, "family_name": FAMILIES[family][0],
            "minimum_publication_delay_minutes": row["delay"], "holding_hours": row["hours"],
            "price_interval": "5m", "event_pool_size": result["pools"][family],
            "round_trip_cost_bps": 20, "stress_cost_bps": [40, 100],
            "phase_results": phases,
            "concentration": {phase: concentration.get((scenario, phase)) for phase in phases},
            "research_decision": FAMILIES[family][2],
            "truly_unseen_validation": False, "execution_enabled": False,
        })
    source_paths = [
        "data/research/alastar_20261021/messages.jsonl",
        "data/research/alastar_response_20261021/taxonomy.json",
        "data/research/alastar_response_20261021/response_protocol.json",
        "data/research/alastar_response_20261021/resolved_response_events.json",
        "data/research/alastar_response_20261021/response_results.json",
        "data/research/alastar_response_20261021/response_outcomes.jsonl",
        "data/research/alastar_response_20261021/admitted_trades.jsonl",
        "data/research/alastar_response_20261021/response_diagnostics.json",
        "data/research/alastar_response_20261021/response_validation.json",
        "data/research/alastar_response_20261021/response_diagnostics_validation.json",
        "data/research/alastar_response_20261021/visual_review.json",
        "reports/alastar_response_20261021/ALL_SCENARIOS.csv",
        "reports/alastar_response_20261021/RESEARCH_NOTES.md",
        "reports/alastar_response_20261021/REPORT_QA_NOTES.json",
        "scripts/research_alastar_responses.py",
        "scripts/analyze_alastar_response_results.py",
        "scripts/archive_alastar_research.py",
    ]
    sources = [{"path": p, "sha256": digest(ROOT / p)} for p in source_paths]
    save(DATA / "strategy_experiment_ledger.json", {
        "schema_version": 1, "archived_at_utc": now,
        "scope": "Alastar public-channel historical study; not private bots or author account returns",
        "taxonomy_summary": taxonomy, "experiment_count": 108, "phase_summary_count": 324,
        "metric_units": "fraction of trade notional unless named portfolio; 0.01 means 1%",
        "zero_trade_policy": "absent metrics remain missing; do not fill with zero",
        "multiple_comparisons": "108 correlated exploratory variants; intervals unadjusted",
        "source_manifest": sources, "experiments": ledger,
    })

    forward_path = DATA / "prospective_paper_protocol_v1.json"
    if not forward_path.exists():
        save(forward_path, {
            "protocol_id": "alastar_broad_72h_paper_v1", "version": 1, "documented_at_utc": now,
            "status": "documented_not_running", "execution_enabled": False,
            "candidate_selected_after_historical_exploration": True,
            "historical_scenario_reference": "broad_directional|d15|h72",
            "historical_reference_sha256": digest(DATA / "response_results.json"),
            "eligibility": {
                "source": "new live first-observation receipts from @alastartrades",
                "historical_imports_eligible": False, "first_receipt_max_age_seconds": 600,
                "unambiguous_timezone_required": True, "edited_or_truncated_before_freeze_eligible": False,
                "reaction_types": ["immediate_view", "context_only"], "chart_only_eligible": False,
                "minimum_interpretation_confidence": 0.65,
                "requires_unavailable_confirmation_eligible": False,
                "opposed_direction_rule": "use explicit higher/longer-timeframe branch for72h; unresolved conflict excluded",
                "later_revision_policy": "append new revision; never retroactively delete a frozen observation, paper trade or loss",
            },
            "freeze_record_required": ["source_id", "message_revision_sha256", "published_at_utc",
                "first_received_at_utc", "label_frozen_at_utc", "reviewer_or_model_version", "rule_version",
                "symbol", "direction", "reaction_type", "original_evidence_quote", "exclusion_reason_if_any"],
            "clock": {
                "timezone": "UTC", "bar_interval": "5m",
                "earliest_entry": "ceil_to_5m(max(published_at+15min, first_received_at+60sec, label_frozen_at+60sec))",
                "fill": "record next eligible open prospectively; never backfill an already-past entry",
                "missed_entry": "retain skipped record; any new timing rule needs a new protocol version before entry",
            },
            "main_book": {"holding_hours": 72, "fixed_initial_notional_fraction": 0.2,
                "maximum_concurrent_positions": 5, "one_position_per_symbol": True,
                "same_direction_update_restarts_clock": False, "require_available_collateral": True,
                "scheduled_exit": "entry+72h; no discretionary early exit in main fixed-hold hypothesis",
                "loss_model": "approximate1x isolated cap; not exact mark-price liquidation",
                "funding": "official complete coverage required; unknown is missing, not zero",
                "round_trip_cost_bps": 20, "stress_round_trip_cost_bps": [40, 100]},
            "controls": [
                "24h standalone paper comparator, same frozen72h labels/directions and clock; own overlap book",
                "same-entry-and-exit inverse diagnostic; not an independently executable portfolio",
                "same-entry-and-exit always-long diagnostic; same limitation",
            ],
            "review_gate": {"minimum_completed_primary_positions": 30, "minimum_elapsed_days": 90,
                "both_required": True, "purpose": "review threshold only; not a power calculation or deployment permission",
                "report": ["all included/skipped observations", "cost/funding completeness", "net mean and median",
                    "week-block interval", "portfolio drawdown", "leave-best-trade/coin-out", "controls", "true latency"]},
            "activation_requirements": ["implement and verify a prospective label-freeze and paper-fill recorder",
                "check receipt integrity and local UTC clock", "record activation timestamp before first eligible fill"],
            "automation_or_live_orders_started_by_this_record": False,
        })
    forward = read(forward_path)
    assert forward["protocol_id"] == "alastar_broad_72h_paper_v1"

    lines = [
        "# Alastar 策略研究与尝试台账", "", "整理日期：2026-10-21。当前状态：历史探索完成；前瞻模拟协议已写入，尚未启动。", "",
        "本台账记录做过什么、为什么修订、哪些结论仍不成立。数值来自本地原始结果，保留未成功的尝试与完整参数矩阵。", "",
        "## 六类尝试", "", "所有行固定最低延迟15分钟，展示历史后段；单元格为平均单笔净收益／持仓数。已计20bps往返成本与实际资金费。", "",
        "| 尝试 | 事件池 | 1小时 | 4小时 | 24小时 | 72小时 | 168小时 | 当前判断 |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for family, (name, rule, decision) in FAMILIES.items():
        cells = []
        for hours in (1, 4, 24, 72, 168):
            r = next(x for x in result["summary"] if x["family"] == family and x["delay"] == 15 and x["hours"] == hours and x["phase"] == "later")
            cells.append(f"{pct(r.get('mean'))}／{r['n']}")
        lines.append(f"| {name} | {result['pools'][family]} | " + " | ".join(cells) + f" | {decision} |")
    lines += ["", "不同周期会改变重叠过滤与持仓数；这些列不是相互独立的实验。完整矩阵还含30分钟周期和1／60分钟延迟，见CSV及JSON台账。", ""]
    for family, (name, rule, decision) in FAMILIES.items():
        lines += [f"### {name}（`{family}`）", "", rule + "。" + decision + "。", ""]
    chosen = grouped["broad_directional|d15|h72"]["later"]
    conc = concentration[("broad_directional|d15|h72", "later")]
    lines += [
        "## 为什么把三天列为下一轮候选", "",
        f"广义方向、15分钟延迟、72小时后段共有{chosen['n']}笔，平均净收益{pct(chosen['mean'])}，周块95%区间{pct(chosen['week_block_95'][0])}至{pct(chosen['week_block_95'][1])}。",
        f"把总往返成本提高到100bps，均值为{pct(chosen['stress100']['mean'])}；去掉最大单笔后为{pct(chosen['without_best'])}；去掉贡献最大的币{conc['best_coin']}后为{pct(conc['mean_without_best_coin'])}。",
        f"20%固定初始本金组合的5分钟开盘标记回撤为{pct(chosen['max_5m_open_drawdown'])}。这是组合指标，不能与单笔均值混用。", "",
        "候选优先级来自已知历史探索，不能把后段重新命名为未见样本。三天区间仍跨零，七天全期约95.4%净利润由RAVE贡献；没有选择最高历史格子作为生产参数。", "",
        "## 已做过的修正和没有继续推进的假设", "",
        "| 尝试／发现 | 本地处理与研究判断 |", "|---|---|",
        "| 26条→40条→全文角色复核 | 前两轮仅是狭义方向子集；最新422条中300条含信息、69条为新方向或条件观点。保留旧标签和旧报告，避免改写研究历史。 |",
        "| 原26条延迟及5%／10%／20%止损 | 先前全期仍为负；未据少数后段正收益采用固定止损。详见上一轮报告，不与新版5分钟模型直接相减。 |",
        "| 小时取整→5分钟取整 | 全422条在最低延迟1分钟时，实际等待中位数31.1→3.5分钟；这是模型等待，不是Telegram传输实测。 |",
        "| 旧40条等模型时刻对照 | 同40个事件24小时均值：小时取整−1.28%、5分钟取整−0.25%；未做组合去重，不能与旧39笔组合直接比较。 |",
        "| 风险提醒当反向开仓 | 已拆开；不宜做空不自动成为做多。旧观点失效也不等于新反向观点。 |",
        "| 条件失效在发布时立即退出 | 已排除；尚未发生的价位失效不能提前当作发生，数值条件与即时无条件更新分开。 |",
        "| 支撑／阻力代理替代鲸鱼、OI、清算确认 | 未采用这种替代；缺原始时序的条件记录为证据缺口。 |",
        "| 看图猜先涨后跌完整交易 | 4张原图只试第一段；不补造转折价和转折时间。 |",
        "| 根据未来缺口剔除已触发或已强平的事件 | 已修正时序；之后无关缺口不能抹去已知的持仓和亏损。 |",
        "| 无资金费记录直接填零 | 不采用；只有完整官方查询证明无结算才认定为已知零。 |",
        "| 将极端空头亏损无限外推 | 使用近似1倍逐仓分配本金损失下限；仍非精确标记价格强平。 |", "",
        "## 已写入的前瞻模拟协议", "",
        "协议ID：`alastar_broad_72h_paper_v1`，状态`documented_not_running`。规则文件已保存，不代表模拟执行器已经接通。主方案为广义文字方向、72小时固定持有；图片首段、纯价格条件和主动提前退出不混入主方案。", "",
        "只接受首次由实时采集收到的新消息，收件不晚于发布600秒；在冻结时无编辑、无截断、时区明确。必须先保存原文版本SHA、实际收件UTC、解释冻结UTC、原句、方向、标的、规则及解释者版本，再登记未来成交。解释信心门槛0.65不代表盈利概率。", "",
        "最早模拟入场取`max(发布+15分钟, 实际收件+60秒, 标签冻结+60秒)`之后或当时的下一5分钟开盘。未能在当时登记的开盘不允许日后回填；记录错过原因。新规则须先建新协议版本。", "",
        "主账每笔为初始模拟本金20%，最多5笔，同币不叠加、不因同向更新重置72小时，并检查可用保证金；计20bps成本、40／100bps压力和实际资金费。固定持有主假设没有临时主观止盈止损。24小时对照使用相同已冻结的72小时标签与方向、独立执行重叠过滤；反向与始终做多只作同时间诊断，不能当成独立可执行组合。", "",
        "归档当前队列会随后续编辑撤销待复核资格；研究必须保存冻结当时的队列／收件依据。冻结后出现的修订追加记录，不追溯删除原观察、已登记交易或亏损。这样避免按未来编辑状态筛掉失败样本。", "",
        "完成至少30笔主账持仓且经过至少90天后再集中复核；二者只是复核门槛，不保证统计显著，也不自动授权上线。复核需包括全部跳过记录、真实延迟、成本完整性、均值／中位数、按周区间、组合回撤、去最佳交易／币、同时间对照。", "",
        "当前缺口：尚未实现并验证前瞻标签冻结与模拟成交登记器，尚未记录激活时刻。现有首次收件归档不等于该策略已经开始模拟运行。", "",
        "## 全量结果与复现", "",
        "- [最新研究报告](../reports/alastar_response_20261021/RESEARCH_NOTES.md)",
        "- [108组情景CSV（324个阶段汇总）](../reports/alastar_response_20261021/ALL_SCENARIOS.csv)",
        "- [结构化尝试台账](../data/research/alastar_response_20261021/strategy_experiment_ledger.json)",
        "- [前瞻模拟协议v1](../data/research/alastar_response_20261021/prospective_paper_protocol_v1.json)",
        "- [方法与完整复现命令](ALASTAR_RESPONSE_RESEARCH.md)",
        "- [上一轮方向／止损／延迟研究](../reports/alastar_followup_20261021/RESEARCH_NOTES.md)",
        "- [研究总入口](ALASTAR_RESEARCH_INDEX.md)", "",
        "结果独立复算回执均为ok、0项差异；相关30项测试通过。13,488条入选记录含情景／阶段重复，不能当成13,488次独立交易。108组探索互相关联，区间未做多重比较校正。", "",
    ]
    (ROOT / "docs/ALASTAR_STRATEGY_EXPERIMENTS.md").write_text("\n".join(lines), encoding="utf-8")
    save(DATA / "research_handoff.json", {
        "archived_at_utc": now, "status": "historical_research_archived_forward_protocol_inactive",
        "entry_document": "docs/ALASTAR_RESEARCH_INDEX.md",
        "experiments_document": "docs/ALASTAR_STRATEGY_EXPERIMENTS.md",
        "experiment_ledger": "data/research/alastar_response_20261021/strategy_experiment_ledger.json",
        "experiment_ledger_sha256": digest(DATA / "strategy_experiment_ledger.json"),
        "forward_protocol": "data/research/alastar_response_20261021/prospective_paper_protocol_v1.json",
        "forward_protocol_sha256": digest(forward_path), "experiment_count": len(ledger),
        "phase_summary_count": len(result["summary"]), "new_market_requests": 0,
        "new_strategy_execution": False, "live_orders_enabled": False,
    })
    print(json.dumps({"experiments": len(ledger), "phase_summaries": len(result["summary"]),
                      "protocol_status": forward["status"], "handoff": str(DATA / "research_handoff.json")}))


if __name__ == "__main__":
    main()
