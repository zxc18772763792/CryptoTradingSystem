"""Write a dated overview of every forward paper tracker: rule, backtest reference, retirement bar
and forward results so far, as docs/RESEARCH_TRACKERS_<YYYY-MM-DD>.md.

Reads the trackers' own state files through core/research/tracker_registry.py, the same rows the
strategy page shows, so the numbers match it. Research only: every tracker is paper.

    python scripts/tracker_overview.py            # today's snapshot (overwrites today's file)
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from core.research import retirement  # noqa: E402
from core.research.tracker_registry import tracker_rows  # noqa: E402

BEIJING = timezone(timedelta(hours=8))
UNITS = {"笔": "%/笔", "天": "%/天", "月": "%/月", "期": "%/期"}
# where the evidence behind each tracker is written up
EVIDENCE = {
    "unlock_short": "docs/UNLOCK_EVENT_STUDY_2026-09-27.md",
    "unlock_short_t7": "docs/UNLOCK_EVENT_STUDY_2026-09-27.md",
    "supply_factor": "docs/LLM_TRADING_RESEARCH_ROUND5_2026-09-27.md",
    "xs_reversal": "docs/AGENT_ML_BIAS_2026-09-30.md, scripts/xs_reversal_backtest.py",
    "xs_reversal_liq_filter": "docs/COINGLASS_COMBINATION_STUDY_2026-10-11.md（C1）",
    "coinglass_composite_7d": "docs/COINGLASS_COMBINATION_STUDY_2026-10-11.md",
    "coinglass_composite_7d_wide_eq": "docs/COINGLASS_WIDE_STUDY_2026-10-12.md",
    "korea_caution_hedged": "docs/KOREA_CAUTION_POOLED_2026-10-11.md",
    "upbit_caution": "docs/LLM_TRADING_RESEARCH_ROUND7_2026-09-28.md",
    "upbit_caution_hedged": "docs/UPBIT_CAUTION_HEDGED_2026-10-04.md",
    "upbit_krw_listing": "docs/LLM_TRADING_RESEARCH_ROUND7_2026-09-28.md",
    "bithumb_caution": "无回测，规则照搬 Upbit（docs/LLM_TRADING_RESEARCH_ROUND7_2026-09-28.md）",
    "bithumb_caution_hedged": "无回测，规则照搬 Upbit 对冲版（docs/UPBIT_CAUTION_HEDGED_2026-10-04.md）",
    "announcement_short_monitor": "docs/ANNOUNCEMENT_INTRADAY_2026-10-07.md",
    "announcement_short_delist": "docs/ANNOUNCEMENT_INTRADAY_2026-10-07.md",
    "listing_short": "docs/LLM_TRADING_RESEARCH_ROUND3_2026-09-26.md",
}


def _pct(value: Optional[float], digits: int = 2) -> str:
    return "—" if value is None else f"{float(value):+.{digits}f}%"


def _ci(ci: Optional[List[float]]) -> str:
    return "—" if not ci else f"[{ci[0]:+.2f}, {ci[1]:+.2f}]"


def _share(value: Optional[float]) -> str:
    return "—" if value is None else f"{float(value) * 100:.0f}%"


def _count(value: Any) -> str:
    return "—" if value is None else str(int(value))


def _day(stamp: Optional[str]) -> str:
    if not stamp:
        return "—"
    try:
        return datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).astimezone(BEIJING).strftime("%m-%d")
    except ValueError:
        return str(stamp)[:10]


def render(rows: List[Dict[str, Any]], now: datetime) -> str:
    out = [
        f"# 研究跟踪器一览（{now.astimezone(BEIJING):%Y-%m-%d %H:%M} 北京时间）",
        "",
        "全部是纸面记录，不下单。每个跟踪器的退役规则都在第一笔结果之前登记并冻结（`core/research/retirement.py`）。",
        "本文由 `python scripts/tracker_overview.py` 生成，数字和策略页的跟踪器表一致。",
        "",
        "## 一、结果",
        "",
        "| 类别 | 跟踪器 | 开始 | 已完成 / 门槛 | 平均 | 90% 区间 | 胜率 | 在持 | 待入场 | 判定 |",
        "|---|---|---|---|---:|---|---:|---:|---:|---|",
    ]
    for r in rows:
        if r.get("error"):
            out.append(f"| {r['group']} | {r['name']} | — | — | — | — | — | — | — | 读取失败：{r['error']} |")
            continue
        unit = "百分点/天" if r.get("metric_kind") == "paired_increment" else UNITS.get(r["unit"], r["unit"])
        mean = "—" if r.get("mean_pct") is None else f"{float(r['mean_pct']):+.2f}{unit}"
        if r.get("metric_kind") == "paired_increment":
            mean += "（影子减同期原版）"
        out.append(f"| {r['group']} | {r['name']} | {_day(r.get('started_at'))} | {r.get('n', 0)} / {r.get('min_n')} {r['unit']} | "
                   f"{mean} | {_ci(r.get('ci90_pct'))} | {_share(r.get('win_rate'))} | {_count(r.get('open'))} | "
                   f"{_count(r.get('waiting'))} | {r.get('verdict_label') or '—'} |")
    out += [
        "",
        "- 平均和区间按各自单位；反转过滤影子显示同日增益（百分点/天），不是自身收益。",
        "- 反转以多空总名义金额计；七日组合、解锁和事件对冲以单腿名义金额计，折算总名义金额需除以二。供给因子未计借币可得性和借币成本。",
        "- 胜率：事件类是赚钱交易的占比；横截面反转是正收益天数的占比；合成组合是正收益期数的占比。",
        "- 区间从第 2 个样本起计算（同一条公告或同一天的几笔算一组），但样本不到门槛一半时只积累、不做判定。",
        "",
        "## 二、策略和依据",
        "",
        "| 跟踪器 | 规则 | 回测参照 | 退役门槛 | 依据 |",
        "|---|---|---:|---:|---|",
    ]
    for r in rows:
        unit = UNITS.get(r["unit"], r["unit"])
        ref = "—" if r.get("backtest_mean_pct") is None else f"{float(r['backtest_mean_pct']):+.3g}{unit}"
        out.append(f"| {r['name']} | {r['rule']} | {ref} | {r.get('min_n')} {r['unit']} | {EVIDENCE.get(r['id'], '—')} |")
    marks = [(r["name"], m) for r in rows for m in (r.get("open_marks") or [])]
    out += ["", "## 三、在持仓位（浮动收益，空头方向）", ""]
    if marks:
        out += ["| 跟踪器 | 币 | 记录浮盈 | 范围 / 价格时间 |", "|---|---|---:|---|"]
        out += [f"| {name} | {m.get('label') or '—'} | {_pct(m.get('mark_pct'))} | "
                f"{m.get('scope_label') or '空头腿'}；{m.get('price_basis') or '账本标价'}；{m.get('price_at') or '价格时间未记录'} |" for name, m in marks]
        out.append("")
        out.append("以上是账本最近完整日标价，不是实时成交报价；缺少对冲腿的浮盈不当作组合净值。因子类结算后才有结果。")
    else:
        out.append("没有逐笔记浮动收益的在持仓位。")
    out += [
        "",
        "## 四、怎么读",
        "",
        f"- **判定规则（全部跟踪器相同）：** 样本不到门槛一半时只积累；之后如果 90% 区间上沿低于 0，提前判退役；"
        f"到门槛后，均值不为正、或区间上沿低于回测参照的 {int(retirement.DECAY_FRACTION * 100)}%（优势衰减）判退役，"
        "区间下沿高于 0 判确认，其余为\"均值为正但不显著\"。退役只是标记和提醒，记录继续。",
        "- **因子类每天都有结果，但单位优势很小，检验力有限：**",
        "  - 横截面反转回测只有 +0.057%/天，180 天只能发现明显亏损或远好于回测；",
        "  - 七日合成组合跑满 52 周约两成概率确认；",
        "  - 等权扩池版若和回测一样好，约四成，但它是看过结果后挑的，实际更低。",
        "- **事件类要等事件发生，样本积累慢：** 大额解锁按排期每年约 30 次；币安监控标签和下架公告每月一两条；"
        "韩国交易所的警示和上币每月有几次，但不少币在币安没有永续合约，进不了样本。多数要一年左右才可能有判定。",
        "- **影子和对冲版**和主版本同时记录，用来比较，不单独决定主版本的去留。",
    ]
    return "\n".join(out) + "\n"


def main() -> int:
    now = datetime.now(timezone.utc)
    rows = tracker_rows()
    path = PROJECT_ROOT / "docs" / f"RESEARCH_TRACKERS_{now.astimezone(BEIJING):%Y-%m-%d}.md"
    path.write_text(render(rows, now), encoding="utf-8")
    print(f"wrote {path} ({len(rows)} trackers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
