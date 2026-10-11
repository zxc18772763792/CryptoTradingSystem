"""One normalized row per forward paper tracker, for the strategies tab.

The paper trackers are research ledgers, not strategy_manager strategies: they
never place orders, so they are not registered as runnable strategies and do
not appear in the strategy performance table. This gives them a read-only
performance table of their own. Every number comes from the tracker's own
summary() and its pre-registered retirement verdict (n, mean, 90% CI), so this
table cannot disagree with the tracker cards on the AI research page.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from core.research import retirement


def _listing() -> Dict[str, Any]:
    from core.research import listing_short_tracker as m
    return m.summary(m.load_state())


def _unlock(variant: str) -> Callable[[], Dict[str, Any]]:
    def load() -> Dict[str, Any]:
        from core.research import unlock_short_tracker as m
        return m.summary(m.load_state(m.VARIANTS[variant]["state_path"]), variant)
    return load


def _supply() -> Dict[str, Any]:
    from core.research import supply_factor_tracker as m
    return m.summary(m.load_state())


def _korean(strategy: str) -> Callable[[], Dict[str, Any]]:
    def load() -> Dict[str, Any]:
        from core.research import upbit_caution_tracker as m
        return m.summary(m.load_state(m.STRATEGIES[strategy]["state_path"]), strategy)
    return load


def _xs_reversal() -> Dict[str, Any]:
    from core.research import xs_reversal_tracker as m
    return m.summary(m.load_state())


def _composite() -> Dict[str, Any]:
    from core.research import coinglass_composite_tracker as m
    return m.summary(m.load_state())


def _xs_liq_filter() -> Dict[str, Any]:
    from core.research import xs_reversal_tracker as m
    summary = m.summary(m.load_state())
    shadow = summary.get("liq_filter") or {}
    return {"retirement": shadow.get("retirement"), "started_at": summary.get("started_at"),
            "updated_at": summary.get("updated_at"), "forward_completed": shadow.get("completed")}


def _announcement(kind: str) -> Callable[[], Dict[str, Any]]:
    def load() -> Dict[str, Any]:
        from core.research import announcement_short_tracker as m
        return m.load_summaries()[kind]["summary"]
    return load


# id, name, group, unit, rule (one line), retirement rule key, summary loader
TRACKERS: List[Dict[str, Any]] = [
    {"id": "unlock_short", "name": "大额解锁前做空 · t−30", "group": "解锁", "unit": "笔", "rule_key": "unlock_short",
     "rule": "≥10% 悬崖解锁前 30 天做空永续、前 1 天平仓，篮子对冲，+40% 止损", "load": _unlock("t30")},
    {"id": "unlock_short_t7", "name": "解锁前最后一周做空 · t−7", "group": "解锁", "unit": "笔", "rule_key": "unlock_short_t7",
     "rule": "≥5% 悬崖解锁前 7 天做空、前 1 天平仓，篮子对冲（影子版本）", "load": _unlock("t7")},
    {"id": "supply_factor", "name": "供给通胀因子", "group": "因子", "unit": "月", "rule_key": "supply_factor",
     "rule": "每月按未来 90 天新增供给排序，多低空高三分位，持有 30 天", "load": _supply},
    {"id": "xs_reversal", "name": "日线横截面反转", "group": "因子", "unit": "天", "rule_key": "xs_reversal",
     "rule": "每天 00:00 UTC 做多跌离均线最深 10%、做空最高 10%，持有 24h。回测仅 +0.057%/天，180 天测不出：只能发现明显亏损，"
             "或远好于回测（约 +0.3%/天以上）", "load": _xs_reversal},
    {"id": "xs_reversal_liq_filter", "name": "日线横截面反转 · 剔除爆仓异常（影子）", "group": "因子", "unit": "天",
     "rule_key": "xs_reversal_liq_filter",
     "rule": "同横截面反转，多头候选剔除当天爆仓异常前 20%；判定看和原版每天的收益差", "load": _xs_liq_filter},
    {"id": "coinglass_composite_7d", "name": "Coinglass 七日合成组合", "group": "因子", "unit": "期",
     "rule_key": "coinglass_composite_7d",
     "rule": "8 个持仓类信号按训练期 IC 加权，每天建一份多空各五分之一、持有 7 天的组合，扣成本和资金费；利润薄，一年内约两成概率能确认",
     "load": _composite},
    {"id": "korea_caution_hedged", "name": "韩国交易所警示后做空 · 对冲版（Upbit + Bithumb 合并）", "group": "韩国交易所",
     "unit": "笔", "rule_key": "korea_caution_hedged",
     "rule": "Upbit 或 Bithumb 交易警示当日收盘做空永续 7 天 + 等额做多成交额前 30 篮子；同一币 7 天内只开一笔，先警示的交易所开仓",
     "load": _korean("korea_caution_hedged")},
    {"id": "upbit_caution", "name": "Upbit 警示后做空", "group": "韩国交易所", "unit": "笔", "rule_key": "upbit_caution",
     "rule": "交易警示当日收盘做空币安永续 7 天，+40% 止损", "load": _korean("caution")},
    {"id": "upbit_caution_hedged", "name": "Upbit 警示后做空 · 对冲版", "group": "韩国交易所", "unit": "笔",
     "rule_key": "upbit_caution_hedged", "rule": "同上 + 等额做多成交额前 30 永续篮子", "load": _korean("caution_hedged")},
    {"id": "upbit_krw_listing", "name": "Upbit 韩元上币后做空", "group": "韩国交易所", "unit": "笔",
     "rule_key": "upbit_krw_listing", "rule": "韩元上币当日收盘做空 7 天，+40% 止损", "load": _korean("krw_listing")},
    {"id": "bithumb_caution", "name": "Bithumb 警示后做空（样本外）", "group": "韩国交易所", "unit": "笔",
     "rule_key": "bithumb_caution", "rule": "规则同 Upbit 警示，信号换成 Bithumb，无回测", "load": _korean("bithumb_caution")},
    {"id": "bithumb_caution_hedged", "name": "Bithumb 警示后做空 · 对冲版（样本外）", "group": "韩国交易所", "unit": "笔",
     "rule_key": "bithumb_caution_hedged", "rule": "同上 + 等额做多成交额前 30 永续篮子", "load": _korean("bithumb_caution_hedged")},
    {"id": "announcement_short_monitor", "name": "币安监控标签公告后做空", "group": "币安公告", "unit": "笔",
     "rule_key": "announcement_short_monitor", "rule": "发现公告即做空永续，持有 24h，+50% 防灾止损",
     "load": _announcement("binance_monitor")},
    {"id": "announcement_short_delist", "name": "币安下架公告后做空", "group": "币安公告", "unit": "笔",
     "rule_key": "announcement_short_delist", "rule": "发现公告即做空永续，持有 4h，+30% 止损",
     "load": _announcement("binance_delist")},
    {"id": "listing_short", "name": "新上市永续做空（仅观察）", "group": "币安公告", "unit": "笔", "rule_key": "listing_short",
     "rule": "上线第 2 天收盘做空、第 14 天平仓；更正后回测不显著，只作观察", "load": _listing},
]


def _open_marks(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Counted open positions with their current short-side mark, where the tracker keeps one."""
    tid = spec["id"]
    if tid in {"unlock_short", "unlock_short_t7"}:
        from core.research import unlock_short_tracker as m
        trades = m.load_state(m.VARIANTS["t30" if tid == "unlock_short" else "t7"]["state_path"]).get("trades", {}).values()
        return [{"label": t.get("token"), "mark_pct": t.get("mark_return_pct")} for t in trades
                if t.get("status") == "open" and not t.get("late")]
    if tid.startswith(("upbit_", "bithumb_", "korea_")):
        from core.research import upbit_caution_tracker as m
        strategy = {"upbit_caution": "caution", "upbit_caution_hedged": "caution_hedged", "upbit_krw_listing": "krw_listing",
                    "bithumb_caution": "bithumb_caution", "bithumb_caution_hedged": "bithumb_caution_hedged",
                    "korea_caution_hedged": "korea_caution_hedged"}[tid]
        trades = m.load_state(m.STRATEGIES[strategy]["state_path"]).get("trades", {}).values()
        return [{"label": t.get("ticker"), "mark_pct": t.get("return_pct")} for t in trades
                if t.get("status") in {"open", "waiting_entry"} and t.get("symbol") and not t.get("backfilled") and not t.get("late")]
    if tid == "listing_short":
        from core.research import listing_short_tracker as m
        trades = m.load_state().get("trades", {}).values()
        return [{"label": t.get("base") or t.get("symbol"), "mark_pct": t.get("return_pct")} for t in trades
                if t.get("status") == "open" and not t.get("backfilled")]
    return []


def _first(summary: Dict[str, Any], *keys: str) -> Optional[Any]:
    for key in keys:
        value = summary.get(key)
        if value is not None:
            return value
    return None


def tracker_row(spec: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {k: spec[k] for k in ("id", "name", "group", "unit", "rule")}
    rule = retirement.RULES.get(spec["rule_key"]) or {}
    row["backtest_mean_pct"] = rule.get("backtest_mean_pct")
    row["min_n"] = rule.get("min_n")
    try:
        summary = spec["load"]()
    except Exception as exc:  # noqa: BLE001 - one broken tracker must not blank the table
        row["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        return row
    verdict = summary.get("retirement") or {}
    n = int(verdict.get("n") or 0)
    mean = verdict.get("mean_pct")
    row.update(
        n=n,
        min_n=verdict.get("min_n") or row["min_n"],
        mean_pct=mean,
        ci90_pct=verdict.get("ci90_pct"),
        total_pct=round(float(mean) * n, 2) if mean is not None else None,
        verdict=verdict.get("verdict"),
        verdict_label=verdict.get("label"),
        verdict_reason=verdict.get("reason"),
        ci_clusters=verdict.get("clusters"),
        win_rate=_first(summary, "forward_win_rate", "forward_positive_days", "forward_positive_months"),
        hedged_mean_pct=_first(summary, "forward_hedged_mean_return_pct"),
        open=_first(summary, "forward_open"),
        waiting=_first(summary, "forward_waiting", "forward_scheduled"),
        late=_first(summary, "forward_late", "late", "late_trades"),
        started_at=summary.get("started_at"),
        updated_at=summary.get("updated_at"),
    )
    try:
        row["open_marks"] = _open_marks(spec)
    except Exception:  # noqa: BLE001 - marks are a convenience
        row["open_marks"] = []
    return row


def tracker_rows() -> List[Dict[str, Any]]:
    return [tracker_row(spec) for spec in TRACKERS]
