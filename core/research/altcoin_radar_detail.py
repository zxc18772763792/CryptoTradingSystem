"""Altcoin radar single-symbol detail payload builder.

Builds the inspector-drawer payload for one row: component sparklines, ranked
top drivers, chain-percentile backfill, and the action plan.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence


from core.research.altcoin_radar_events import (
    get_recent_symbol_events,
)
from core.research.altcoin_radar_metrics import (
    STATE_ANOMALY,
    STATE_CONTROL_TRACK,
    STATE_CONTROL_WARN,
    STATE_DISTRIBUTION,
    STATE_LAYOUT,
    _announcement_value,
    _community_flow_value,
    _to_float,
    _whale_context_value,
)
from core.research.altcoin_radar_ranking import _series_percentiles, sort_rows



def _normalized_sparkline(values: Sequence[Any]) -> List[float]:
    numeric = [_to_float(value, 0.0) for value in values if value is not None]
    if not numeric:
        return []
    base = numeric[0] if abs(numeric[0]) > 1e-12 else 1.0
    return [round((value / base) * 100.0, 4) for value in numeric]


def _detail_top_components(
    items: Sequence[tuple[str, float]],
    *,
    minimum: float = 0.0,
    limit: int = 3,
) -> List[str]:
    ranked = [
        (label, _to_float(score, 0.0))
        for label, score in items
        if _to_float(score, 0.0) > minimum
    ]
    ranked.sort(key=lambda item: item[1], reverse=True)
    return [label for label, _ in ranked[:limit]]


def _row_metrics(row: Mapping[str, Any]) -> Dict[str, Any]:
    metrics = row.get("metrics") or {}
    return metrics if isinstance(metrics, dict) else {}


def _build_detail_component_raw_maps(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Optional[float]]]:
    raw_maps: Dict[str, Dict[str, Optional[float]]] = {
        "community_flow": {},
        "announcements": {},
        "funding_basis": {},
        "whale_context": {},
    }
    for row in rows:
        symbol = str((row or {}).get("symbol") or "").strip().upper()
        if not symbol:
            continue
        metrics = _row_metrics(row)
        raw_maps["community_flow"][symbol] = max(_to_float(metrics.get("community_flow_imbalance"), 0.0), 0.0)
        raw_maps["announcements"][symbol] = max(_to_float(metrics.get("announcement_count"), 0.0), 0.0)
        raw_maps["funding_basis"][symbol] = (
            max(_to_float(metrics.get("funding_rate"), 0.0), 0.0) + max(_to_float(metrics.get("basis_pct"), 0.0), 0.0)
        )
        raw_maps["whale_context"][symbol] = max(_to_float(metrics.get("whale_count"), 0.0), 0.0)
    return raw_maps


def _backfill_detail_chain_percentiles(
    *,
    rows: Sequence[Mapping[str, Any]],
    symbol: str,
    percentiles: Mapping[str, Any],
    detail_community_snapshot: Optional[Mapping[str, Any]] = None,
    detail_whale_snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Optional[float]]:
    normalized_symbol = str(symbol or "").strip().upper()
    raw_maps = _build_detail_component_raw_maps(rows)

    community_snapshot = dict(detail_community_snapshot or {})
    whale_snapshot = dict(detail_whale_snapshot or {})

    community_flow = _community_flow_value(community_snapshot)
    if community_flow is not None:
        raw_maps["community_flow"][normalized_symbol] = community_flow

    announcements = _announcement_value(community_snapshot)
    if announcements is not None:
        raw_maps["announcements"][normalized_symbol] = announcements

    whale_context = _whale_context_value(whale_snapshot)
    if whale_context is not None:
        raw_maps["whale_context"][normalized_symbol] = whale_context

    backfilled: Dict[str, Optional[float]] = {}
    for key in ("community_flow", "announcements", "funding_basis", "whale_context"):
        if percentiles.get(key) is not None:
            continue
        pct_map = _series_percentiles(raw_maps.get(key) or {})
        if normalized_symbol in pct_map:
            backfilled[key] = pct_map.get(normalized_symbol)
    return backfilled


def _build_action_plan(
    selected: Mapping[str, Any],
    invalidate_conditions: Sequence[str],
) -> Dict[str, Any]:
    source = str(selected.get("signal_source") or "").strip()
    ignition_score = _to_float(selected.get("ignition_score"), 0.0)
    continuation_score = _to_float(selected.get("continuation_score"), 0.0)
    crowding_score = _to_float(selected.get("crowding_late_score"), 0.0)
    narrative_heat = _to_float(selected.get("narrative_heat_score"), 0.0)
    meme_rotation = _to_float(selected.get("meme_rotation_score"), 0.0)
    in_watchlist = bool(selected.get("in_watchlist"))
    data_quality = selected.get("data_quality") or {}
    market_freshness = _to_float(data_quality.get("market_data_freshness"), 0.0)

    tone = "muted"
    stance = "先观察，等待确认"
    summary = "当前证据还不够集中，先别把它当成明确埋伏位。"
    primary_action = "加入 Watchlist"
    secondary_action = "等下一轮扫描"
    actions = [
        "先留在观察池，不要急着把榜单名次当成交点。",
        "下一次扫描若信号来源切成点火/延续，再升级处理。",
    ]

    if crowding_score >= 0.65 or source == "crowded_late_stage":
        tone = "danger"
        stance = "高拥挤，别追"
        summary = "更像末端拥挤，不是舒服的埋伏位，优先防止追在情绪末端。"
        primary_action = "建拥挤预警"
        secondary_action = "等拥挤回落"
        actions = [
            "先不要把它当新点火，优先等拥挤分和资金过热回落。",
            "如果只是想跟踪，保留在 Watchlist 即可，不要因为榜单靠前就直接追。",
        ]
    elif ignition_score >= 0.60 or source == "perp_ignition":
        tone = "ignition"
        stance = "点火观察，先等确认"
        summary = "这类最接近“刚启动”，但更适合预警加确认，不适合把榜单本身当作追价理由。"
        primary_action = "建点火预警"
        secondary_action = "等首轮回踩确认"
        actions = [
            "重点盯 15m / 1h 是否继续放量、OI 继续抬升，而不是只看一根启动K。",
            "更稳的埋伏方式是等首轮回踩不破，再观察是否有二次发力。",
        ]
    elif continuation_score >= 0.55 or source == "perp_continuation":
        tone = "control"
        stance = "延续跟踪，别当首爆"
        summary = "这更像走势已经发动后的延续段，不是最早的点火位。"
        primary_action = "建跃升预警"
        secondary_action = "看回踩确认"
        actions = [
            "适合跟踪回踩后的承接，不适合把它当成“刚启动”的埋伏点。",
            "如果 funding 和 crowding 继续抬升，要及时降级成观察而不是硬追。",
        ]
    elif source.startswith("narrative_") or narrative_heat >= 0.55 or meme_rotation >= 0.55 or in_watchlist:
        tone = "narrative"
        stance = "叙事观察，等合约跟随"
        summary = "更偏题材轮动 / 板块升温，适合先收藏和跟踪，不够像纯 Perp 点火。"
        primary_action = "建叙事预警"
        secondary_action = "保留 Watchlist"
        actions = [
            "先看同板块是不是一起升温，再看合约侧点火分会不会补上来。",
            "如果只是单币热度抬头但没有合约跟随，优先当观察，不要急着追。",
        ]

    if market_freshness and market_freshness < 0.35:
        summary = f"当前数据新鲜度偏低，{summary}"
        actions.insert(0, "先等下一次刷新确认，避免拿旧快照直接下判断。")

    if invalidate_conditions:
        actions.append(f"失效先看：{invalidate_conditions[0]}")

    return {
        "tone": tone,
        "stance": stance,
        "summary": summary,
        "primary_action": primary_action,
        "secondary_action": secondary_action,
        "actions": actions[:4],
    }


def build_detail_payload(
    *,
    rows: Sequence[Mapping[str, Any]],
    symbol: str,
    sort_by: str = "layout",
    onchain_context: Optional[Mapping[str, Any]] = None,
    detail_community_snapshot: Optional[Mapping[str, Any]] = None,
    detail_whale_snapshot: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    ordered = sort_rows(rows, sort_by=sort_by)
    selected = next((dict(row) for row in ordered if str(row.get("symbol") or "").upper() == normalized_symbol), None)
    if selected is None:
        return {
            "selected_row": None,
            "derivatives_context": {},
            "proxy_breakdown": {},
            "chain_breakdown": {},
            "sparkline": [],
            "ignition_path": {},
            "narrative_linkage": {},
            "event_timeline": [],
            "invalidate_conditions": [],
            "action_plan": {},
            "related_candidates": [],
        }
    metrics = dict(selected.get("metrics") or {})
    percentiles = dict(metrics.get("percentiles") or {})
    percentiles.update(
        {
            key: value
            for key, value in _backfill_detail_chain_percentiles(
                rows=ordered,
                symbol=normalized_symbol,
                percentiles=percentiles,
                detail_community_snapshot=detail_community_snapshot,
                detail_whale_snapshot=detail_whale_snapshot,
            ).items()
            if value is not None
        }
    )
    if percentiles:
        metrics["percentiles"] = percentiles
        selected["metrics"] = metrics
    state = str(selected.get("signal_state") or "").strip()
    metrics_raw = _row_metrics(selected)
    proxy_breakdown = {
        "engine": "代理行为引擎",
        "dominant_sort": sort_by,
        "scores": {
            "upside": selected.get("upside_score"),
            "layout": selected.get("layout_score"),
            "alert": selected.get("alert_score"),
            "anomaly": selected.get("anomaly_score"),
            "accumulation": selected.get("accumulation_score"),
            "control": selected.get("control_score"),
            "derivatives_heat": selected.get("derivatives_heat_score"),
            "squeeze": selected.get("squeeze_score"),
            "crowding_risk": selected.get("crowding_risk_score"),
            "liquidity_trap": selected.get("liquidity_trap_score"),
            "flow_confirmation": selected.get("flow_confirmation_score"),
            "risk_penalty": selected.get("risk_penalty"),
        },
        "components": [
            {"label": "收益冲击", "pctile": percentiles.get("return_shock"), "weight": 0.45},
            {"label": "量能爆发", "pctile": percentiles.get("volume_burst"), "weight": 0.35},
            {"label": "真实波幅扩张", "pctile": percentiles.get("range_expansion"), "weight": 0.20},
            {"label": "波动压缩", "pctile": percentiles.get("compression_inverse"), "weight": 0.35},
            {"label": "路径稳定", "pctile": percentiles.get("drift_stability"), "weight": 0.25},
            {"label": "承接吸收", "pctile": percentiles.get("absorption_proxy"), "weight": 0.20},
            {"label": "收盘控制", "pctile": percentiles.get("close_control"), "weight": 0.30},
            {"label": "流动性稀薄", "pctile": percentiles.get("liquidity_thinness"), "weight": 0.25},
            {"label": "Derivatives Heat", "pctile": percentiles.get("derivatives_heat"), "weight": 0.55},
            {"label": "Squeeze Setup", "pctile": percentiles.get("squeeze_signal"), "weight": 0.45},
            {"label": "Crowding Risk", "pctile": percentiles.get("crowding_risk"), "weight": 1.00},
            {"label": "Liquidity Trap", "pctile": percentiles.get("liquidity_trap"), "weight": 0.65},
        ],
        "reasons": list(selected.get("reasons_proxy") or []),
    }
    chain_breakdown = {
        "engine": "链上/外生确认引擎",
        "score": selected.get("chain_confirmation_score"),
        "chain_quality": (selected.get("data_quality") or {}).get("chain_quality"),
        "components": [
            {"label": "community flow", "pctile": percentiles.get("community_flow"), "weight": 0.40},
            {"label": "announcements", "pctile": percentiles.get("announcements"), "weight": 0.25},
            {"label": "funding/basis", "pctile": percentiles.get("funding_basis"), "weight": 0.20},
            {"label": "whale context", "pctile": percentiles.get("whale_context"), "weight": 0.15},
            {"label": "flow confirmation", "pctile": percentiles.get("flow_confirmation"), "weight": 0.45},
        ],
        "reasons": list(selected.get("reasons_chain") or []),
        "onchain_context": dict(onchain_context or {}),
    }

    ignition_components = _detail_top_components(
        [
            ("OI 异动", metrics_raw.get("oi_change_1h")),
            ("量能爆发", metrics_raw.get("volume_burst_ratio")),
            ("空头清算占比", metrics_raw.get("short_liq_share")),
            ("压缩后突破", metrics_raw.get("impulse_after_compression")),
            ("盘口/交易所扩散", metrics_raw.get("exchange_breadth")),
        ],
        minimum=0.0,
        limit=4,
    )
    ignition_path = {
        "source": str(selected.get("signal_source") or ""),
        "summary": (
            "当前更像合约驱动的点火候选。"
            if str(selected.get("signal_source") or "") == "perp_ignition"
            else "当前更像延续/确认阶段，点火优先级次于结构确认。"
            if str(selected.get("signal_source") or "") == "perp_continuation"
            else "当前不是典型的 Perp 点火路径，需结合叙事与风险面一起看。"
        ),
        "scores": {
            "ignition": selected.get("ignition_score"),
            "continuation": selected.get("continuation_score"),
            "crowding": selected.get("crowding_late_score"),
            "rank_jump": selected.get("rank_jump_score"),
        },
        "drivers": ignition_components,
        "event_flags": list(selected.get("event_flags") or []),
    }

    selected_sector = str(selected.get("sector") or "").strip()
    narrative_peers: List[Dict[str, Any]] = []
    for row in ordered:
        row_symbol = str(row.get("symbol") or "").strip().upper()
        if row_symbol == normalized_symbol:
            continue
        same_sector = selected_sector and str(row.get("sector") or "").strip() == selected_sector
        same_watchlist = bool(selected.get("in_watchlist")) and bool(row.get("in_watchlist"))
        if not same_sector and not same_watchlist:
            continue
        narrative_peers.append(
            {
                "symbol": row.get("symbol"),
                "sector": row.get("sector"),
                "in_watchlist": bool(row.get("in_watchlist")),
                "signal_source": row.get("signal_source"),
                "narrative_heat_score": row.get("narrative_heat_score"),
                "rank": row.get("rank"),
            }
        )
        if len(narrative_peers) >= 5:
            break

    narrative_drivers = _detail_top_components(
        [
            ("板块热度", percentiles.get("announcements")),
            ("社区流动性", percentiles.get("community_flow")),
            ("量能轮动", percentiles.get("volume_burst")),
            ("鲸鱼/活跃地址", percentiles.get("whale_context")),
            ("短线弹性", percentiles.get("return_shock")),
        ],
        minimum=0.0,
        limit=4,
    )
    narrative_linkage = {
        "sector": selected_sector,
        "in_watchlist": bool(selected.get("in_watchlist")),
        "source": str(selected.get("signal_source") or ""),
        "summary": (
            "当前候选具备叙事先行特征，即使 Perp 数据一般，也可以进入叙事雷达。"
            if str(selected.get("signal_source") or "").startswith("narrative_")
            else "当前更偏合约或结构驱动，叙事层更多是辅助确认。"
        ),
        "scores": {
            "narrative_heat": selected.get("narrative_heat_score"),
            "meme_rotation": selected.get("meme_rotation_score"),
        },
        "drivers": narrative_drivers,
        "board_peers": narrative_peers,
    }

    invalidate_conditions: List[str] = []
    if state == STATE_LAYOUT:
        invalidate_conditions.extend(
            [
                "4h 结构重新放量下破，且 accumulation_score 回落到 0.45 以下",
                "risk_penalty 抬升到 0.25 以上，布局优先级自动失效",
                "close_control 明显走弱，右侧承接不再成立",
            ]
        )
    elif state == STATE_ANOMALY:
        invalidate_conditions.extend(
            [
                "异动后无法站稳，下一轮回落吞没启动 K 线",
                "量能脉冲回落到币池中位以下，说明启动延续性不足",
                "链上/外生确认持续缺失，且 control_score 无法跟上",
            ]
        )
    elif state in {STATE_CONTROL_TRACK, STATE_CONTROL_WARN}:
        invalidate_conditions.extend(
            [
                "spread_bps 继续走阔，价差/流动性风险放大",
                "上影回落继续增加，派发迹象盖过拉抬迹象",
                "安全事件或快照过旧导致 risk_penalty 继续攀升",
            ]
        )
    elif state == STATE_DISTRIBUTION:
        invalidate_conditions.extend(
            [
                "若回踩后吸收重新建立，需重新评估是否由派发转回布局",
                "若异常量价无法延续，派发风险权重可下调",
                "若链上确认转正且 security risk 消退，可降级为高控盘跟踪",
            ]
        )
    else:
        invalidate_conditions.extend(
            [
                "当前候选未形成稳定主标签，等待下一次扫描确认",
                "若 accumulation/control 任一维度站上阈值，将进入正式预警视野",
            ]
        )

    action_plan = _build_action_plan(selected, invalidate_conditions)

    related_candidates = []
    for row in ordered:
        if str(row.get("symbol") or "").upper() == normalized_symbol:
            continue
        related_state = str(row.get("signal_state") or "").strip()
        same_group = bool(state) and related_state == state
        same_sector = selected_sector and str(row.get("sector") or "").strip() == selected_sector
        if same_group or not related_candidates:
            related_candidates.append(
                {
                    "symbol": row.get("symbol"),
                    "signal_state": related_state,
                    "layout_score": row.get("layout_score"),
                    "alert_score": row.get("alert_score"),
                    "control_score": row.get("control_score"),
                    "derivatives_heat_score": row.get("derivatives_heat_score"),
                    "narrative_heat_score": row.get("narrative_heat_score"),
                    "sector": row.get("sector"),
                    "rank": row.get("rank"),
                }
            )
        elif same_sector:
            related_candidates.append(
                {
                    "symbol": row.get("symbol"),
                    "signal_state": related_state,
                    "layout_score": row.get("layout_score"),
                    "alert_score": row.get("alert_score"),
                    "control_score": row.get("control_score"),
                    "derivatives_heat_score": row.get("derivatives_heat_score"),
                    "narrative_heat_score": row.get("narrative_heat_score"),
                    "sector": row.get("sector"),
                    "rank": row.get("rank"),
                }
            )
        if len(related_candidates) >= 5:
            break

    seen_events = set()
    event_timeline: List[Dict[str, Any]] = []
    for event in list(selected.get("recent_events") or []):
        event_key = (event.get("event_type"), event.get("ts_iso"), event.get("symbol"))
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        event_timeline.append(dict(event))
    for event in get_recent_symbol_events(normalized_symbol, limit=12, max_age_sec=6 * 3600.0):
        event_key = (event.get("event_type"), event.get("ts_iso"), event.get("symbol"))
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        event_timeline.append(dict(event))
    event_timeline.sort(key=lambda item: _to_float(item.get("ts"), 0.0), reverse=True)

    return {
        "selected_row": selected,
        "derivatives_context": dict(selected.get("derivatives_context") or {}),
        "proxy_breakdown": proxy_breakdown,
        "chain_breakdown": chain_breakdown,
        "sparkline": _normalized_sparkline(selected.get("sparkline") or []),
        "ignition_path": ignition_path,
        "narrative_linkage": narrative_linkage,
        "event_timeline": event_timeline[:12],
        "invalidate_conditions": invalidate_conditions,
        "action_plan": action_plan,
        "related_candidates": related_candidates,
    }
