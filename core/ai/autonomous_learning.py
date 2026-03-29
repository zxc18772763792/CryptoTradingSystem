from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


_DEFAULT_LOOKBACK_HOURS = 72
_DEFAULT_RECENT_HOURS = 24


def _utc_now(now: Optional[datetime] = None) -> datetime:
    if isinstance(now, datetime):
        return now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return float(default)
    if math.isnan(number) or math.isinf(number):
        return float(default)
    return float(number)


def safe_positive_float(value: Any, default: float = 0.0) -> float:
    number = safe_float(value, default)
    return number if number > 0 else float(default)


def parse_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[0].strip()
    if "_" in text and "/" not in text:
        left, right = text.split("_", 1)
        text = f"{left}/{right}"
    if "/" not in text and text.endswith("USDT") and len(text) > 4:
        text = f"{text[:-4]}/USDT"
    return text


def _signal_type_from_trade_item(item: Dict[str, Any]) -> str:
    signal = item.get("signal") if isinstance(item.get("signal"), dict) else {}
    return str(signal.get("signal_type") or item.get("signal_type") or "").strip().lower()


def trade_item_position_side(item: Dict[str, Any]) -> str:
    signal_type = _signal_type_from_trade_item(item)
    if signal_type in {"buy", "close_long"}:
        return "long" if signal_type == "buy" else "long"
    if signal_type in {"sell", "close_short"}:
        return "short" if signal_type == "sell" else "short"

    action = str(item.get("action") or "").strip().lower()
    side = str(item.get("side") or "").strip().lower()
    if action == "open_or_add":
        if side == "buy":
            return "long"
        if side == "sell":
            return "short"
    if action == "close":
        if side == "buy":
            return "short"
        if side == "sell":
            return "long"
    return ""


def _trade_item_timestamp(item: Dict[str, Any]) -> Optional[datetime]:
    for key in ("timestamp", "opened_at", "updated_at", "recorded_at"):
        parsed = parse_dt(item.get(key))
        if parsed is not None:
            return parsed
    signal = item.get("signal") if isinstance(item.get("signal"), dict) else {}
    return parse_dt(signal.get("timestamp"))


def _row_timestamp(row: Dict[str, Any]) -> Optional[datetime]:
    for key in ("timestamp", "created_at", "updated_at", "ts"):
        parsed = parse_dt(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _filter_recent(
    rows: Iterable[Dict[str, Any]],
    *,
    hours: int,
    now: Optional[datetime] = None,
    timestamp_reader,
) -> List[Dict[str, Any]]:
    anchor = _utc_now(now)
    cutoff = anchor - timedelta(hours=max(1, int(hours or 1)))
    items: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ts = timestamp_reader(row)
        if ts is None or ts < cutoff:
            continue
        items.append(row)
    return items


def default_learning_memory(base_min_confidence: float = 0.58) -> Dict[str, Any]:
    base_conf = max(0.0, min(1.0, float(base_min_confidence or 0.58)))
    return {
        "generated_at": None,
        "version": 1,
        "window": {
            "recent_hours": int(_DEFAULT_RECENT_HOURS),
            "lookback_hours": int(_DEFAULT_LOOKBACK_HOURS),
            "recent_journal_rows": 0,
            "recent_live_trades": 0,
        },
        "summary": {
            "recent_executed_entry_count": 0,
            "recent_close_count": 0,
            "recent_close_loss_count": 0,
            "recent_close_win_count": 0,
            "recent_close_net_pnl": 0.0,
            "recent_model_issue_count": 0,
            "recent_no_price_count": 0,
            "recent_researchless_entry_count": 0,
            "recent_same_direction_reentry_count": 0,
            "recent_latency_avg_ms": 0.0,
            "current_open_position_count": 0,
            "current_open_unrealized_pnl": 0.0,
            "current_open_losing_count": 0,
        },
        "adaptive_risk": {
            "effective_min_confidence": float(base_conf),
            "same_direction_max_exposure_ratio": 0.5,
            "entry_size_scale": 1.0,
            "require_research_for_new_entries": False,
            "force_close_on_data_outage_losing_position": False,
            "avoid_new_entries_during_service_instability": False,
            "data_quality_hold_bias": False,
        },
        "blocked_symbol_sides": [],
        "symbol_side_stats": [],
        "guardrails": [],
        "lessons": [],
    }


def coerce_learning_memory(memory: Any, *, base_min_confidence: float = 0.58) -> Dict[str, Any]:
    merged = default_learning_memory(base_min_confidence=base_min_confidence)
    if not isinstance(memory, dict):
        return merged

    for key in ("generated_at", "version"):
        if key in memory:
            merged[key] = memory[key]

    for top_key in ("window", "summary", "adaptive_risk"):
        raw = memory.get(top_key)
        if isinstance(raw, dict):
            merged[top_key].update(raw)

    for top_key in ("blocked_symbol_sides", "symbol_side_stats", "guardrails", "lessons"):
        raw = memory.get(top_key)
        if isinstance(raw, list):
            merged[top_key] = list(raw)
    return merged


def build_blocked_symbol_side_map(
    memory: Any,
    *,
    now: Optional[datetime] = None,
    base_min_confidence: float = 0.58,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    merged = coerce_learning_memory(memory, base_min_confidence=base_min_confidence)
    anchor = _utc_now(now)
    blocked: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in merged.get("blocked_symbol_sides") or []:
        if not isinstance(item, dict):
            continue
        symbol = normalize_symbol(item.get("symbol"))
        side = str(item.get("side") or "").strip().lower()
        if not symbol or side not in {"long", "short"}:
            continue
        cooldown_until = parse_dt(item.get("cooldown_until"))
        active = bool(item.get("cooldown_active"))
        if cooldown_until is not None and cooldown_until <= anchor:
            active = False
        if not active:
            continue
        blocked[(symbol, side)] = dict(item)
    return blocked


def _position_payload(position: Any) -> Optional[Dict[str, Any]]:
    payload: Optional[Dict[str, Any]]
    if isinstance(position, dict):
        payload = dict(position)
    elif hasattr(position, "to_dict"):
        try:
            payload = position.to_dict()
        except Exception:
            payload = None
    else:
        payload = None
    if not isinstance(payload, dict):
        return None

    side_value = payload.get("side")
    if hasattr(side_value, "value"):
        side_value = side_value.value
    return {
        "symbol": normalize_symbol(payload.get("symbol")),
        "side": str(side_value or "").strip().lower(),
        "strategy": str(payload.get("strategy") or "").strip(),
        "unrealized_pnl": safe_float(payload.get("unrealized_pnl"), 0.0),
        "unrealized_pnl_pct": safe_float(payload.get("unrealized_pnl_pct"), 0.0),
        "updated_at": payload.get("updated_at"),
    }


def build_learning_memory(
    *,
    journal_rows: Optional[List[Dict[str, Any]]] = None,
    live_review: Optional[Dict[str, Any]] = None,
    positions: Optional[List[Any]] = None,
    base_min_confidence: float = 0.58,
    recent_hours: int = _DEFAULT_RECENT_HOURS,
    lookback_hours: int = _DEFAULT_LOOKBACK_HOURS,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    anchor = _utc_now(now)
    memory = default_learning_memory(base_min_confidence=base_min_confidence)

    journal_rows = list(journal_rows or [])
    live_items = []
    if isinstance(live_review, dict):
        raw_items = live_review.get("items")
        if isinstance(raw_items, list):
            live_items = list(raw_items)
    elif isinstance(live_review, list):
        live_items = list(live_review)

    current_positions = []
    for item in positions or []:
        payload = _position_payload(item)
        if payload and payload.get("symbol") and payload.get("side") in {"long", "short"}:
            current_positions.append(payload)

    recent_journal = _filter_recent(
        journal_rows,
        hours=recent_hours,
        now=anchor,
        timestamp_reader=_row_timestamp,
    )
    recent_live_items = _filter_recent(
        live_items,
        hours=lookback_hours,
        now=anchor,
        timestamp_reader=_trade_item_timestamp,
    )

    latency_values: List[float] = []
    model_issue_count = 0
    no_price_count = 0
    researchless_entry_count = 0
    recent_executed_entry_count = 0
    recent_issue_labels: defaultdict[str, int] = defaultdict(int)

    for row in recent_journal:
        latency = safe_float(row.get("latency_ms"), -1.0)
        if latency > 0:
            latency_values.append(latency)

        decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
        execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
        context = row.get("context") if isinstance(row.get("context"), dict) else {}
        research = context.get("research_context") if isinstance(context.get("research_context"), dict) else {}
        diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
        primary = diagnostics.get("primary") if isinstance(diagnostics.get("primary"), dict) else {}

        reason = str(decision.get("reason") or row.get("rejection_reason") or "").strip().lower()
        price = safe_float(context.get("price"), 0.0)
        market_structure = context.get("market_structure") if isinstance(context.get("market_structure"), dict) else {}
        if reason.startswith("model_error:"):
            model_issue_count += 1
            label = str(primary.get("label") or "model_error").strip()
            if label:
                recent_issue_labels[label] += 1
        if reason == "no_price" or price <= 0 or not bool(market_structure.get("available", price > 0)):
            no_price_count += 1

        if bool(execution.get("submitted")):
            action = str(decision.get("action") or "").strip().lower()
            if action in {"buy", "sell"}:
                recent_executed_entry_count += 1
                if not bool(research.get("available")):
                    researchless_entry_count += 1

    symbol_side_stats: defaultdict[Tuple[str, str], Dict[str, Any]] = defaultdict(
        lambda: {
            "symbol": "",
            "side": "",
            "entry_count": 0,
            "close_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "net_pnl": 0.0,
            "last_entry_at": None,
            "last_loss_at": None,
            "current_open_count": 0,
            "current_open_unrealized_pnl": 0.0,
            "current_open_unrealized_pnl_pct": 0.0,
        }
    )

    recent_close_count = 0
    recent_close_loss_count = 0
    recent_close_win_count = 0
    recent_close_net_pnl = 0.0
    entry_sequence: List[Tuple[datetime, str, str]] = []

    for item in sorted(recent_live_items, key=lambda row: _trade_item_timestamp(row) or anchor):
        symbol = normalize_symbol(item.get("symbol") or ((item.get("signal") or {}) if isinstance(item.get("signal"), dict) else {}).get("symbol"))
        side = trade_item_position_side(item)
        if not symbol or side not in {"long", "short"}:
            continue
        stat = symbol_side_stats[(symbol, side)]
        stat["symbol"] = symbol
        stat["side"] = side

        action = str(item.get("action") or "").strip().lower()
        ts = _trade_item_timestamp(item)
        if action == "open_or_add":
            stat["entry_count"] += 1
            stat["last_entry_at"] = ts.isoformat() if ts else stat.get("last_entry_at")
            if ts is not None:
                entry_sequence.append((ts, symbol, side))
        elif action == "close":
            pnl = safe_float(item.get("pnl"), 0.0)
            stat["close_count"] += 1
            stat["net_pnl"] += pnl
            recent_close_count += 1
            recent_close_net_pnl += pnl
            if pnl > 0:
                stat["win_count"] += 1
                recent_close_win_count += 1
            elif pnl < 0:
                stat["loss_count"] += 1
                stat["last_loss_at"] = ts.isoformat() if ts else stat.get("last_loss_at")
                recent_close_loss_count += 1

    last_entry_by_pair: Dict[Tuple[str, str], datetime] = {}
    recent_same_direction_reentry_count = 0
    for ts, symbol, side in entry_sequence:
        pair_key = (symbol, side)
        previous = last_entry_by_pair.get(pair_key)
        if previous is not None and (ts - previous) <= timedelta(hours=12):
            recent_same_direction_reentry_count += 1
        last_entry_by_pair[pair_key] = ts

    current_open_unrealized_pnl = 0.0
    current_open_losing_count = 0
    for position in current_positions:
        symbol = normalize_symbol(position.get("symbol"))
        side = str(position.get("side") or "").strip().lower()
        if not symbol or side not in {"long", "short"}:
            continue
        stat = symbol_side_stats[(symbol, side)]
        stat["symbol"] = symbol
        stat["side"] = side
        stat["current_open_count"] += 1
        stat["current_open_unrealized_pnl"] += safe_float(position.get("unrealized_pnl"), 0.0)
        stat["current_open_unrealized_pnl_pct"] = safe_float(position.get("unrealized_pnl_pct"), 0.0)
        current_open_unrealized_pnl += safe_float(position.get("unrealized_pnl"), 0.0)
        if safe_float(position.get("unrealized_pnl"), 0.0) < 0:
            current_open_losing_count += 1

    recent_journal_rows = len(recent_journal)
    model_issue_rate = model_issue_count / max(1, recent_journal_rows)
    avg_latency_ms = sum(latency_values) / len(latency_values) if latency_values else 0.0

    effective_min_confidence = float(base_min_confidence or 0.58)
    if recent_close_count > 0 and recent_close_loss_count > recent_close_win_count:
        effective_min_confidence += 0.04
    if current_open_unrealized_pnl < 0:
        effective_min_confidence += 0.02
    if model_issue_rate >= 0.25:
        effective_min_confidence += 0.03
    if no_price_count > 0:
        effective_min_confidence += 0.03
    if avg_latency_ms >= 60000.0:
        effective_min_confidence += 0.02
    effective_min_confidence = round(min(0.82, max(float(base_min_confidence or 0.58), effective_min_confidence)), 4)

    same_direction_ratio = 0.5
    if recent_same_direction_reentry_count > 0:
        same_direction_ratio = min(same_direction_ratio, 0.4)
    if recent_close_loss_count > 0 or current_open_losing_count > 0:
        same_direction_ratio = min(same_direction_ratio, 0.35)
    if no_price_count > 0 or model_issue_rate >= 0.35:
        same_direction_ratio = min(same_direction_ratio, 0.3)
    same_direction_ratio = round(max(0.2, same_direction_ratio), 4)

    entry_size_scale = 1.0
    if recent_close_count > 0 and recent_close_loss_count > recent_close_win_count:
        entry_size_scale = min(entry_size_scale, 0.8)
    if no_price_count > 0 or model_issue_rate >= 0.25:
        entry_size_scale = min(entry_size_scale, 0.7)
    if current_open_losing_count > 0:
        entry_size_scale = min(entry_size_scale, 0.65)
    entry_size_scale = round(max(0.25, entry_size_scale), 4)

    blocked_symbol_sides: List[Dict[str, Any]] = []
    for (symbol, side), stat in symbol_side_stats.items():
        current_loss = safe_float(stat.get("current_open_unrealized_pnl"), 0.0) < 0
        recent_loss_bias = int(stat.get("loss_count") or 0) > int(stat.get("win_count") or 0) and int(stat.get("loss_count") or 0) >= 1
        if not current_loss and not recent_loss_bias:
            continue

        last_anchor = (
            parse_dt(stat.get("last_loss_at"))
            or parse_dt(stat.get("last_entry_at"))
            or anchor
        )
        cooldown_minutes = 360 if current_loss else 180
        cooldown_until = last_anchor + timedelta(minutes=cooldown_minutes)
        blocked_symbol_sides.append(
            {
                "symbol": symbol,
                "side": side,
                "cooldown_minutes": int(cooldown_minutes),
                "cooldown_until": cooldown_until.isoformat(),
                "cooldown_active": cooldown_until > anchor,
                "reason": (
                    f"net_pnl={safe_float(stat.get('net_pnl'), 0.0):+.4f}, "
                    f"losses={int(stat.get('loss_count') or 0)}, "
                    f"open_unrealized={safe_float(stat.get('current_open_unrealized_pnl'), 0.0):+.4f}"
                ),
            }
        )

    blocked_symbol_sides.sort(
        key=lambda item: (
            1 if safe_float(next(
                (
                    stat.get("current_open_unrealized_pnl")
                    for (_, _), stat in symbol_side_stats.items()
                    if stat.get("symbol") == item.get("symbol") and stat.get("side") == item.get("side")
                ),
                0.0,
            ), 0.0) < 0 else 0,
            -abs(safe_float(next(
                (
                    stat.get("net_pnl")
                    for (_, _), stat in symbol_side_stats.items()
                    if stat.get("symbol") == item.get("symbol") and stat.get("side") == item.get("side")
                ),
                0.0,
            ), 0.0)),
        ),
        reverse=True,
    )

    lessons: List[str] = []
    if recent_close_count > 0:
        lessons.append(
            f"最近 {recent_close_count} 次平仓里，亏损 {recent_close_loss_count} 次，净 PnL {recent_close_net_pnl:+.4f}。"
        )
    if no_price_count > 0:
        lessons.append(f"最近 {recent_hours} 小时出现 {no_price_count} 次价格/行情缺失，持仓管理需要优先走防守逻辑。")
    if model_issue_count > 0:
        lessons.append(
            f"最近 {recent_hours} 小时模型异常 {model_issue_count} 次，平均链路耗时 {avg_latency_ms:.0f} ms。"
        )
    if researchless_entry_count > 0 and recent_executed_entry_count > 0:
        lessons.append(
            f"最近新开仓里有 {researchless_entry_count}/{recent_executed_entry_count} 笔没有研究候选支撑。"
        )
    if current_open_unrealized_pnl < 0:
        lessons.append(
            f"当前自治持仓浮盈亏合计 {current_open_unrealized_pnl:+.4f}，应优先处理已持仓风险。"
        )
    if blocked_symbol_sides:
        top = blocked_symbol_sides[0]
        lessons.append(
            f"{top['symbol']} {top['side']} 已进入复盘冷静期，新的同向开仓默认应回避。"
        )
    if not lessons:
        lessons.append("近期样本量有限，先维持基础阈值并继续积累交易与复盘样本。")

    guardrails: List[str] = [
        f"fresh entry min confidence >= {effective_min_confidence:.2f}",
        f"same-side exposure ratio <= {same_direction_ratio:.2f}",
    ]
    if researchless_entry_count > 0 and recent_close_loss_count > 0:
        guardrails.append("require research context before fresh entries")
    if no_price_count > 0:
        guardrails.append("close losing positions when market data is unavailable")
    if model_issue_rate >= 0.25:
        guardrails.append("avoid fresh entries while model service is unstable")

    symbol_side_rows = []
    for (_, _), stat in symbol_side_stats.items():
        row = dict(stat)
        row["net_pnl"] = round(safe_float(row.get("net_pnl"), 0.0), 6)
        row["current_open_unrealized_pnl"] = round(safe_float(row.get("current_open_unrealized_pnl"), 0.0), 6)
        row["current_open_unrealized_pnl_pct"] = round(safe_float(row.get("current_open_unrealized_pnl_pct"), 0.0), 6)
        symbol_side_rows.append(row)
    symbol_side_rows.sort(
        key=lambda item: (
            safe_float(item.get("current_open_unrealized_pnl"), 0.0),
            safe_float(item.get("net_pnl"), 0.0),
            -int(item.get("win_count") or 0),
        )
    )

    adaptive_risk = memory["adaptive_risk"]
    adaptive_risk.update(
        {
            "effective_min_confidence": float(effective_min_confidence),
            "same_direction_max_exposure_ratio": float(same_direction_ratio),
            "entry_size_scale": float(entry_size_scale),
            "require_research_for_new_entries": bool(
                researchless_entry_count > 0 and recent_close_loss_count > 0
            ),
            "force_close_on_data_outage_losing_position": bool(no_price_count > 0),
            "avoid_new_entries_during_service_instability": bool(model_issue_rate >= 0.25),
            "data_quality_hold_bias": bool(no_price_count > 0 or model_issue_rate >= 0.35),
        }
    )

    memory["generated_at"] = anchor.isoformat()
    memory["window"] = {
        "recent_hours": int(recent_hours),
        "lookback_hours": int(lookback_hours),
        "recent_journal_rows": int(recent_journal_rows),
        "recent_live_trades": int(len(recent_live_items)),
    }
    memory["summary"] = {
        "recent_executed_entry_count": int(recent_executed_entry_count),
        "recent_close_count": int(recent_close_count),
        "recent_close_loss_count": int(recent_close_loss_count),
        "recent_close_win_count": int(recent_close_win_count),
        "recent_close_net_pnl": round(float(recent_close_net_pnl), 6),
        "recent_model_issue_count": int(model_issue_count),
        "recent_no_price_count": int(no_price_count),
        "recent_researchless_entry_count": int(researchless_entry_count),
        "recent_same_direction_reentry_count": int(recent_same_direction_reentry_count),
        "recent_latency_avg_ms": round(float(avg_latency_ms), 3),
        "current_open_position_count": int(len(current_positions)),
        "current_open_unrealized_pnl": round(float(current_open_unrealized_pnl), 6),
        "current_open_losing_count": int(current_open_losing_count),
        "recent_issue_labels": [
            {"label": label, "count": int(count)}
            for label, count in sorted(recent_issue_labels.items(), key=lambda item: (-item[1], item[0]))[:5]
        ],
    }
    memory["blocked_symbol_sides"] = blocked_symbol_sides[:8]
    memory["symbol_side_stats"] = symbol_side_rows[:12]
    memory["guardrails"] = guardrails[:6]
    memory["lessons"] = lessons[:8]
    return memory
