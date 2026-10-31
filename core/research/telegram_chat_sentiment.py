"""Read-only diagnostics of frozen BinanceChinese sentiment observations.

Use the receipt-time journal's observations, never reclassify messages or rebuild
past signals from today's labels. This view does not change the frozen protocol.
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

SOURCE = "BinanceChinese"
REASONS = {
    "qualified": "达到情绪门槛",
    "no_directional_views": "无有效 BTC 观点",
    "insufficient_authors": "独立作者不足",
    "low_consensus": "多空分歧较大",
    "coverage_gap": "窗口覆盖不完整",
    "warming_up": "等待首个完整窗口",
    "coverage_unverified": "覆盖尚未确认",
    "collection_error": "采集异常",
    "interrupted": "采集已中断",
    "inaccessible": "来源不可访问",
    "inactive": "未激活",
    "labels_pending": "方向标签待解析",
    "awaiting_observation": "等待窗口记录",
    "unavailable": "窗口记录暂不可读",
}


def _utc(value: Any) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return stamp.astimezone(timezone.utc) if stamp.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def _boundary(at: datetime, hours: int) -> datetime:
    return at.replace(hour=at.hour // hours * hours, minute=0, second=0, microsecond=0)


def _observation(blob: str, hours: int, as_of: datetime) -> dict[str, Any]:
    """Reject malformed observations instead of displaying an invented zero."""
    value = json.loads(blob)
    start, end, available = (_utc(value.get(k)) for k in ("window_start", "window_end", "available_at"))
    n, imbalance, reason = value.get("directional_authors"), value.get("imbalance"), value.get("reason")
    if (value.get("source") != SOURCE or value.get("bucket_hours") != hours
            or not start or not end or not available or end > as_of or available > as_of
            or start != _boundary(start, hours) or end - start != timedelta(hours=hours) or available < end
            or type(n) is not int or n < 0
            or reason not in {"qualified", "no_directional_views", "insufficient_authors", "low_consensus", "coverage_gap"}):
        raise ValueError("Invalid sentiment observation")
    if n == 0:
        if imbalance is not None or reason not in {"no_directional_views", "coverage_gap"}:
            raise ValueError("Invalid empty sentiment observation")
    elif (type(imbalance) not in (int, float) or not math.isfinite(imbalance) or abs(imbalance) > 1
          or reason == "no_directional_views"
          or (reason == "qualified" and (n < 5 or abs(imbalance) < .4))):
        raise ValueError("Invalid sentiment balance")
    return {"hours": hours, "window_start": start.isoformat(), "window_end": end.isoformat(),
            "available_at": available.isoformat(), "authors": n,
            "score": round(imbalance * 100, 2) if imbalance is not None and reason != "coverage_gap" else None,
            "status": reason, "label": REASONS[reason], "qualified": reason == "qualified"}


def snapshot(base: Path, *, now: datetime, activation: dict, status: dict, health: dict) -> dict[str, Any]:
    """Small metadata-only view; missing data, coverage and opinions stay distinct."""
    as_of = min(_utc(status.get("checked_at")) or now, now)
    active_at, clean_since = _utc(activation.get("at")), _utc(health.get("clean_since"))
    result: dict[str, Any] = {"as_of": as_of.isoformat(), "asset": "BTCUSDT", "source": SOURCE,
                              "min_authors": 5, "min_abs_score": 40, "score_range": [-100, 100],
                              "windows": [], "history": [], "diagnostics": [], "read_only": True}
    records: dict[int, list[dict]] = {1: [], 6: []}
    store_error = False
    path = base / "observations.sqlite"
    if path.is_file():
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as db:
                db.execute("PRAGMA query_only=ON")
                # Bound the number of existing records per horizon, using the PK.
                for hours, limit in ((1, 168), (6, 28)):
                    prefix = f"{SOURCE}:{hours}h:"
                    upper = prefix + _boundary(as_of, hours).isoformat()
                    for (blob,) in db.execute(
                            "SELECT payload_json FROM observations WHERE feature_id>=? AND feature_id<=? AND recorded_at<=? "
                            "ORDER BY feature_id DESC LIMIT ?", (prefix, upper, as_of.isoformat(), limit)):
                        records[hours].append(_observation(blob, hours, as_of))
        except (sqlite3.Error, ValueError, TypeError, AttributeError, OverflowError):
            store_error = True
            records = {1: [], 6: []}
    else:
        store_error = True

    for hours in (1, 6):
        end = _boundary(as_of, hours)
        start = end - timedelta(hours=hours)
        window = {"hours": hours, "window_start": start.isoformat(), "window_end": end.isoformat(),
                  "authors": None, "score": None, "qualified": False, "available_at": None,
                  "next_complete_at": None}
        reason = health.get("status", "coverage_unverified")
        if not active_at:
            reason = "inactive"
        elif reason == "healthy":
            if not clean_since:
                reason = "coverage_unverified"
            elif start < max(active_at, clean_since):
                reason = "warming_up" if start < active_at else "coverage_gap"
                first = max(active_at, clean_since)
                first_start = _boundary(first, hours)
                if first_start < first:
                    first_start += timedelta(hours=hours)
                window["next_complete_at"] = (first_start + timedelta(hours=hours)).isoformat()
            elif (status.get("pending_labels") or {}).get(SOURCE, 0):
                reason = "labels_pending"
            elif store_error:
                reason = "unavailable"
            else:
                observed = next((r for r in records[hours] if r["window_end"] == end.isoformat()), None)
                if observed:
                    window.update(observed)
                    reason = observed["status"]
                else:
                    reason = "awaiting_observation"
        window.update(status=reason, label=REASONS.get(reason, REASONS["coverage_unverified"]))
        result["windows"].append(window)
        values = records[hours]
        result["diagnostics"].append({"hours": hours, "recorded_windows": len(values) if not store_error else None,
                                       "max_authors": max((r["authors"] for r in values), default=None),
                                       "reasons": [{"status": key, "label": REASONS[key], "count": n}
                                                   for key, n in Counter(r["status"] for r in values).items()]})
    result["history"] = sorted(records[1][:8] + records[6][:4], key=lambda r: r["window_end"], reverse=True)
    result["store_available"] = not store_error
    return result
