"""Read-only views of the Telegram forward paper books for the tracker table.

The five ledgers are written only by ``scripts/telegram_forward_research.py cycle``
(frozen protocol, implementation-hashed). Nothing here imports that script - it
removes every loguru handler on import - and nothing here writes to the research
directory. Two rows are exposed:

* ``cryptoquant``    - the CryptoQuant channel, ledger ``cq_follow`` (24h).
* ``binance_chat``   - the BinanceChinese group sentiment, four ledgers (hourly /
  6h windows, follow / contrarian). They share one chat stream and mirror each
  other, so they are one tracker, not four independent samples.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIR = ROOT / "data" / "research" / "telegram_live_20261024"
MIN_MATURE_TRADES = 100  # report_telegram_forward.py's descriptive-screen threshold
FRESH_SECONDS = 15 * 60  # three missed 5-minute cycles

CRYPTOQUANT_BOOKS = ("cq_follow",)
CHAT_BOOKS = ("chat_follow", "chat_contrarian", "chat_6h_follow", "chat_6h_contrarian")
BOOK_LABELS = {
    "cq_follow": "CryptoQuant 顺势24h",
    "chat_follow": "主群小时顺势6h",
    "chat_contrarian": "主群小时反向6h",
    "chat_6h_follow": "主群六小时顺势6h",
    "chat_6h_contrarian": "主群六小时反向6h",
}
SOURCE_OF = {"cryptoquant": "cryptoquant_official", "binance_chat": "BinanceChinese"}
PRIMARY = {"cryptoquant": "cq_follow", "binance_chat": "chat_follow"}


def research_dir() -> Path:
    override = str(os.environ.get("TELEGRAM_FORWARD_DIR") or "").strip()
    return Path(override) if override else DEFAULT_DIR


def _read(name: str, base: Optional[Path] = None) -> Dict[str, Any]:
    try:
        data = json.loads(((base or research_dir()) / name).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _utc(value: Any) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _age_seconds(value: Any, now: datetime) -> Optional[float]:
    stamp = _utc(value)
    return None if stamp is None else max(0.0, (now - stamp).total_seconds())


def _pct(value: Any) -> Optional[float]:
    return None if value is None else round(float(value) * 100, 4)


def _runner_state() -> Dict[str, Any]:
    try:
        data = json.loads((ROOT / "runtime" / "telegram_forward_runner.json").read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _book_row(name: str, book: Dict[str, Any]) -> Dict[str, Any]:
    return {"book": name, "label": BOOK_LABELS[name], "decisions": book.get("decisions", 0),
            "mature_settled": book.get("mature_settled", 0), "pending": book.get("pending", 0),
            "unavailable": book.get("unavailable", 0), "mean_net_pct": _pct(book.get("mean_net_per_notional")),
            "mean_excess_pct": _pct(book.get("mean_excess_vs_same_entry_long")),
            "win_rate": book.get("win_rate"), "status": book.get("status")}


def summary(kind: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Registry-shaped summary for ``cryptoquant`` or ``binance_chat``; never raises on missing files."""
    now = now or datetime.now(timezone.utc)
    names = CRYPTOQUANT_BOOKS if kind == "cryptoquant" else CHAT_BOOKS
    activation = _read("activation.json")
    status = _read("forward_status.json")
    books = (_read("forward_summary.json").get("books") or {})
    ledgers = [_book_row(n, books.get(n) or {}) for n in names]
    primary = next((r for r in ledgers if r["book"] == PRIMARY[kind]), None) or {}
    n = int(primary.get("mature_settled") or 0)
    mean = primary.get("mean_net_pct") if n else None

    checked_age = _age_seconds(status.get("checked_at"), now)
    source = (status.get("sources") or {}).get(SOURCE_OF[kind]) or {}
    error = _read("last_error.json")
    error_after_success = bool(error) and (_utc(error.get("at")) or now) > (_utc(status.get("checked_at")) or datetime.min.replace(tzinfo=timezone.utc))
    runner = _runner_state()
    fresh = checked_age is not None and checked_age <= FRESH_SECONDS
    gap = bool(source.get("difference_gap")) or source.get("complete_batch") is False
    if not activation:
        label = "未激活"
    elif error_after_success:
        label = "采集异常"
    elif not fresh:
        label = "采集已中断" if checked_age is None else f"采集已中断 · 最近成功 {int(checked_age // 60)} 分钟前"
    elif gap:
        label = "运行中 · 本轮有缺口"
    else:
        label = "运行中 · 前向观察"
    decisions = sum(int(r["decisions"] or 0) for r in ledgers)
    reason = (f"{len(ledgers)} 本账本；已冻结 {decisions} 笔，主账本成熟 {n} 笔，至少 {MIN_MATURE_TRADES} 笔且满 8 个日历周才做描述性筛查。"
              "空样本不是 0% 收益；缺口窗口不补；promotion_allowed 恒为 false。")
    return {
        "retirement": {"n": n, "min_n": MIN_MATURE_TRADES, "mean_pct": mean, "ci90_pct": None,
                       "verdict": "collecting", "label": "前向积累样本" if activation else "未激活", "reason": reason},
        "forward_win_rate": primary.get("win_rate"),
        "forward_open": sum(int(r["pending"] or 0) for r in ledgers),
        "started_at": activation.get("at"),
        "updated_at": status.get("checked_at"),
        "runner_active": bool(activation) and fresh and not error_after_success,
        "tracker_status_label": label,
        "paper_only": True,
        "source_health": {"source": SOURCE_OF[kind], "member": source.get("member"),
                          "last_collection_at": source.get("last_collection_at"),
                          "clean_since": source.get("clean_since"), "difference_gap": source.get("difference_gap")},
        "ledgers": ledgers,
        "last_error": {"error_type": error.get("error_type"), "at": error.get("at"),
                       "retry_at": error.get("retry_at")} if error_after_success else None,
        "runner": {"enabled": runner.get("enabled"), "last_exit_code": runner.get("last_exit_code"),
                   "last_run_at": runner.get("last_run_at"), "last_outcome": runner.get("last_outcome")},
    }


def cryptoquant() -> Dict[str, Any]:
    return summary("cryptoquant")


def binance_chat() -> Dict[str, Any]:
    return summary("binance_chat")


def detail() -> Dict[str, Any]:
    ledgers: List[Dict[str, Any]] = []
    books = (_read("forward_summary.json").get("books") or {})
    for name in CRYPTOQUANT_BOOKS + CHAT_BOOKS:
        ledgers.append(_book_row(name, books.get(name) or {}))
    return {"cryptoquant": summary("cryptoquant"), "binance_chat": summary("binance_chat"), "ledgers": ledgers,
            "status": _read("forward_status.json"), "technical_stress": _read("technical_features.json")}
