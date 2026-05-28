from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _strip_allowed_legacy_samples(rel_path: str, text: str) -> str:
    if rel_path == "web/static/js/ai_research.js":
        return re.sub(
            r"const LEGACY_MOJIBAKE_REPAIRS = \[.*?\n\s*\];",
            "const LEGACY_MOJIBAKE_REPAIRS = [];",
            text,
            flags=re.S,
        )
    return text


def test_touched_content_files_do_not_expose_mojibake_markers():
    files = [
        "config/database.py",
        "core/data/funding_rate_collector.py",
        "core/risk/risk_manager.py",
        "strategies/technical/macd_strategy.py",
        "web/api/trading.py",
        "web/static/js/altcoin_radar.js",
        "web/static/js/ai_research.js",
    ]
    markers = (
        "鈹",
        "鈥",
        "鍊欓",
        "鐮旂",
        "娴嬭",
        "璧勯",
        "闃熷",
        "鎺掑",
        "寰呬汉",
        "缁戝",
        "瀹屽",
        "鑾峰",
        "鍒濆",
        "鍋滄",
        "涓婂",
        "涓嬮",
        "璐︽",
        "浜氱",
        "娆х",
        "缇庣",
        "鏇存柊",
        "澶辫触",
        "????????",
    )

    offenders: list[str] = []
    for rel_path in files:
        text = _strip_allowed_legacy_samples(rel_path, _read(rel_path))
        for line_no, line in enumerate(text.splitlines(), 1):
            if any(marker in line for marker in markers):
                offenders.append(f"{rel_path}:{line_no}:{line.strip()}")

    assert offenders == []
