from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_touched_content_files_do_not_expose_mojibake_markers():
    files = [
        "config/database.py",
        "core/data/funding_rate_collector.py",
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
    )

    offenders: list[str] = []
    for rel_path in files:
        text = _read(rel_path)
        if rel_path.endswith("ai_research.js"):
            text = re.sub(r"const replacements = \[.*?\n\s*\];", "const replacements = [];", text, flags=re.S)
        for line_no, line in enumerate(text.splitlines(), 1):
            if any(marker in line for marker in markers):
                offenders.append(f"{rel_path}:{line_no}:{line.strip()}")

    assert offenders == []
