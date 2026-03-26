"""Shared text cleanup helpers for news titles and summaries."""
from __future__ import annotations

import html
import re
from typing import Any


def _text_score(text: str) -> int:
    if not text:
        return 0
    cjk = sum(0x4E00 <= ord(ch) <= 0x9FFF for ch in text)
    latin1 = sum(0x80 <= ord(ch) <= 0xFF for ch in text)
    controls = sum(0x80 <= ord(ch) <= 0x9F for ch in text)
    replacement = text.count("\ufffd")
    punctuation = sum(ch in "【】《》：，。、“”！？；（）·" for ch in text)
    return (cjk * 6) + (punctuation * 2) - (latin1 * 2) - (controls * 6) - (replacement * 8)


def repair_mojibake_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    best = text
    best_score = _text_score(text)
    encodings = ("latin-1", "cp1252")
    for source_encoding in encodings:
        try:
            candidate = text.encode(source_encoding).decode("utf-8")
        except Exception:
            continue
        candidate = candidate.strip()
        if not candidate or candidate == best:
            continue
        score = _text_score(candidate)
        if score >= best_score + 6:
            best = candidate
            best_score = score
    return best


def clean_news_text(value: Any) -> str:
    text = html.unescape(str(value or "").strip())
    if not text:
        return ""
    text = repair_mojibake_text(text)
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
