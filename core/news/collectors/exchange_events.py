"""Classify official exchange announcements into event types for research.

Event studies (listing/delisting reactions) need the announcement kind as a
structured field rather than re-deriving it from free text downstream.
"""
from __future__ import annotations

import re

_RULES = (
    # Order matters: "delist" must win over "list", futures before spot listing.
    ("delisting", re.compile(r"\bdelist|\bremoval of\b|\bwill remove\b|\bto remove\b|\bcease trading\b|\bwill cease\b", re.I)),
    (
        "futures_launch",
        re.compile(
            r"\bfutures will launch\b|\bwill launch\b.*\b(perpetual|futures|contract)\b"
            r"|\bto list\b.*\b(perpetual|futures|x-perps?|swaps?)\b|\bperpetual contract\b",
            re.I,
        ),
    ),
    (
        "listing",
        re.compile(
            r"\bwill list\b|\bto list\b|\bwill add\b|\badds\b.*\btrading pairs?\b"
            r"|\bnew\b.{0,40}\btrading pairs?\b",
            re.I,
        ),
    ),
)


def classify_exchange_announcement(title: str) -> str:
    """Return ``delisting``/``futures_launch``/``listing``/``other`` from the title.

    The exchange's own category is not used: OKX files contract adjustments and
    Binance files Earn/Margin additions under "new listings".
    """
    text = str(title or "")
    for event_type, pattern in _RULES:
        if pattern.search(text):
            return event_type
    return "other"
