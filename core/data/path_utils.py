"""Helpers for canonical market-data storage paths."""
from __future__ import annotations

from pathlib import Path
from typing import List


_KNOWN_QUOTES = ("USDT", "USDC", "USD", "BTC", "ETH", "BNB")


def normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    raw = raw.replace("-", "/").replace("_", "/")
    if "/" not in raw:
        for quote in _KNOWN_QUOTES:
            if raw.endswith(quote) and len(raw) > len(quote):
                return f"{raw[:-len(quote)]}/{quote}"
        return raw
    base, quote = [part.strip() for part in raw.split("/", 1)]
    if not base or not quote:
        return ""
    return f"{base}/{quote}"


def canonical_symbol_dirname(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return ""
    return normalized.replace("/", "_")


def symbol_from_storage_dirname(folder_name: str) -> str:
    return normalize_symbol(folder_name)


def candidate_symbol_dirnames(symbol: str) -> List[str]:
    raw = str(symbol or "").strip().upper()
    normalized = normalize_symbol(symbol)
    candidates: List[str] = []

    def add(value: str) -> None:
        text = str(value or "").strip()
        if text and text not in candidates:
            candidates.append(text)

    canonical = canonical_symbol_dirname(symbol)
    add(canonical)
    add(raw.replace("/", "_"))
    if normalized:
        add(normalized.replace("/", ""))
        add(normalized.replace("/", "-"))
    return candidates


def canonical_symbol_dir(storage_root: Path, exchange: str, symbol: str) -> Path:
    return Path(storage_root) / str(exchange or "").lower() / canonical_symbol_dirname(symbol)


def candidate_symbol_dirs(storage_root: Path, exchange: str, symbol: str) -> List[Path]:
    root = Path(storage_root) / str(exchange or "").lower()
    return [root / dirname for dirname in candidate_symbol_dirnames(symbol) if dirname]
