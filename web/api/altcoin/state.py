"""Process-wide mutable caches for the altcoin radar scan pipeline.

These dicts are shared, imported-by-reference state: the package __init__
re-exports them, and tests mutate altcoin_api._ALTCOIN_SCAN_CACHE etc. directly,
so there must be exactly one instance of each. Never rebind these names.
"""
from __future__ import annotations

import asyncio
import threading
from typing import Any, Dict


_ALTCOIN_SCAN_CACHE: Dict[str, Dict[str, Any]] = {}
_ALTCOIN_SCAN_LOCKS: Dict[str, asyncio.Lock] = {}
_ALTCOIN_SCAN_LOCKS_GUARD = threading.Lock()
_ALTCOIN_SCAN_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_PUBLIC_MARKET_SNAPSHOT_CACHE: Dict[str, Dict[str, Any]] = {}
