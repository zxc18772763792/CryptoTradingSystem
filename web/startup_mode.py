from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


_VALID_TRADING_MODES = {"paper", "live"}


def normalize_trading_mode(value: Any, *, fallback: str = "paper") -> str:
    text = str(value or "").strip().lower()
    return text if text in _VALID_TRADING_MODES else fallback


@dataclass(frozen=True)
class StartupModeDecision:
    configured_mode: str
    persisted_mode: str
    effective_mode: str
    source: str
    blocked_persisted_live_restore: bool


def resolve_startup_trading_mode(
    *,
    configured_mode: Any,
    persisted_account: Mapping[str, Any] | None,
    allow_persisted_live_mode_start: bool = False,
) -> StartupModeDecision:
    configured = normalize_trading_mode(configured_mode, fallback="paper")
    persisted = normalize_trading_mode(
        (persisted_account or {}).get("mode"),
        fallback="",
    )

    if configured == "live":
        return StartupModeDecision(
            configured_mode=configured,
            persisted_mode=persisted,
            effective_mode="live",
            source="configured",
            blocked_persisted_live_restore=False,
        )

    if persisted == "live":
        if allow_persisted_live_mode_start:
            return StartupModeDecision(
                configured_mode=configured,
                persisted_mode=persisted,
                effective_mode="live",
                source="persisted",
                blocked_persisted_live_restore=False,
            )
        return StartupModeDecision(
            configured_mode=configured,
            persisted_mode=persisted,
            effective_mode="paper",
            source="guarded_configured",
            blocked_persisted_live_restore=True,
        )

    if persisted == "paper":
        return StartupModeDecision(
            configured_mode=configured,
            persisted_mode=persisted,
            effective_mode="paper",
            source="persisted",
            blocked_persisted_live_restore=False,
        )

    return StartupModeDecision(
        configured_mode=configured,
        persisted_mode=persisted,
        effective_mode=configured,
        source="configured",
        blocked_persisted_live_restore=False,
    )
