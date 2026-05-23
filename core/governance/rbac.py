from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass
from typing import Dict, Optional, Set

from loguru import logger
from sqlalchemy import select

from config.database import ApiUser, async_session_maker
from config.settings import settings


def _get_rbac_pepper() -> str:
    """Return the RBAC HMAC pepper.

    Order of resolution:
        1. `settings.RBAC_SECRET` if the Settings model exposes it.
        2. `RBAC_SECRET` environment variable.
        3. Empty string with a one-time warning (degrades to keyed-but-unsalted HMAC).
    """
    pepper = getattr(settings, "RBAC_SECRET", None)
    if pepper:
        return str(pepper)
    pepper = os.environ.get("RBAC_SECRET", "")
    if not pepper and not _get_rbac_pepper._warned:  # type: ignore[attr-defined]
        logger.warning(
            "RBAC_SECRET not set; using empty pepper, security weakened. "
            "Set RBAC_SECRET in the environment or config/settings.py."
        )
        _get_rbac_pepper._warned = True  # type: ignore[attr-defined]
    return pepper


_get_rbac_pepper._warned = False  # type: ignore[attr-defined]


ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "RESEARCH_LEAD": {
        "propose_strategy",
        "approve_strategy",
        "promote_paper",
        "request_live",
        "retire_strategy",
        "manage_ai_research",
        "manage_ml",
        "manage_strategies",
    },
    "RISK_OWNER": {
        "approve_risk_change",
        "set_kill_switch",
        "set_reduce_only",
        "approve_live",
        "change_leverage_caps",
        "retire_strategy",
        "reset_paper_runtime",
        "manage_accounts",
        "manage_orders",
        "manage_strategies",
        "close_positions",
    },
    "OPERATOR": {
        "pause_engine",
        "resume_engine",
        "set_reduce_only",
        "rotate_runtime",
        "ack_alerts",
        "request_live",
        "reset_paper_runtime",
        "manage_accounts",
        "manage_ai_research",
        "manage_data_sources",
        "manage_ml",
        "manage_news",
        "manage_orders",
        "manage_strategies",
        "close_positions",
        "manage_notifications",
        "manage_ai_agent",
    },
    "AUDITOR": {
        "read_audit",
        "export_audit",
        "annotate_incident",
    },
    "ENGINEER": {
        "migrations",
        "manage_data_sources",
        "deploy_config",
        "manage_ml",
        "manage_news",
        "manage_notifications",
    },
    "SYSTEM": {"*"},
}


@dataclass
class GovernanceIdentity:
    actor: str
    role: str
    api_key_present: bool = False
    token_present: bool = False
    client_ip: str = ""

    @property
    def permissions(self) -> Set[str]:
        return permission_set_for_role(self.role)


def hash_api_key(api_key: str) -> str:
    """Hash an API key with HMAC-SHA256 using a server-side pepper.

    Using HMAC (vs plain sha256) means an attacker who exfiltrates the
    `ApiUser.api_key_hash` column cannot brute-force the raw keys without
    also obtaining the `RBAC_SECRET` pepper. Falls back to keyed-with-empty-
    pepper HMAC if the secret is unset (with a logged warning).
    """
    key = str(api_key or "").encode("utf-8")
    pepper = _get_rbac_pepper().encode("utf-8")
    return hmac.new(pepper, key, hashlib.sha256).hexdigest()


def permission_set_for_role(role: str) -> Set[str]:
    return set(ROLE_PERMISSIONS.get(str(role or "").upper(), set()))


def has_permission(role: str, permission: str) -> bool:
    perms = permission_set_for_role(role)
    return "*" in perms or str(permission) in perms


async def resolve_api_key_identity(api_key: str) -> Optional[GovernanceIdentity]:
    key_hash = hash_api_key(api_key)
    async with async_session_maker() as session:
        result = await session.execute(
            select(ApiUser).where(ApiUser.api_key_hash == key_hash, ApiUser.is_active.is_(True))
        )
        row = result.scalars().first()
        if row is None:
            return None
        return GovernanceIdentity(
            actor=str(row.name or "api_user"),
            role=str(row.role or "OPERATOR").upper(),
            api_key_present=True,
            token_present=False,
            client_ip="",
        )

