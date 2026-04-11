from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, Optional, Set

from sqlalchemy import select

from config.database import ApiUser, async_session_maker


ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "RESEARCH_LEAD": {
        "propose_strategy",
        "approve_strategy",
        "promote_paper",
        "request_live",
        "retire_strategy",
    },
    "RISK_OWNER": {
        "approve_risk_change",
        "set_kill_switch",
        "set_reduce_only",
        "approve_live",
        "change_leverage_caps",
        "retire_strategy",
        "reset_paper_runtime",
        "manage_orders",
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
        "manage_orders",
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
    return hashlib.sha256(str(api_key or "").encode("utf-8")).hexdigest()


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

