"""Governance domain: RBAC, approvals, risk change gate, and audit."""

from core.governance.audit import GovernanceAuditEvent, new_trace_id, write_audit
from core.governance.rbac import (
    GovernanceIdentity,
    hash_api_key,
    has_permission,
    permission_set_for_role,
    resolve_api_key_identity,
)

__all__ = [
    "GovernanceAuditEvent",
    "GovernanceIdentity",
    "new_trace_id",
    "write_audit",
    "hash_api_key",
    "has_permission",
    "permission_set_for_role",
    "resolve_api_key_identity",
]

