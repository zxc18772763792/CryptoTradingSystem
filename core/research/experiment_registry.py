"""Persistent registry helpers for AI proposals and research experiments."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from core.ai.proposal_schemas import ResearchProposal


class ProposalRegistry:
    """Simple JSON-backed proposal registry.

    The first version keeps storage deliberately small and explicit:
    one JSON file holding all proposals, keyed by ``proposal_id``.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._cache: Optional[Dict[str, ResearchProposal]] = None

    def _load(self) -> Dict[str, ResearchProposal]:
        if self._cache is not None:
            return self._cache
        if not self.path.exists():
            self._cache = {}
            return self._cache
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        rows = payload.get("proposals") if isinstance(payload, dict) else []
        items: Dict[str, ResearchProposal] = {}
        for row in rows or []:
            proposal = ResearchProposal.model_validate(row)
            items[proposal.proposal_id] = proposal
        self._cache = items
        return items

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.model_dump(mode="json") for item in self.list(limit=None)]
        self.path.write_text(
            json.dumps({"proposals": rows}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def save(self, proposal: ResearchProposal) -> ResearchProposal:
        items = self._load()
        items[proposal.proposal_id] = proposal
        self._flush()
        return proposal

    def get(self, proposal_id: str) -> Optional[ResearchProposal]:
        return self._load().get(str(proposal_id))

    def list(self, limit: int | None = 50) -> List[ResearchProposal]:
        rows = sorted(
            self._load().values(),
            key=lambda item: (item.updated_at, item.created_at),
            reverse=True,
        )
        if limit is None:
            return list(rows)
        return list(rows[: max(0, int(limit))])
