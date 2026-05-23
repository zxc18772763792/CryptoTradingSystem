import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from core.ai.proposal_schemas import ResearchProposal
from core.research.experiment_registry import ProposalRegistry


def _proposal(proposal_id: str) -> ResearchProposal:
    now = datetime.now(timezone.utc)
    return ResearchProposal(
        proposal_id=proposal_id,
        created_at=now,
        updated_at=now,
        status="draft",
        source="ai",
        thesis=f"proposal {proposal_id}",
    )


def test_registry_save_list_are_thread_safe_on_one_instance(tmp_path):
    registry = ProposalRegistry(tmp_path / "proposals.json")

    def _save(idx: int) -> None:
        registry.save(_proposal(f"proposal-{idx:03d}"))
        assert registry.get(f"proposal-{idx:03d}") is not None
        registry.list(limit=None)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_save, idx) for idx in range(40)]
        for future in futures:
            future.result(timeout=5)

    payload = json.loads((tmp_path / "proposals.json").read_text(encoding="utf-8"))
    assert len(payload["proposals"]) == 40
    assert len(registry.list(limit=None)) == 40


def test_registry_retries_windows_permission_error_on_replace(tmp_path, monkeypatch):
    import core.research.experiment_registry as registry_module

    calls = {"count": 0}
    original_replace = registry_module.os.replace

    def _flaky_replace(src: str, dst: str) -> None:
        calls["count"] += 1
        if calls["count"] < 3:
            raise PermissionError("file temporarily locked")
        original_replace(src, dst)

    monkeypatch.setattr(registry_module.os, "replace", _flaky_replace)
    monkeypatch.setattr(registry_module.time, "sleep", lambda _delay: None)

    registry = ProposalRegistry(tmp_path / "proposals.json")
    registry.save(_proposal("proposal-retry"))

    assert calls["count"] == 3
    assert registry.get("proposal-retry") is not None


def test_registry_save_many_flushes_once(tmp_path, monkeypatch):
    import core.research.experiment_registry as registry_module

    calls = {"count": 0}
    original_replace = registry_module.os.replace

    def _counting_replace(src: str, dst: str) -> None:
        calls["count"] += 1
        original_replace(src, dst)

    monkeypatch.setattr(registry_module.os, "replace", _counting_replace)

    registry = ProposalRegistry(tmp_path / "proposals.json")
    registry.save_many([_proposal("proposal-a"), _proposal("proposal-b"), _proposal("proposal-c")])

    assert calls["count"] == 1
    assert len(registry.list(limit=None)) == 3
