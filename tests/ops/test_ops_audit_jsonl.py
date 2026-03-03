from __future__ import annotations

from pathlib import Path

from core.audit import ops_audit


def test_ops_audit_jsonl_masks_sensitive_values(client, ops_app, ops_headers, tmp_path: Path, monkeypatch):
    log_path = tmp_path / "ops_audit.jsonl"
    monkeypatch.setattr(ops_audit, "_LOG_PATH", log_path)

    async def fake_audit_log(**kwargs):
        return None

    monkeypatch.setattr(ops_audit.audit_logger, "log", fake_audit_log)

    response = client.post(
        "/ops/trading/start_live",
        headers={**ops_headers, "X-OPS-APPROVAL": "SECRET1234"},
    )
    assert response.status_code == 403
    assert log_path.exists()

    content = log_path.read_text(encoding="utf-8")
    assert "/ops/trading/start_live" in content
    assert "test-token" not in content
    assert "SECRET1234" not in content
    assert "***" in content
