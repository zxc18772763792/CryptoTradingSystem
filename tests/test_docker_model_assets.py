from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_docker_context_includes_canonical_ml_model_and_manifest():
    dockerignore = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8-sig")
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8-sig")

    assert "models/*" in dockerignore
    assert "!models/ml_signal_xgb.json" in dockerignore
    assert "!models/ml_signal_xgb.manifest.json" in dockerignore
    assert (REPO_ROOT / "models" / "ml_signal_xgb.json").is_file()
    assert (REPO_ROOT / "models" / "ml_signal_xgb.manifest.json").is_file()
    assert "/app/models" not in compose
