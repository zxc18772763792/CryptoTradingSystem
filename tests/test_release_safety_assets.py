from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_release_scripts_surface_agent_and_startup_safety_guards():
    web_ps = _read("scripts/web.ps1")
    pre_release = _read("scripts/pre_release.ps1")

    assert "blocked_persisted_live_restore" in web_ps
    assert "allow_live=" in web_ps
    assert "AI agent config remains armed" in web_ps
    assert "AllowExecuteAgent" in pre_release
    assert "allow_live=true" in pre_release
    assert "auto_start=true" in pre_release
