from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_research_universe_refresh_scripts_exist_and_are_wired():
    runner = _read("scripts/run_research_universe_refresh.ps1")
    ensure = _read("scripts/ensure_research_universe_refresh_task.ps1")
    python_script = _read("scripts/maintain_research_universe_data.py")
    bat = _read("refresh_research_universe.bat")
    once = _read("_once.ps1")
    web_ps = _read("scripts/web.ps1")

    assert "maintain_research_universe_data.py" in runner
    assert "1m,5m,15m,1h" in runner
    assert "BTC/USDT,ETH/USDT" in runner
    assert "CryptoTradingSystem_ResearchUniverseRefresh" in ensure
    assert "Register-ScheduledTask" in ensure
    assert "schtasks /Create" in ensure
    assert "StartNowIfCreated" in ensure
    assert "1m,5m,15m,1h" in ensure
    assert "BTC/USDT,ETH/USDT" in ensure
    assert "DEFAULT_RESEARCH_SYMBOLS" in python_script
    assert "1m,5m,15m,1h" in python_script
    assert "BTC/USDT,ETH/USDT" in python_script
    assert "run_research_universe_refresh.ps1" in bat
    assert "Ensure-ResearchUniverseRefreshTask" in once
    assert "ensure_research_universe_refresh_task.ps1" in once
    assert "Research universe incremental refresh task is auto-ensured on start." in web_ps
