import os
from pathlib import Path
import subprocess

import pytest


@pytest.mark.skipif(os.name != "nt", reason="Windows process ownership predicates")
def test_windows_managed_process_scope_uses_only_mock_processes():
    script = Path(__file__).parent / "helpers" / "check_managed_process_scope.ps1"
    result = subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "isolation PASS" in result.stdout


def test_news_worker_health_requires_recent_worker_heartbeat(tmp_path):
    from scripts.check_news_worker_health import heartbeat_is_fresh
    path = tmp_path / "heartbeat"
    assert not heartbeat_is_fresh(path, 10, now=100)
    path.touch()
    os.utime(path, (100, 100))
    assert heartbeat_is_fresh(path, 10, now=105)
    assert not heartbeat_is_fresh(path, 10, now=111)
    assert not heartbeat_is_fresh(path, 10, now=99)
