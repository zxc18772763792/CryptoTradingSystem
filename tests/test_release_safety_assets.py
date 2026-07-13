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


def test_managed_start_fails_closed_when_web_health_never_becomes_ready():
    once = _read("_once.ps1")

    assert "RedirectStandardOutput $webStdoutPath" in once
    assert "RedirectStandardError $webStderrPath" in once
    assert "Stopping unhealthy web process because /readyz never became ready" in once
    assert "Stop-Process -Id $proc.Id -Force" in once
    assert "Start-WebSupervisor" in once
    assert "DisableSupervisorBootstrap" in once
    assert "exit 1" in once


def test_managed_supervisor_has_bounded_restart_and_operator_stop_protocol():
    supervisor = _read("scripts/supervise_web.ps1")
    web_ps = _read("scripts/web.ps1")

    assert "MaxRestarts = 5" in supervisor
    assert "RestartWindowMinutes = 15" in supervisor
    assert "restart_budget_exhausted" in supervisor
    assert "Web process missing; restarting" in supervisor
    assert "function Get-WebProcesses" in supervisor
    assert '$portToken = "--port $Port"' in supervisor
    assert "-EncodedCommand" in supervisor
    assert "Bool-PowerShellLiteral" in supervisor
    assert '"-DisableSupervisorBootstrap;"' in supervisor
    assert "web_supervisor_{0}.stop" in supervisor
    assert "operator_stop" in web_ps
    assert "Get-WebSupervisorProcesses" in web_ps
    assert "supervise_web\\.ps1" in web_ps


def test_managed_start_checks_native_runtime_before_launch():
    once = _read("_once.ps1")
    check = _read("scripts/check_native_runtime.py")

    assert "Invoke-NativeRuntimePrecheck -PythonExecutable $pythonExe" in once
    assert "pyarrow_conda_managed=" in check
    assert 'mode == "live"' in check
    assert "Live startup is blocked" in check
    assert "Paper startup may continue" in check
