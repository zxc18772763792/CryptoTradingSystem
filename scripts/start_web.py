from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def _powershell_executable() -> str:
    return shutil.which("pwsh") or shutil.which("powershell") or "powershell"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Start the managed Crypto Trading System web service."
    )
    parser.add_argument("--mode", choices=("paper", "live"), default="live")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--health-wait-sec", type=int, default=150)
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument("--start-autonomous-agent", action="store_true")
    parser.add_argument("--start-pm-worker", action="store_true")
    parser.add_argument("--enable-analytics-history", action="store_true")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    web_script = project_root / "scripts" / "web.ps1"
    cmd = [
        _powershell_executable(),
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(web_script),
        "start",
        "-BindHost",
        str(args.host),
        "-Port",
        str(args.port),
        "-HealthWaitSec",
        str(args.health_wait_sec),
    ]
    if args.open_browser:
        cmd.append("-OpenBrowser")
    if args.mode == "live":
        cmd.append("-AllowPersistedLiveMode")
    else:
        cmd.append("-PaperMode")
    if args.start_autonomous_agent:
        cmd.append("-StartAutonomousAgent")
    if args.start_pm_worker:
        cmd.append("-StartPmWorker")
    if args.enable_analytics_history:
        cmd.append("-EnableAnalyticsHistory")

    print("Starting managed web service")
    print(f"  mode: {args.mode}")
    print(f"  url : http://127.0.0.1:{args.port}")
    return subprocess.run(cmd, cwd=str(project_root)).returncode


if __name__ == "__main__":
    sys.exit(main())
