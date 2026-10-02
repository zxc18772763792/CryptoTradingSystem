"""Run working-tree tests in a fresh, credential-free, network-restricted copy."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
base = Path(tempfile.mkdtemp(prefix="crypto_remediation_20261001_"))
source = base / "source"
source.mkdir()
paths = subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True).splitlines()
paths += subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "--", "tests", "core", "web", "scripts"], cwd=ROOT, text=True).splitlines()
for name in dict.fromkeys(paths):
    original = ROOT / name
    if original.is_file():
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, target)
guard = base / "guard"
guard.mkdir()
shutil.copy2(ROOT / "reports/full_audit_2026-10-01_parts/test_isolation_guard.py", guard / "sitecustomize.py")
keep = {"SYSTEMROOT", "WINDIR", "COMSPEC", "PATH", "PATHEXT", "TEMP", "TMP", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "PROCESSOR_ARCHITECTURE", "NUMBER_OF_PROCESSORS"}
env = {k: v for k, v in os.environ.items() if k.upper() in keep}
env.update(PYTHONPATH=str(guard), PYTHONUTF8="1", PYTHONDONTWRITEBYTECODE="1", TRADING_MODE="paper", OPS_TOKEN="audit-test-dummy-token", AI_AUTONOMOUS_AGENT_AUTO_START="false", MARKET_WS_ENABLED="false", MARKET_WS_MODE="off", NEWS_WORKER_AUTO_START="false", NEWS_LLM_WORKER_AUTO_START="false", PM_WORKER_AUTO_START="false", PREMIUM_EXTERNAL_WORKERS_ENABLED="false", PUBLIC_MACRO_WORKERS_ENABLED="false", COINGLASS_WORKER_ENABLED="false", BINANCE_ALPHA_COLLECTOR_ENABLED="false", MPLCONFIGDIR=str(base / "mpl"), GATE_COUNTERFACTUAL_AUDIT_PATH=str(base / "counterfactual.jsonl"))
stamp = time.strftime("%H%M%S")
log = OUT / f"pytest_{stamp}.log"
xml = base / "pytest_results.xml"
cmd = [sys.executable, "-m", "pytest", "-q", "-m", "not live", *sys.argv[1:], "--basetemp=" + str(base / "pytest_tmp"), "--junitxml=" + str(xml)]
started = time.monotonic()
print("Isolated test copy:", base, flush=True)
with log.open("w", encoding="utf-8") as stream:
    result = subprocess.run(cmd, cwd=source, env=env, stdout=stream, stderr=subprocess.STDOUT)
if xml.exists():
    shutil.copy2(xml, OUT / f"pytest_{stamp}.xml")
summary = {"command": cmd[2:], "exit_code": result.returncode, "seconds": round(time.monotonic() - started, 2), "snapshot": str(source), "log": str(log), "xml": str(xml), "credentials_removed": True, "network_restricted": True}
(OUT / f"run_{stamp}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
print(log.read_text(encoding="utf-8")[-24000:])
raise SystemExit(result.returncode)
