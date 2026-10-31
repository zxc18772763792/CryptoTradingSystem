"""Validate a credential-free source snapshot without touching live services."""
from __future__ import annotations

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
MANIFEST = OUT / "snapshot.json"


def snapshot() -> dict:
    # A running validation must keep an immutable copy when the next run starts.
    base = Path(tempfile.mkdtemp(prefix="crypto_october_audit_"))
    info = {"base": str(base), "source": str(base / "source")}
    MANIFEST.write_text(json.dumps(info, indent=2), encoding="utf-8")
    source, base = Path(info["source"]), Path(info["base"])
    for folder in (source, base / "guard", base / "temp"):
        folder.mkdir(parents=True, exist_ok=True)
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
    ).decode("utf-8").split("\0")
    for name in names:
        original = ROOT / name
        if not name or not original.is_file() or name.startswith("reports/october_audit_20261025/"):
            continue
        if original.name == "keys.txt" or (original.name.startswith(".env") and original.name != ".env.example") or original.suffix in {".key", ".pem"}:
            continue
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, target)
    shutil.copy2(ROOT / "reports/full_audit_2026-10-22_parts/test_guard.py", base / "guard/sitecustomize.py")
    return info


def main() -> int:
    info = snapshot()
    base, source = Path(info["base"]), Path(info["source"])
    keep = {"SYSTEMROOT", "WINDIR", "COMSPEC", "PATH", "PATHEXT", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "PROCESSOR_ARCHITECTURE", "NUMBER_OF_PROCESSORS"}
    env = {k: v for k, v in os.environ.items() if k.upper() in keep}
    env.update(
        CTS_AUDIT_BASE=str(base), TEMP=str(base / "temp"), TMP=str(base / "temp"),
        PYTHONPATH=str(base / "guard"), PYTHONUTF8="1", PYTHONDONTWRITEBYTECODE="1",
        TRADING_MODE="paper", OPS_TOKEN="audit-synthetic-ops-token",
        AI_AUTONOMOUS_AGENT_AUTO_START="false", MARKET_WS_ENABLED="false", MARKET_WS_MODE="off",
        NEWS_WORKER_AUTO_START="false", NEWS_LLM_WORKER_AUTO_START="false", PM_WORKER_AUTO_START="false",
        PREMIUM_EXTERNAL_WORKERS_ENABLED="false", PUBLIC_MACRO_WORKERS_ENABLED="false",
        COINGLASS_WORKER_ENABLED="false", BINANCE_ALPHA_COLLECTOR_ENABLED="false",
        MPLCONFIGDIR=str(base / "mpl"), HF_HOME=str(base / "hf"), HF_HUB_OFFLINE="1",
        TRANSFORMERS_OFFLINE="1", XDG_CACHE_HOME=str(base / "cache"), NUMBA_CACHE_DIR=str(base / "numba"),
        JOBLIB_TEMP_FOLDER=str(base / "temp"), TORCH_HOME=str(base / "torch"),
        GATE_COUNTERFACTUAL_AUDIT_PATH=str(base / "counterfactual.jsonl"),
    )
    stamp = time.strftime("%H%M%S")
    log, xml = OUT / f"pytest_{stamp}.log", base / f"pytest_{stamp}.xml"
    command = [sys.executable, "-m", "pytest", "-q", "-m", "not live", *sys.argv[1:],
               f"--basetemp={base / 't'}", f"--junitxml={xml}"]
    started = time.monotonic()
    print(f"Isolated offline validation: {source}", flush=True)
    print(f"Progress log: {log}", flush=True)
    with log.open("w", encoding="utf-8") as stream:
        result = subprocess.run(command, cwd=source, env=env, stdout=stream, stderr=subprocess.STDOUT)
    if xml.exists():
        shutil.copy2(xml, OUT / xml.name)
    summary = {"exit_code": result.returncode, "seconds": round(time.monotonic() - started, 2),
               "arguments": sys.argv[1:], "source": str(source), "log": str(log),
               "credentials_removed": True, "network_and_live_services_blocked": True}
    (OUT / f"run_{stamp}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary), flush=True)
    print(log.read_text(encoding="utf-8", errors="replace")[-14000:])
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
