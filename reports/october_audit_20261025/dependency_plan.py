"""Read-only feasibility check of the declared environment contract; installs nothing."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time

OUT = Path(__file__).resolve().parent
PREFIX = Path(r"F:/9_Crypto/.conda/miniforge3/envs/crypto_trading")
SPECS = ["python=3.11", "numpy=1.26.*", "pandas=2.3.*", "pyarrow=24.0.*",
         "certifi>=2026.10.19", "urllib3>=2.8.0,<3", "aiohttp>=3.14.4,<4", "multidict>=6.9.1,<7",
         "pyjwt>=2.15.0,<3", "tornado>=6.5.9,<7", "pillow>=13.0.0,<14", "pytest>=9.0.3,<10",
         "pytest-asyncio>=1,<2", "pytest-cov>=6,<7", "mypy>=1.14,<2", "types-redis>=4.6,<5",
         "schedule>=1.2,<2", "web3>=7.6,<8"]
command = [r"F:/9_Crypto/.conda/miniforge3/Scripts/conda.exe", "install", "--prefix", str(PREFIX),
           "--override-channels", "-c", "conda-forge", "--freeze-installed", "--dry-run", "--json", *SPECS]
env = dict(os.environ, CONDA_AUTO_UPDATE_CONDA="false")
started = time.monotonic()
log = OUT / "dependency_plan.log"
try:
    with log.open("w", encoding="utf-8") as stream:
        result = subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=180)
    code = result.returncode
except subprocess.TimeoutExpired:
    code = 124
receipt = {"exit_code": code, "seconds": round(time.monotonic() - started, 2),
           "dry_run": True, "specs": SPECS, "prefix": str(PREFIX)}
(OUT / "dependency_plan.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
print(json.dumps(receipt), flush=True)
print(log.read_text(encoding="utf-8", errors="replace")[-2500:])
raise SystemExit(code)
