"""Fail-closed startup check for the Windows/Conda native analytics stack."""

from __future__ import annotations

import json
import os
import platform
import sys
from pathlib import Path


def _conda_managed(package_name: str) -> bool:
    conda_meta = Path(sys.prefix) / "conda-meta"
    if not conda_meta.is_dir():
        return False
    normalized = package_name.strip().lower().replace("_", "-")
    for path in conda_meta.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        name = str(payload.get("name") or "").strip().lower().replace("_", "-")
        if name == normalized:
            return True
    return False


def main() -> int:
    mode = str(os.getenv("TRADING_MODE") or "paper").strip().lower()
    is_conda = (Path(sys.prefix) / "conda-meta").is_dir()
    try:
        import numpy
        import pandas
        import pyarrow
    except Exception as exc:
        print(f"[ERROR] Native runtime import failed: {type(exc).__name__}: {exc}")
        return 43

    pyarrow_managed = _conda_managed("pyarrow") if is_conda else False
    print(
        "Native runtime: "
        f"python={platform.python_version()} "
        f"pyarrow={pyarrow.__version__} "
        f"numpy={numpy.__version__} "
        f"pandas={pandas.__version__} "
        f"prefix={sys.prefix} "
        f"conda={str(is_conda).lower()} "
        f"pyarrow_conda_managed={str(pyarrow_managed).lower()}"
    )
    if is_conda and not pyarrow_managed:
        message = (
            "PyArrow is not registered in conda-meta. Mixing a pip PyArrow wheel "
            "with a Conda native stack can load incompatible MSVC runtimes."
        )
        if mode == "live":
            print(f"[ERROR] {message} Live startup is blocked.")
            return 42
        print(f"[WARN] {message} Paper startup may continue for remediation/testing only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
