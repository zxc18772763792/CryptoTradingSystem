"""Verify the Python environment can actually boot this project.

Split out of setup_local_env.ps1 deliberately: passing a multi-line Python script
through PowerShell's `-c` mangles embedded quotes, so the check lives in a real file.

Two tiers:

* REQUIRED - unguarded top-level imports on the startup path. Missing => exit 1.
  Checking only fastapi+uvicorn used to pass while `.\\web.bat` still died at import,
  because web/main.py:23 imports Jinja2Templates and jinja2 is an optional FastAPI
  extra that pip does not pull in on its own.
* OPTIONAL - absence degrades a feature instead of blocking startup, so it warns.
  These matter because the degradation is otherwise near-silent: without xgboost the
  ML signal returns FLAT and the aggregator keeps trading on a dead input.

Usage:
    python scripts/verify_env.py            # exit 1 if a required module is missing
    python scripts/verify_env.py --strict   # exit 1 if ANY module is missing
"""

from __future__ import annotations

import argparse
import importlib
import sys
from typing import List, Tuple

REQUIRED: Tuple[str, ...] = (
    "fastapi",
    "uvicorn",
    "jinja2",
    "pydantic",
    "pydantic_settings",
    "sqlalchemy",
    "loguru",
    "ccxt",
    "pandas",
    "numpy",
)

OPTIONAL: Tuple[Tuple[str, str], ...] = (
    ("xgboost", "ML signal degrades to FLAT (ML weight = 0), aggregator keeps trading"),
    ("sklearn", "research validation modules unavailable"),
    # talib/pandas_ta are deliberately absent: nothing imports them, and ta-lib's C
    # library made clean installs painful. See the note in requirements.txt.
    ("matplotlib", "analysis/report scripts fail at import"),
    ("web3", "DEX connectors unavailable"),
    ("polars", "fast dataframe paths unavailable"),
    ("asyncpg", "PostgreSQL backend unavailable"),
    ("pyarrow", "Parquet read/write unavailable"),
)


def _probe(name: str) -> str | None:
    """Return None if importable, else a short reason."""
    try:
        importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001 - any import failure is a failure
        return "{}: {}".format(type(exc).__name__, exc)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="treat missing optional dependencies as failures too",
    )
    args = parser.parse_args()

    print("interpreter: {}".format(sys.executable))
    print("python:      {}".format(sys.version.split()[0]))

    missing_required: List[str] = []
    for name in REQUIRED:
        reason = _probe(name)
        if reason:
            missing_required.append("{}  ({})".format(name, reason))

    missing_optional: List[str] = []
    for name, impact in OPTIONAL:
        if _probe(name):
            missing_optional.append("{} -> {}".format(name, impact))

    if missing_required:
        print("\nMISSING REQUIRED MODULES (the app cannot start):")
        for item in missing_required:
            print("  - {}".format(item))

    if missing_optional:
        print("\n[WARN] missing optional dependencies (features silently degraded):")
        for item in missing_optional:
            print("  - {}".format(item))

    if missing_required:
        print("\nFix: pip install -r requirements.txt   (natives via conda-forge, see AGENTS.md)")
        return 1

    if missing_optional and args.strict:
        print("\n--strict: failing because optional dependencies are missing.")
        return 1

    if not missing_optional:
        print("\nEnvironment OK: all required and optional dependencies present.")
    else:
        print("\nEnvironment OK for startup (optional gaps listed above).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
