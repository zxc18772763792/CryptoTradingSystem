"""Verify installed versions against the repository's requirements contract.

Use --include-dev for the complete developer/test environment. Does not import
application code, install packages, or read credentials.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
from pathlib import Path
from typing import Callable

from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]
DEV_SECTIONS = {'testing', 'type checking'}


def check_contract(path: Path, *, include_dev: bool = False,
                   version: Callable[[str], str] = importlib.metadata.version) -> list[dict]:
    results = []
    section = ''
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        if line.startswith('# '):
            section = line[2:].strip().lower()
        text = line.split('#', 1)[0].strip()
        if not text:
            continue
        if section in DEV_SECTIONS and not include_dev:
            continue
        req = Requirement(text)
        if req.marker and not req.marker.evaluate():
            continue
        try:
            installed = version(req.name)
        except importlib.metadata.PackageNotFoundError:
            installed = None
        results.append({'name': req.name, 'specifier': str(req.specifier), 'installed': installed,
                        'satisfies': installed is not None and installed in req.specifier})
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requirements', type=Path, default=ROOT/'requirements.txt')
    parser.add_argument('--include-dev', action='store_true')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    results = check_contract(args.requirements, include_dev=args.include_dev)
    failed = [row for row in results if not row['satisfies']]
    if args.json:
        print(json.dumps({'passed': not failed, 'include_dev': args.include_dev, 'packages': results}))
    else:
        for row in failed:
            print(f"[FAIL] {row['name']}{row['specifier']}: installed {row['installed'] or 'MISSING'}")
        print(f"Dependency contract: {len(results)-len(failed)}/{len(results)} satisfied")
    return int(bool(failed))


if __name__ == '__main__':
    raise SystemExit(main())
