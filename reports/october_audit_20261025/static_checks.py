"""Parse project sources and record the October audit scope without importing the app."""
from __future__ import annotations

import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess

import yaml

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
NODE = Path(r"C:\Users\zxc\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe")
PREFIXES = ("core/", "web/", "scripts/", "strategies/", "prediction_markets/", "config/", "tests/")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode("utf-8").splitlines()


def main():
    names = sorted(set(git("ls-files") + git("ls-files", "--others", "--exclude-standard")))
    changed = git("diff", "--name-only", "2fecd05", "HEAD")
    scope = [p for p in changed if p.startswith(PREFIXES)]
    counts, errors, hashes = Counter(), [], {}
    for name in names:
        path = ROOT / name
        if not name.startswith(PREFIXES) or not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix in {".py", ".js", ".cjs", ".json", ".yaml", ".yml", ".ps1"}:
            raw = path.read_bytes()
            hashes[name] = hashlib.sha256(raw).hexdigest()
            counts[suffix] += 1
            try:
                text = raw.decode("utf-8-sig")
                if suffix == ".py":
                    ast.parse(text, filename=name)
                elif suffix == ".json":
                    json.loads(text)
                elif suffix in {".yaml", ".yml"}:
                    yaml.safe_load(text)
                elif suffix in {".js", ".cjs"}:
                    result = subprocess.run([str(NODE), "--check", str(path)], capture_output=True, text=True)
                    if result.returncode:
                        raise ValueError(result.stderr[:1000])
            except Exception as exc:
                errors.append({"path": name, "error": str(exc)})
    result = {"head": git("rev-parse", "HEAD")[0], "october_base": "2fecd05",
              "october_changed_source_and_test_paths": len(scope), "october_paths": scope,
              "parsed_by_suffix": counts, "errors": errors, "sha256": hashes}
    (OUT / "static_checks.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in {"october_paths", "sha256"}}))
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
