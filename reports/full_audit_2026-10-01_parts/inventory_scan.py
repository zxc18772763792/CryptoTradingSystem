"""Read-only audit inventory; never imports the application or reads local secrets."""
from __future__ import annotations

import ast
import collections
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tokenize

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
raw = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
paths = [p for p in raw.decode("utf-8").split("\0") if p]
patterns = {
    "private_key_header": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "github_token": re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\b"),
    "aws_access_key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "openai_token": re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{40,}\b"),
}
text_extensions = {".py", ".js", ".cjs", ".ps1", ".bat", ".yml", ".yaml", ".toml", ".ini", ".html", ".css", ".md"}
entries, syntax_errors, candidates, secret_hits = [], [], [], []
groups = collections.Counter()
for rel in paths:
    p = ROOT / rel
    entry = {"path": rel, "suffix": p.suffix.lower(), "bytes": p.stat().st_size}
    groups[rel.split("/")[0]] += 1
    # Real secret files are neither tracked nor included. Also skip example env.
    if p.suffix.lower() in text_extensions and p.name.lower() != "keys.txt" and not p.name.startswith(".env"):
        data = p.read_bytes()
        entry["sha256"] = hashlib.sha256(data).hexdigest()
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            entry["text_decode"] = "non_utf8"
            entries.append(entry)
            continue
        entry["lines"] = len(text.splitlines())
        for label, regex in patterns.items():
            for match in regex.finditer(text):
                secret_hits.append({"path": rel, "line": text.count("\n", 0, match.start())+1, "pattern": label})
        if p.suffix == ".py":
            try:
                tree = ast.parse(text, filename=rel)
                entry["syntax"] = "ok"
                entry["functions"] = sum(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) for n in ast.walk(tree))
                for n in ast.walk(tree):
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        names = collections.Counter()
                        for child in n.body:
                            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                                names[child.name] += 1
                        for name, count in names.items():
                            if count > 1:
                                candidates.append({"path":rel,"line":n.lineno,"pattern":"duplicate_nested_name","name":name,"count":count})
                    if isinstance(n, ast.Call):
                        name = ast.unparse(n.func)
                        if name in {"eval", "exec", "pickle.load", "pickle.loads", "joblib.load", "os.system"}:
                            candidates.append({"path":rel,"line":n.lineno,"pattern":"execution_or_deserialization","call":name})
                        if any(k.arg == "shell" and isinstance(k.value,ast.Constant) and k.value.value is True for k in n.keywords):
                            candidates.append({"path":rel,"line":n.lineno,"pattern":"shell_true","call":name})
            except SyntaxError as exc:
                entry["syntax"] = "error"
                syntax_errors.append({"path":rel,"line":exc.lineno,"error":exc.msg})
    entries.append(entry)
summary = {"tracked_files":len(paths),"by_top_directory":dict(sorted(groups.items())),"python_files":sum(e["suffix"]==".py" for e in entries),"python_lines":sum(e.get("lines",0) for e in entries if e["suffix"]==".py"),"syntax_errors":syntax_errors,"dangerous_primitive_candidates":candidates,"credential_pattern_hits":secret_hits,"largest_python_modules":sorted([{"path":e["path"],"lines":e.get("lines",0)} for e in entries if e["suffix"]==".py"], key=lambda x:x["lines"],reverse=True)[:15]}
(OUT/"inventory.json").write_text(json.dumps({"summary":summary,"files":entries},ensure_ascii=False,indent=2),encoding="utf-8")
print(json.dumps(summary,ensure_ascii=False,indent=2))
