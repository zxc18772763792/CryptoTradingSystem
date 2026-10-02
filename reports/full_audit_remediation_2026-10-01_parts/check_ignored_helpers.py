"""Mock-only checks; AST replaces historical paths/tokens before executing helpers."""
import ast
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace

root = Path(__file__).resolve().parents[2]
out = Path(__file__).resolve().parent
results = {}

# Compile only the pure probe function; do not import the helper or its proxy config.
tree = ast.parse((root / "data/_ws_proxy_monitor.py").read_text(encoding="utf-8"))
probe = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "probe")
import time
scope = {"time": time, "PROXIES": {}, "requests": SimpleNamespace()}
exec(compile(ast.Module(body=[probe], type_ignores=[]), "mock_proxy_probe", "exec"), scope)
for status in (200, 401, 429, 500):
    scope["requests"].get = lambda *a, status=status, **kw: SimpleNamespace(status_code=status)
    assert scope["probe"]("https://mock.invalid")[0] == (status == 200)
results["IS2"] = "200 healthy; 401/429/500 unhealthy; no HTTP requests"

tree = ast.parse((root / "data/_level1_relaxed_run.py").read_text(encoding="utf-8"))
with tempfile.TemporaryDirectory(prefix="crypto_ignored_helper_test_") as temporary:
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {n.id for n in node.targets if isinstance(n, ast.Name)}
            if names & {"ROOT", "WT", "TOKEN"}:
                node.value = ast.Constant("dummy" if "TOKEN" in names else temporary)
    ast.fix_missing_locations(tree)
    code = compile(tree, "mock_level1", "exec")
    actual_run = subprocess.run
    observed = []
    for source_rc, evaluator_rc, report_size, expected in [(5, 0, 2, 5), (0, 0, 0, 1), (0, 7, 2, 7), (0, 0, 2, 0)]:
        Path(temporary, "logs").mkdir(exist_ok=True)
        def fake_run(*args, **kwargs):
            if kwargs.get("stdout") is not None:
                kwargs["stdout"].write("{}" if report_size else "")
                return SimpleNamespace(returncode=source_rc)
            return SimpleNamespace(returncode=evaluator_rc, stdout="mock evaluator", stderr="")
        subprocess.run = fake_run
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                try:
                    exec(code, {"__name__": "__main__"})
                    exit_code = 0
                except SystemExit as exc:
                    exit_code = exc.code
            assert exit_code == expected
            observed.append(exit_code)
        finally:
            subprocess.run = actual_run
    results["IS1"] = {"exit_codes": observed, "subprocesses": "mocked", "writes": "temporary directory only"}
(out / "ignored_helpers_results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(results, ensure_ascii=False))
