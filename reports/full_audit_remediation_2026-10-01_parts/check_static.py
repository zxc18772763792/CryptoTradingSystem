import ast
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
out = Path(__file__).resolve().parent
tracked = subprocess.check_output(["git", "ls-files"], cwd=root, text=True).splitlines()
new = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "--", "core", "scripts", "tests", "web"], cwd=root, text=True).splitlines()
paths = sorted(set(tracked + new))
results = {"python_checked": 0, "python_errors": [], "javascript": {}, "compose": {}}
for name in paths:
    if name.endswith('.py'):
        try:
            ast.parse((root / name).read_text(encoding="utf-8-sig"), filename=name)
            results["python_checked"] += 1
        except Exception as exc:
            results["python_errors"].append({"file": name, "error": str(exc)})
node = r'C:\Program Files\nodejs\node.exe'
checked = subprocess.run([node, '--check', 'web/static/js/app.js'], cwd=root, capture_output=True, text=True)
results["javascript"] = {"exit_code": checked.returncode, "stderr": checked.stderr}
original = (root / 'reports/full_audit_2026-10-01_parts/web_security_xss_repro.cjs').read_text(encoding='utf-8')
script = out / 'check_xss.cjs'
script.write_text(original.replace('web_security_xss_results.json', 'xss_fixed.json'), encoding='utf-8')
run = subprocess.run([node, str(script)], cwd=root, capture_output=True, text=True, encoding="utf-8")
result = json.loads(run.stdout)
assert result['stored_name_reaches_innerHTML_unescaped'] is False
results['xss'] = {'unescaped_html': False, 'exit_code': run.returncode}
import yaml
compose = yaml.safe_load((root / 'docker-compose.yml').read_text(encoding='utf-8'))
results['compose'] = {'news_healthcheck': compose['services']['news_service']['healthcheck'], 'parsed': True}
(out / 'static_results.json').write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding='utf-8')
print(json.dumps(results, indent=2, ensure_ascii=False))
assert not results['python_errors'] and checked.returncode == 0
