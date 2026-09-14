import base64
import re
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("script,name", [
    ("_once.ps1", "Import-DotEnvFile"),
    ("scripts/web.ps1", "Get-EnvFileValues"),
    ("scripts/start_live_shadow_news.ps1", "Import-EnvFileValues"),
])
def test_windows_launcher_preserves_unicode_model_id(tmp_path, script, name):
    shell = shutil.which("powershell.exe")
    if not shell:
        pytest.skip("Windows PowerShell startup regression")
    model = "deepseek-v4.1-flash-特价"
    (tmp_path / ".env").write_text("AI_RESEARCH_MODEL=old-model\n", encoding="utf-8")
    env_file = tmp_path / ".env.local"
    env_file.write_text(f"AI_RESEARCH_MODEL='{model}'\n", encoding="utf-8")
    source = (Path(__file__).resolve().parents[1] / script).read_text(encoding="utf-8-sig")
    function = re.search(rf"(?ms)^function {name}\b.*?(?=^function |\Z)", source).group()
    root = str(tmp_path).replace("'", "''")
    if name == "Import-DotEnvFile":
        call = f"{name} -Path (Join-Path $projectRoot '.env.local'); $actual = $env:AI_RESEARCH_MODEL"
    else:
        call = f"$actual = ({name})['AI_RESEARCH_MODEL']"
    command = f"$ErrorActionPreference='Stop'\n$projectRoot='{root}'\n{function}\n{call}\n[Console]::Out.Write([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($actual)))"
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    result = subprocess.run([shell, "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded], capture_output=True, check=True, timeout=20)
    assert base64.b64decode(result.stdout.strip()).decode("utf-8") == model
