$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (Test-Path ".venv\Scripts\Activate.ps1") {
    & ".venv\Scripts\Activate.ps1"
}

$pyVersion = (python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
if ($pyVersion -ne "3.11") {
    Write-Warning "Current Python is $pyVersion. Recommended is 3.11."
}

$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = "1"
$hasAsyncioPlugin = $false
python -c "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('pytest_asyncio') else 1)"
if ($LASTEXITCODE -eq 0) {
    $hasAsyncioPlugin = $true
}

if ($hasAsyncioPlugin) {
    python -m pytest -q -p pytest_asyncio tests
}
else {
    Write-Warning "pytest-asyncio not installed in current environment, running tests without async plugin."
    python -m pytest -q tests
}
