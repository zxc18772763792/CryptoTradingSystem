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

Remove-Item Env:PYTEST_DISABLE_PLUGIN_AUTOLOAD -ErrorAction SilentlyContinue
python -m pytest -q tests

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
