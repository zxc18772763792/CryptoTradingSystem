param(
    [switch]$Upgrade,
    [switch]$SkipRequirements
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$localCondaRoot = Join-Path (Split-Path -Parent $projectRoot) ".conda\miniforge3"
$localConda = Join-Path $localCondaRoot "condabin\conda.bat"
$localEnv = Join-Path $localCondaRoot "envs\crypto_trading"
$localPython = Join-Path $localEnv "python.exe"
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (Test-Path $localPython) {
    $python = $localPython
    Write-Host "Using project Conda environment: $python" -ForegroundColor Green
} elseif (Test-Path $localConda) {
    Write-Host "Creating project Conda environment under $localEnv" -ForegroundColor Cyan
    & $localConda env create -p $localEnv -f (Join-Path $projectRoot "environment.yml")
    if ($LASTEXITCODE -ne 0) { throw "Local Conda environment creation failed." }
    $python = $localPython
} else {
    if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
        throw "No project environment found. Install Python 3.11, then rerun this script; all files will stay under $projectRoot\.venv."
    }
    Write-Host "Creating project virtual environment under $projectRoot\.venv" -ForegroundColor Cyan
    & py -3.11 -m venv (Join-Path $projectRoot ".venv")
    if ($LASTEXITCODE -ne 0) { throw "Local .venv creation failed." }
    $python = $venvPython
}

if (-not $SkipRequirements) {
    if ($python -eq $localPython) {
        & $localConda install -p $localEnv -c conda-forge --freeze-installed -y `
            "numpy=1.26.*" "pandas=2.3.*" "pyarrow=24.0.*" `
            "aiohttp>=3.14.4,<4" "multidict>=6.9.1,<7" "pillow>=13.0.0,<14" "tornado>=6.5.9,<7"
        if ($LASTEXITCODE -ne 0) { throw "Conda native security dependency installation failed." }
    }
    $pipArgs = @("-m", "pip", "install")
    if ($Upgrade) { $pipArgs += "--upgrade" }
    $pipArgs += @("-r", (Join-Path $projectRoot "requirements.txt"))
    & $python @pipArgs
    if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }
}

# Verification lives in scripts/verify_env.py rather than an inline here-string:
# PowerShell mangles embedded quotes when passing a multi-line script to python -c.
# It fails on missing REQUIRED modules (unguarded top-level imports on the startup
# path) and warns on missing OPTIONAL ones. Checking only fastapi+uvicorn used to
# pass while `.\web.bat` still died at import on a missing jinja2.
& $python (Join-Path $projectRoot "scripts\verify_env.py")
if ($LASTEXITCODE -ne 0) {
    throw "Environment verification failed. Rerun this script with -Upgrade, or install manually: pip install -r requirements.txt"
}

if (-not $SkipRequirements) {
    & $python (Join-Path $projectRoot "scripts\check_dependency_contract.py") --include-dev
    if ($LASTEXITCODE -ne 0) { throw "Installed versions do not satisfy requirements.txt." }
}

Write-Host "Done. Start with .\web.bat" -ForegroundColor Green
