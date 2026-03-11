$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Activate-CondaEnv([string]$EnvName) {
    $hookCandidates = @(
        "C:\ProgramData\anaconda3\shell\condabin\conda-hook.ps1",
        "$env:USERPROFILE\anaconda3\shell\condabin\conda-hook.ps1",
        "$env:USERPROFILE\miniconda3\shell\condabin\conda-hook.ps1"
    )

    foreach ($hook in $hookCandidates) {
        if (Test-Path $hook) {
            . $hook
            conda activate $EnvName
            return ($env:CONDA_DEFAULT_ENV -eq $EnvName)
        }
    }

    if (Get-Command conda -ErrorAction SilentlyContinue) {
        conda activate $EnvName
        return ($env:CONDA_DEFAULT_ENV -eq $EnvName)
    }

    return $false
}

function Test-EnvReady {
    try {
        python -c "import fastapi, uvicorn" | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

function Get-VenvCreator {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        try {
            py -3.11 -c "import sys; print(sys.version)" | Out-Null
            return @{ cmd = "py"; args = @("-3.11", "-m", "venv") }
        }
        catch {
            try {
                py -3 -c "import sys; print(sys.version)" | Out-Null
                return @{ cmd = "py"; args = @("-3", "-m", "venv") }
            }
            catch {
            }
        }
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @{ cmd = "python"; args = @("-m", "venv") }
    }
    throw "No Python launcher found for creating venv. Install Python or conda env first."
}

function Ensure-Venv([string]$VenvPath) {
    if (-not (Test-Path $VenvPath)) {
        $creator = Get-VenvCreator
        & $creator.cmd @($creator.args) $VenvPath
    }

    & "$VenvPath\Scripts\Activate.ps1"
}

$usingConda = $false

if (Activate-CondaEnv "crypto_trading") {
    $usingConda = $true
    $pyVersion = (python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    Write-Host "Using conda env: crypto_trading (Python $pyVersion)"
}
else {
    Write-Host "Conda env not available."
}

if (-not (Test-EnvReady)) {
    if ($usingConda) {
        Write-Warning "Conda env missing web dependencies; fallback to .venv."
    }
    else {
        Write-Host "Using .venv."
    }
    Ensure-Venv ".venv"
}

if (-not (Test-EnvReady)) {
    Write-Host "Installing requirements..."
    python -m pip install --upgrade pip
    pip install -r requirements.txt
}

if (-not (Test-EnvReady)) {
    throw "Python environment is not ready. fastapi/uvicorn import failed."
}

Write-Host "Starting web service on 0.0.0.0:8000"
Write-Host ""
Write-Host "Available data sources:" -ForegroundColor Cyan
Write-Host "  - Funding Rate (Binance, Bybit, OKX, Gate)" -ForegroundColor Gray
Write-Host "  - Fear & Greed Index" -ForegroundColor Gray
Write-Host "  - Order Book Level 2" -ForegroundColor Gray
Write-Host "  - Open Interest" -ForegroundColor Gray
Write-Host ""
Write-Host "Tip: Run 'python scripts/test_api_direct.py' to test API connectivity" -ForegroundColor Yellow
Write-Host ""

python main.py --mode web
