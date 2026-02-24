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
            & $hook
            conda activate $EnvName
            return $true
        }
    }

    if (Get-Command conda -ErrorAction SilentlyContinue) {
        conda activate $EnvName
        return $true
    }

    return $false
}

function Ensure-Venv([string]$VenvPath) {
    if (-not (Test-Path $VenvPath)) {
        if (Get-Command py -ErrorAction SilentlyContinue) {
            py -3.11 -m venv $VenvPath
        }
        else {
            python -m venv $VenvPath
        }
    }

    & "$VenvPath\Scripts\Activate.ps1"
}

$pythonReady = $false

if (Activate-CondaEnv "crypto_trading") {
    $pyVersion = (python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    if ($pyVersion -eq "3.11") {
        $pythonReady = $true
        Write-Host "Using conda env: crypto_trading (Python $pyVersion)"
    }
    else {
        Write-Warning "Conda env python is $pyVersion, expected 3.11. Fallback to .venv."
        Ensure-Venv ".venv"
        $pythonReady = $true
    }
}
else {
    Write-Host "Conda env not available, using .venv"
    Ensure-Venv ".venv"
    $pythonReady = $true
}

if (-not $pythonReady) {
    throw "Python environment is not ready."
}

python -m pip install --upgrade pip
pip install -r requirements.txt

Write-Host "Starting web service on 0.0.0.0:8000"
python main.py --mode web
