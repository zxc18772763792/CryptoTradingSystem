$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Test-PytestAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExe
    )

    if (-not (Test-Path $PythonExe)) {
        return $false
    }

    & $PythonExe -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('pytest') else 1)" *> $null
    return $LASTEXITCODE -eq 0
}

function Get-TestPython {
    $candidates = New-Object System.Collections.Generic.List[string]

    $venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $candidates.Add($venvPython)
    }

    foreach ($envRoot in @(
        (Join-Path $projectRoot "..\.conda\miniforge3\envs"),
        (Join-Path $projectRoot "..\.conda\anaconda3\envs")
    )) {
        if (-not (Test-Path $envRoot)) {
            continue
        }

        foreach ($envName in @("crypto_trading", "crypto_trading_311")) {
            $envPython = Join-Path $envRoot "$envName\python.exe"
            if (Test-Path $envPython) {
                $candidates.Add($envPython)
            }
        }
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand -and $pythonCommand.Source) {
        $candidates.Add($pythonCommand.Source)
    }

    $seen = @{}
    foreach ($candidate in $candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate) -or $seen.ContainsKey($candidate)) {
            continue
        }
        $seen[$candidate] = $true

        if (Test-PytestAvailable -PythonExe $candidate) {
            return $candidate
        }
    }

    throw "No Python interpreter with pytest was found. Expected .venv or a repo-local .conda env."
}

$pythonExe = Get-TestPython
$pyVersion = (& $pythonExe -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
if ($pyVersion -ne "3.11") {
    Write-Warning "Selected Python is $pyVersion. Recommended is 3.11."
}

Remove-Item Env:PYTEST_DISABLE_PLUGIN_AUTOLOAD -ErrorAction SilentlyContinue
& $pythonExe -m pytest -q tests

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
