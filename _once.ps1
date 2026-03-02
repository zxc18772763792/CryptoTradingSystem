param(
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8000,
    [bool]$OpenBrowser = $true,
    [int]$HealthWaitSec = 20,
    [bool]$StartNewsWorker = $false
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Open-WebConsole {
    param([int]$WebPort)
    try {
        Start-Process "http://127.0.0.1:$WebPort/" | Out-Null
    } catch {
        Write-Host "Browser open skipped: $($_.Exception.Message)"
    }
}

function Get-ListeningPid {
    param([int]$PortNumber)
    $line = netstat -ano | Select-String -Pattern "LISTENING\s+(\d+)$" | Select-String -Pattern "[:\.]$PortNumber\s"
    if (-not $line) { return $null }
    $text = ($line | Select-Object -First 1).Line.Trim()
    $parts = $text -split "\s+"
    if ($parts.Count -lt 5) { return $null }
    return [int]$parts[-1]
}

function Get-WorkerPid {
    $workers = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -like "*core.news.service.worker*"
    }
    if (-not $workers) { return $null }
    return [int]($workers | Select-Object -First 1).ProcessId
}

function Enable-CondaEnv {
    param([string]$Name)
    $hookCandidates = @(
        "C:\ProgramData\anaconda3\shell\condabin\conda-hook.ps1",
        "$env:USERPROFILE\anaconda3\shell\condabin\conda-hook.ps1",
        "$env:USERPROFILE\miniconda3\shell\condabin\conda-hook.ps1"
    )

    foreach ($hook in $hookCandidates) {
        if (-not (Test-Path $hook)) { continue }
        . $hook
        conda activate $Name
        if ($env:CONDA_DEFAULT_ENV -eq $Name) {
            return $true
        }
    }

    if (Get-Command conda -ErrorAction SilentlyContinue) {
        $condaBase = (& conda info --base).Trim()
        if ($condaBase) {
            $condaHook = Join-Path $condaBase "shell\condabin\conda-hook.ps1"
            if (Test-Path $condaHook) {
                . $condaHook
                conda activate $Name
                if ($env:CONDA_DEFAULT_ENV -eq $Name) {
                    return $true
                }
            }
        }
    }

    return $false
}

function Resolve-PythonExecutable {
    if ($env:CONDA_PREFIX) {
        $condaPython = Join-Path $env:CONDA_PREFIX "python.exe"
        if (Test-Path $condaPython) {
            return $condaPython
        }
    }

    $venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        return $venvPython
    }

    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd) {
        return $pyCmd.Source
    }

    throw "Cannot find Python executable. Please install Conda or create .venv."
}

$pidOnPort = Get-ListeningPid -PortNumber $Port
if ($pidOnPort) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$pidOnPort" -ErrorAction SilentlyContinue
    if ($proc -and ($proc.CommandLine -like "*uvicorn*web.main:app*" -or $proc.CommandLine -like "*main.py --mode web*")) {
        Write-Host "Service already listening on 0.0.0.0:$Port (PID=$pidOnPort)."
        if ($OpenBrowser) {
            Open-WebConsole -WebPort $Port
        }
        exit 0
    }
    throw "Port $Port is already in use by PID $pidOnPort."
}

if (Enable-CondaEnv -Name $EnvName) {
    Write-Host "Using conda env: $EnvName"
} else {
    Write-Host "Conda env '$EnvName' not found from common paths/PATH. Falling back to .venv or system python."
}

$pythonExe = Resolve-PythonExecutable
Write-Host "Python executable: $pythonExe"

$proc = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList @("-m", "uvicorn", "web.main:app", "--host", $BindHost, "--port", "$Port") `
    -WorkingDirectory $PSScriptRoot `
    -PassThru

$shouldStartWorker = $StartNewsWorker
if (-not $shouldStartWorker) {
    $rawToggle = [string]($env:START_NEWS_WORKER)
    if ($rawToggle) {
        $shouldStartWorker = $rawToggle.Trim().ToLower() -in @("1", "true", "yes", "on")
    }
}

if ($shouldStartWorker) {
    $workerPid = Get-WorkerPid
    if ($workerPid) {
        Write-Host "News worker already running (PID=$workerPid)."
    } else {
        $workerProc = Start-Process `
            -FilePath $pythonExe `
            -ArgumentList @("-m", "core.news.service.worker") `
            -WorkingDirectory $PSScriptRoot `
            -PassThru
        Write-Host "Started news worker PID=$($workerProc.Id)"
    }
}

Start-Sleep -Seconds 2

$status = $null
$deadline = (Get-Date).AddSeconds([Math]::Max(3, $HealthWaitSec))
while ((Get-Date) -lt $deadline) {
    try {
        $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -TimeoutSec 4
        break
    } catch {
        Start-Sleep -Milliseconds 800
    }
}

if ($status) {
    Write-Host "Started PID=$($proc.Id), status=$($status.status), mode=$($status.trading_mode), url=http://127.0.0.1:$Port"
    if ($OpenBrowser) {
        Open-WebConsole -WebPort $Port
    }
} else {
    Write-Host "Process started (PID=$($proc.Id)) but status endpoint not ready within ${HealthWaitSec}s."
}
