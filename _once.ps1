param(
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Get-ListeningPid {
    param([int]$PortNumber)
    $line = netstat -ano | Select-String -Pattern "LISTENING\s+(\d+)$" | Select-String -Pattern "[:\.]$PortNumber\s"
    if (-not $line) { return $null }
    $text = ($line | Select-Object -First 1).Line.Trim()
    $parts = $text -split "\s+"
    if ($parts.Count -lt 5) { return $null }
    return [int]$parts[-1]
}

function Enable-CondaEnv {
    param([string]$Name)

    $null = Get-Command conda -ErrorAction Stop
    $condaBase = (& conda info --base).Trim()
    if (-not $condaBase) {
        throw "Cannot resolve conda base path."
    }
    $condaHook = Join-Path $condaBase "shell\condabin\conda-hook.ps1"
    if (-not (Test-Path $condaHook)) {
        throw "Conda hook not found: $condaHook"
    }
    . $condaHook
    conda activate $Name
    if ($env:CONDA_DEFAULT_ENV -ne $Name) {
        throw "Failed to activate conda env: $Name"
    }
}

$pidOnPort = Get-ListeningPid -PortNumber $Port
if ($pidOnPort) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$pidOnPort" -ErrorAction SilentlyContinue
    if ($proc -and ($proc.CommandLine -like "*uvicorn*web.main:app*" -or $proc.CommandLine -like "*main.py --mode web*")) {
        Write-Host "Service already listening on 0.0.0.0:$Port (PID=$pidOnPort)."
        exit 0
    }
    throw "Port $Port is already in use by PID $pidOnPort."
}

Enable-CondaEnv -Name $EnvName

$pythonExe = Join-Path $env:CONDA_PREFIX "python.exe"
if (-not (Test-Path $pythonExe)) {
    throw "Python not found in active env: $pythonExe"
}

$proc = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList @("-m", "uvicorn", "web.main:app", "--host", $BindHost, "--port", "$Port") `
    -WorkingDirectory $PSScriptRoot `
    -PassThru

Start-Sleep -Seconds 2

try {
    $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -TimeoutSec 6
    Write-Host "Started PID=$($proc.Id), status=$($status.status), mode=$($status.trading_mode), url=http://127.0.0.1:$Port"
} catch {
    Write-Host "Process started (PID=$($proc.Id)) but status endpoint not ready yet: $($_.Exception.Message)"
}
