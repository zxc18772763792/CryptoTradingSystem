param(
    [string]$EnvName = "crypto_trading",
    [string]$Exchange = "binance",
    [string]$Timeframes = "1m,5m,15m,1h",
    [int]$Days = 90,
    [int]$OverlapBars = 48,
    [string]$SecondsSymbols = "BTC/USDT,ETH/USDT",
    [int]$SecondsDays = 1,
    [switch]$DisableIdleSeconds,
    [string]$LogPath = "",
    [switch]$Quiet,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

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

    $venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        return $venvPython
    }

    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd) {
        return $pyCmd.Source
    }

    throw "Cannot find Python executable for research universe refresh."
}

function Get-RunningRefreshProcess {
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                if ($name -and $name.ToLowerInvariant() -notin @("python.exe", "pythonw.exe")) {
                    return $false
                }
                $cmd = [string]$_.CommandLine
                if (-not $cmd) {
                    return $false
                }
                return $cmd.ToLowerInvariant().Contains("maintain_research_universe_data.py")
            }
    )
}

if ((-not $Force) -and (Get-RunningRefreshProcess).Count) {
    if (-not $Quiet) {
        Write-Host "Research universe refresh is already running. Skipping duplicate launch." -ForegroundColor Yellow
    }
    exit 0
}

if (-not (Enable-CondaEnv -Name $EnvName) -and (-not $Quiet)) {
    Write-Host "Conda env '$EnvName' not found from common paths/PATH. Falling back to .venv or system python." -ForegroundColor Yellow
}

$pythonExe = Resolve-PythonExecutable
$scriptPath = Join-Path $PSScriptRoot "maintain_research_universe_data.py"
if (-not (Test-Path $scriptPath)) {
    throw "Script not found: $scriptPath"
}

$resolvedLogPath = if ([string]::IsNullOrWhiteSpace($LogPath)) {
    Join-Path $projectRoot "logs\research_universe_refresh.log"
} else {
    if ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $projectRoot $LogPath }
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedLogPath) | Out-Null

$args = @(
    $scriptPath,
    "--exchange", $Exchange,
    "--timeframes", $Timeframes,
    "--days", "$Days",
    "--overlap-bars", "$OverlapBars",
    "--seconds-symbols", $SecondsSymbols,
    "--seconds-days", "$SecondsDays"
)
if ($DisableIdleSeconds) {
    $args += "--disable-idle-seconds"
}

if (-not $Quiet) {
    Write-Host "Starting research universe refresh..."
    Write-Host "  Python    : $pythonExe"
    Write-Host "  Exchange  : $Exchange"
    Write-Host "  Timeframes: $Timeframes"
    Write-Host "  Days      : $Days"
    Write-Host "  Overlap   : $OverlapBars"
    Write-Host "  1s Symbols: $SecondsSymbols"
    Write-Host "  1s Days   : $SecondsDays"
    Write-Host "  Log       : $resolvedLogPath"
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedLogPath) | Out-Null
$stdoutPath = Join-Path ([System.IO.Path]::GetTempPath()) ("research_universe_refresh_stdout_{0}.log" -f ([guid]::NewGuid().ToString("N")))
$stderrPath = Join-Path ([System.IO.Path]::GetTempPath()) ("research_universe_refresh_stderr_{0}.log" -f ([guid]::NewGuid().ToString("N")))
$process = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList $args `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -Wait `
    -PassThru
$exitCode = $process.ExitCode

$logLines = @(
    "",
    ("[{0}] Research universe refresh start" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
)
if (Test-Path $stdoutPath) {
    $logLines += Get-Content $stdoutPath
}
if (Test-Path $stderrPath) {
    $stderrLines = Get-Content $stderrPath
    if ($stderrLines.Count) {
        $logLines += "--- stderr ---"
        $logLines += $stderrLines
    }
}
$logLines += ("[{0}] Research universe refresh exit={1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $exitCode)
$logLines | Add-Content -Path $resolvedLogPath

if (-not $Quiet) {
    $consoleLines = $logLines | Where-Object { $_ -ne "" }
    if ($consoleLines.Count) {
        $consoleLines | ForEach-Object { Write-Host $_ }
    }
}

Remove-Item $stdoutPath,$stderrPath -Force -ErrorAction SilentlyContinue
if ($exitCode -ne 0) {
    throw "Research universe refresh failed with exit code $exitCode"
}

if (-not $Quiet) {
    Write-Host "Research universe refresh completed." -ForegroundColor Green
}
