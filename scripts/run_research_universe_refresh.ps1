param(
    [string]$EnvName = "crypto_trading",
    [string]$Exchange = "binance",
    # Must include every timeframe the altcoin radar / research views read. The
    # radar defaults to 4h (DEFAULT_TIMEFRAME) and there is NO 1h->4h aggregation
    # in _load_local_or_aggregate, so 4h and 1d must be fetched natively or the
    # radar sees empty/stale frames no matter how fresh 1h is.
    [string]$Timeframes = "1m,5m,15m,1h,4h,1d",
    [int]$Days = 90,
    [int]$OverlapBars = 48,
    [string]$SecondsSymbols = "BTC/USDT,ETH/USDT",
    [int]$SecondsDays = 1,
    [switch]$DisableIdleSeconds,
    [string]$LogPath = "",
    [switch]$Quiet,
    [switch]$Force,
    # A healthy incremental refresh finishes in a few minutes. If an existing
    # maintain_research process is older than this, it is treated as hung and
    # killed so it cannot block the pipeline (a dangling connector once wedged
    # one for ~1.6 days, silently starving every scheduled refresh).
    [int]$MaxRefreshAgeMinutes = 45
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
    # Pin the F: conda env first. This project's authoritative home is F:, but
    # the Task Scheduler context often has no conda on PATH and the conda-hook
    # candidates in Enable-CondaEnv do not know about F:\9_Crypto\.conda\
    # miniforge3 -- so resolution previously fell through to a stale E:-drive
    # python found on PATH. See the crypto-test-env note.
    $pinnedPython = "F:\9_Crypto\.conda\miniforge3\envs\$EnvName\python.exe"
    if (Test-Path $pinnedPython) {
        return $pinnedPython
    }

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

if (-not $Force) {
    $runningRefreshes = @(Get-RunningRefreshProcess)
    if ($runningRefreshes.Count) {
        $now = Get-Date
        $staleRefreshes = @($runningRefreshes | Where-Object {
            $started = $null
            try { $started = [datetime]$_.CreationDate } catch { $started = $null }
            ($null -ne $started) -and (($now - $started).TotalMinutes -ge $MaxRefreshAgeMinutes)
        })
        # Only intervene when EVERY running refresh is stale: a fresh one is
        # legitimately in progress and must not be killed. When all are stale,
        # they are hung (a dangling connector once wedged one for ~1.6 days,
        # silently blocking every scheduled refresh) -- kill them and proceed.
        if ($staleRefreshes.Count -and ($staleRefreshes.Count -eq $runningRefreshes.Count)) {
            foreach ($proc in $staleRefreshes) {
                Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
                if (-not $Quiet) {
                    Write-Host ("Killed hung research refresh PID={0} (age >= {1} min); proceeding with a fresh run." -f $proc.ProcessId, $MaxRefreshAgeMinutes) -ForegroundColor Yellow
                }
            }
            $killDeadline = (Get-Date).AddSeconds(10)
            do {
                $survivors = @(Get-RunningRefreshProcess)
                if (-not $survivors.Count) { break }
                Start-Sleep -Milliseconds 250
            } while ((Get-Date) -lt $killDeadline)
            if (@(Get-RunningRefreshProcess).Count) {
                throw "Hung research refresh process did not terminate within 10 seconds."
            }
        } else {
            if (-not $Quiet) {
                Write-Host "Research universe refresh is already running (within max age). Skipping duplicate launch." -ForegroundColor Yellow
            }
            exit 0
        }
    }
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

# Size-based rotation: keep current log under 10MB. When it exceeds the limit,
# rename to .1 (overwriting any prior .1) so we retain ~2 rotations worth of
# history without unbounded growth. The previous run had no rotation at all
# and the file had grown to 35MB+ in production.
$rotateThresholdBytes = 10MB
if (Test-Path $resolvedLogPath) {
    try {
        $sizeBytes = (Get-Item $resolvedLogPath).Length
        if ($sizeBytes -ge $rotateThresholdBytes) {
            $rotated = "$resolvedLogPath.1"
            if (Test-Path $rotated) {
                Remove-Item $rotated -Force -ErrorAction SilentlyContinue
            }
            Move-Item -Path $resolvedLogPath -Destination $rotated -Force -ErrorAction SilentlyContinue
        }
    } catch {
        # Rotation is best-effort; never let a logging hiccup break the refresh.
    }
}

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
