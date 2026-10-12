param(
    [string]$PythonExe = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe",
    [string]$LogPath = "",
    # A healthy top-up takes a few minutes; an older run is treated as hung.
    [int]$MaxAgeMinutes = 60,
    [switch]$Quiet
)

# Daily top-up of the local Binance klines of the 234-coin wide universe (perps, and spot where
# Binance lists it, at 1h/4h/1d): scripts/audit_fill_klines.py --tail-only asks only for the bars
# after each series' last local one. Registered 2026-10-12 as the scheduled task
# CryptoTradingSystem_WideKlineTopUp (daily; docs/LOCAL_KLINE_STORE_AUDIT_2026-10-12.md).

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
$scriptPath = Join-Path $PSScriptRoot "audit_fill_klines.py"
if (-not (Test-Path -LiteralPath $PythonExe)) { throw "Pinned Python executable not found: $PythonExe" }
if (-not (Test-Path -LiteralPath $scriptPath)) { throw "Script not found: $scriptPath" }

function Get-RunningTopUp {
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                ($name.ToLowerInvariant() -in @("python.exe", "pythonw.exe")) -and $cmd -and
                    $cmd.ToLowerInvariant().Contains("audit_fill_klines.py")
            }
    )
}

$running = @(Get-RunningTopUp)
if ($running.Count) {
    $now = Get-Date
    $stale = @($running | Where-Object {
        $started = $null
        try { $started = [datetime]$_.CreationDate } catch { $started = $null }
        ($null -ne $started) -and (($now - $started).TotalMinutes -ge $MaxAgeMinutes)
    })
    if ($stale.Count -and ($stale.Count -eq $running.Count)) {
        foreach ($proc in $stale) { Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue }
        Start-Sleep -Seconds 2
    } else {
        if (-not $Quiet) { Write-Host "A kline top-up or audit is already running; skipping." }
        exit 0
    }
}

$resolvedLogPath = if ([string]::IsNullOrWhiteSpace($LogPath)) {
    Join-Path $projectRoot "logs\wide_kline_topup.log"
} elseif ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $projectRoot $LogPath }
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedLogPath) | Out-Null

$stdoutPath = Join-Path ([System.IO.Path]::GetTempPath()) ("wide_kline_topup_stdout_{0}.log" -f ([guid]::NewGuid().ToString("N")))
$stderrPath = Join-Path ([System.IO.Path]::GetTempPath()) ("wide_kline_topup_stderr_{0}.log" -f ([guid]::NewGuid().ToString("N")))
$started = Get-Date
$process = Start-Process -FilePath $PythonExe -ArgumentList @($scriptPath, "--tail-only") -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -WindowStyle Hidden -Wait -PassThru
$exitCode = $process.ExitCode

$logLines = @("", ("[{0}] wide kline top-up start" -f $started.ToString("yyyy-MM-dd HH:mm:ss")))
if (Test-Path $stdoutPath) { $logLines += Get-Content $stdoutPath }
if (Test-Path $stderrPath) {
    # loguru writes to stderr; keep only warnings and errors
    $errLines = @(Get-Content $stderrPath | Where-Object { $_ -match "WARNING|ERROR|Traceback|Error" })
    if ($errLines.Count) { $logLines += "--- stderr (warnings/errors) ---"; $logLines += $errLines }
}
$logLines += ("[{0}] wide kline top-up exit={1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $exitCode)

if (Test-Path $resolvedLogPath) {
    try {
        if ((Get-Item $resolvedLogPath).Length -ge 10MB) {
            Move-Item -Path $resolvedLogPath -Destination "$resolvedLogPath.1" -Force -ErrorAction SilentlyContinue
        }
    } catch { }
}
$logLines | Add-Content -Path $resolvedLogPath -Encoding utf8
Remove-Item $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
if (-not $Quiet) { $logLines | Where-Object { $_ -ne "" } | ForEach-Object { Write-Host $_ } }
if ($exitCode -ne 0) { throw "Wide kline top-up failed with exit code $exitCode" }
