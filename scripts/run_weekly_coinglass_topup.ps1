param(
    [string]$PythonExe = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe",
    [string]$LogPath = "",
    # A healthy weekly top-up takes about a day (one request every 12 s); older runs are hung.
    [int]$MaxAgeHours = 72,
    [switch]$Quiet
)

# Weekly top-up of the local Coinglass history (registered 2026-10-13 at the user's request as the
# scheduled task CryptoTradingSystem_WeeklyCoinglassTopUp; docs/LOCAL_DATA_INVENTORY.md):
#   1. scripts/collect_coinglass_history.py --update   the 12 per-coin daily datasets of every Binance
#      USDT perp, only the days after each series' newest bar (~5,800 relay requests, about a day;
#      it pauses while the daily store updates);
#   2. scripts/collect_coinglass_macro.py              ~70 market-wide series (~35 minutes);
#   3. scripts/collect_coinglass_history.py --panels   one parquet per dataset for research.
# Skips the week when a collection is still running (the first full download takes ~2 days).

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
if (-not (Test-Path -LiteralPath $PythonExe)) { throw "Pinned Python executable not found: $PythonExe" }
$history = Join-Path $PSScriptRoot "collect_coinglass_history.py"
$macro = Join-Path $PSScriptRoot "collect_coinglass_macro.py"

$resolvedLogPath = if ([string]::IsNullOrWhiteSpace($LogPath)) {
    Join-Path $projectRoot "logs\weekly_coinglass_topup.log"
} elseif ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $projectRoot $LogPath }
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedLogPath) | Out-Null
if ((Test-Path $resolvedLogPath) -and ((Get-Item $resolvedLogPath).Length -ge 10MB)) {
    Move-Item -Path $resolvedLogPath -Destination "$resolvedLogPath.1" -Force -ErrorAction SilentlyContinue
}
function Write-Log([string[]]$Lines) { $Lines | Add-Content -Path $resolvedLogPath -Encoding utf8 }

$running = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    ([string]$_.Name).ToLowerInvariant() -in @("python.exe", "pythonw.exe") -and [string]$_.CommandLine -and
        (([string]$_.CommandLine).ToLowerInvariant().Contains("collect_coinglass_history.py") -or
         ([string]$_.CommandLine).ToLowerInvariant().Contains("collect_coinglass_macro.py"))
})
if ($running.Count) {
    $now = Get-Date
    $stale = @($running | Where-Object {
        $started = $null
        try { $started = [datetime]$_.CreationDate } catch { $started = $null }
        ($null -ne $started) -and (($now - $started).TotalHours -ge $MaxAgeHours)
    })
    if ($stale.Count -and ($stale.Count -eq $running.Count)) {
        foreach ($proc in $stale) { Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue }
        Write-Log @("", ("[{0}] killed {1} collection process(es) older than {2}h" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $stale.Count, $MaxAgeHours))
        Start-Sleep -Seconds 2
    } else {
        Write-Log @("", ("[{0}] a Coinglass collection is still running; this week is skipped" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss")))
        if (-not $Quiet) { Write-Host "A Coinglass collection is still running; skipping." }
        exit 0
    }
}

$failed = @()
foreach ($step in @(@{Name = "history --update"; Args = @($history, "--update")},
                    @{Name = "macro"; Args = @($macro)},
                    @{Name = "panels"; Args = @($history, "--panels")})) {
    $stdout = Join-Path ([System.IO.Path]::GetTempPath()) ("weekly_cg_stdout_{0}.log" -f ([guid]::NewGuid().ToString("N")))
    $stderr = Join-Path ([System.IO.Path]::GetTempPath()) ("weekly_cg_stderr_{0}.log" -f ([guid]::NewGuid().ToString("N")))
    $started = Get-Date
    $process = Start-Process -FilePath $PythonExe -ArgumentList $step.Args -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -Wait -PassThru
    $lines = @("", ("[{0}] weekly Coinglass {1} start" -f $started.ToString("yyyy-MM-dd HH:mm:ss"), $step.Name))
    if (Test-Path $stdout) { $lines += Get-Content $stdout }
    if (Test-Path $stderr) {
        $errLines = @(Get-Content $stderr | Where-Object { $_ -match "WARNING|ERROR|Traceback|Error" })
        if ($errLines.Count) { $lines += "--- stderr (warnings/errors) ---"; $lines += $errLines }
    }
    $lines += ("[{0}] weekly Coinglass {1} exit={2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $step.Name, $process.ExitCode)
    Write-Log $lines
    Remove-Item $stdout, $stderr -Force -ErrorAction SilentlyContinue
    if ($process.ExitCode -ne 0) { $failed += $step.Name }
}
if ($failed.Count) { throw ("Weekly Coinglass top-up failed: {0}" -f ($failed -join ", ")) }
if (-not $Quiet) { Write-Host "Weekly Coinglass top-up completed." }
