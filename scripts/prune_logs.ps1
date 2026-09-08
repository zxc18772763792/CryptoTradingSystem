<#
.SYNOPSIS
    Prune generated runtime logs so a long paper run does not fill logs\.

.DESCRIPTION
    The managed startup path launches uvicorn directly and redirects stdout and
    stderr to logs\uvicorn_web_<stamp>.{out,err}.log. Those files are created
    fresh on every restart and were never cleaned up, so logs\ grew without
    bound across a long run (210 MB / 412 files before this script existed).

    Only files matching the generated-log patterns below are considered. Files
    newer than -RetentionDays are always kept, and the newest -KeepLatest files
    of every pattern are kept regardless of age so the last few restarts stay
    debuggable. Logs that are still open by a running process are skipped
    instead of failing the run.

.PARAMETER RetentionDays
    Delete matching logs whose last write time is older than this. Default 14.

.PARAMETER KeepLatest
    Per pattern, always keep this many newest files. Default 4.

.PARAMETER DryRun
    Report what would be deleted without deleting anything.
#>
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$RetentionDays = 14,
    [int]$KeepLatest = 4,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$logRoot = Join-Path $ProjectRoot "logs"
if (-not (Test-Path $logRoot)) {
    Write-Host "No logs directory at $logRoot; nothing to prune."
    exit 0
}

# Generated per-restart or rotated logs. Anything not listed here is never touched.
$patterns = @(
    "uvicorn_web_*.out.log",
    "uvicorn_web_*.err.log",
    "web_restart_*.out.log",
    "web_restart_*.err.log",
    "ui_primary_service_*.log",
    "research_universe_refresh.log.*"
)

$cutoff = (Get-Date).AddDays(-[Math]::Abs($RetentionDays))
$keep = [Math]::Max(0, $KeepLatest)

$deletedCount = 0
$deletedBytes = 0L
$skippedInUse = 0

foreach ($pattern in $patterns) {
    $files = @(
        Get-ChildItem -Path $logRoot -Filter $pattern -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending
    )
    if ($files.Count -le $keep) {
        continue
    }

    foreach ($file in $files[$keep..($files.Count - 1)]) {
        if ($file.LastWriteTime -ge $cutoff) {
            continue
        }
        if ($DryRun) {
            Write-Host ("  would delete {0} ({1:N1} MB, {2:yyyy-MM-dd})" -f $file.Name, ($file.Length / 1MB), $file.LastWriteTime)
            $deletedCount++
            $deletedBytes += $file.Length
            continue
        }
        try {
            $size = $file.Length
            Remove-Item -LiteralPath $file.FullName -Force -ErrorAction Stop
            $deletedCount++
            $deletedBytes += $size
        }
        catch {
            # Still open by a live process (or locked by an indexer); leave it.
            $skippedInUse++
        }
    }
}

$verb = if ($DryRun) { "Would prune" } else { "Pruned" }
Write-Host ("{0} {1} log file(s), {2:N1} MB (retention {3}d, keep newest {4} per pattern)." -f $verb, $deletedCount, ($deletedBytes / 1MB), $RetentionDays, $keep)
if ($skippedInUse -gt 0) {
    Write-Host ("Skipped {0} log file(s) still in use." -f $skippedInUse) -ForegroundColor Yellow
}
exit 0
