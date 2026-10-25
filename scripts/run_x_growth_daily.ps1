param(
    [string]$LogPath = "",
    [string[]]$TrackerArgs = @("daily")
)

# Runs scripts/track_x_growth.py with the project interpreter and appends everything it prints to a
# log. The scheduled task passes no arguments, so it runs "daily"; pass -TrackerArgs "daily","--dry-run"
# to check the plan without sending a request.
$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path (Split-Path -Parent $projectRoot) ".conda\miniforge3\envs\crypto_trading\python.exe"
$tracker = Join-Path $PSScriptRoot "track_x_growth.py"
if (-not $LogPath) {
    $LogPath = Join-Path $projectRoot "logs\x_growth_daily.log"
}
if (-not (Test-Path $python)) {
    throw "Project interpreter not found: $python (see AGENTS.md section 1)"
}
if (-not (Test-Path $tracker)) {
    throw "Tracker script not found: $tracker"
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogPath) | Out-Null
if ((Test-Path $LogPath) -and ((Get-Item $LogPath).Length -gt 1MB)) {
    Move-Item -Force $LogPath "$LogPath.1"
}
$argString = ($TrackerArgs | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }) -join " "
"=== $(Get-Date -Format o) track_x_growth.py $argString" | Add-Content -Path $LogPath -Encoding UTF8

# cmd does the redirection: Windows PowerShell 5.1 turns a native command's stderr lines into
# error records, which would abort this script on the first line the tracker prints to stderr.
Push-Location $projectRoot
try {
    $command = '"{0}" -u -X utf8 -W ignore "{1}" {2} >> "{3}" 2>&1' -f $python, $tracker, $argString, $LogPath
    & cmd.exe /c ('"' + $command + '"')
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}
"=== exit $code" | Add-Content -Path $LogPath -Encoding UTF8
exit $code
