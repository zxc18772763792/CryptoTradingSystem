param(
    [string]$LogPath = "",
    [string[]]$PollArgs = @()
)

# Runs scripts/poll_x_archive.py with the project interpreter and appends everything it prints to a log.
# Pass -PollArgs "--dry-run" to see the plan without sending a request.
$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path (Split-Path -Parent $projectRoot) ".conda\miniforge3\envs\crypto_trading\python.exe"
$poller = Join-Path $PSScriptRoot "poll_x_archive.py"
if (-not $LogPath) {
    $LogPath = Join-Path $projectRoot "logs\x_archive_poll.log"
}
if (-not (Test-Path $python)) {
    throw "Project interpreter not found: $python (see AGENTS.md section 1)"
}
if (-not (Test-Path $poller)) {
    throw "Poller script not found: $poller"
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogPath) | Out-Null
if ((Test-Path $LogPath) -and ((Get-Item $LogPath).Length -gt 1MB)) {
    Move-Item -Force $LogPath "$LogPath.1"
}
$argString = ($PollArgs | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }) -join " "
"=== $(Get-Date -Format o) poll_x_archive.py $argString" | Add-Content -Path $LogPath -Encoding UTF8

# cmd does the redirection: Windows PowerShell 5.1 turns a native command's stderr lines into
# error records, which would abort this script on the first line the poller prints to stderr.
Push-Location $projectRoot
try {
    $command = '"{0}" -u -X utf8 -W ignore "{1}" {2} >> "{3}" 2>&1' -f $python, $poller, $argString, $LogPath
    & cmd.exe /c ('"' + $command + '"')
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}
"=== exit $code" | Add-Content -Path $LogPath -Encoding UTF8
exit $code
