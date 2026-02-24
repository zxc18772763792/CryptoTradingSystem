$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}

& "$PSScriptRoot\dev_web.ps1" 2>&1 | Tee-Object -FilePath "logs/web_ps.log" -Append
