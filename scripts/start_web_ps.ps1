param(
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8000,
    [int]$HealthWaitSec = 150,
    [switch]$OpenBrowser,
    [switch]$StartNewsWorker,
    [switch]$StartNewsLlmWorker,
    [switch]$StartPmWorker,
    [switch]$TestDataSources
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" | Out-Null
}

$onceScript = Join-Path $projectRoot "_once.ps1"
if (-not (Test-Path $onceScript)) {
    throw "One-click startup script not found: $onceScript"
}

$logPath = Join-Path $projectRoot "logs\web_ps.log"
Start-Transcript -Path $logPath -Append | Out-Null
try {
    $onceParams = @{
        EnvName = $EnvName
        BindHost = $BindHost
        Port = $Port
        HealthWaitSec = $HealthWaitSec
        OpenBrowser = $OpenBrowser.IsPresent
        StartNewsWorker = $StartNewsWorker.IsPresent
        StartNewsLlmWorker = $StartNewsLlmWorker.IsPresent
        StartPmWorker = $StartPmWorker.IsPresent
        TestDataSources = $TestDataSources.IsPresent
    }
    & $onceScript @onceParams
}
finally {
    Stop-Transcript | Out-Null
}
