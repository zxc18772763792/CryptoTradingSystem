param(
    [string]$PythonExe = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe",
    [int]$MinuteDays = 30,
    [int]$HigherTimeframeDays = 365,
    [int]$FuturesBars = 300,
    [int]$MaxWorkers = 4,
    [double]$MaxRequestsPerSecond = 5.0
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $PSScriptRoot "maintain_all_binance_market_data.py"

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Pinned Python executable not found: $PythonExe"
}
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "Maintainer not found: $scriptPath"
}

Set-Location $projectRoot
& $PythonExe $scriptPath `
    --refresh `
    --minute-days $MinuteDays `
    --htf-days $HigherTimeframeDays `
    --futures-bars $FuturesBars `
    --max-workers $MaxWorkers `
    --max-requests-per-second $MaxRequestsPerSecond

if ($LASTEXITCODE -ne 0) {
    throw "All-Binance data refresh failed closed with exit code $LASTEXITCODE"
}
