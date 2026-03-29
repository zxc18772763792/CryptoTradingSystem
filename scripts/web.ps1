param(
    [ValidateSet("help", "start", "status", "stop")]
    [string]$Action = "help",
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8000,
    [int]$HealthWaitSec = 150,
    [switch]$OpenBrowser,
    [switch]$StartNewsWorker,
    [switch]$StartNewsLlmWorker,
    [switch]$StartPmWorker,
    [switch]$TestDataSources,
    [switch]$IncludeWorkers
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Get-ListeningPid {
    param([int]$PortNumber)

    $line = netstat -ano |
        Select-String -Pattern "LISTENING\s+(\d+)$" |
        Select-String -Pattern "[:\.]$PortNumber\s"

    if (-not $line) {
        return $null
    }

    $text = ($line | Select-Object -First 1).Line.Trim()
    $parts = $text -split "\s+"
    if ($parts.Count -lt 5) {
        return $null
    }

    return [int]$parts[-1]
}

function Get-ProcessRecord {
    param([int]$ProcessId)

    if (-not $ProcessId) {
        return $null
    }

    return Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction SilentlyContinue
}

function Get-MatchingProcesses {
    param([string]$Pattern)

    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { $_.CommandLine -like $Pattern }
    )
}

function Test-IsManagedWebProcess {
    param($ProcessRecord)

    if (-not $ProcessRecord) {
        return $false
    }

    $cmd = [string]$ProcessRecord.CommandLine
    return $cmd -like "*uvicorn*web.main:app*" -or $cmd -like "*main.py --mode web*"
}

function Get-HealthSummary {
    param([int]$PortNumber)

    try {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/health" -TimeoutSec 4
    }
    catch {
        return $null
    }

    $status = $null
    try {
        $status = Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/api/status" -TimeoutSec 4
    }
    catch {
    }

    return [pscustomobject]@{
        Health = $health
        Status = $status
    }
}

function Show-Help {
    Write-Host ""
    Write-Host "CryptoTradingSystem web control" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Canonical commands from project root:"
    Write-Host "  .\web.bat start"
    Write-Host "  .\web.bat status"
    Write-Host "  .\web.bat stop -IncludeWorkers"
    Write-Host ""
    Write-Host "Common examples:"
    Write-Host "  .\web.bat start -OpenBrowser"
    Write-Host "  .\web.bat start -StartNewsWorker -StartNewsLlmWorker"
    Write-Host "  .\web.bat start -StartPmWorker"
    Write-Host "  .\web.bat start -Port 8000 -HealthWaitSec 150"
    Write-Host ""
    Write-Host "Notes:"
    Write-Host "  - Use '.\web.bat start' as the default entry point in new sessions."
    Write-Host "  - 'start_web_oneclick.bat' and 'start_once.bat' remain as compatibility wrappers."
    Write-Host "  - Logs: logs\web_ps.log"
    Write-Host ""
}

function Show-Status {
    param([int]$PortNumber)

    $webPid = Get-ListeningPid -PortNumber $PortNumber
    $webProc = Get-ProcessRecord -ProcessId $webPid
    $health = Get-HealthSummary -PortNumber $PortNumber
    $newsWorkers = Get-MatchingProcesses -Pattern "*core.news.service.worker*"
    $llmWorkers = Get-MatchingProcesses -Pattern "*core.news.service.llm_worker*"
    $pmWorkers = Get-MatchingProcesses -Pattern "*prediction_markets.polymarket.worker*"

    Write-Host ""
    Write-Host "Web service status" -ForegroundColor Cyan
    Write-Host ("  Project root : {0}" -f $projectRoot)
    Write-Host ("  Port         : {0}" -f $PortNumber)

    if (-not $webPid) {
        Write-Host "  Web          : stopped"
    }
    elseif (Test-IsManagedWebProcess -ProcessRecord $webProc) {
        $mode = if ($health -and $health.Status) { [string]$health.Status.trading_mode } else { "unknown" }
        $state = if ($health -and $health.Health) { [string]$health.Health.status } else { "listening_no_health" }
        Write-Host ("  Web          : running (PID={0}, state={1}, mode={2})" -f $webPid, $state, $mode)
        Write-Host ("  URL          : http://127.0.0.1:{0}" -f $PortNumber)
        if (-not ($health -and $health.Health)) {
            Write-Host "  Hint         : service is listening but health checks did not answer yet." -ForegroundColor Yellow
            Write-Host "                 Try '.\web.bat stop -IncludeWorkers' then '.\web.bat start' if it stays stuck." -ForegroundColor Yellow
        }
        elseif ($mode -eq "live") {
            Write-Host "  Warning      : service is currently running in live mode." -ForegroundColor Yellow
        }
    }
    else {
        Write-Host ("  Web          : port occupied by unmanaged PID={0}" -f $webPid) -ForegroundColor Yellow
    }

    Write-Host ("  News worker  : {0}" -f ($(if ($newsWorkers.Count) { ($newsWorkers | ForEach-Object { $_.ProcessId }) -join ", " } else { "stopped" })))
    Write-Host ("  LLM worker   : {0}" -f ($(if ($llmWorkers.Count) { ($llmWorkers | ForEach-Object { $_.ProcessId }) -join ", " } else { "stopped" })))
    Write-Host ("  PM worker    : {0}" -f ($(if ($pmWorkers.Count) { ($pmWorkers | ForEach-Object { $_.ProcessId }) -join ", " } else { "stopped" })))
    Write-Host ""
    Write-Host "Quick commands:"
    Write-Host "  .\web.bat start"
    Write-Host "  .\web.bat stop -IncludeWorkers"
    Write-Host ""
}

function Stop-ManagedProcesses {
    param(
        [int]$PortNumber,
        [switch]$StopWorkers
    )

    $stopped = $false
    $webPid = Get-ListeningPid -PortNumber $PortNumber
    $webProc = Get-ProcessRecord -ProcessId $webPid

    if ($webPid -and (Test-IsManagedWebProcess -ProcessRecord $webProc)) {
        Stop-Process -Id $webPid -Force
        Write-Host ("Stopped web service PID={0}" -f $webPid)
        $stopped = $true
    }
    elseif ($webPid) {
        throw "Port $PortNumber is occupied by PID $webPid, but it does not look like the managed web process."
    }
    else {
        Write-Host "Web service is already stopped."
    }

    if ($StopWorkers) {
        $patterns = @(
            @{ Label = "news worker"; Pattern = "*core.news.service.worker*" },
            @{ Label = "news LLM worker"; Pattern = "*core.news.service.llm_worker*" },
            @{ Label = "Polymarket worker"; Pattern = "*prediction_markets.polymarket.worker*" }
        )

        foreach ($item in $patterns) {
            $matched = Get-MatchingProcesses -Pattern $item.Pattern
            if (-not $matched.Count) {
                Write-Host ("{0} already stopped." -f $item.Label)
                continue
            }
            foreach ($proc in $matched) {
                Stop-Process -Id $proc.ProcessId -Force
                Write-Host ("Stopped {0} PID={1}" -f $item.Label, $proc.ProcessId)
                $stopped = $true
            }
        }
    }

    if (-not $stopped) {
        Write-Host "Nothing needed stopping."
    }
}

switch ($Action) {
    "help" {
        Show-Help
    }
    "status" {
        Show-Status -PortNumber $Port
    }
    "start" {
        $startScript = Join-Path $PSScriptRoot "start_web_ps.ps1"
        if (-not (Test-Path $startScript)) {
            throw "Startup script not found: $startScript"
        }

        & $startScript `
            -EnvName $EnvName `
            -BindHost $BindHost `
            -Port $Port `
            -HealthWaitSec $HealthWaitSec `
            -OpenBrowser:$OpenBrowser.IsPresent `
            -StartNewsWorker:$StartNewsWorker.IsPresent `
            -StartNewsLlmWorker:$StartNewsLlmWorker.IsPresent `
            -StartPmWorker:$StartPmWorker.IsPresent `
            -TestDataSources:$TestDataSources.IsPresent
    }
    "stop" {
        Stop-ManagedProcesses -PortNumber $Port -StopWorkers:$IncludeWorkers.IsPresent
    }
}
