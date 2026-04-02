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
    [switch]$NoNewsWorkers,
    [switch]$NoNewsLlmWorker,
    [switch]$StartPmWorker,
    [switch]$EnableAnalyticsHistory,
    [switch]$TestDataSources,
    [switch]$IncludeWorkers
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$script:WorkerDefinitions = @(
    [pscustomobject]@{
        Label = "News worker"
        Token = "core.news.service.worker"
        EnvName = "START_NEWS_WORKER"
        StartFlag = "-StartNewsWorker"
    },
    [pscustomobject]@{
        Label = "LLM worker"
        Token = "core.news.service.llm_worker"
        EnvName = "START_NEWS_LLM_WORKER"
        StartFlag = "-StartNewsLlmWorker"
    },
    [pscustomobject]@{
        Label = "PM worker"
        Token = "prediction_markets.polymarket.worker"
        EnvName = "START_PM_WORKER"
        StartFlag = "-StartPmWorker"
    }
)
$script:ResearchUniverseTaskName = "CryptoTradingSystem_ResearchUniverseRefresh"

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

function Test-TruthyText {
    param([AllowNull()][string]$Value)

    if ($null -eq $Value) {
        return $false
    }

    return $Value.Trim().ToLower() -in @("1", "true", "yes", "on")
}

function Get-EnvFileValues {
    $values = @{}
    foreach ($path in @(
        (Join-Path $projectRoot ".env"),
        (Join-Path $projectRoot ".env.local")
    )) {
        if (-not (Test-Path $path)) {
            continue
        }
        foreach ($line in Get-Content $path) {
            $text = [string]$line
            if (-not $text) {
                continue
            }
            $trimmed = $text.Trim()
            if (-not $trimmed -or $trimmed.StartsWith("#")) {
                continue
            }
            $eq = $trimmed.IndexOf("=")
            if ($eq -lt 1) {
                continue
            }
            $name = $trimmed.Substring(0, $eq).Trim()
            $value = $trimmed.Substring($eq + 1).Trim()
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            $values[$name] = $value
        }
    }
    return $values
}

function Format-ConfigValue {
    param([AllowNull()][string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "unset"
    }
    return $Value
}

function Get-ObservedWorkerProcesses {
    param([string]$CommandToken)

    $token = [string]$CommandToken
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
                if ([int]$_.ProcessId -eq [int]$PID) {
                    return $false
                }
                return $cmd.ToLowerInvariant().Contains($token.ToLowerInvariant())
            }
    )
}

function Format-ObservedWorkerState {
    param(
        [string]$EnvName,
        [object[]]$Processes,
        [hashtable]$EnvValues
    )

    $envValue = $null
    if ($EnvValues.ContainsKey($EnvName)) {
        $envValue = [string]$EnvValues[$EnvName]
    }

    $baseText = if ($Processes.Count) {
        "running (PID=" + (($Processes | Select-Object -ExpandProperty ProcessId) -join ", ") + ")"
    } else {
        "not observed"
    }

    $note = if (Test-TruthyText $envValue) {
        "[env $EnvName=$envValue, managed start ignores env flags]"
    } elseif (-not [string]::IsNullOrWhiteSpace($envValue)) {
        "[env $EnvName=$envValue]"
    } else {
        "[env $EnvName=unset]"
    }

    if ($Processes.Count) {
        return "$baseText $note"
    }

    return "$baseText $note"
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

function Get-ResearchUniverseTaskSummary {
    try {
        $task = Get-ScheduledTask -TaskName $script:ResearchUniverseTaskName -ErrorAction Stop
        $info = Get-ScheduledTaskInfo -TaskName $script:ResearchUniverseTaskName -ErrorAction SilentlyContinue
        $nextRun = if ($info -and $info.NextRunTime) { $info.NextRunTime } else { $null }
        if ($nextRun) {
            return ("{0} (next {1})" -f $task.State, $nextRun)
        }
        return [string]$task.State
    } catch {
        return "not registered"
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
    Write-Host "Compatibility alias:"
    Write-Host "  .\start_web_oneclick.bat"
    Write-Host ""
    Write-Host "Common examples:"
    Write-Host "  .\web.bat start -OpenBrowser"
    Write-Host "  .\web.bat start -NoNewsWorkers"
    Write-Host "  .\web.bat start -NoNewsLlmWorker"
    Write-Host "  .\web.bat start -EnableAnalyticsHistory"
    Write-Host "  .\web.bat start -StartNewsWorker"
    Write-Host "  .\web.bat start -StartNewsWorker -StartNewsLlmWorker"
    Write-Host "  .\web.bat start -StartPmWorker"
    Write-Host "  .\web.bat start -Port 8000 -HealthWaitSec 150"
    Write-Host ""
    Write-Host "Notes:"
    Write-Host "  - Use '.\web.bat start' as the default entry point."
    Write-Host "  - Default start launches web + news worker + news LLM worker."
    Write-Host "  - News page should stay in automatic background mode when those workers are observed."
    Write-Host "  - Use -NoNewsWorkers for a web-only start, or -NoNewsLlmWorker to skip only the LLM worker."
    Write-Host "  - Managed startup ignores .env START_* worker flags and uses command-line policy instead."
    Write-Host "  - Default start disables analytics history; opt in with -EnableAnalyticsHistory."
    Write-Host "  - PM worker remains opt-in via -StartPmWorker."
    Write-Host "  - Research universe incremental refresh task is auto-ensured on start."
    Write-Host "  - 'start_web_oneclick.bat' and 'start_once.bat' remain as compatibility wrappers."
    Write-Host "  - Logs: logs\web_ps.log"
    Write-Host ""
}

function Show-Status {
    param([int]$PortNumber)

    $webPid = Get-ListeningPid -PortNumber $PortNumber
    $webProc = Get-ProcessRecord -ProcessId $webPid
    $health = Get-HealthSummary -PortNumber $PortNumber
    $envValues = Get-EnvFileValues

    Write-Host ""
    Write-Host "Web service status" -ForegroundColor Cyan
    Write-Host ("  Project root : {0}" -f $projectRoot)
    Write-Host ("  Port         : {0}" -f $PortNumber)
    Write-Host "  Start policy : default web + news engine (analytics-history disabled)"
    Write-Host "  Worker start : news workers auto-start by default; PM worker stays opt-in"
    $analyticsEnvValue = if ($envValues.ContainsKey("ANALYTICS_HISTORY_ENABLED")) { [string]$envValues["ANALYTICS_HISTORY_ENABLED"] } else { $null }
    Write-Host ("  Analytics    : default off unless -EnableAnalyticsHistory is used [env ANALYTICS_HISTORY_ENABLED={0}]" -f (Format-ConfigValue -Value $analyticsEnvValue))
    Write-Host ("  Research job : {0}" -f (Get-ResearchUniverseTaskSummary))

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

    foreach ($worker in $script:WorkerDefinitions) {
        $observed = Get-ObservedWorkerProcesses -CommandToken $worker.Token
        Write-Host ("  {0,-12}: {1}" -f $worker.Label, (Format-ObservedWorkerState -EnvName $worker.EnvName -Processes $observed -EnvValues $envValues))
    }
    Write-Host ""
    Write-Host "Quick commands:"
    Write-Host "  .\web.bat start"
    Write-Host "  .\web.bat start -NoNewsWorkers"
    Write-Host "  .\web.bat start -EnableAnalyticsHistory"
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

    Write-Host ""
    Write-Host "Stop request" -ForegroundColor Cyan
    Write-Host ("  Scope        : {0}" -f ($(if ($StopWorkers) { "web + observed external workers" } else { "web only" })))

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
        foreach ($worker in $script:WorkerDefinitions) {
            $matched = Get-ObservedWorkerProcesses -CommandToken $worker.Token
            if (-not $matched.Count) {
                Write-Host ("{0} already stopped." -f $worker.Label)
                continue
            }
            foreach ($proc in $matched) {
                Stop-Process -Id $proc.ProcessId -Force
                Write-Host ("Stopped {0} PID={1}" -f $worker.Label, $proc.ProcessId)
                $stopped = $true
            }
        }
    } else {
        $observedWorkers = @(
            foreach ($worker in $script:WorkerDefinitions) {
                $matched = Get-ObservedWorkerProcesses -CommandToken $worker.Token
                if ($matched.Count) {
                    [pscustomobject]@{
                        Label = $worker.Label
                        Pids = (($matched | Select-Object -ExpandProperty ProcessId) -join ", ")
                    }
                }
            }
        )
        if ($observedWorkers.Count) {
            $summary = $observedWorkers | ForEach-Object { "$($_.Label) PID=$($_.Pids)" }
            Write-Host ("Observed external workers still running: {0}" -f ($summary -join "; ")) -ForegroundColor Yellow
            Write-Host "Use '.\web.bat stop -IncludeWorkers' to stop them as well." -ForegroundColor Yellow
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

        $effectiveStartNewsWorker = $false
        $effectiveStartNewsLlmWorker = $false

        if (-not $NoNewsWorkers.IsPresent) {
            $effectiveStartNewsWorker = $true
        }
        if ((-not $NoNewsWorkers.IsPresent) -and (-not $NoNewsLlmWorker.IsPresent)) {
            $effectiveStartNewsLlmWorker = $true
        }
        if ($StartNewsWorker.IsPresent) {
            $effectiveStartNewsWorker = $true
        }
        if ($StartNewsLlmWorker.IsPresent) {
            $effectiveStartNewsLlmWorker = $true
        }

        & $startScript `
            -EnvName $EnvName `
            -BindHost $BindHost `
            -Port $Port `
            -HealthWaitSec $HealthWaitSec `
            -OpenBrowser:$OpenBrowser.IsPresent `
            -StartNewsWorker:$effectiveStartNewsWorker `
            -StartNewsLlmWorker:$effectiveStartNewsLlmWorker `
            -StartPmWorker:$StartPmWorker.IsPresent `
            -EnableAnalyticsHistory:$EnableAnalyticsHistory.IsPresent `
            -TestDataSources:$TestDataSources.IsPresent
    }
    "stop" {
        Stop-ManagedProcesses -PortNumber $Port -StopWorkers:$IncludeWorkers.IsPresent
    }
}
