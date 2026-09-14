param(
    [ValidateSet("help", "start", "status", "stop", "live-shadow-news")]
    [string]$Action = "help",
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000,
    [int]$HealthWaitSec = 150,
    [double]$MaxWsAgeMs = 10000,
    [switch]$OpenBrowser,
    [switch]$AllowPersistedLiveMode,
    [switch]$PaperMode,
    [switch]$ConfirmLive,
    [switch]$ResetNewsLlmFailover,
    [switch]$SkipPrecheck,
    [switch]$StartAutonomousAgent,
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

function Get-WebSupervisorProcesses {
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                $name -and $name.ToLowerInvariant() -eq "powershell.exe" -and
                $cmd -and
                [int]$_.ProcessId -ne [int]$PID -and
                $cmd -match '(?i)-File\s+(?:"[^"]*supervise_web\.ps1"|[^\s"]*supervise_web\.ps1)(?:\s|$)' -and
                $cmd -match ("(?i)-Port\s+{0}(?:\s|$)" -f [int]$Port)
            }
    )
}

function Get-ListeningPid {
    param([int]$PortNumber)

    try {
        $listening = Get-NetTCPConnection -LocalPort $PortNumber -State Listen -ErrorAction Stop |
            Select-Object -First 1
        if ($listening -and $listening.OwningProcess) {
            return [int]$listening.OwningProcess
        }
    } catch {
    }

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
        foreach ($line in Get-Content -LiteralPath $path -Encoding UTF8) {
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

function Get-OpsAuthHeaders {
    param([hashtable]$EnvValues)

    $opsToken = [string]($env:OPS_TOKEN)
    if ([string]::IsNullOrWhiteSpace($opsToken) -and $EnvValues -and $EnvValues.ContainsKey("OPS_TOKEN")) {
        $opsToken = [string]$EnvValues["OPS_TOKEN"]
    }

    $headers = @{}
    if (-not [string]::IsNullOrWhiteSpace($opsToken)) {
        $headers["X-OPS-TOKEN"] = $opsToken.Trim()
        $headers["X-OPS-CALLER"] = "web_status"
    }
    return $headers
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
    $matches = @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                (
                    $name -and
                    $name.ToLowerInvariant() -in @("python.exe", "pythonw.exe") -and
                    $cmd -and
                    [int]$_.ProcessId -ne [int]$PID -and
                    $cmd.ToLowerInvariant().Contains($token.ToLowerInvariant())
                )
            }
    )
    return $matches
}

function Get-ManagedWebProcesses {
    $matches = @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                (
                    $name -and
                    $name.ToLowerInvariant() -in @("python.exe", "pythonw.exe") -and
                    $cmd -and
                    (
                        $cmd -like "*uvicorn*web.main:app*" -or
                        $cmd -like "*main.py --mode web*"
                    )
                )
            }
    )
    return $matches
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

    $healthTimeoutSec = 18
    $statusTimeoutSec = 15
    $envValues = Get-EnvFileValues
    $statusHeaders = Get-OpsAuthHeaders -EnvValues $envValues

    try {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/readyz" -TimeoutSec $healthTimeoutSec
    }
    catch {
        return $null
    }

    $status = $null
    try {
        if ($statusHeaders.Count -gt 0) {
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/api/status" -Headers $statusHeaders -TimeoutSec $statusTimeoutSec
        } else {
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/api/status" -TimeoutSec $statusTimeoutSec
        }
    }
    catch {
    }

    return [pscustomobject]@{
        Health = $health
        Status = $status
    }
}

function Get-AutonomousAgentSummary {
    param([int]$PortNumber)

    # The endpoint is behind read_trading_state and answers 401 without the ops
    # token, which used to surface as a bare "AI Agent : status unavailable"
    # even while the agent was running. The first call after boot can also warm
    # a symbol scan, so allow more than the old 8s.
    $headers = Get-OpsAuthHeaders -EnvValues (Get-EnvFileValues)
    try {
        if ($headers.Count -gt 0) {
            return Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/api/ai/autonomous-agent/status" -Headers $headers -TimeoutSec 20
        }
        return Invoke-RestMethod -Uri "http://127.0.0.1:$PortNumber/api/ai/autonomous-agent/status" -TimeoutSec 20
    }
    catch {
        return $null
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

function Get-WebSupervisorTaskSummary {
    param([int]$PortNumber)
    try {
        $task = Get-ScheduledTask -TaskName ("CryptoTradingSystem_WebSupervisor_{0}" -f $PortNumber) -ErrorAction Stop
        $state = [string]$task.State
        # A paper managed start registers a delayed logon trigger; a live start
        # registers none. Surface it so reboot behaviour is visible from status.
        $hasLogonTrigger = @($task.Triggers | Where-Object { $_.CimClass.CimClassName -eq "MSFT_TaskLogonTrigger" }).Count -gt 0
        $bootText = if ($hasLogonTrigger) { "auto-start at logon" } else { "manual start only" }
        return ("{0}, {1}" -f $state, $bootText)
    } catch {
        return "not registered"
    }
}

function Show-Help {
    Write-Host ""
    Write-Host "CryptoTradingSystem web control" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Single entry point:"
    Write-Host "  .\web.bat"
    Write-Host "    no args -> one-click startup with browser"
    Write-Host "    start   -> managed startup without auto-opening browser"
    Write-Host "    status  -> show web / worker / agent state"
    Write-Host "    stop    -> stop web, optionally observed workers"
    Write-Host ""
    Write-Host "Commands to remember:"
    Write-Host "  .\web.bat"
    Write-Host "  .\web.bat help"
    Write-Host "  .\web.bat start"
    Write-Host "  .\web.bat live-shadow-news -ConfirmLive -ResetNewsLlmFailover"
    Write-Host "  .\web.bat status"
    Write-Host "  .\web.bat stop -IncludeWorkers"
    Write-Host ""
    Write-Host "Common start variants:"
    Write-Host "  .\web.bat start -OpenBrowser"
    Write-Host "  .\web.bat start -PaperMode"
    Write-Host "  .\web.bat start -AllowPersistedLiveMode"
    Write-Host "  .\web.bat start -StartAutonomousAgent"
    Write-Host "  .\web.bat start -NoNewsWorkers"
    Write-Host "  .\web.bat start -NoNewsLlmWorker"
    Write-Host "  .\web.bat start -EnableAnalyticsHistory"
    Write-Host "  .\web.bat start -StartNewsWorker"
    Write-Host "  .\web.bat start -StartNewsWorker -StartNewsLlmWorker"
    Write-Host "  .\web.bat start -StartPmWorker"
    Write-Host "  .\web.bat start -Port 8000 -HealthWaitSec 150"
    Write-Host ""
    Write-Host "Managed default profile:"
    Write-Host "  - '.\web.bat start' launches web + news worker + news LLM worker."
    Write-Host "  - Managed start defaults to PAPER mode with market WS primary and REST quality fallback."
    Write-Host "  - Pass -AllowPersistedLiveMode for an explicit LIVE + strategy_primary startup."
    Write-Host "  - Managed start ignores .env START_* worker flags and uses command-line flags."
    Write-Host "  - Analytics history stays off unless you pass -EnableAnalyticsHistory."
    Write-Host "  - PM worker remains opt-in via -StartPmWorker."
    Write-Host "  - AI autonomous agent stays separate unless env auto-start is true or you pass -StartAutonomousAgent."
    Write-Host "  - Research universe incremental refresh task is auto-ensured on start."
    Write-Host "  - Use 'live-shadow-news' for the explicit LIVE + market WS shadow + news engine profile."
    Write-Host ""
    Write-Host "Live shadow with news:"
    Write-Host "  .\web.bat live-shadow-news -ConfirmLive -ResetNewsLlmFailover"
    Write-Host "    restarts web + news worker + news LLM worker in live mode"
    Write-Host "    forces MARKET_WS_MODE=shadow and MARKET_WS_FAIL_CLOSED_FOR_LIVE=true"
    Write-Host "    clears sticky news LLM failover when -ResetNewsLlmFailover is present"
    Write-Host ""
    Write-Host "Troubleshooting:"
    Write-Host "  - Use '.\web.bat status' after every startup."
    Write-Host "  - If worker mix is wrong, run '.\web.bat stop -IncludeWorkers' and start again."
    Write-Host "  - Startup transcript: logs\web_ps.log"
    Write-Host "  - Full startup reference: STARTUP.md"
    Write-Host ""
}

function Show-Status {
    param([int]$PortNumber)

    $webPid = Get-ListeningPid -PortNumber $PortNumber
    $webProc = Get-ProcessRecord -ProcessId $webPid
    $managedWebProcesses = @(Get-ManagedWebProcesses | Sort-Object CreationDate -Descending)
    if ((-not $webPid) -and $managedWebProcesses.Count) {
        $webProc = $managedWebProcesses | Select-Object -First 1
        $webPid = [int]$webProc.ProcessId
    }
    $health = Get-HealthSummary -PortNumber $PortNumber
    $agentSummary = if ($health -and $health.Health) {
        Get-AutonomousAgentSummary -PortNumber $PortNumber
    } else {
        $null
    }
    $envValues = Get-EnvFileValues

    Write-Host ""
    Write-Host "Web service status" -ForegroundColor Cyan
    Write-Host ("  Project root : {0}" -f $projectRoot)
    Write-Host ("  Port         : {0}" -f $PortNumber)
    Write-Host "  Start policy : default web + news engine (analytics-history disabled)"
    Write-Host "  Live restore : default disabled; -AllowPersistedLiveMode opts in"
    Write-Host "  Worker start : news workers auto-start by default; PM worker stays opt-in"
    $analyticsEnvValue = if ($envValues.ContainsKey("ANALYTICS_HISTORY_ENABLED")) { [string]$envValues["ANALYTICS_HISTORY_ENABLED"] } else { $null }
    $runtimeAnalyticsKnown = $false
    $runtimeAnalyticsText = "unknown"
    if ($health -and $health.Status -and $health.Status.runtime -and $null -ne $health.Status.runtime.analytics_history_enabled) {
        $runtimeAnalyticsKnown = $true
        $runtimeAnalyticsText = if ([bool]$health.Status.runtime.analytics_history_enabled) { "runtime on" } else { "runtime off" }
    }
    if ($runtimeAnalyticsKnown) {
        Write-Host ("  Analytics    : {0} [env ANALYTICS_HISTORY_ENABLED={1}]" -f $runtimeAnalyticsText, (Format-ConfigValue -Value $analyticsEnvValue))
    } else {
        Write-Host ("  Analytics    : default off unless -EnableAnalyticsHistory is used [env ANALYTICS_HISTORY_ENABLED={0}]" -f (Format-ConfigValue -Value $analyticsEnvValue))
    }
    Write-Host ("  Research job : {0}" -f (Get-ResearchUniverseTaskSummary))
    $supervisors = @(Get-WebSupervisorProcesses)
    $supervisorText = if ($supervisors.Count) {
        "running (PID=" + (($supervisors | Select-Object -ExpandProperty ProcessId) -join ", ") + ")"
    } else {
        "not observed"
    }
    Write-Host ("  Supervisor   : {0} [task {1}]" -f $supervisorText, (Get-WebSupervisorTaskSummary -PortNumber $PortNumber))

    if ((-not $webPid) -and (-not $managedWebProcesses.Count)) {
        Write-Host "  Web          : stopped"
    }
    elseif (Test-IsManagedWebProcess -ProcessRecord $webProc) {
        $mode = if ($health -and $health.Status) { [string]$health.Status.trading_mode } else { "unknown" }
        $state = if ($health -and $health.Health -and $health.Status) {
            [string]$health.Health.status
        } elseif ($health -and $health.Health) {
            "warming_up"
        } else {
            "warming_up"
        }
        $startupMode = if ($health -and $health.Status -and $health.Status.runtime) { $health.Status.runtime.startup_mode } else { $null }
        Write-Host ("  Web          : running (PID={0}, state={1}, mode={2})" -f $webPid, $state, $mode)
        Write-Host ("  URL          : http://127.0.0.1:{0}" -f $PortNumber)
        if ($startupMode) {
            $configuredMode = [string]$startupMode.configured_mode
            $persistedMode = [string]$startupMode.persisted_mode
            $startupSource = [string]$startupMode.source
            if ([string]::IsNullOrWhiteSpace($persistedMode)) {
                $persistedMode = "unset"
            }
            Write-Host ("  Startup mode : configured={0}, persisted={1}, source={2}" -f $configuredMode, $persistedMode, $startupSource)
            if ([bool]$startupMode.blocked_persisted_live_restore) {
                Write-Host "  Guard        : blocked persisted live-mode restore during startup." -ForegroundColor Yellow
            }
        }
        if (-not ($health -and $health.Health)) {
            Write-Host "  Hint         : managed web process exists but health checks are still warming up." -ForegroundColor Yellow
            Write-Host "                 If analytics-history was enabled, allow extra startup time before restarting it." -ForegroundColor Yellow
        } elseif (-not ($health -and $health.Status)) {
            Write-Host "  Hint         : /health is ready but /api/status is still warming up." -ForegroundColor Yellow
            Write-Host "                 This can happen briefly during analytics-history startup." -ForegroundColor Yellow
        }
        elseif ($mode -eq "live") {
            Write-Host "  Warning      : service is currently running in live mode." -ForegroundColor Yellow
        }
        if ($agentSummary -and $agentSummary.status) {
            $agentRunning = [bool]$agentSummary.status.running
            $agentConfig = if ($agentSummary.config) { $agentSummary.config } else { @{} }
            $agentMode = [string]($agentConfig.mode)
            $agentSymbolMode = [string]($agentConfig.symbol_mode)
            $agentAutoStart = if ([bool]$agentConfig.auto_start) { "true" } else { "false" }
            $agentAllowLive = if ([bool]$agentConfig.allow_live) { "true" } else { "false" }
            $selectedSymbol = [string]($agentSummary.status.last_selected_symbol)
            if ([string]::IsNullOrWhiteSpace($selectedSymbol)) {
                $selectedSymbol = [string]($agentConfig.symbol)
            }
            if ([string]::IsNullOrWhiteSpace($selectedSymbol)) {
                $selectedSymbol = "n/a"
            }
            Write-Host (
                "  AI Agent     : {0} (mode={1}, auto_start={2}, allow_live={3}, symbol_mode={4}, symbol={5})" -f
                $(if ($agentRunning) { "running" } else { "stopped" }),
                $(if ([string]::IsNullOrWhiteSpace($agentMode)) { "unknown" } else { $agentMode }),
                $agentAutoStart,
                $agentAllowLive,
                $(if ([string]::IsNullOrWhiteSpace($agentSymbolMode)) { "unknown" } else { $agentSymbolMode }),
                $selectedSymbol
            )
            $agentSafety = $null
            if ($agentSummary.status.safety) {
                $agentSafety = $agentSummary.status.safety
            }
            elseif ($agentConfig.safety) {
                $agentSafety = $agentConfig.safety
            }
            if ($agentSafety) {
                $safetyStatus = [string]($agentSafety.status)
                if ([string]::IsNullOrWhiteSpace($safetyStatus)) {
                    $safetyStatus = "unknown"
                }
                $runtimeProfile = [string]($agentSafety.runtime_profile)
                if ([string]::IsNullOrWhiteSpace($runtimeProfile)) {
                    $runtimeProfile = [string]($agentConfig.runtime_profile)
                }
                if ([string]::IsNullOrWhiteSpace($runtimeProfile)) {
                    $runtimeProfile = "custom"
                }
                $reasonCodes = @($agentSafety.reason_codes | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
                $reasonText = if ($reasonCodes.Count) { $reasonCodes -join ", " } else { "none" }
                $safetyLine = switch ($safetyStatus.ToLowerInvariant()) {
                    "ready" { "ready for paper long-run" }
                    "unsafe" { "unsafe for paper long-run" }
                    default { "attention needed for paper long-run" }
                }
                $safetyColor = switch ($safetyStatus.ToLowerInvariant()) {
                    "ready" { "Green" }
                    "unsafe" { "Yellow" }
                    default { "Yellow" }
                }
                Write-Host ("  AI Safety    : {0} (profile={1}, reasons={2})" -f $safetyLine, $runtimeProfile, $reasonText) -ForegroundColor $safetyColor
            }
            else {
                $agentModeText = [string]$agentMode
                if (
                    $agentModeText.Trim().ToLowerInvariant() -eq "execute" -or
                    $agentAutoStart -eq "true" -or
                    $agentAllowLive -eq "true"
                ) {
                    Write-Host "  Warning      : AI agent config remains armed (execute/auto-start/live-capable)." -ForegroundColor Yellow
                }
            }
        }
        else {
            Write-Host "  AI Agent     : status unavailable" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host ("  Web          : port occupied by unmanaged PID={0}" -f $webPid) -ForegroundColor Yellow
    }

    foreach ($worker in $script:WorkerDefinitions) {
        $observed = @(Get-ObservedWorkerProcesses -CommandToken $worker.Token)
        Write-Host ("  {0,-12}: {1}" -f $worker.Label, (Format-ObservedWorkerState -EnvName $worker.EnvName -Processes $observed -EnvValues $envValues))
    }
    Write-Host ""
    Write-Host "Quick commands:"
    Write-Host "  .\web.bat"
    Write-Host "  .\web.bat help"
    Write-Host "  .\web.bat start"
    Write-Host "  .\web.bat live-shadow-news -ConfirmLive -ResetNewsLlmFailover"
    Write-Host "  .\web.bat start -StartAutonomousAgent"
    Write-Host "  .\web.bat start -NoNewsWorkers"
    Write-Host "  .\web.bat start -EnableAnalyticsHistory"
    Write-Host "  .\web.bat stop -IncludeWorkers"
    Write-Host "  Transcript: logs\web_ps.log"
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
    $managedWebProcesses = @(Get-ManagedWebProcesses | Sort-Object ProcessId -Unique)

    Write-Host ""
    Write-Host "Stop request" -ForegroundColor Cyan
    Write-Host ("  Scope        : {0}" -f ($(if ($StopWorkers) { "web + observed external workers" } else { "web only" })))

    # Stop the detached supervisor first so an intentional operator stop is
    # never mistaken for a crash that needs an immediate restart.
    $supervisorStopPath = Join-Path $projectRoot ("runtime\web_supervisor_{0}.stop" -f $PortNumber)
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $supervisorStopPath) | Out-Null
    "operator_stop" | Set-Content -Path $supervisorStopPath -Encoding ASCII
    $supervisors = @(Get-WebSupervisorProcesses)
    if ($supervisors.Count) {
        Start-Sleep -Milliseconds 750
        foreach ($supervisor in $supervisors) {
            if (Get-Process -Id $supervisor.ProcessId -ErrorAction SilentlyContinue) {
                Stop-Process -Id $supervisor.ProcessId -Force -ErrorAction SilentlyContinue
            }
            Write-Host ("Stopped web supervisor PID={0}" -f $supervisor.ProcessId)
            $stopped = $true
        }
    }

    if ($webPid -and (-not (Test-IsManagedWebProcess -ProcessRecord $webProc))) {
        throw "Port $PortNumber is occupied by PID $webPid, but it does not look like the managed web process."
    }
    if ($managedWebProcesses.Count) {
        foreach ($proc in $managedWebProcesses) {
            Stop-Process -Id $proc.ProcessId -Force
            if ([int]$proc.ProcessId -eq [int]$webPid) {
                Write-Host ("Stopped web service PID={0}" -f $proc.ProcessId)
            } else {
                Write-Host ("Stopped stale managed web PID={0}" -f $proc.ProcessId)
            }
            $stopped = $true
        }
    } else {
        Write-Host "Web service is already stopped."
    }

    if ($StopWorkers) {
        foreach ($worker in $script:WorkerDefinitions) {
            $matched = @(Get-ObservedWorkerProcesses -CommandToken $worker.Token)
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
                $matched = @(Get-ObservedWorkerProcesses -CommandToken $worker.Token)
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

        $effectiveAllowPersistedLiveMode = $AllowPersistedLiveMode.IsPresent -and (-not $PaperMode.IsPresent)

        & $startScript `
            -EnvName $EnvName `
            -BindHost $BindHost `
            -Port $Port `
            -HealthWaitSec $HealthWaitSec `
            -OpenBrowser:$OpenBrowser.IsPresent `
            -AllowPersistedLiveMode:$effectiveAllowPersistedLiveMode `
            -StartAutonomousAgent:$StartAutonomousAgent.IsPresent `
            -StartNewsWorker:$effectiveStartNewsWorker `
            -StartNewsLlmWorker:$effectiveStartNewsLlmWorker `
            -StartPmWorker:$StartPmWorker.IsPresent `
            -EnableAnalyticsHistory:$EnableAnalyticsHistory.IsPresent `
            -TestDataSources:$TestDataSources.IsPresent
    }
    "live-shadow-news" {
        $liveShadowNewsScript = Join-Path $PSScriptRoot "start_live_shadow_news.ps1"
        if (-not (Test-Path $liveShadowNewsScript)) {
            throw "Live shadow news startup script not found: $liveShadowNewsScript"
        }

        & $liveShadowNewsScript `
            -Action restart `
            -EnvName $EnvName `
            -BindHost $BindHost `
            -Port $Port `
            -HealthWaitSec $HealthWaitSec `
            -MaxWsAgeMs $MaxWsAgeMs `
            -ConfirmLive:$ConfirmLive.IsPresent `
            -ResetNewsLlmFailover:$ResetNewsLlmFailover.IsPresent `
            -SkipPrecheck:$SkipPrecheck.IsPresent `
            -OpenBrowser:$OpenBrowser.IsPresent
    }
    "stop" {
        Stop-ManagedProcesses -PortNumber $Port -StopWorkers:$IncludeWorkers.IsPresent
    }
}
