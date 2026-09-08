param(
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000,
    [bool]$OpenBrowser = $true,
    [int]$HealthWaitSec = 20,
    [bool]$AllowPersistedLiveMode = $false,
    [bool]$StartAutonomousAgent = $false,
    [bool]$StartNewsWorker = $false,
    [bool]$StartNewsLlmWorker = $false,
    [bool]$StartPmWorker = $false,
    [bool]$EnableAnalyticsHistory = $false,
    [bool]$TestDataSources = $false,
    [switch]$DisableSupervisorBootstrap
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Open-WebConsole {
    param([int]$WebPort)
    try {
        Start-Process "http://127.0.0.1:$WebPort/" | Out-Null
    } catch {
        Write-Host "Browser open skipped: $($_.Exception.Message)"
    }
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

    $line = netstat -ano | Select-String -Pattern "LISTENING\s+(\d+)$" | Select-String -Pattern "[:\.]$PortNumber\s"
    if (-not $line) { return $null }
    $text = ($line | Select-Object -First 1).Line.Trim()
    $parts = $text -split "\s+"
    if ($parts.Count -lt 5) { return $null }
    return [int]$parts[-1]
}

function Get-WorkerPid {
    $workers = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -like "*core.news.service.worker*"
    }
    if (-not $workers) { return $null }
    return [int]($workers | Select-Object -First 1).ProcessId
}

function Get-LlmWorkerPid {
    $workers = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -like "*core.news.service.llm_worker*"
    }
    if (-not $workers) { return $null }
    return [int]($workers | Select-Object -First 1).ProcessId
}

function Get-PmWorkerPid {
    $workers = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -like "*prediction_markets.polymarket.worker*"
    }
    if (-not $workers) { return $null }
    return [int]($workers | Select-Object -First 1).ProcessId
}

function Enable-CondaEnv {
    param([string]$Name)
    $workspaceRoot = Split-Path -Parent $PSScriptRoot
    $localCondaRoot = Join-Path $workspaceRoot ".conda\miniforge3"
    $localCondaHook = Join-Path $localCondaRoot "shell\condabin\conda-hook.ps1"
    $localCondabin = Join-Path $localCondaRoot "condabin"
    $localEnvRoot = Join-Path $localCondaRoot ("envs\{0}" -f $Name)
    $localEnvPython = Join-Path $localEnvRoot "python.exe"

    if (Test-Path $localEnvPython) {
        $env:CONDA_PREFIX = $localEnvRoot
        $env:CONDA_DEFAULT_ENV = $Name
        if ([string]::IsNullOrWhiteSpace($env:CONDA_SHLVL)) {
            $env:CONDA_SHLVL = "1"
        }

        $preferredPaths = @(
            $localEnvRoot,
            (Join-Path $localEnvRoot "Library\mingw-w64\bin"),
            (Join-Path $localEnvRoot "Library\usr\bin"),
            (Join-Path $localEnvRoot "Library\bin"),
            (Join-Path $localEnvRoot "Scripts"),
            (Join-Path $localEnvRoot "bin"),
            $localCondabin
        )
        $pathParts = @($env:Path -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
        $orderedPreferredPaths = @($preferredPaths)
        [array]::Reverse($orderedPreferredPaths)
        foreach ($entry in $orderedPreferredPaths) {
            if ((Test-Path $entry) -and ($pathParts -notcontains $entry)) {
                $env:Path = $entry + ";" + $env:Path
                $pathParts = @($env:Path -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
            }
        }
        return $true
    }

    if (Test-Path $localCondabin) {
        $pathParts = @($env:Path -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
        if ($pathParts -notcontains $localCondabin) {
            $env:Path = $localCondabin + ";" + $env:Path
        }
    }

    # Startup is intentionally hermetic: never search user/global Conda
    # installations. Use scripts\setup_local_env.ps1 to provision this path.
    return $false
}

function Resolve-PythonExecutable {
    if ($env:CONDA_PREFIX) {
        $condaPython = Join-Path $env:CONDA_PREFIX "python.exe"
        if (Test-Path $condaPython) {
            return $condaPython
        }
    }

    $venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        return $venvPython
    }

    throw "Cannot find the project Python environment. Run .\scripts\setup_local_env.ps1 from $PSScriptRoot."
}

function Normalize-ProcessEnvironment {
    # PowerShell 7 on Windows can expose the inherited PATH variable twice
    # (PATH and Path). Start-Process builds a case-insensitive environment map
    # and then fails with "Key ... PATH ... Path". Keep the canonical Path entry
    # and remove the duplicate alias before spawning web/workers/supervisor.
    $pathValue = [string]$env:Path
    if (-not [string]::IsNullOrWhiteSpace($pathValue)) {
        Remove-Item Env:PATH -ErrorAction SilentlyContinue
        $env:Path = $pathValue
    }
}

function Import-DotEnvFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return }
    foreach ($line in Get-Content $Path) {
        $text = [string]$line
        if (-not $text) { continue }
        $trimmed = $text.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) { continue }
        $eq = $trimmed.IndexOf("=")
        if ($eq -lt 1) { continue }
        $name = $trimmed.Substring(0, $eq).Trim()
        $value = $trimmed.Substring($eq + 1).Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        if ($name) {
            Set-Item -Path ("Env:" + $name) -Value $value
        }
    }
}

function Test-TruthyText {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return $false }
    return $Value.Trim().ToLower() -in @("1", "true", "yes", "on")
}

function Get-OpsAuthHeaders {
    $headers = @{}
    $opsToken = [string]($env:OPS_TOKEN)
    if (-not [string]::IsNullOrWhiteSpace($opsToken)) {
        $headers["X-OPS-TOKEN"] = $opsToken.Trim()
        $headers["X-OPS-CALLER"] = "web_startup"
    }
    return $headers
}

function Get-RequestedWorkerLabels {
    param(
        [bool]$NewsWorker,
        [bool]$NewsLlmWorker,
        [bool]$PmWorker,
        [bool]$AutonomousAgent
    )

    $labels = @()
    if ($NewsWorker) { $labels += "news-worker" }
    if ($NewsLlmWorker) { $labels += "news-llm-worker" }
    if ($PmWorker) { $labels += "pm-worker" }
    if ($AutonomousAgent) { $labels += "autonomous-agent" }
    return $labels
}

function Set-EffectiveWorkerEnvFlags {
    param(
        [bool]$NewsWorker,
        [bool]$NewsLlmWorker,
        [bool]$PmWorker
    )

    Set-Item -Path Env:START_NEWS_WORKER -Value $(if ($NewsWorker) { "1" } else { "0" })
    Set-Item -Path Env:START_NEWS_LLM_WORKER -Value $(if ($NewsLlmWorker) { "1" } else { "0" })
    Set-Item -Path Env:START_PM_WORKER -Value $(if ($PmWorker) { "1" } else { "0" })
    Set-Item -Path Env:NEWS_LLM_EXTERNAL_ONLY -Value $(if ($NewsLlmWorker) { "1" } else { "0" })
}

function Start-AutonomousAgent {
    param([int]$WebPort)

    $opsToken = [string]($env:OPS_TOKEN)
    if ([string]::IsNullOrWhiteSpace($opsToken)) {
        throw "OPS_TOKEN is required to start the autonomous agent through the protected API. Configure OPS_TOKEN in .env or .env.local before using -StartAutonomousAgent."
    }

    $headers = @{
        "X-OPS-TOKEN"  = $opsToken.Trim()
        "X-OPS-CALLER" = "web_startup"
    }

    $response = Invoke-RestMethod `
        -Method POST `
        -Uri "http://127.0.0.1:$WebPort/api/ai/autonomous-agent/start" `
        -Headers $headers `
        -TimeoutSec 20

    if (-not $response) {
        throw "Autonomous agent start request returned an empty response."
    }

    return $response
}

function Show-AutonomousAgentStartSummary {
    param($Response)

    $status = if ($Response) { $Response.status } else { $null }
    $config = if ($Response) { $Response.config } else { $null }
    $running = if ($status) { [bool]$status.running } else { $false }
    $mode = if ($config -and $config.mode) { [string]$config.mode } else { "unknown" }
    $symbolMode = if ($config -and $config.symbol_mode) { [string]$config.symbol_mode } else { "unknown" }
    $symbol = if ($status -and $status.last_selected_symbol) {
        [string]$status.last_selected_symbol
    } elseif ($config -and $config.symbol) {
        [string]$config.symbol
    } else {
        "n/a"
    }
    Write-Host (
        "Autonomous agent start requested: running={0}, mode={1}, symbol_mode={2}, symbol={3}" -f
        $running, $mode, $symbolMode, $symbol
    ) -ForegroundColor Green
}

function Ensure-ResearchUniverseRefreshTask {
    param([string]$EnvName)

    $ensureScript = Join-Path $PSScriptRoot "scripts\ensure_research_universe_refresh_task.ps1"
    if (-not (Test-Path $ensureScript)) {
        return
    }

    try {
        $result = & $ensureScript -EnvName $EnvName -StartNowIfCreated -Quiet
        if ($result -and $result.created) {
            Write-Host "Research universe refresh scheduled task was missing and has been created." -ForegroundColor Green
            if ($result.started_now) {
                Write-Host "Research universe refresh scheduled task was started immediately." -ForegroundColor Green
            }
        }
    } catch {
        Write-Host "Research universe refresh task ensure skipped: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

function Get-RunningWebSupervisors {
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                $name -and $name.ToLowerInvariant() -eq "powershell.exe" -and
                $cmd -and [int]$_.ProcessId -ne [int]$PID -and
                $cmd -match '(?i)-File\s+(?:"[^"]*supervise_web\.ps1"|[^\s"]*supervise_web\.ps1)(?:\s|$)' -and
                $cmd -match ("(?i)-Port\s+{0}(?:\s|$)" -f [int]$Port)
            }
    )
}

function Start-WebSupervisor {
    param([string]$PythonExecutable)

    if ($DisableSupervisorBootstrap.IsPresent) { return }
    $supervisorScript = Join-Path $PSScriptRoot "scripts\supervise_web.ps1"
    if (-not (Test-Path $supervisorScript)) { throw "Web supervisor script not found: $supervisorScript" }
    $existing = @(Get-RunningWebSupervisors)
    if ($existing.Count) {
        Write-Host ("Web supervisor already running (PID={0})." -f (($existing | Select-Object -ExpandProperty ProcessId) -join ", "))
        return
    }
    $stopPath = Join-Path $PSScriptRoot ("runtime\web_supervisor_{0}.stop" -f $Port)
    Remove-Item $stopPath -Force -ErrorAction SilentlyContinue

    # Prefer launching through Task Scheduler: a supervisor spawned with
    # Start-Process stays inside the launching console's kill-on-close job
    # object and dies with the host app (2026-07-16: an app auto-update took
    # down web + workers + supervisor at once, leaving nothing to restart).
    $ensureScript = Join-Path $PSScriptRoot "scripts\ensure_web_supervisor_task.ps1"
    if (Test-Path $ensureScript) {
        try {
            $ensureParams = @{
                ProjectRoot = $PSScriptRoot
                PythonExecutable = $PythonExecutable
                EnvName = $EnvName
                BindHost = $BindHost
                Port = $Port
                HealthWaitSec = $HealthWaitSec
                AllowPersistedLiveMode = [bool]$AllowPersistedLiveMode
                StartAutonomousAgent = [bool]$StartAutonomousAgent
                StartNewsWorker = [bool]$StartNewsWorker
                StartNewsLlmWorker = [bool]$StartNewsLlmWorker
                StartPmWorker = [bool]$StartPmWorker
                EnableAnalyticsHistory = [bool]$EnableAnalyticsHistory
                # Paper starts get a delayed logon trigger so a reboot does not end an
                # unattended long run. A live start never does: the task action would
                # carry -AllowPersistedLiveMode and resurrect LIVE trading unattended.
                RegisterLogonTrigger = (-not [bool]$AllowPersistedLiveMode)
                StartNow = $true
                Quiet = $true
            }
            $null = & $ensureScript @ensureParams
            $deadline = (Get-Date).AddSeconds(12)
            while ((Get-Date) -lt $deadline) {
                $spawned = @(Get-RunningWebSupervisors)
                if ($spawned.Count) {
                    Write-Host ("Started web supervisor via scheduled task (PID={0})." -f (($spawned | Select-Object -ExpandProperty ProcessId) -join ", "))
                    Write-Host "Web supervisor log: $(Join-Path $PSScriptRoot 'logs\web_supervisor.log')"
                    return
                }
                Start-Sleep -Milliseconds 500
            }
            Write-Host "Supervisor task started but no supervisor process appeared within 12s." -ForegroundColor Yellow
        } catch {
            Write-Host "Supervisor scheduled-task launch failed: $($_.Exception.Message)" -ForegroundColor Yellow
        }
        Write-Host "Falling back to a session-bound supervisor; it will not survive this console closing." -ForegroundColor Yellow
    }

    $args = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $supervisorScript,
        "-ProjectRoot", $PSScriptRoot, "-PythonExecutable", $PythonExecutable,
        "-EnvName", $EnvName, "-BindHost", $BindHost, "-Port", "$Port",
        "-HealthWaitSec", "$HealthWaitSec"
    )
    if ($AllowPersistedLiveMode) { $args += "-AllowPersistedLiveMode" }
    if ($StartAutonomousAgent) { $args += "-StartAutonomousAgent" }
    if ($StartNewsWorker) { $args += "-StartNewsWorker" }
    if ($StartNewsLlmWorker) { $args += "-StartNewsLlmWorker" }
    if ($StartPmWorker) { $args += "-StartPmWorker" }
    if ($EnableAnalyticsHistory) { $args += "-EnableAnalyticsHistory" }
    $supervisorStdout = Join-Path $PSScriptRoot "logs\web_supervisor.out.log"
    $supervisorStderr = Join-Path $PSScriptRoot "logs\web_supervisor.err.log"
    $supervisor = Start-Process -FilePath "powershell.exe" -ArgumentList $args `
        -WorkingDirectory $PSScriptRoot -WindowStyle Hidden `
        -RedirectStandardOutput $supervisorStdout -RedirectStandardError $supervisorStderr -PassThru
    Write-Host "Started web supervisor PID=$($supervisor.Id)."
    Write-Host "Web supervisor log: $(Join-Path $PSScriptRoot 'logs\web_supervisor.log')"
}

function Invoke-NativeRuntimePrecheck {
    param([string]$PythonExecutable)
    $checkScript = Join-Path $PSScriptRoot "scripts\check_native_runtime.py"
    if (-not (Test-Path $checkScript)) { throw "Native runtime precheck not found: $checkScript" }
    & $PythonExecutable $checkScript
    if ($LASTEXITCODE -ne 0) {
        throw "Native runtime precheck failed with exit code $LASTEXITCODE. Web startup is blocked."
    }
}

Import-DotEnvFile -Path (Join-Path $PSScriptRoot ".env")
Import-DotEnvFile -Path (Join-Path $PSScriptRoot ".env.local")
Normalize-ProcessEnvironment

$ignoredEnvWorkerFlags = @()
if ((Test-TruthyText ([string]$env:START_NEWS_WORKER)) -and (-not $StartNewsWorker)) {
    $ignoredEnvWorkerFlags += "START_NEWS_WORKER"
}
if ((Test-TruthyText ([string]$env:START_NEWS_LLM_WORKER)) -and (-not $StartNewsLlmWorker)) {
    $ignoredEnvWorkerFlags += "START_NEWS_LLM_WORKER"
}
if ((Test-TruthyText ([string]$env:NEWS_LLM_EXTERNAL_ONLY)) -and (-not $StartNewsLlmWorker)) {
    $ignoredEnvWorkerFlags += "NEWS_LLM_EXTERNAL_ONLY"
}
if ((Test-TruthyText ([string]$env:START_PM_WORKER)) -and (-not $StartPmWorker)) {
    $ignoredEnvWorkerFlags += "START_PM_WORKER"
}

$requestedExternalWorkerLabels = Get-RequestedWorkerLabels `
    -NewsWorker $StartNewsWorker `
    -NewsLlmWorker $StartNewsLlmWorker `
    -PmWorker $StartPmWorker `
    -AutonomousAgent $false
$requestedStartupLabels = Get-RequestedWorkerLabels `
    -NewsWorker $StartNewsWorker `
    -NewsLlmWorker $StartNewsLlmWorker `
    -PmWorker $StartPmWorker `
    -AutonomousAgent $StartAutonomousAgent
$startupProfile = if ($requestedStartupLabels.Count) {
    "web + " + ($requestedStartupLabels -join ", ")
} else {
    "web-only"
}
if (-not $EnableAnalyticsHistory) {
    $startupProfile += " + analytics-history off"
    Set-Item -Path Env:ANALYTICS_HISTORY_ENABLED -Value "0"
} else {
    Set-Item -Path Env:ANALYTICS_HISTORY_ENABLED -Value "1"
}
$managedTradingMode = if ($AllowPersistedLiveMode) { "live" } else { "paper" }
$managedLiveRestore = if ($AllowPersistedLiveMode) { "1" } else { "0" }
Set-Item -Path Env:TRADING_MODE -Value $managedTradingMode
Set-Item -Path Env:ALLOW_PERSISTED_LIVE_MODE_START -Value $managedLiveRestore
if ($AllowPersistedLiveMode) {
    Set-Item -Path Env:MARKET_WS_ENABLED -Value "1"
    Set-Item -Path Env:MARKET_WS_MODE -Value "strategy_primary"
    Set-Item -Path Env:MARKET_WS_FORCE_REST -Value "0"
    Set-Item -Path Env:MARKET_WS_FAIL_CLOSED_FOR_LIVE -Value "1"
    Set-Item -Path Env:MARKET_WS_QUALITY_GUARD_ENABLED -Value "1"
    Set-Item -Path Env:MARKET_WS_SYMBOL_MAX_AGE_SEC -Value "60"
} else {
    # Paper trading still needs real-time market data. Keep WS authoritative
    # for strategy/runtime prices and let the quality guard fall back to REST
    # only when the socket is unavailable or stale.
    Set-Item -Path Env:MARKET_WS_ENABLED -Value "1"
    Set-Item -Path Env:MARKET_WS_MODE -Value "strategy_primary"
    Set-Item -Path Env:MARKET_WS_FORCE_REST -Value "0"
    Set-Item -Path Env:MARKET_WS_QUALITY_GUARD_ENABLED -Value "1"
    Set-Item -Path Env:MARKET_WS_SYMBOL_MAX_AGE_SEC -Value "60"
}
Set-EffectiveWorkerEnvFlags `
    -NewsWorker $StartNewsWorker `
    -NewsLlmWorker $StartNewsLlmWorker `
    -PmWorker $StartPmWorker
Ensure-ResearchUniverseRefreshTask -EnvName $EnvName

$pidOnPort = Get-ListeningPid -PortNumber $Port
if ($pidOnPort) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$pidOnPort" -ErrorAction SilentlyContinue
    if ($proc -and ($proc.CommandLine -like "*uvicorn*web.main:app*" -or $proc.CommandLine -like "*main.py --mode web*")) {
        Write-Host ("Service already listening on {0}:{1} (PID={2})." -f $BindHost, $Port, $pidOnPort)
        Write-Host "Requested startup profile: $startupProfile"
        if ($ignoredEnvWorkerFlags.Count) {
            Write-Host ("Managed start ignored .env worker flags: {0}" -f ($ignoredEnvWorkerFlags -join ", ")) -ForegroundColor Yellow
            Write-Host "Worker mix is controlled by '.\web.bat start' flags, not by .env START_* values." -ForegroundColor Yellow
        }
        if (-not $EnableAnalyticsHistory) {
            Write-Host "Default start forces ANALYTICS_HISTORY_ENABLED=0." -ForegroundColor Yellow
            Write-Host "Use '.\web.bat start -EnableAnalyticsHistory' to opt into analytics history collectors." -ForegroundColor Yellow
        }
        if ($AllowPersistedLiveMode) {
            Write-Host "Managed start uses explicit LIVE mode with MARKET_WS_MODE=strategy_primary and fail-closed quality guards." -ForegroundColor Yellow
        } else {
            Write-Host "Managed start uses PAPER mode with market WS primary and REST quality fallback." -ForegroundColor Yellow
        }
        if ($requestedExternalWorkerLabels.Count) {
            Write-Host "Worker mix was not changed because the web service is already running." -ForegroundColor Yellow
            Write-Host "Use '.\web.bat stop -IncludeWorkers' and then start again with the desired worker flags." -ForegroundColor Yellow
        }
        if ($StartAutonomousAgent) {
            $agentResponse = Start-AutonomousAgent -WebPort $Port
            Show-AutonomousAgentStartSummary -Response $agentResponse
        }
        Start-WebSupervisor -PythonExecutable (Resolve-PythonExecutable)
        if ($OpenBrowser) {
            Open-WebConsole -WebPort $Port
        }
        exit 0
    }
    throw "Port $Port is already in use by PID $pidOnPort."
}

if (Enable-CondaEnv -Name $EnvName) {
    Write-Host "Using conda env: $EnvName"
} else {
    Write-Host "Local environment '$EnvName' was not found under $PSScriptRoot\.conda\miniforge3. Run .\scripts\setup_local_env.ps1 first." -ForegroundColor Red
}

$pythonExe = Resolve-PythonExecutable
Write-Host "Python executable: $pythonExe"
Invoke-NativeRuntimePrecheck -PythonExecutable $pythonExe
Write-Host "Startup profile: $startupProfile"
if ($ignoredEnvWorkerFlags.Count) {
    Write-Host ("Managed start ignored .env worker flags: {0}" -f ($ignoredEnvWorkerFlags -join ", ")) -ForegroundColor Yellow
    Write-Host "Worker mix is controlled by '.\web.bat start' flags, not by .env START_* values." -ForegroundColor Yellow
}
if (-not $EnableAnalyticsHistory) {
    Write-Host "Default start forces ANALYTICS_HISTORY_ENABLED=0." -ForegroundColor Yellow
    Write-Host "Use '.\web.bat start -EnableAnalyticsHistory' when you want analytics history collectors." -ForegroundColor Yellow
}
if ($AllowPersistedLiveMode) {
    Write-Host "Managed start uses explicit LIVE mode with MARKET_WS_MODE=strategy_primary and fail-closed quality guards." -ForegroundColor Yellow
} else {
    Write-Host "Managed start uses PAPER mode with market WS primary and REST quality fallback." -ForegroundColor Yellow
}

# Managed startup creates a fresh uvicorn out/err log pair every time and nothing
# else ever removes them, so prune before adding another pair. Never fatal.
$pruneLogsScript = Join-Path $PSScriptRoot "scripts\prune_logs.ps1"
if (Test-Path $pruneLogsScript) {
    try {
        & $pruneLogsScript -ProjectRoot $PSScriptRoot
    }
    catch {
        Write-Host ("Log prune skipped: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
    }
}

$startupStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$webStdoutPath = Join-Path $PSScriptRoot ("logs\uvicorn_web_{0}.out.log" -f $startupStamp)
$webStderrPath = Join-Path $PSScriptRoot ("logs\uvicorn_web_{0}.err.log" -f $startupStamp)

$proc = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList @("-m", "uvicorn", "web.main:app", "--host", $BindHost, "--port", "$Port") `
    -WorkingDirectory $PSScriptRoot `
    -RedirectStandardOutput $webStdoutPath `
    -RedirectStandardError $webStderrPath `
    -PassThru

Write-Host "Web stdout log: $webStdoutPath"
Write-Host "Web stderr log: $webStderrPath"

$shouldStartWorker = $StartNewsWorker
$shouldStartLlmWorker = $StartNewsLlmWorker
$shouldStartPmWorker = $StartPmWorker

if ($shouldStartWorker) {
    $workerPid = Get-WorkerPid
    if ($workerPid) {
        Write-Host "News worker already running (PID=$workerPid)."
    } else {
        $workerProc = Start-Process `
            -FilePath $pythonExe `
            -ArgumentList @("-m", "core.news.service.worker") `
            -WorkingDirectory $PSScriptRoot `
            -PassThru
        Write-Host "Started news worker PID=$($workerProc.Id)"
    }
}

if ($shouldStartLlmWorker) {
    $llmWorkerPid = Get-LlmWorkerPid
    if ($llmWorkerPid) {
        Write-Host "News LLM worker already running (PID=$llmWorkerPid)."
    } else {
        $llmProc = Start-Process `
            -FilePath $pythonExe `
            -ArgumentList @("-m", "core.news.service.llm_worker") `
            -WorkingDirectory $PSScriptRoot `
            -PassThru
        Write-Host "Started news LLM worker PID=$($llmProc.Id)"
    }
}

if ($shouldStartPmWorker) {
    $pmWorkerPid = Get-PmWorkerPid
    if ($pmWorkerPid) {
        Write-Host "Polymarket worker already running (PID=$pmWorkerPid)."
    } else {
        $pmProc = Start-Process `
            -FilePath $pythonExe `
            -ArgumentList @("-m", "prediction_markets.polymarket.worker") `
            -WorkingDirectory $PSScriptRoot `
            -PassThru
        Write-Host "Started Polymarket worker PID=$($pmProc.Id)"
    }
}

Start-Sleep -Seconds 2

$status = $null
$health = $null
$live = $null
$healthTimeoutSec = if ($EnableAnalyticsHistory) { 18 } else { 12 }
$statusTimeoutSec = if ($EnableAnalyticsHistory) { 15 } else { 8 }
$pollIntervalMs = if ($EnableAnalyticsHistory) { 1200 } else { 800 }
$healthDeadline = (Get-Date).AddSeconds([Math]::Max(3, $HealthWaitSec))
$statusDeadline = $healthDeadline.AddSeconds($(if ($EnableAnalyticsHistory) { 45 } else { 12 }))
$healthReadyAt = $null
$lastProbeError = $null
while ((Get-Date) -lt $statusDeadline) {
    if ($proc.HasExited) {
        break
    }
    try {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/readyz" -TimeoutSec $healthTimeoutSec
        if ($health) {
            if (-not $healthReadyAt) {
                $healthReadyAt = Get-Date
            }
            try {
                $statusHeaders = Get-OpsAuthHeaders
                if ($statusHeaders.Count -gt 0) {
                    $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -Headers $statusHeaders -TimeoutSec $statusTimeoutSec
                } else {
                    $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -TimeoutSec $statusTimeoutSec
                }
            }
            catch {
                $lastProbeError = $_.Exception.Message
                if ((Get-Date) -ge $healthDeadline) {
                    break
                }
                Start-Sleep -Milliseconds $pollIntervalMs
                continue
            }
            if ($status) {
                break
            }
        }
    }
    catch {
        $lastProbeError = $_.Exception.Message
        try {
            $live = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/livez" -TimeoutSec 5
        } catch {
            $live = $null
        }
        if ((Get-Date) -ge $healthDeadline) {
            break
        }
        Start-Sleep -Milliseconds $pollIntervalMs
    }
}

if ($proc.HasExited) {
    Write-Host "Managed web process exited during startup (PID=$($proc.Id))." -ForegroundColor Red
    if ($lastProbeError) {
        Write-Host ("Last probe error: {0}" -f $lastProbeError) -ForegroundColor Yellow
    }
    Write-Host "Inspect startup logs:" -ForegroundColor Yellow
    Write-Host "  stdout: $webStdoutPath" -ForegroundColor Yellow
    Write-Host "  stderr: $webStderrPath" -ForegroundColor Yellow
    if ($StartAutonomousAgent) {
        Write-Host "Autonomous agent start skipped because the web process exited early." -ForegroundColor Yellow
    }
    exit 1
} elseif ($health -and $status) {
    $runtimeStatus = if ($status) { $status.status } else { $health.status }
    $tradingMode = if ($status -and $status.trading_mode) { $status.trading_mode } else { "unknown" }
    Write-Host "Started PID=$($proc.Id), status=$runtimeStatus, mode=$tradingMode, profile=$startupProfile, url=http://127.0.0.1:$Port"
    if ($StartAutonomousAgent) {
        $agentResponse = Start-AutonomousAgent -WebPort $Port
        Show-AutonomousAgentStartSummary -Response $agentResponse
    }
    if ($OpenBrowser) {
        Open-WebConsole -WebPort $Port
    }
} elseif ($health) {
    $warmNote = if ($EnableAnalyticsHistory) {
        " analytics-history warm-up can take longer than the default status probe window."
    } else {
        ""
    }
    Write-Host "Process started (PID=$($proc.Id)) and /health is responding, but /api/status is still warming up. Startup profile: $startupProfile.$warmNote" -ForegroundColor Yellow
    if ($StartAutonomousAgent) {
        Write-Host "Autonomous agent start skipped because the full runtime status endpoint was not ready yet." -ForegroundColor Yellow
    }
} elseif ($live -and -not $proc.HasExited) {
    Write-Host "Process started (PID=$($proc.Id)) and /livez is responding, but /readyz is still degraded (usually an exchange or market-data dependency). Keeping the service up; inspect status and logs." -ForegroundColor Yellow
    if ($lastProbeError) {
        Write-Host ("Last readiness probe: {0}" -f $lastProbeError) -ForegroundColor Yellow
    }
    if ($StartAutonomousAgent) {
        Write-Host "Autonomous agent start skipped because readiness was not complete." -ForegroundColor Yellow
    }
} else {
    Write-Host "Process started (PID=$($proc.Id)) but readiness is still warming up. Startup profile: $startupProfile." -ForegroundColor Yellow
    if ($lastProbeError) {
        Write-Host ("Last probe error: {0}" -f $lastProbeError) -ForegroundColor Yellow
    }
    Write-Host "Stopping unhealthy web process because /readyz never became ready inside the startup window." -ForegroundColor Red
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    Write-Host "Inspect startup logs:" -ForegroundColor Yellow
    Write-Host "  stdout: $webStdoutPath" -ForegroundColor Yellow
    Write-Host "  stderr: $webStderrPath" -ForegroundColor Yellow
    if ($StartAutonomousAgent) {
        Write-Host "Autonomous agent start skipped because the web health endpoint was not ready yet." -ForegroundColor Yellow
    }
    exit 1
}

# 测试数据源 (可选)
Start-WebSupervisor -PythonExecutable $pythonExe

$shouldTestDataSources = $TestDataSources
if (-not $shouldTestDataSources) {
    $rawTestToggle = [string]($env:TEST_DATA_SOURCES)
    if ($rawTestToggle) {
        $shouldTestDataSources = $rawTestToggle.Trim().ToLower() -in @("1", "true", "yes", "on")
    }
}

if ($shouldTestDataSources) {
    Write-Host ""
    Write-Host "Testing data sources..." -ForegroundColor Cyan
    $testProc = Start-Process `
        -FilePath $pythonExe `
        -ArgumentList @("scripts/test_api_direct.py") `
        -WorkingDirectory $PSScriptRoot `
        -NoNewWindow `
        -Wait
    Write-Host "Data source test complete."
}
