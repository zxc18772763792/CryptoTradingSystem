param(
    [ValidateSet("gate", "precheck", "start-service", "start-selfcheck", "status", "stop")]
    [string]$Action = "status",
    # Real-money guard: every start action refuses unless this switch is present.
    [switch]$ConfirmLive,
    # Hard Level-2 -> Level-3 gate: path to the COMPLETED 24h live-shadow selfcheck
    # JSON and its service stderr log. start-service refuses unless the evaluator
    # passes on these artifacts. There is no skip switch by design.
    [string]$EvaluatedReport = "",
    [string]$ServiceErrLog = "",
    # Disable noisy background workers for a controlled observation run.
    # Default OFF = production-like service (all configured workers enabled).
    [switch]$CleanEvidence,
    # Fallback drill (s28.5 Level-4 prerequisite): start the IDENTICAL Level-3
    # service but with MARKET_WS_FORCE_REST=true, proving the first rollback
    # switch restores REST authority. All gates (-ConfirmLive + Level-2
    # evaluator) still apply. Restore by re-running start-service without it.
    [switch]$DrillForceRest,
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000,
    [string]$Token = "",
    [string]$Stamp = "",
    [int]$DurationSec = 21600,
    [int]$IntervalSec = 60,
    [int]$MinSamples = 355,
    [int]$HealthWaitSec = 120,
    [double]$MaxWsAgeMs = 10000,
    [string]$PythonExe = "C:\Users\lenovo\.conda\envs\crypto_trading\python.exe"
)

# Level-3 "UI primary" launcher: runs the trading system in LIVE mode with the
# exchange market-WS feed as the PRIMARY source for UI market ticks. REST stays
# the authority for strategy/order/valuation paths (fail-closed adapter), and
# the WS quality guard is force-enabled so persistent feed degradation
# auto-falls back to REST without a human in the loop.
#
# Gating (no skip path):
#   * every start action requires -ConfirmLive (real money),
#   * start-service re-runs the Level-2 evaluator on the completed 24h
#     live-shadow report (-EvaluatedReport/-ServiceErrLog) and aborts on FAIL,
#   * the observation selfcheck pins --expect-mode ui_primary --expect-runtime live.
# Rollback: set MARKET_WS_FORCE_REST=true (or restart via the Level-2 launcher
# in shadow mode); both restore REST authority without code changes.

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$logRoot = Join-Path $projectRoot "logs"
if (-not (Test-Path $logRoot)) {
    New-Item -ItemType Directory -Path $logRoot | Out-Null
}
if ([string]::IsNullOrWhiteSpace($Stamp)) {
    $Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
}

function Require-Token {
    if ([string]::IsNullOrWhiteSpace($Token)) {
        throw "Token is required for $Action. Pass -Token and use X-OPS-TOKEN for probes."
    }
}

function Require-LiveConfirm {
    if (-not $ConfirmLive) {
        throw ("REFUSING: '$Action' starts LIVE trading mode (real money) with WS as UI-primary. " +
               "Re-run with -ConfirmLive only after Level 2 (24h live shadow) has passed evaluation.")
    }
    Write-Host "!!! LIVE MODE CONFIRMED (-ConfirmLive). Real orders are possible. !!!" -ForegroundColor Red
}

function ConvertTo-CmdLiteral {
    param([string]$Value)
    return '"' + ($Value -replace '"', '\"') + '"'
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

function Test-IsMarketWebProcess {
    param($ProcessRecord)
    if (-not $ProcessRecord) {
        return $false
    }
    $cmd = [string]$ProcessRecord.CommandLine
    return $cmd -like "*uvicorn*web.main:app*"
}

function Get-SelfcheckProcesses {
    $portText = "--base-url http://127.0.0.1:$Port"
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                (
                    $name -and
                    $name.ToLowerInvariant() -in @("python.exe", "pythonw.exe", "cmd.exe") -and
                    $cmd -and
                    $cmd.Contains("scripts\selfcheck_market_ws_shadow.py") -and
                    $cmd.Contains($portText)
                )
            }
    )
}

function New-LaunchCmdFile {
    param(
        [string]$Kind,
        [string[]]$Lines
    )
    $cmdPath = Join-Path $logRoot ("ui_primary_{0}_{1}.cmd" -f $Kind, $Stamp)
    $content = @("@echo off")
    $content += $Lines
    $content | Set-Content -Path $cmdPath -Encoding ASCII
    return $cmdPath
}

function New-InstrumentedCmdCommand {
    param(
        [string]$Kind,
        [string]$Command,
        [string]$MarkerLog
    )
    $quotedMarkerLog = ConvertTo-CmdLiteral $MarkerLog
    return @(
        "echo MARKET_WS_UI_PRIMARY_${Kind}_START %DATE% %TIME% >> $quotedMarkerLog",
        $Command,
        "set `"MARKET_WS_UI_PRIMARY_EXIT_CODE=%ERRORLEVEL%`"",
        "echo MARKET_WS_UI_PRIMARY_${Kind}_EXIT %MARKET_WS_UI_PRIMARY_EXIT_CODE% %DATE% %TIME% >> $quotedMarkerLog",
        "exit /b %MARKET_WS_UI_PRIMARY_EXIT_CODE%"
    )
}

function Get-TaskName {
    param([string]$Kind)
    return "CryptoMarketWsUiPrimary_{0}_{1}" -f $Kind, $Stamp
}

function Start-LaunchTask {
    param(
        [string]$Kind,
        [string]$CmdPath
    )
    $taskName = Get-TaskName -Kind $Kind
    $cmdExe = Join-Path $env:SystemRoot "System32\cmd.exe"
    $action = New-ScheduledTaskAction `
        -Execute $cmdExe `
        -Argument ("/c " + (ConvertTo-CmdLiteral $CmdPath)) `
        -WorkingDirectory $projectRoot
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit (New-TimeSpan -Hours 26) `
        -MultipleInstances IgnoreNew
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Settings $settings `
        -Description "Controlled market WS UI-primary $Kind" `
        -Force | Out-Null
    Start-ScheduledTask -TaskName $taskName
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if (-not $task) {
        throw "Failed to register scheduled task: $taskName"
    }
    return $taskName
}

function Get-MarketWsTasks {
    return @(
        Get-ScheduledTask -ErrorAction SilentlyContinue |
            Where-Object { [string]$_.TaskName -like "CryptoMarketWsUiPrimary_*" }
    )
}

function Write-LaunchMetadata {
    param(
        [string]$Kind,
        [string]$TaskName,
        [hashtable]$Paths
    )
    $metaPath = Join-Path $logRoot ("ui_primary_{0}_{1}.launch.json" -f $Kind, $Stamp)
    $metadata = [ordered]@{
        kind = $Kind
        stamp = $Stamp
        started_at = (Get-Date).ToString("o")
        project_root = $projectRoot
        bind_host = $BindHost
        port = $Port
        task_name = $TaskName
        auth_header = "X-OPS-TOKEN"
        trading_mode = "live"
        market_ws_mode = "ui_primary"
        market_ws_exchanges = "binance"
        quality_guard_enabled = $true
        clean_evidence = [bool]$CleanEvidence
        level2_report = $EvaluatedReport
        paths = $Paths
    }
    $metadata | ConvertTo-Json -Depth 4 | Set-Content -Path $metaPath -Encoding UTF8
    return $metaPath
}

function Get-CommonEnvCommands {
    Require-Token
    $commands = @(
        'set "TRADING_MODE=live"',
        'set "ALLOW_PERSISTED_LIVE_MODE_START=true"',
        "set `"OPS_TOKEN=$Token`"",
        'set "MARKET_WS_ENABLED=true"',
        'set "MARKET_WS_MODE=ui_primary"',
        'set "MARKET_WS_FORCE_REST=false"',
        'set "MARKET_WS_FAIL_CLOSED_FOR_LIVE=true"',
        'set "MARKET_WS_QUALITY_GUARD_ENABLED=true"',
        'set "MARKET_WS_EXCHANGES=binance"',
        'set "MARKET_WS_SYMBOL_LIMIT=16"',
        'set "MARKET_WS_REST_RECONCILE_SEC=30"',
        'set "MARKET_WS_WATCH_TIMEOUT_SEC=25"',
        'set "MARKET_WS_MAX_PRICE_DIFF_BPS=20"',
        'set "MARKET_WS_SYMBOL_MAX_AGE_SEC=10"',
        'set "MARKET_WS_MARK_PRICE_ENABLED=false"'
    )
    if ($DrillForceRest) {
        # Same pinned env, kill switch ON: REST becomes authoritative and the
        # WS stream stays down for the duration of the drill.
        $commands = $commands -replace '^set "MARKET_WS_FORCE_REST=false"$', 'set "MARKET_WS_FORCE_REST=true"'
    }
    if ($CleanEvidence) {
        $commands += @(
            'set "COINGLASS_WORKER_ENABLED=false"',
            'set "NEWS_BACKGROUND_ENABLED=false"',
            'set "NEWS_LLM_BACKGROUND_ENABLED=false"',
            'set "DATA_MAINTENANCE_ENABLED=false"',
            'set "PUBLIC_MACRO_WORKERS_ENABLED=false"',
            'set "PREMIUM_EXTERNAL_WORKERS_ENABLED=false"',
            'set "ANALYTICS_HISTORY_ENABLED=false"'
        )
    }
    return $commands
}

function Invoke-Level2Gate {
    # Re-evaluate the completed Level-2 24h live-shadow artifacts. This is the
    # only path into start-service; failing or missing artifacts block Level 3.
    if ([string]::IsNullOrWhiteSpace($EvaluatedReport)) {
        throw "Level-2 gate requires -EvaluatedReport <path to completed 24h live-shadow selfcheck JSON>."
    }
    if (-not (Test-Path $EvaluatedReport)) {
        throw "Level-2 report not found: $EvaluatedReport"
    }
    $evaluator = Join-Path $projectRoot "scripts\evaluate_market_ws_shadow_report.py"
    if (-not (Test-Path $evaluator)) {
        throw "Evaluator script not found: $evaluator"
    }
    $evalArgs = @(
        $evaluator,
        "--report", $EvaluatedReport,
        "--expect-mode", "shadow",
        "--expect-runtime", "live",
        "--min-samples", "1430",
        "--min-ws-tick-delta", "1",
        "--min-shadow-compare-delta", "1",
        "--max-shadow-violation-delta", "30",
        "--tolerate-transient",
        "--max-degraded-samples", "24",
        "--max-consecutive-degraded", "2",
        "--max-degraded-oldest-age-ms", "60000",
        "--max-invalid-payload-delta", "0",
        "--max-timestamp-regression-delta", "0",
        "--max-shadow-stale-skip-delta", "0",
        "--max-feed-watch-empty-delta", "0",
        "--max-stale-symbol-count", "0",
        "--max-price-diff-bps", "20",
        "--max-ws-age-p95-ms", "$MaxWsAgeMs",
        "--require-final-runtime-fields"
    )
    if (-not [string]::IsNullOrWhiteSpace($ServiceErrLog)) {
        if (-not (Test-Path $ServiceErrLog)) {
            throw "Service stderr log not found: $ServiceErrLog"
        }
        $evalArgs += @("--service-err-log", $ServiceErrLog, "--max-log-count", "0")
    } else {
        Write-Host "WARNING: no -ServiceErrLog given; log-pollution gates are NOT being checked." -ForegroundColor Yellow
    }
    Write-Host "Running Level-2 -> Level-3 evaluator gate on $EvaluatedReport ..."
    & $PythonExe @evalArgs
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        Write-Host "Level-2 gate PASS" -ForegroundColor Green
        return $true
    }
    Write-Host ("Level-2 gate FAIL (exit {0})" -f $code) -ForegroundColor Red
    return $false
}

function Invoke-UiPrimaryPrecheck {
    Require-Token
    # Reuse the read-only live precheck for runtime/fail-closed/feed health; it
    # requires mode=shadow, so it is only meaningful BEFORE the ui_primary
    # restart (on the still-running shadow service).
    $precheckScript = Join-Path $projectRoot "scripts\precheck_market_ws_live_shadow.py"
    if (-not (Test-Path $precheckScript)) {
        throw "Precheck script not found: $precheckScript"
    }
    Write-Host "Running read-only live precheck (expects current service still in shadow)..."
    & $PythonExe $precheckScript --base-url "http://127.0.0.1:$Port" --token $Token --max-ws-age-ms $MaxWsAgeMs
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        Write-Host "Precheck PASS" -ForegroundColor Green
        return $true
    }
    Write-Host ("Precheck FAIL (exit {0})" -f $code) -ForegroundColor Red
    return $false
}

function Start-MarketWsService {
    Require-Token
    Require-LiveConfirm
    if ($DrillForceRest) {
        Write-Host ("!!! FALLBACK DRILL: starting with MARKET_WS_FORCE_REST=true " +
            "(REST authority, WS stream disabled). Restore with a normal start-service. !!!") -ForegroundColor Yellow
    }
    if (-not (Invoke-Level2Gate)) {
        throw "Refusing to start UI-primary service: Level-2 evaluator gate FAILED."
    }
    if (-not (Test-Path $PythonExe)) {
        throw "Python executable not found: $PythonExe"
    }
    $pidOnPort = Get-ListeningPid -PortNumber $Port
    if ($pidOnPort) {
        throw ("Port {0} is already in use by PID {1}. Stop the previous (shadow) service first " +
               "so the ui_primary restart is explicit.") -f $Port, $pidOnPort
    }
    $serviceOut = Join-Path $logRoot ("ui_primary_service_{0}.out.log" -f $Stamp)
    $serviceErr = Join-Path $logRoot ("ui_primary_service_{0}.err.log" -f $Stamp)
    $commands = @("cd /d $(ConvertTo-CmdLiteral $projectRoot)")
    $commands += Get-CommonEnvCommands
    $commands += New-InstrumentedCmdCommand `
        -Kind "SERVICE" `
        -Command "$(ConvertTo-CmdLiteral $PythonExe) -m uvicorn web.main:app --host $BindHost --port $Port >> $(ConvertTo-CmdLiteral $serviceOut) 2>> $(ConvertTo-CmdLiteral $serviceErr)" `
        -MarkerLog $serviceErr
    $cmdPath = New-LaunchCmdFile -Kind "service" -Lines $commands
    $taskName = Start-LaunchTask -Kind "service" -CmdPath $cmdPath
    $metadataPath = Write-LaunchMetadata -Kind "service" -TaskName $taskName -Paths @{
        launch_cmd = $cmdPath
        stdout = $serviceOut
        stderr = $serviceErr
    }
    Write-Host ("Started LIVE UI-primary service task: {0}" -f $taskName)
    Write-Host ("Service launch cmd: {0}" -f $cmdPath)
    Write-Host ("Service stderr: {0}" -f $serviceErr)
    Write-Host ("Launch metadata: {0}" -f $metadataPath)

    $deadline = (Get-Date).AddSeconds([Math]::Max(3, $HealthWaitSec))
    $headers = @{
        "X-OPS-TOKEN" = $Token
        "X-OPS-CALLER" = "market_ws_ui_primary_launcher"
    }
    $lastError = $null
    while ((Get-Date) -lt $deadline) {
        $pidOnPort = Get-ListeningPid -PortNumber $Port
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 5
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -Headers $headers -TimeoutSec 8
            $market = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/market-data/status" -Headers $headers -TimeoutSec 8
            if ($health -and $status -and $market) {
                if ([string]$market.mode -ne "ui_primary") {
                    throw "Service came up with market_ws mode '$($market.mode)', expected 'ui_primary'."
                }
                if ([bool]$market.force_rest -ne [bool]$DrillForceRest) {
                    throw "Service came up with force_rest=$($market.force_rest), expected $([bool]$DrillForceRest)."
                }
                Write-Host ("READY pid={0} status={1} trading_mode={2} market_ws_mode={3} quality_guard={4} force_rest={5}" -f `
                    $pidOnPort, $status.status, $status.trading_mode, $market.mode, $market.quality_guard.enabled, $market.force_rest)
                return
            }
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 1000
    }
    if ($lastError) {
        Write-Host ("READY_ERR: {0}" -f $lastError) -ForegroundColor Yellow
    }
    throw "Service did not become ready within $HealthWaitSec seconds."
}

function Start-MarketWsSelfcheck {
    Require-Token
    Require-LiveConfirm
    if (-not (Test-Path $PythonExe)) {
        throw "Python executable not found: $PythonExe"
    }
    $pidOnPort = Get-ListeningPid -PortNumber $Port
    if (-not $pidOnPort) {
        throw "No service is listening on port $Port. Start the ui_primary service first."
    }

    $selfcheckOut = Join-Path $logRoot ("ui_primary_selfcheck_{0}.out.json" -f $Stamp)
    $selfcheckErr = Join-Path $logRoot ("ui_primary_selfcheck_{0}.err.log" -f $Stamp)
    $commands = @("cd /d $(ConvertTo-CmdLiteral $projectRoot)")
    $commands += Get-CommonEnvCommands
    # NOTE: --min-shadow-compare-delta 0 — in ui_primary the periodic shadow
    # REST reconcile is suppressed while WS is healthy, so compare counters only
    # move during REST fallback windows; requiring growth would punish health.
    $selfcheckCommand = @(
        "$(ConvertTo-CmdLiteral $PythonExe) scripts\selfcheck_market_ws_shadow.py",
        "--base-url http://127.0.0.1:$Port",
        "--token $Token",
        "--expect-mode ui_primary",
        "--expect-runtime live",
        "--timeout 20",
        "--duration-sec $DurationSec",
        "--interval-sec $IntervalSec",
        "--min-samples $MinSamples",
        "--min-ws-tick-delta 1",
        "--min-shadow-compare-delta 0",
        "--max-shadow-violation-delta 30",
        "--tolerate-transient",
        "--max-degraded-samples 24",
        "--max-consecutive-degraded 2",
        "--max-degraded-oldest-age-ms 60000",
        "--max-invalid-payload-delta 0",
        "--max-timestamp-regression-delta 0",
        "--max-shadow-stale-skip-delta 0",
        "--max-feed-watch-empty-delta 0",
        "--max-stale-symbol-count 0",
        "--max-price-diff-bps 20",
        "--max-ws-age-p95-ms $MaxWsAgeMs",
        ">> $(ConvertTo-CmdLiteral $selfcheckOut) 2>> $(ConvertTo-CmdLiteral $selfcheckErr)"
    ) -join " "
    $commands += New-InstrumentedCmdCommand `
        -Kind "SELFCHECK" `
        -Command $selfcheckCommand `
        -MarkerLog $selfcheckErr
    $cmdPath = New-LaunchCmdFile -Kind "selfcheck" -Lines $commands
    $taskName = Start-LaunchTask -Kind "selfcheck" -CmdPath $cmdPath
    $metadataPath = Write-LaunchMetadata -Kind "selfcheck" -TaskName $taskName -Paths @{
        launch_cmd = $cmdPath
        stdout_json = $selfcheckOut
        stderr = $selfcheckErr
    }
    Write-Host ("Started LIVE ui_primary observation selfcheck task: {0}" -f $taskName)
    Write-Host ("Selfcheck JSON: {0}" -f $selfcheckOut)
    Write-Host ("Selfcheck stderr: {0}" -f $selfcheckErr)
    Write-Host ("Launch metadata: {0}" -f $metadataPath)
}

function Show-Status {
    $pidOnPort = Get-ListeningPid -PortNumber $Port
    if ($pidOnPort) {
        Write-Host ("Port {0} listening PID={1}" -f $Port, $pidOnPort)
    } else {
        Write-Host ("Port {0} is not listening." -f $Port)
    }
    $selfchecks = @(Get-SelfcheckProcesses)
    if ($selfchecks.Count) {
        Write-Host ("Selfcheck processes: {0}" -f (($selfchecks | Select-Object -ExpandProperty ProcessId) -join ", "))
    } else {
        Write-Host "Selfcheck processes: none"
    }
    $tasks = @(Get-MarketWsTasks)
    if ($tasks.Count) {
        foreach ($task in $tasks) {
            Write-Host ("Task {0}: {1}" -f $task.TaskName, $task.State)
        }
    } else {
        Write-Host "Scheduled tasks: none"
    }
    if (-not [string]::IsNullOrWhiteSpace($Token) -and $pidOnPort) {
        $headers = @{
            "X-OPS-TOKEN" = $Token
            "X-OPS-CALLER" = "market_ws_ui_primary_status"
        }
        try {
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -Headers $headers -TimeoutSec 8
            $market = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/market-data/status" -Headers $headers -TimeoutSec 8
            Write-Host ("Runtime status={0} trading_mode={1} paper_trading={2}" -f $status.status, $status.trading_mode, $status.paper_trading)
            Write-Host ("Market WS mode={0} feed_healthy={1} ws_hub_healthy={2} fail_closed_for_live={3}" -f $market.mode, $market.feed_healthy, $market.ws_hub_healthy, $market.fail_closed_for_live)
            Write-Host ("Quality guard enabled={0} state={1} degrade_count={2}" -f $market.quality_guard.enabled, $market.quality_guard.state, $market.quality_guard.degrade_count)
            Write-Host ("WS trust={0} rest_fallback_count={1} ws_stale_symbol_count={2}" -f $market.ws_trusted, $market.rest_fallback_count, $market.ws_stale_symbol_count)
        } catch {
            Write-Host ("Status probe failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
        }
    }
}

function Stop-MarketWsUiPrimary {
    $stopped = $false
    foreach ($proc in @(Get-SelfcheckProcesses)) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host ("Stopped selfcheck PID={0}" -f $proc.ProcessId)
        $stopped = $true
    }
    foreach ($task in @(Get-MarketWsTasks)) {
        if ($task.State -eq "Running") {
            Stop-ScheduledTask -TaskName $task.TaskName -ErrorAction SilentlyContinue
            Write-Host ("Stopped scheduled task {0}" -f $task.TaskName)
        }
        Unregister-ScheduledTask -TaskName $task.TaskName -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host ("Unregistered scheduled task {0}" -f $task.TaskName)
        $stopped = $true
    }
    $pidOnPort = Get-ListeningPid -PortNumber $Port
    if ($pidOnPort) {
        $record = Get-ProcessRecord -ProcessId $pidOnPort
        if (-not (Test-IsMarketWebProcess -ProcessRecord $record)) {
            throw "Port $Port is occupied by PID $pidOnPort, but it is not a managed web.main uvicorn process."
        }
        Stop-Process -Id $pidOnPort -Force -ErrorAction SilentlyContinue
        Write-Host ("Stopped service PID={0}" -f $pidOnPort)
        $stopped = $true
    }
    if (-not $stopped) {
        Write-Host "Nothing needed stopping."
    }
}

switch ($Action) {
    "gate" { if (-not (Invoke-Level2Gate)) { exit 1 } }
    "precheck" { [void](Invoke-UiPrimaryPrecheck) }
    "start-service" { Start-MarketWsService }
    "start-selfcheck" { Start-MarketWsSelfcheck }
    "status" { Show-Status }
    "stop" { Stop-MarketWsUiPrimary }
}
