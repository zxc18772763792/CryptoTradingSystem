param(
    [ValidateSet("start-service", "start-selfcheck", "status", "stop")]
    [string]$Action = "status",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8012,
    [string]$Token = "",
    [string]$Stamp = "",
    [int]$DurationSec = 21600,
    [int]$IntervalSec = 60,
    [int]$HealthWaitSec = 120,
    [string]$PythonExe = "F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe"
)

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

    $cmdPath = Join-Path $logRoot ("paper_shadow_6h_{0}_{1}.cmd" -f $Kind, $Stamp)
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
        "echo MARKET_WS_SHADOW_${Kind}_START %DATE% %TIME% >> $quotedMarkerLog",
        $Command,
        "set `"MARKET_WS_SHADOW_EXIT_CODE=%ERRORLEVEL%`"",
        "echo MARKET_WS_SHADOW_${Kind}_EXIT %MARKET_WS_SHADOW_EXIT_CODE% %DATE% %TIME% >> $quotedMarkerLog",
        "exit /b %MARKET_WS_SHADOW_EXIT_CODE%"
    )
}

function Get-TaskName {
    param([string]$Kind)
    return "CryptoMarketWsPaperShadow_{0}_{1}" -f $Kind, $Stamp
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
        -ExecutionTimeLimit (New-TimeSpan -Hours 12) `
        -MultipleInstances IgnoreNew

    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Settings $settings `
        -Description "Codex controlled market WS paper shadow $Kind" `
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
            Where-Object { [string]$_.TaskName -like "CryptoMarketWsPaperShadow_*" }
    )
}

function Write-LaunchMetadata {
    param(
        [string]$Kind,
        [string]$TaskName,
        [hashtable]$Paths
    )

    $metaPath = Join-Path $logRoot ("paper_shadow_6h_{0}_{1}.launch.json" -f $Kind, $Stamp)
    $metadata = [ordered]@{
        kind = $Kind
        stamp = $Stamp
        started_at = (Get-Date).ToString("o")
        project_root = $projectRoot
        bind_host = $BindHost
        port = $Port
        task_name = $TaskName
        auth_header = "X-OPS-TOKEN"
        trading_mode = "paper"
        market_ws_mode = "shadow"
        market_ws_exchanges = "binance"
        paths = $Paths
    }
    $metadata | ConvertTo-Json -Depth 4 | Set-Content -Path $metaPath -Encoding UTF8
    return $metaPath
}

function Get-CommonEnvCommands {
    Require-Token
    return @(
        'set "TRADING_MODE=paper"',
        'set "ALLOW_PERSISTED_LIVE_MODE_START=false"',
        "set `"OPS_TOKEN=$Token`"",
        'set "MARKET_WS_ENABLED=true"',
        'set "MARKET_WS_MODE=shadow"',
        'set "MARKET_WS_FORCE_REST=false"',
        'set "MARKET_WS_EXCHANGES=binance"',
        'set "MARKET_WS_SYMBOL_LIMIT=16"',
        'set "MARKET_WS_REST_RECONCILE_SEC=30"',
        'set "MARKET_WS_WATCH_TIMEOUT_SEC=25"',
        'set "MARKET_WS_MAX_PRICE_DIFF_BPS=20"',
        'set "MARKET_WS_SYMBOL_MAX_AGE_SEC=10"',
        'set "MARKET_WS_MARK_PRICE_ENABLED=false"',
        'set "EXCHANGE_WATCHDOG_ENABLED=false"',
        'set "COINGLASS_WORKER_ENABLED=false"',
        'set "NEWS_BACKGROUND_ENABLED=false"',
        'set "NEWS_LLM_BACKGROUND_ENABLED=false"',
        'set "DATA_MAINTENANCE_ENABLED=false"',
        'set "PUBLIC_MACRO_WORKERS_ENABLED=false"',
        'set "PREMIUM_EXTERNAL_WORKERS_ENABLED=false"',
        'set "ANALYTICS_HISTORY_ENABLED=false"'
    )
}

function Start-MarketWsService {
    Require-Token
    if (-not (Test-Path $PythonExe)) {
        throw "Python executable not found: $PythonExe"
    }

    $pidOnPort = Get-ListeningPid -PortNumber $Port
    if ($pidOnPort) {
        $existing = Get-ProcessRecord -ProcessId $pidOnPort
        if (Test-IsMarketWebProcess -ProcessRecord $existing) {
            Write-Host ("Service already listening on {0}:{1} (PID={2})." -f $BindHost, $Port, $pidOnPort)
            return
        }
        throw "Port $Port is already in use by PID $pidOnPort."
    }

    $serviceOut = Join-Path $logRoot ("paper_shadow_6h_service_{0}.out.log" -f $Stamp)
    $serviceErr = Join-Path $logRoot ("paper_shadow_6h_service_{0}.err.log" -f $Stamp)
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
    Write-Host ("Started service task: {0}" -f $taskName)
    Write-Host ("Service launch cmd: {0}" -f $cmdPath)
    Write-Host ("Service stdout: {0}" -f $serviceOut)
    Write-Host ("Service stderr: {0}" -f $serviceErr)
    Write-Host ("Launch metadata: {0}" -f $metadataPath)

    $deadline = (Get-Date).AddSeconds([Math]::Max(3, $HealthWaitSec))
    $headers = @{
        "X-OPS-TOKEN" = $Token
        "X-OPS-CALLER" = "market_ws_shadow_launcher"
    }
    $lastError = $null
    while ((Get-Date) -lt $deadline) {
        $pidOnPort = Get-ListeningPid -PortNumber $Port
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 5
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -Headers $headers -TimeoutSec 8
            $market = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/market-data/status" -Headers $headers -TimeoutSec 8
            if ($health -and $status -and $market) {
                Write-Host ("READY pid={0} status={1} mode={2} market_ws_mode={3} configured_enabled={4}" -f $pidOnPort, $status.status, $status.trading_mode, $market.mode, $market.configured_enabled)
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
    if (-not (Test-Path $PythonExe)) {
        throw "Python executable not found: $PythonExe"
    }

    $pidOnPort = Get-ListeningPid -PortNumber $Port
    if (-not $pidOnPort) {
        throw "No service is listening on port $Port."
    }

    $selfcheckOut = Join-Path $logRoot ("paper_shadow_6h_selfcheck_{0}.out.json" -f $Stamp)
    $selfcheckErr = Join-Path $logRoot ("paper_shadow_6h_selfcheck_{0}.err.log" -f $Stamp)
    $commands = @("cd /d $(ConvertTo-CmdLiteral $projectRoot)")
    $commands += Get-CommonEnvCommands
    $selfcheckCommand = @(
        "$(ConvertTo-CmdLiteral $PythonExe) scripts\selfcheck_market_ws_shadow.py",
        "--base-url http://127.0.0.1:$Port",
        "--token $Token",
        "--duration-sec $DurationSec",
        "--interval-sec $IntervalSec",
        "--min-samples 361",
        "--min-ws-tick-delta 1",
        "--min-shadow-compare-delta 1",
        "--max-shadow-violation-delta 0",
        "--max-invalid-payload-delta 0",
        "--max-timestamp-regression-delta 0",
        "--max-shadow-stale-skip-delta 0",
        "--max-feed-watch-empty-delta 0",
        "--max-stale-symbol-count 0",
        "--max-price-diff-bps 20",
        "--max-ws-age-p95-ms 10000",
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
    Write-Host ("Started selfcheck task: {0}" -f $taskName)
    Write-Host ("Selfcheck launch cmd: {0}" -f $cmdPath)
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
            "X-OPS-CALLER" = "market_ws_shadow_status"
        }
        try {
            $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -Headers $headers -TimeoutSec 8
            $market = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/market-data/status" -Headers $headers -TimeoutSec 8
            Write-Host ("Runtime status={0} trading_mode={1} paper_trading={2}" -f $status.status, $status.trading_mode, $status.paper_trading)
            Write-Host ("Market WS mode={0} configured_enabled={1} feed_healthy={2} ws_hub_healthy={3} ws_tick_count={4} shadow_compare_count={5}" -f $market.mode, $market.configured_enabled, $market.feed_healthy, $market.ws_hub_healthy, $market.ws_tick_count, $market.shadow_compare_count)
        } catch {
            Write-Host ("Status probe failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
        }
    }
}

function Stop-MarketWsShadow {
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
    "start-service" { Start-MarketWsService }
    "start-selfcheck" { Start-MarketWsSelfcheck }
    "status" { Show-Status }
    "stop" { Stop-MarketWsShadow }
}
