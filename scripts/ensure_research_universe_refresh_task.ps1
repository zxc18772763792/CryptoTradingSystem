param(
    [string]$TaskName = "CryptoTradingSystem_ResearchUniverseRefresh",
    [string]$EnvName = "crypto_trading",
    [string]$Exchange = "binance",
    [string]$Timeframes = "1m,5m,15m",
    [int]$Days = 90,
    [int]$OverlapBars = 48,
    [string]$SecondsSymbols = "BTC/USDT,ETH/USDT",
    [int]$SecondsDays = 1,
    [switch]$DisableIdleSeconds,
    [int]$IntervalMinutes = 15,
    [switch]$StartNow,
    [switch]$StartNowIfCreated,
    [switch]$Quiet
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $PSScriptRoot "run_research_universe_refresh.ps1"
$fallbackBatch = Join-Path $projectRoot "refresh_research_universe.bat"
$powershellExe = (Get-Command powershell -ErrorAction Stop).Source
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

if (-not (Test-Path $runnerScript)) {
    throw "Runner script not found: $runnerScript"
}
if (-not (Test-Path $fallbackBatch)) {
    throw "Fallback batch file not found: $fallbackBatch"
}

function Get-NextAlignedTriggerTime {
    param([int]$Minutes)
    $step = [Math]::Max(5, [int]$Minutes)
    $now = Get-Date
    $base = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day -Hour $now.Hour -Minute 0 -Second 0
    $offset = [Math]::Ceiling(($now - $base).TotalMinutes / $step) * $step
    $candidate = $base.AddMinutes($offset)
    if ($candidate -le $now) {
        $candidate = $candidate.AddMinutes($step)
    }
    return $candidate
}

$logPath = Join-Path $projectRoot "logs\research_universe_refresh.log"
$actionArgs = @(
    "-NoProfile",
    "-WindowStyle", "Hidden",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$runnerScript`"",
    "-EnvName", "`"$EnvName`"",
    "-Exchange", "`"$Exchange`"",
    "-Timeframes", "`"$Timeframes`"",
    "-Days", "$Days",
    "-OverlapBars", "$OverlapBars",
    "-SecondsSymbols", "`"$SecondsSymbols`"",
    "-SecondsDays", "$SecondsDays",
    "-LogPath", "`"$logPath`"",
    "-Quiet"
) -join " "
if ($DisableIdleSeconds) {
    $actionArgs += " -DisableIdleSeconds"
}

$action = New-ScheduledTaskAction -Execute $powershellExe -Argument $actionArgs -WorkingDirectory $projectRoot
$startupTrigger = New-ScheduledTaskTrigger -AtStartup
$repeatTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-NextAlignedTriggerTime -Minutes $IntervalMinutes) `
    -RepetitionInterval (New-TimeSpan -Minutes ([Math]::Max(5, $IntervalMinutes))) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$created = $null -eq $existingTask

$taskRegistered = $false
try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger @($startupTrigger, $repeatTrigger) `
        -Settings $settings `
        -Principal $principal `
        -Description "Incrementally refresh the default 30-symbol research universe without loading the web process." `
        -Force | Out-Null
    Enable-ScheduledTask -TaskName $TaskName | Out-Null
    $taskRegistered = $true
} catch {
    $fallbackCommand = "`"$fallbackBatch`" -Quiet"
    $null = & schtasks /Create /F /TN $TaskName /SC MINUTE /MO ([Math]::Max(5, $IntervalMinutes)) /TR $fallbackCommand /RL LIMITED
    if ($LASTEXITCODE -ne 0) {
        throw
    }
    $taskRegistered = $true
}

$startedNow = $false
if ($taskRegistered -and ($StartNow.IsPresent -or ($created -and $StartNowIfCreated.IsPresent))) {
    try {
        try {
            Start-ScheduledTask -TaskName $TaskName
        } catch {
            $null = & schtasks /Run /TN $TaskName
            if ($LASTEXITCODE -ne 0) {
                throw
            }
        }
        $startedNow = $true
    } catch {
        if (-not $Quiet) {
            Write-Host "Scheduled task start skipped: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$taskInfo = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
$result = [pscustomobject]@{
    task_name = $TaskName
    created = $created
    started_now = $startedNow
    state = [string]$task.State
    interval_minutes = [Math]::Max(5, $IntervalMinutes)
    env_name = $EnvName
    exchange = $Exchange
    timeframes = $Timeframes
    days = $Days
    overlap_bars = $OverlapBars
    seconds_symbols = $SecondsSymbols
    seconds_days = $SecondsDays
    next_run_time = if ($taskInfo) { $taskInfo.NextRunTime } else { $null }
    last_run_time = if ($taskInfo) { $taskInfo.LastRunTime } else { $null }
}

if (-not $Quiet) {
    $verb = if ($created) { "Created" } else { "Updated" }
    Write-Host "$verb scheduled task '$TaskName' (state=$($result.state), every $($result.interval_minutes) minutes)." -ForegroundColor Green
    if ($startedNow) {
        Write-Host "Started scheduled task immediately." -ForegroundColor Green
    }
    if ($result.next_run_time) {
        Write-Host ("Next run: {0}" -f $result.next_run_time)
    }
}

$result
