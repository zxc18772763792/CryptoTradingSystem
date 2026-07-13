param(
    [string]$TaskName = "CryptoTradingSystem_ResearchUniverseRefresh",
    [string]$EnvName = "crypto_trading",
    [string]$Exchange = "binance",
    [string]$Timeframes = "1m,5m,15m,1h,4h,1d",
    [int]$Days = 90,
    [int]$OverlapBars = 48,
    [string]$SecondsSymbols = "BTC/USDT,ETH/USDT",
    [int]$SecondsDays = 1,
    [switch]$DisableIdleSeconds,
    [int]$IntervalMinutes = 60,
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

function Test-IsAdministrator {
    try {
        $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
        $principal = New-Object System.Security.Principal.WindowsPrincipal($identity)
        return $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)
    } catch {
        return $false
    }
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

function Register-LimitedFallbackTask {
    param(
        [string]$Name,
        [string]$Executable,
        [string]$Arguments,
        [string]$WorkingDirectory,
        [int]$EveryMinutes
    )

    # schtasks /Create with command-line scheduling silently installs the
    # Windows defaults (IgnoreNew + PT72H). Import explicit XML instead so the
    # non-elevated fallback has the same bounded self-healing policy as the
    # ScheduledTasks-cmdlet path.
    $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $startBoundary = (Get-NextAlignedTriggerTime -Minutes $EveryMinutes).ToString("s")
    $interval = "PT{0}M" -f ([Math]::Max(5, $EveryMinutes))
    $escapedExecutable = [System.Security.SecurityElement]::Escape($Executable)
    $escapedArguments = [System.Security.SecurityElement]::Escape($Arguments)
    $escapedWorkingDirectory = [System.Security.SecurityElement]::Escape($WorkingDirectory)
    $escapedSid = [System.Security.SecurityElement]::Escape($sid)
    $xml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Bounded incremental research-universe refresh.</Description></RegistrationInfo>
  <Triggers>
    <TimeTrigger>
      <StartBoundary>$startBoundary</StartBoundary>
      <Enabled>true</Enabled>
      <Repetition><Interval>$interval</Interval><Duration>P3650D</Duration><StopAtDurationEnd>false</StopAtDurationEnd></Repetition>
    </TimeTrigger>
  </Triggers>
  <Principals><Principal id="Author"><UserId>$escapedSid</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>StopExisting</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT45M</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author"><Exec><Command>$escapedExecutable</Command><Arguments>$escapedArguments</Arguments><WorkingDirectory>$escapedWorkingDirectory</WorkingDirectory></Exec></Actions>
</Task>
"@
    $xmlPath = Join-Path ([System.IO.Path]::GetTempPath()) ("research_refresh_task_{0}.xml" -f ([guid]::NewGuid().ToString("N")))
    try {
        $xml | Set-Content -Path $xmlPath -Encoding Unicode
        $null = & schtasks /Create /F /TN $Name /XML $xmlPath
        if ($LASTEXITCODE -ne 0) {
            throw "schtasks XML import failed with exit code $LASTEXITCODE"
        }
    } finally {
        Remove-Item $xmlPath -Force -ErrorAction SilentlyContinue
    }
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
    "-MaxRefreshAgeMinutes", "45",
    "-LogPath", "`"$logPath`"",
    "-Quiet"
) -join " "
if ($DisableIdleSeconds) {
    $actionArgs += " -DisableIdleSeconds"
}

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$created = $null -eq $existingTask

# Windows' ScheduledTasks PowerShell enum does not expose the XML schema's
# StopExisting policy.  Register the explicit XML in every privilege mode so
# elevated and non-elevated installs cannot silently diverge back to IgnoreNew.
Register-LimitedFallbackTask -Name $TaskName -Executable $powershellExe `
    -Arguments $actionArgs -WorkingDirectory $projectRoot `
    -EveryMinutes ([Math]::Max(5, $IntervalMinutes))
$taskRegistered = $true

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
