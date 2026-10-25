param(
    [string]$TaskName = "CryptoTradingSystem_XArchivePoll",
    [int]$IntervalHours = 2,
    [int]$MinuteOfHour = 35,
    [int]$TimeLimitMinutes = 30,
    [switch]$Quiet
)

# Registers (or updates) the standalone X archive poll for the current user, the same way the other
# CryptoTradingSystem_* tasks are installed: explicit XML, interactive token, least privilege, a bounded run time
# and a catch-up run if the machine was off. It runs every $IntervalHours hours at $MinuteOfHour past an even hour
# and does nothing while the news worker is polling X itself (scripts/poll_x_archive.py checks that), so it is a
# fallback for when the worker is stopped or X is switched off there. It does not touch the news engine.
$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $PSScriptRoot "run_x_archive_poll.ps1"
$powershellExe = (Get-Command powershell -ErrorAction Stop).Source
if (-not (Test-Path $runnerScript)) {
    throw "Runner script not found: $runnerScript"
}

# first run: the next slot at $MinuteOfHour past an hour that is a multiple of $IntervalHours, at least 2 minutes away
$now = Get-Date
$first = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day -Hour 0 -Minute $MinuteOfHour -Second 0
while ($first -le $now.AddMinutes(2)) {
    $first = $first.AddHours($IntervalHours)
}

$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$arguments = '-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}"' -f $runnerScript
$esc = { param($text) [System.Security.SecurityElement]::Escape($text) }
$xml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Standalone X archive poll (research archive only; steps aside while the news worker polls X). See docs/X_AGENT_HANDOFF.md.</Description></RegistrationInfo>
  <Triggers>
    <TimeTrigger>
      <StartBoundary>$($first.ToString("s"))</StartBoundary>
      <Enabled>true</Enabled>
      <Repetition><Interval>PT${IntervalHours}H</Interval><Duration>P3650D</Duration><StopAtDurationEnd>false</StopAtDurationEnd></Repetition>
    </TimeTrigger>
  </Triggers>
  <Principals><Principal id="Author"><UserId>$(& $esc $sid)</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT${TimeLimitMinutes}M</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author"><Exec><Command>$(& $esc $powershellExe)</Command><Arguments>$(& $esc $arguments)</Arguments><WorkingDirectory>$(& $esc $projectRoot)</WorkingDirectory></Exec></Actions>
</Task>
"@

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$xmlPath = Join-Path ([System.IO.Path]::GetTempPath()) ("x_archive_poll_task_{0}.xml" -f ([guid]::NewGuid().ToString("N")))
try {
    $xml | Set-Content -Path $xmlPath -Encoding Unicode
    $null = & schtasks /Create /F /TN $TaskName /XML $xmlPath
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks XML import failed with exit code $LASTEXITCODE"
    }
} finally {
    Remove-Item $xmlPath -Force -ErrorAction SilentlyContinue
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
$result = [pscustomobject]@{
    task_name = $TaskName
    created = ($null -eq $existing)
    state = [string]$task.State
    every_hours = $IntervalHours
    first_run = $first
    next_run_time = if ($info) { $info.NextRunTime } else { $null }
    time_limit_minutes = $TimeLimitMinutes
    log = (Join-Path $projectRoot "logs\x_archive_poll.log")
}
if (-not $Quiet) {
    $verb = if ($result.created) { "Created" } else { "Updated" }
    Write-Host "$verb scheduled task '$TaskName' (state=$($result.state), every $IntervalHours h, next run $($result.next_run_time))." -ForegroundColor Green
}
$result
