param(
    [string]$TaskName = "CryptoTradingSystem_XGrowthDaily",
    [string]$StartTime = "08:30",
    [int]$TimeLimitHours = 3,
    [switch]$StartNow,
    [switch]$Quiet
)

# Registers (or updates) the daily X growth-tracker task for the current user, the same way the other
# CryptoTradingSystem_* tasks are installed: explicit XML, interactive token, least privilege, a bounded
# run time and a catch-up run if the machine was off. 08:30 China time is 00:30 UTC, just after
# Scweet's per-day request budget resets at UTC midnight, so a late catch-up run still counts as that day.
$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $PSScriptRoot "run_x_growth_daily.ps1"
$powershellExe = (Get-Command powershell -ErrorAction Stop).Source
if (-not (Test-Path $runnerScript)) {
    throw "Runner script not found: $runnerScript"
}

$time = [datetime]::ParseExact($StartTime, "HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
$now = Get-Date
$first = Get-Date -Year $now.Year -Month $now.Month -Day $now.Day -Hour $time.Hour -Minute $time.Minute -Second 0
if ($first -le $now) {
    $first = $first.AddDays(1)
}

$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$arguments = '-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}"' -f $runnerScript
$esc = { param($text) [System.Security.SecurityElement]::Escape($text) }
$xml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Daily X KOL candidate-pool and follower snapshots (records only; no trading). See docs/X_KOL_GROWTH_TRACKER.md.</Description></RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>$($first.ToString("s"))</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals><Principal id="Author"><UserId>$(& $esc $sid)</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT${TimeLimitHours}H</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author"><Exec><Command>$(& $esc $powershellExe)</Command><Arguments>$(& $esc $arguments)</Arguments><WorkingDirectory>$(& $esc $projectRoot)</WorkingDirectory></Exec></Actions>
</Task>
"@

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$xmlPath = Join-Path ([System.IO.Path]::GetTempPath()) ("x_growth_task_{0}.xml" -f ([guid]::NewGuid().ToString("N")))
try {
    $xml | Set-Content -Path $xmlPath -Encoding Unicode
    $null = & schtasks /Create /F /TN $TaskName /XML $xmlPath
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks XML import failed with exit code $LASTEXITCODE"
    }
} finally {
    Remove-Item $xmlPath -Force -ErrorAction SilentlyContinue
}

if ($StartNow.IsPresent) {
    Start-ScheduledTask -TaskName $TaskName
}
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
$result = [pscustomobject]@{
    task_name = $TaskName
    created = ($null -eq $existing)
    state = [string]$task.State
    daily_at = $StartTime
    next_run_time = if ($info) { $info.NextRunTime } else { $null }
    time_limit_hours = $TimeLimitHours
    log = (Join-Path $projectRoot "logs\x_growth_daily.log")
}
if (-not $Quiet) {
    $verb = if ($result.created) { "Created" } else { "Updated" }
    Write-Host "$verb scheduled task '$TaskName' (state=$($result.state), daily at $StartTime, next run $($result.next_run_time))." -ForegroundColor Green
}
$result
