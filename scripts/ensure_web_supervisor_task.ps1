param(
    [string]$TaskName = "",
    [string]$ProjectRoot = "",
    [string]$PythonExecutable = "",
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000,
    [int]$HealthWaitSec = 150,
    [switch]$AllowPersistedLiveMode,
    [switch]$StartAutonomousAgent,
    [switch]$StartNewsWorker,
    [switch]$StartNewsLlmWorker,
    [switch]$StartPmWorker,
    [switch]$EnableAnalyticsHistory,
    [switch]$StartNow,
    [switch]$Quiet
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}
$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
if ([string]::IsNullOrWhiteSpace($TaskName)) {
    $TaskName = "CryptoTradingSystem_WebSupervisor_{0}" -f $Port
}

$supervisorScript = Join-Path $PSScriptRoot "supervise_web.ps1"
if (-not (Test-Path $supervisorScript)) {
    throw "Web supervisor script not found: $supervisorScript"
}
if ([string]::IsNullOrWhiteSpace($PythonExecutable)) {
    throw "PythonExecutable is required so the supervisor can respawn workers."
}
$powershellExe = (Get-Command powershell -ErrorAction Stop).Source
$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value

$actionArgs = @(
    "-NoProfile",
    "-WindowStyle", "Hidden",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$supervisorScript`"",
    "-ProjectRoot", "`"$ProjectRoot`"",
    "-PythonExecutable", "`"$PythonExecutable`"",
    "-EnvName", "`"$EnvName`"",
    "-BindHost", "`"$BindHost`"",
    "-Port", "$Port",
    "-HealthWaitSec", "$HealthWaitSec"
) -join " "
if ($AllowPersistedLiveMode) { $actionArgs += " -AllowPersistedLiveMode" }
if ($StartAutonomousAgent) { $actionArgs += " -StartAutonomousAgent" }
if ($StartNewsWorker) { $actionArgs += " -StartNewsWorker" }
if ($StartNewsLlmWorker) { $actionArgs += " -StartNewsLlmWorker" }
if ($StartPmWorker) { $actionArgs += " -StartPmWorker" }
if ($EnableAnalyticsHistory) { $actionArgs += " -EnableAnalyticsHistory" }

# The supervisor must run under the Task Scheduler service instead of the
# console that invoked the startup script. Console hosts (Windows Terminal,
# IDE/agent terminals) wrap children in a kill-on-close job object, so a host
# update or exit silently destroys web + workers + supervisor in one sweep
# (observed 2026-07-16 19:44: a packaged-app auto-update killed the tree).
# Task-hosted processes are parented to the Schedule service and survive.
#
# Settings rationale:
# - No triggers: on-demand only; reboot recovery stays an operator decision
#   because a managed start can restore LIVE trading mode.
# - ExecutionTimeLimit PT0S: the Windows default of PT72H would hard-kill the
#   supervisor tree after 3 days and reintroduce the exact failure mode.
# - Priority 4: task defaults run at below-normal CPU/IO priority, which the
#   respawned uvicorn would inherit; 4 keeps the tree at normal priority.
# - Parallel instances: a lingering instance whose job still holds worker
#   processes must not block a fresh supervisor launch; the named mutex in
#   supervise_web.ps1 already guarantees a single active supervisor per port.
$escapedExecutable = [System.Security.SecurityElement]::Escape($powershellExe)
$escapedArguments = [System.Security.SecurityElement]::Escape($actionArgs)
$escapedWorkingDirectory = [System.Security.SecurityElement]::Escape($ProjectRoot)
$escapedSid = [System.Security.SecurityElement]::Escape($sid)
$xml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Keeps the CryptoTradingSystem web stack alive outside any console job object.</Description></RegistrationInfo>
  <Triggers />
  <Principals><Principal id="Author"><UserId>$escapedSid</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>Parallel</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>false</StartWhenAvailable>
    <IdleSettings><StopOnIdleEnd>false</StopOnIdleEnd><RestartOnIdle>false</RestartOnIdle></IdleSettings>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>4</Priority>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author"><Exec><Command>$escapedExecutable</Command><Arguments>$escapedArguments</Arguments><WorkingDirectory>$escapedWorkingDirectory</WorkingDirectory></Exec></Actions>
</Task>
"@

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$created = $null -eq $existingTask

$xmlPath = Join-Path ([System.IO.Path]::GetTempPath()) ("web_supervisor_task_{0}.xml" -f ([guid]::NewGuid().ToString("N")))
try {
    $xml | Set-Content -Path $xmlPath -Encoding Unicode
    $null = & schtasks /Create /F /TN $TaskName /XML $xmlPath
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks XML import failed with exit code $LASTEXITCODE"
    }
} finally {
    Remove-Item $xmlPath -Force -ErrorAction SilentlyContinue
}

$startedNow = $false
if ($StartNow.IsPresent) {
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
            Write-Host "Supervisor task start skipped: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$result = [pscustomobject]@{
    task_name = $TaskName
    created = $created
    started_now = $startedNow
    state = [string]$task.State
    port = $Port
    supervisor_script = $supervisorScript
    python_executable = $PythonExecutable
}

if (-not $Quiet) {
    $verb = if ($created) { "Created" } else { "Updated" }
    Write-Host "$verb scheduled task '$TaskName' (state=$($result.state))." -ForegroundColor Green
    if ($startedNow) {
        Write-Host "Started supervisor task immediately." -ForegroundColor Green
    }
}

$result
