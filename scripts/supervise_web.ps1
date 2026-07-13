param(
    [string]$ProjectRoot,
    [string]$PythonExecutable,
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
    [int]$MonitorIntervalSec = 5,
    [int]$MaxRestarts = 5,
    [int]$RestartWindowMinutes = 15,
    [int]$MaxBackoffSec = 60,
    [int]$MaxLivenessFailures = 6
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}
$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
Set-Location $ProjectRoot

$runtimeDir = Join-Path $ProjectRoot "runtime"
$logDir = Join-Path $ProjectRoot "logs"
$stopPath = Join-Path $runtimeDir ("web_supervisor_{0}.stop" -f $Port)
$pidPath = Join-Path $runtimeDir ("web_supervisor_{0}.pid" -f $Port)
$statePath = Join-Path $runtimeDir ("web_supervisor_{0}.json" -f $Port)
$logPath = Join-Path $logDir "web_supervisor.log"
$onceScript = Join-Path $ProjectRoot "_once.ps1"
New-Item -ItemType Directory -Force -Path $runtimeDir,$logDir | Out-Null

function Write-SupervisorLog {
    param([string]$Message, [string]$Level = "INFO")
    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    $line | Add-Content -Path $logPath -Encoding UTF8
}

function Write-SupervisorState {
    param([string]$Status, [string]$Component = "", [string]$Detail = "")
    $payload = [ordered]@{
        supervisor_pid = $PID
        status = $Status
        component = $Component
        detail = $Detail
        port = $Port
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
    }
    $tempPath = "$statePath.$PID.tmp"
    $payload | ConvertTo-Json -Depth 4 | Set-Content -Path $tempPath -Encoding UTF8
    Move-Item -Path $tempPath -Destination $statePath -Force
}

function Get-MatchingPythonProcesses {
    param([string]$CommandToken)
    return @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $name = [string]$_.Name
                $cmd = [string]$_.CommandLine
                $name -and
                $name.ToLowerInvariant() -in @("python.exe", "pythonw.exe") -and
                $cmd -and
                $cmd.ToLowerInvariant().Contains($CommandToken.ToLowerInvariant())
            }
    )
}

function Get-WebProcesses {
    $portToken = "--port $Port"
    $matches = @(
        @(Get-MatchingPythonProcesses -CommandToken "web.main:app") +
        @(Get-MatchingPythonProcesses -CommandToken "main.py --mode web")
    )
    return @(
        $matches |
            Where-Object { ([string]$_.CommandLine).Contains($portToken) } |
            Sort-Object ProcessId -Unique
    )
}

function Test-WebRunning {
    return @(Get-WebProcesses).Count -gt 0
}

function Test-WebResponsive {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/livez" -TimeoutSec 5
        return [int]$response.StatusCode -eq 200
    } catch {
        return $false
    }
}

$restartTimes = @()
$consecutiveFailures = 0
$livenessFailures = 0
function Reserve-Restart {
    param([string]$Component)
    $now = Get-Date
    $cutoff = $now.AddMinutes(-[Math]::Max(1, $RestartWindowMinutes))
    $script:restartTimes = @($script:restartTimes | Where-Object { $_ -ge $cutoff })
    if ($script:restartTimes.Count -ge [Math]::Max(1, $MaxRestarts)) {
        Write-SupervisorLog "Restart budget exhausted for $Component ($($script:restartTimes.Count) restarts in $RestartWindowMinutes minutes)." "ERROR"
        Write-SupervisorState "restart_budget_exhausted" $Component "manual intervention required"
        return $false
    }
    $script:restartTimes += $now
    return $true
}

function Invoke-WebRestart {
    function Quote-PowerShellLiteral([string]$Value) {
        return "'" + ([string]$Value).Replace("'", "''") + "'"
    }
    function Bool-PowerShellLiteral([bool]$Value) {
        return $(if ($Value) { '$true' } else { '$false' })
    }
    $commandParts = @(
        "&", (Quote-PowerShellLiteral $onceScript),
        "-EnvName", (Quote-PowerShellLiteral $EnvName),
        "-BindHost", (Quote-PowerShellLiteral $BindHost),
        "-Port", "$Port",
        "-HealthWaitSec", "$HealthWaitSec",
        "-OpenBrowser:" + (Bool-PowerShellLiteral $false),
        "-AllowPersistedLiveMode:" + (Bool-PowerShellLiteral $AllowPersistedLiveMode.IsPresent),
        "-StartAutonomousAgent:" + (Bool-PowerShellLiteral $StartAutonomousAgent.IsPresent),
        "-StartNewsWorker:" + (Bool-PowerShellLiteral $StartNewsWorker.IsPresent),
        "-StartNewsLlmWorker:" + (Bool-PowerShellLiteral $StartNewsLlmWorker.IsPresent),
        "-StartPmWorker:" + (Bool-PowerShellLiteral $StartPmWorker.IsPresent),
        "-EnableAnalyticsHistory:" + (Bool-PowerShellLiteral $EnableAnalyticsHistory.IsPresent),
        "-TestDataSources:" + (Bool-PowerShellLiteral $false),
        "-DisableSupervisorBootstrap;",
        "exit `$LASTEXITCODE"
    )
    $restartCommand = $commandParts -join " "
    $encodedCommand = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($restartCommand))
    $args = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-EncodedCommand", $encodedCommand)
    $restartStamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $stdout = Join-Path $logDir ("web_restart_{0}.out.log" -f $restartStamp)
    $stderr = Join-Path $logDir ("web_restart_{0}.err.log" -f $restartStamp)
    $proc = Start-Process -FilePath "powershell.exe" -ArgumentList $args -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr -Wait -PassThru
    return [int]$proc.ExitCode
}

function Start-MissingWorker {
    param([string]$Label, [string]$Module)
    if ((Get-MatchingPythonProcesses -CommandToken $Module).Count) {
        return $true
    }
    if (-not (Reserve-Restart -Component $Label)) {
        return $false
    }
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $stdout = Join-Path $logDir ("{0}_{1}.out.log" -f ($Label -replace "[^a-zA-Z0-9]", "_"), $stamp)
    $stderr = Join-Path $logDir ("{0}_{1}.err.log" -f ($Label -replace "[^a-zA-Z0-9]", "_"), $stamp)
    $proc = Start-Process -FilePath $PythonExecutable -ArgumentList @("-m", $Module) `
        -WorkingDirectory $ProjectRoot -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
    Write-SupervisorLog "Restarted $Label PID=$($proc.Id)."
    Write-SupervisorState "running" $Label "restarted pid=$($proc.Id)"
    return $true
}

$createdNew = $false
$mutex = New-Object System.Threading.Mutex($true, "Local\CryptoTradingSystem_WebSupervisor_$Port", [ref]$createdNew)
if (-not $createdNew) {
    Write-SupervisorLog "A supervisor already owns port $Port; duplicate exiting." "WARN"
    exit 0
}

try {
    Remove-Item $stopPath -Force -ErrorAction SilentlyContinue
    "$PID" | Set-Content -Path $pidPath -Encoding ASCII
    Write-SupervisorLog "Supervisor started PID=$PID port=$Port max_restarts=$MaxRestarts/$RestartWindowMinutes min."
    Write-SupervisorState "running" "web" "monitoring"

    while (-not (Test-Path $stopPath)) {
        if (-not (Test-WebRunning)) {
            if (-not (Reserve-Restart -Component "web")) {
                break
            }
            $delay = [Math]::Min([Math]::Pow(2, $consecutiveFailures), [Math]::Max(1, $MaxBackoffSec))
            Write-SupervisorLog "Web process missing; restarting after $delay second(s)." "WARN"
            Write-SupervisorState "restarting" "web" "backoff=$delay"
            Start-Sleep -Seconds $delay
            if (Test-Path $stopPath) { break }
            try {
                $exitCode = Invoke-WebRestart
                if ($exitCode -eq 0 -and (Test-WebRunning)) {
                    $consecutiveFailures = 0
                    Write-SupervisorLog "Web restart succeeded."
                    Write-SupervisorState "running" "web" "restart succeeded"
                } else {
                    $consecutiveFailures++
                    Write-SupervisorLog "Web restart failed exit=$exitCode." "ERROR"
                    Write-SupervisorState "restart_failed" "web" "exit=$exitCode"
                }
            } catch {
                $consecutiveFailures++
                Write-SupervisorLog "Web restart raised: $($_.Exception.Message)" "ERROR"
                Write-SupervisorState "restart_failed" "web" $_.Exception.Message
            }
        } else {
            $consecutiveFailures = 0
            if (Test-WebResponsive) {
                $livenessFailures = 0
            } else {
                $livenessFailures++
                Write-SupervisorLog "Liveness probe failed ($livenessFailures/$MaxLivenessFailures)." "WARN"
                if ($livenessFailures -ge [Math]::Max(2, $MaxLivenessFailures)) {
                    Write-SupervisorLog "Liveness failure threshold reached; recycling web process." "ERROR"
                    Write-SupervisorState "unresponsive" "web" "recycling after $livenessFailures failures"
                    foreach ($webProc in @(Get-WebProcesses)) {
                        Stop-Process -Id $webProc.ProcessId -Force -ErrorAction SilentlyContinue
                    }
                    $livenessFailures = 0
                    continue
                }
            }
            if ($StartNewsWorker.IsPresent) {
                if (-not (Start-MissingWorker "news_worker" "core.news.service.worker")) { break }
            }
            if ($StartNewsLlmWorker.IsPresent) {
                if (-not (Start-MissingWorker "news_llm_worker" "core.news.service.llm_worker")) { break }
            }
            if ($StartPmWorker.IsPresent) {
                if (-not (Start-MissingWorker "pm_worker" "prediction_markets.polymarket.worker")) { break }
            }
        }
        Start-Sleep -Seconds ([Math]::Max(2, $MonitorIntervalSec))
    }
    if (Test-Path $stopPath) {
        Write-SupervisorLog "Stop signal observed; supervisor exiting."
        Write-SupervisorState "stopped" "supervisor" "stop signal"
    }
} finally {
    Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
    Remove-Item $stopPath -Force -ErrorAction SilentlyContinue
    if ($mutex) {
        $mutex.ReleaseMutex()
        $mutex.Dispose()
    }
}
