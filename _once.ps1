param(
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "0.0.0.0",
    [int]$Port = 8000,
    [bool]$OpenBrowser = $true,
    [int]$HealthWaitSec = 20,
    [bool]$StartNewsWorker = $false,
    [bool]$StartNewsLlmWorker = $false,
    [bool]$StartPmWorker = $false,
    [bool]$EnableAnalyticsHistory = $false,
    [bool]$TestDataSources = $false
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
    $hookCandidates = @(
        "C:\ProgramData\anaconda3\shell\condabin\conda-hook.ps1",
        "$env:USERPROFILE\anaconda3\shell\condabin\conda-hook.ps1",
        "$env:USERPROFILE\miniconda3\shell\condabin\conda-hook.ps1"
    )

    foreach ($hook in $hookCandidates) {
        if (-not (Test-Path $hook)) { continue }
        . $hook
        conda activate $Name
        if ($env:CONDA_DEFAULT_ENV -eq $Name) {
            return $true
        }
    }

    if (Get-Command conda -ErrorAction SilentlyContinue) {
        $condaBase = (& conda info --base).Trim()
        if ($condaBase) {
            $condaHook = Join-Path $condaBase "shell\condabin\conda-hook.ps1"
            if (Test-Path $condaHook) {
                . $condaHook
                conda activate $Name
                if ($env:CONDA_DEFAULT_ENV -eq $Name) {
                    return $true
                }
            }
        }
    }

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

    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd) {
        return $pyCmd.Source
    }

    throw "Cannot find Python executable. Please install Conda or create .venv."
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

function Get-RequestedWorkerLabels {
    param(
        [bool]$NewsWorker,
        [bool]$NewsLlmWorker,
        [bool]$PmWorker
    )

    $labels = @()
    if ($NewsWorker) { $labels += "news-worker" }
    if ($NewsLlmWorker) { $labels += "news-llm-worker" }
    if ($PmWorker) { $labels += "pm-worker" }
    return $labels
}

Import-DotEnvFile -Path (Join-Path $PSScriptRoot ".env")
Import-DotEnvFile -Path (Join-Path $PSScriptRoot ".env.local")

$ignoredEnvWorkerFlags = @()
if ((Test-TruthyText ([string]$env:START_NEWS_WORKER)) -and (-not $StartNewsWorker)) {
    $ignoredEnvWorkerFlags += "START_NEWS_WORKER"
}
if ((Test-TruthyText ([string]$env:START_NEWS_LLM_WORKER)) -and (-not $StartNewsLlmWorker)) {
    $ignoredEnvWorkerFlags += "START_NEWS_LLM_WORKER"
}
if ((Test-TruthyText ([string]$env:START_PM_WORKER)) -and (-not $StartPmWorker)) {
    $ignoredEnvWorkerFlags += "START_PM_WORKER"
}

$requestedWorkerLabels = Get-RequestedWorkerLabels `
    -NewsWorker $StartNewsWorker `
    -NewsLlmWorker $StartNewsLlmWorker `
    -PmWorker $StartPmWorker
$startupProfile = if ($requestedWorkerLabels.Count) {
    "web + " + ($requestedWorkerLabels -join ", ")
} else {
    "safe web-only"
}
if (-not $EnableAnalyticsHistory) {
    $startupProfile += " + analytics-history off"
    Set-Item -Path Env:ANALYTICS_HISTORY_ENABLED -Value "0"
}

$pidOnPort = Get-ListeningPid -PortNumber $Port
if ($pidOnPort) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$pidOnPort" -ErrorAction SilentlyContinue
    if ($proc -and ($proc.CommandLine -like "*uvicorn*web.main:app*" -or $proc.CommandLine -like "*main.py --mode web*")) {
        Write-Host "Service already listening on 0.0.0.0:$Port (PID=$pidOnPort)."
        Write-Host "Requested startup profile: $startupProfile"
        if ($ignoredEnvWorkerFlags.Count) {
            Write-Host ("Safe start ignored .env worker flags: {0}" -f ($ignoredEnvWorkerFlags -join ", ")) -ForegroundColor Yellow
            Write-Host "Use explicit flags such as '.\web.bat start -StartNewsWorker -StartNewsLlmWorker' to opt in." -ForegroundColor Yellow
        }
        if (-not $EnableAnalyticsHistory) {
            Write-Host "Safe start forces ANALYTICS_HISTORY_ENABLED=0." -ForegroundColor Yellow
            Write-Host "Use '.\web.bat start -EnableAnalyticsHistory' to opt into analytics history collectors." -ForegroundColor Yellow
        }
        if ($requestedWorkerLabels.Count) {
            Write-Host "Worker mix was not changed because the web service is already running." -ForegroundColor Yellow
            Write-Host "Use '.\web.bat stop -IncludeWorkers' and then start again with the desired worker flags." -ForegroundColor Yellow
        }
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
    Write-Host "Conda env '$EnvName' not found from common paths/PATH. Falling back to .venv or system python."
}

$pythonExe = Resolve-PythonExecutable
Write-Host "Python executable: $pythonExe"
Write-Host "Startup profile: $startupProfile"
if ($ignoredEnvWorkerFlags.Count) {
    Write-Host ("Safe start ignored .env worker flags: {0}" -f ($ignoredEnvWorkerFlags -join ", ")) -ForegroundColor Yellow
    Write-Host "Use explicit worker flags on '.\web.bat start' when you want external workers." -ForegroundColor Yellow
}
if (-not $EnableAnalyticsHistory) {
    Write-Host "Safe start forces ANALYTICS_HISTORY_ENABLED=0." -ForegroundColor Yellow
    Write-Host "Use '.\web.bat start -EnableAnalyticsHistory' when you want analytics history collectors." -ForegroundColor Yellow
}

$proc = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList @("-m", "uvicorn", "web.main:app", "--host", $BindHost, "--port", "$Port") `
    -WorkingDirectory $PSScriptRoot `
    -PassThru

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
$deadline = (Get-Date).AddSeconds([Math]::Max(3, $HealthWaitSec))
while ((Get-Date) -lt $deadline) {
    try {
        $status = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/status" -TimeoutSec 4
        break
    } catch {
        Start-Sleep -Milliseconds 800
    }
}

if ($status) {
    Write-Host "Started PID=$($proc.Id), status=$($status.status), mode=$($status.trading_mode), profile=$startupProfile, url=http://127.0.0.1:$Port"
    if ($OpenBrowser) {
        Open-WebConsole -WebPort $Port
    }
} else {
    Write-Host "Process started (PID=$($proc.Id)) but status endpoint not ready within ${HealthWaitSec}s. Startup profile: $startupProfile."
}

# 测试数据源 (可选)
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
