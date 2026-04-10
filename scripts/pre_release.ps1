param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$Port = 8000,
    [switch]$AllowLiveMode,
    [switch]$AllowExecuteAgent
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Action
    )

    Write-Host ""
    Write-Host ("== {0} ==" -f $Name) -ForegroundColor Cyan
    & $Action
}

$failures = New-Object System.Collections.Generic.List[string]

try {
    Invoke-Step -Name "Python Version" -Action {
        $version = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
        Write-Host ("Python: {0}" -f $version)
        if ($version -notmatch '^3\.11\.') {
            Write-Warning "Recommended Python is 3.11.x."
        }
    }
} catch {
    $failures.Add("Python version check failed: $($_.Exception.Message)")
}

try {
    Invoke-Step -Name "Web Status" -Action {
        & .\scripts\web.ps1 status -Port $Port
    }
} catch {
    $failures.Add("web.ps1 status failed: $($_.Exception.Message)")
}

try {
    Invoke-Step -Name "Config Contract" -Action {
        & .\scripts\check_config_contract.ps1
    }
} catch {
    $failures.Add("config contract failed: $($_.Exception.Message)")
}

try {
    Invoke-Step -Name "Pytest" -Action {
        & .\scripts\test.ps1
        if ($LASTEXITCODE -ne 0) {
            throw "scripts/test.ps1 exited with code $LASTEXITCODE"
        }
    }
} catch {
    $failures.Add("pytest failed: $($_.Exception.Message)")
}

try {
    Invoke-Step -Name "Health Endpoint" -Action {
        $health = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $Port) -TimeoutSec 15
        Write-Host ($health | ConvertTo-Json -Depth 6)
    }
} catch {
    $failures.Add("health endpoint failed: $($_.Exception.Message)")
}

try {
    Invoke-Step -Name "Status Endpoint" -Action {
        $status = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/api/status" -f $Port) -TimeoutSec 15
        Write-Host ($status | ConvertTo-Json -Depth 8)
        $mode = [string]($status.trading_mode)
        if ($mode.Trim().ToLowerInvariant() -eq "live" -and (-not $AllowLiveMode.IsPresent)) {
            throw "Service is currently running in live mode. Re-run with -AllowLiveMode only if this is intentional."
        }
    }
} catch {
    $failures.Add("status endpoint failed: $($_.Exception.Message)")
}

try {
    Invoke-Step -Name "Autonomous Agent Safety" -Action {
        $agent = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/api/ai/autonomous-agent/status" -f $Port) -TimeoutSec 15
        Write-Host ($agent | ConvertTo-Json -Depth 8)
        $running = [bool]($agent.status.running)
        $mode = [string]($agent.config.mode)
        $allowLive = [bool]($agent.config.allow_live)
        $autoStart = [bool]($agent.config.auto_start)
        $armedReasons = @()
        if ($running -and $mode.Trim().ToLowerInvariant() -eq "execute") {
            $armedReasons += "running execute mode"
        }
        if ($allowLive) {
            $armedReasons += "allow_live=true"
        }
        if ($autoStart) {
            $armedReasons += "auto_start=true"
        }
        if ($armedReasons.Count -gt 0 -and (-not $AllowExecuteAgent.IsPresent)) {
            throw ("Autonomous agent safety gate blocked: {0}. Re-run with -AllowExecuteAgent only if this is intentional." -f ($armedReasons -join ", "))
        }
    }
} catch {
    $failures.Add("autonomous agent safety failed: $($_.Exception.Message)")
}

if ($failures.Count -gt 0) {
    Write-Host ""
    Write-Host "Pre-release checks failed:" -ForegroundColor Red
    foreach ($failure in $failures) {
        Write-Host (" - {0}" -f $failure) -ForegroundColor Red
    }
    exit 1
}

Write-Host ""
Write-Host "Pre-release checks passed." -ForegroundColor Green
