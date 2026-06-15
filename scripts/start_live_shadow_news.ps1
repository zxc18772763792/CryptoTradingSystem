param(
    [ValidateSet("restart", "start", "status", "stop")]
    [string]$Action = "restart",
    [string]$EnvName = "crypto_trading",
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000,
    [int]$HealthWaitSec = 180,
    [double]$MaxWsAgeMs = 10000,
    [string]$NewsNimModel = "google/gemma-3n-e4b-it",
    [switch]$ConfirmLive,
    [switch]$ResetNewsLlmFailover,
    [switch]$SkipPrecheck,
    [switch]$OpenBrowser
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Initialize-LocalCondaPath {
    $parentRoot = Split-Path -Parent $projectRoot
    $condaRoot = Join-Path $parentRoot ".conda\miniforge3"
    $condabin = Join-Path $condaRoot "condabin"
    if (-not (Test-Path $condabin)) {
        return
    }

    $pathParts = @($env:Path -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    if ($pathParts -notcontains $condabin) {
        $env:Path = $condabin + ";" + $env:Path
    }
}

function Require-LiveConfirm {
    if (-not $ConfirmLive) {
        throw "REFUSING: live-shadow-news starts TRADING_MODE=live. Re-run with -ConfirmLive after explicit approval."
    }
    Write-Host "LIVE shadow with news confirmed (-ConfirmLive)." -ForegroundColor Yellow
}

function Import-EnvFileValues {
    $values = @{}
    foreach ($path in @(
        (Join-Path $projectRoot ".env"),
        (Join-Path $projectRoot ".env.local")
    )) {
        if (-not (Test-Path $path)) {
            continue
        }
        foreach ($line in Get-Content $path) {
            $text = [string]$line
            if (-not $text) {
                continue
            }
            $trimmed = $text.Trim()
            if (-not $trimmed -or $trimmed.StartsWith("#")) {
                continue
            }
            $eq = $trimmed.IndexOf("=")
            if ($eq -lt 1) {
                continue
            }
            $name = $trimmed.Substring(0, $eq).Trim()
            $value = $trimmed.Substring($eq + 1).Trim()
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            if ($name) {
                $values[$name] = $value
            }
        }
    }
    return $values
}

function Get-OpsToken {
    param([hashtable]$EnvValues)

    $token = [string]$env:OPS_TOKEN
    if ([string]::IsNullOrWhiteSpace($token) -and $EnvValues.ContainsKey("OPS_TOKEN")) {
        $token = [string]$EnvValues["OPS_TOKEN"]
    }
    return $token.Trim()
}

function Resolve-CryptoPython {
    $parentRoot = Split-Path -Parent $projectRoot
    $localCondaPython = Join-Path $parentRoot (".conda\miniforge3\envs\{0}\python.exe" -f $EnvName)
    if (Test-Path $localCondaPython) {
        return $localCondaPython
    }
    if ($env:CONDA_PREFIX) {
        $activePython = Join-Path $env:CONDA_PREFIX "python.exe"
        if (Test-Path $activePython) {
            return $activePython
        }
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return $python.Source
    }
    throw "Cannot find Python for precheck. Expected local conda env '$EnvName'."
}

function Assert-NewsNimPrimary {
    param([hashtable]$EnvValues)

    $baseUrl = [string]$EnvValues["NEWS_LLM_BASE_URL"]
    $model = [string]$EnvValues["NEWS_LLM_MODEL"]
    if ($baseUrl.TrimEnd("/") -ne "https://integrate.api.nvidia.com/v1") {
        throw "NEWS_LLM_BASE_URL must point to NIM for this profile. Current value: $baseUrl"
    }
    if ($model -ne $NewsNimModel) {
        throw "NEWS_LLM_MODEL must be '$NewsNimModel' for this profile. Current value: $model"
    }
    Write-Host ("News LLM primary: NIM {0}" -f $model)
}

function Reset-NewsFailoverState {
    $statePath = Join-Path $projectRoot "runtime\openai_failover_state.json"
    if (-not (Test-Path $statePath)) {
        Write-Host "News LLM failover state: no state file to reset."
        return
    }

    $state = Get-Content $statePath -Raw | ConvertFrom-Json
    if (-not $state.scopes) {
        Write-Host "News LLM failover state: no scoped state present."
        return
    }

    $newsScope = $state.scopes.PSObject.Properties["news"]
    if (-not $newsScope) {
        Write-Host "News LLM failover state: news scope was already clear."
        return
    }

    $state.scopes.PSObject.Properties.Remove("news")
    if ($state.scopes.PSObject.Properties.Count -eq 0) {
        Remove-Item -LiteralPath $statePath -Force
    } else {
        $state | ConvertTo-Json -Depth 12 | Set-Content -Path $statePath -Encoding UTF8
    }
    Write-Host "News LLM failover state: cleared news scope."
}

function Set-LiveShadowNewsEnv {
    Set-Item -Path Env:TRADING_MODE -Value "live"
    Set-Item -Path Env:ALLOW_PERSISTED_LIVE_MODE_START -Value "true"

    Set-Item -Path Env:MARKET_WS_ENABLED -Value "true"
    Set-Item -Path Env:MARKET_WS_MODE -Value "shadow"
    Set-Item -Path Env:MARKET_WS_FORCE_REST -Value "false"
    Set-Item -Path Env:MARKET_WS_FAIL_CLOSED_FOR_LIVE -Value "true"
    Set-Item -Path Env:MARKET_WS_EXCHANGES -Value "binance"
    Set-Item -Path Env:MARKET_WS_SYMBOL_LIMIT -Value "16"
    Set-Item -Path Env:MARKET_WS_REST_RECONCILE_SEC -Value "30"
    Set-Item -Path Env:MARKET_WS_WATCH_TIMEOUT_SEC -Value "25"
    Set-Item -Path Env:MARKET_WS_MAX_PRICE_DIFF_BPS -Value "20"
    Set-Item -Path Env:MARKET_WS_SYMBOL_MAX_AGE_SEC -Value "10"
    Set-Item -Path Env:MARKET_WS_MARK_PRICE_ENABLED -Value "false"

    Set-Item -Path Env:NEWS_BACKGROUND_ENABLED -Value "true"
    Set-Item -Path Env:NEWS_LLM_BACKGROUND_ENABLED -Value "true"
}

function Invoke-WebBat {
    param([string[]]$Arguments)

    $webBat = Join-Path $projectRoot "web.bat"
    if (-not (Test-Path $webBat)) {
        throw "web.bat not found: $webBat"
    }
    & $webBat @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "web.bat failed with exit code $LASTEXITCODE"
    }
}

function Invoke-LiveShadowPrecheck {
    param([hashtable]$EnvValues)

    if ($SkipPrecheck) {
        Write-Host "Live shadow precheck skipped by -SkipPrecheck." -ForegroundColor Yellow
        return
    }

    $token = Get-OpsToken -EnvValues $EnvValues
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw "OPS_TOKEN is required for live shadow precheck."
    }

    $pythonExe = Resolve-CryptoPython
    $precheck = Join-Path $projectRoot "scripts\precheck_market_ws_live_shadow.py"
    Write-Host "Running live shadow market WS precheck..."
    & $pythonExe $precheck --base-url "http://127.0.0.1:$Port" --token $token --max-ws-age-ms $MaxWsAgeMs
    if ($LASTEXITCODE -ne 0) {
        throw "Live shadow market WS precheck failed with exit code $LASTEXITCODE"
    }
}

function Show-NewsHealthSummary {
    param([hashtable]$EnvValues)

    $headers = @{}
    $token = Get-OpsToken -EnvValues $EnvValues
    if (-not [string]::IsNullOrWhiteSpace($token)) {
        $headers["X-OPS-TOKEN"] = $token
        $headers["X-OPS-CALLER"] = "live_shadow_news_start"
    }
    try {
        $news = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/news/health" -Headers $headers -TimeoutSec 12
        $targets = @($news.llm_runtime.targets)
        $primary = $targets | Where-Object { [string]$_.role -eq "primary" } | Select-Object -First 1
        if ($primary) {
            Write-Host ("News LLM runtime primary: {0} / {1}" -f $primary.base_url, $primary.model)
        }
        Write-Host ("News queue: pending={0}, running={1}, done={2}" -f $news.llm_queue.counts.pending, $news.llm_queue.counts.running, $news.llm_queue.counts.done)
    } catch {
        Write-Host ("News health summary unavailable: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
    }
}

Initialize-LocalCondaPath

$envValues = Import-EnvFileValues

switch ($Action) {
    "status" {
        Invoke-WebBat -Arguments @("status", "-Port", "$Port")
        break
    }
    "stop" {
        Invoke-WebBat -Arguments @("stop", "-IncludeWorkers", "-Port", "$Port")
        break
    }
    "start" {
        Require-LiveConfirm
        Assert-NewsNimPrimary -EnvValues $envValues
        if ($ResetNewsLlmFailover) {
            Reset-NewsFailoverState
        }
        Set-LiveShadowNewsEnv
        $args = @("start", "-EnvName", $EnvName, "-BindHost", $BindHost, "-Port", "$Port", "-HealthWaitSec", "$HealthWaitSec", "-AllowPersistedLiveMode", "-StartNewsWorker", "-StartNewsLlmWorker")
        if ($OpenBrowser) {
            $args += "-OpenBrowser"
        }
        Invoke-WebBat -Arguments $args
        Invoke-WebBat -Arguments @("status", "-Port", "$Port")
        Invoke-LiveShadowPrecheck -EnvValues $envValues
        Show-NewsHealthSummary -EnvValues $envValues
        break
    }
    "restart" {
        Require-LiveConfirm
        Assert-NewsNimPrimary -EnvValues $envValues
        Invoke-WebBat -Arguments @("stop", "-IncludeWorkers", "-Port", "$Port")
        if ($ResetNewsLlmFailover) {
            Reset-NewsFailoverState
        }
        Set-LiveShadowNewsEnv
        $args = @("start", "-EnvName", $EnvName, "-BindHost", $BindHost, "-Port", "$Port", "-HealthWaitSec", "$HealthWaitSec", "-AllowPersistedLiveMode", "-StartNewsWorker", "-StartNewsLlmWorker")
        if ($OpenBrowser) {
            $args += "-OpenBrowser"
        }
        Invoke-WebBat -Arguments $args
        Invoke-WebBat -Arguments @("status", "-Port", "$Port")
        Invoke-LiveShadowPrecheck -EnvValues $envValues
        Show-NewsHealthSummary -EnvValues $envValues
        break
    }
}
