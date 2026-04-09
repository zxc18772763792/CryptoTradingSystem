param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

function Get-FileText {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        throw "Missing required file: $Path"
    }
    return [System.IO.File]::ReadAllText((Resolve-Path $Path).ProviderPath, [System.Text.UTF8Encoding]::new($true))
}

function Get-RegexValue {
    param(
        [string]$Text,
        [string]$Pattern,
        [string]$Label
    )

    $match = [regex]::Match($Text, $Pattern, [System.Text.RegularExpressions.RegexOptions]::Multiline)
    if (-not $match.Success -or $match.Groups.Count -lt 2) {
        throw "Unable to find $Label with pattern: $Pattern"
    }
    return $match.Groups[1].Value.Trim()
}

function Assert-Equal {
    param(
        [string]$Name,
        [string]$Expected,
        [string]$Actual
    )

    if ($Expected -ne $Actual) {
        throw "$Name mismatch. Expected '$Expected' but found '$Actual'."
    }
    Write-Host ("[OK] {0} = {1}" -f $Name, $Actual)
}

$settingsText = Get-FileText "config/settings.py"
$envExampleText = Get-FileText ".env.example"
$webPsText = Get-FileText "scripts/web.ps1"
$startWebPsText = Get-FileText "scripts/start_web_ps.ps1"
$oncePsText = Get-FileText "_once.ps1"

$checks = @(
    @{
        Name = "WEB_HOST"
        Settings = Get-RegexValue -Text $settingsText -Pattern 'WEB_HOST:\s*str\s*=\s*"([^"]+)"' -Label "config/settings.py WEB_HOST"
        Example = Get-RegexValue -Text $envExampleText -Pattern '^WEB_HOST=(.+)$' -Label ".env.example WEB_HOST"
        WebPs = Get-RegexValue -Text $webPsText -Pattern '\[string\]\$BindHost\s*=\s*"([^"]+)"' -Label "scripts/web.ps1 BindHost"
        StartWebPs = Get-RegexValue -Text $startWebPsText -Pattern '\[string\]\$BindHost\s*=\s*"([^"]+)"' -Label "scripts/start_web_ps.ps1 BindHost"
        OncePs = Get-RegexValue -Text $oncePsText -Pattern '\[string\]\$BindHost\s*=\s*"([^"]+)"' -Label "_once.ps1 BindHost"
    },
    @{
        Name = "OPENAI_BASE_URL"
        Settings = Get-RegexValue -Text $settingsText -Pattern 'OPENAI_BASE_URL:\s*str\s*=\s*"([^"]+)"' -Label "config/settings.py OPENAI_BASE_URL"
        Example = Get-RegexValue -Text $envExampleText -Pattern '^OPENAI_BASE_URL=(.+)$' -Label ".env.example OPENAI_BASE_URL"
        WebPs = $null
    },
    @{
        Name = "OPENAI_BACKUP_BASE_URL"
        Settings = Get-RegexValue -Text $settingsText -Pattern 'OPENAI_BACKUP_BASE_URL:\s*str\s*=\s*"([^"]*)"' -Label "config/settings.py OPENAI_BACKUP_BASE_URL"
        Example = Get-RegexValue -Text $envExampleText -Pattern '^OPENAI_BACKUP_BASE_URL=(.*)$' -Label ".env.example OPENAI_BACKUP_BASE_URL"
        WebPs = $null
    },
    @{
        Name = "ANALYTICS_HISTORY_ENABLED"
        Settings = Get-RegexValue -Text $settingsText -Pattern 'ANALYTICS_HISTORY_ENABLED:\s*bool\s*=\s*(True|False)' -Label "config/settings.py ANALYTICS_HISTORY_ENABLED"
        Example = Get-RegexValue -Text $envExampleText -Pattern '^ANALYTICS_HISTORY_ENABLED=(.+)$' -Label ".env.example ANALYTICS_HISTORY_ENABLED"
        WebPs = if ($webPsText -match 'default off unless -EnableAnalyticsHistory') { 'default off' } else { throw "scripts/web.ps1 is missing analytics-history default-off text." }
    },
    @{
        Name = "ALLOW_PERSISTED_LIVE_MODE_START"
        Settings = Get-RegexValue -Text $settingsText -Pattern 'ALLOW_PERSISTED_LIVE_MODE_START:\s*bool\s*=\s*(True|False)' -Label "config/settings.py ALLOW_PERSISTED_LIVE_MODE_START"
        Example = Get-RegexValue -Text $envExampleText -Pattern '^ALLOW_PERSISTED_LIVE_MODE_START=(.+)$' -Label ".env.example ALLOW_PERSISTED_LIVE_MODE_START"
        WebPs = if ($webPsText -match '\[switch\]\$AllowPersistedLiveMode') { 'supported' } else { throw "scripts/web.ps1 is missing -AllowPersistedLiveMode." }
        StartWebPs = if ($startWebPsText -match '\[switch\]\$AllowPersistedLiveMode') { 'supported' } else { throw "scripts/start_web_ps.ps1 is missing -AllowPersistedLiveMode." }
        OncePs = if ($oncePsText -match 'Set-Item -Path Env:ALLOW_PERSISTED_LIVE_MODE_START') { 'managed-override' } else { throw "_once.ps1 is missing managed ALLOW_PERSISTED_LIVE_MODE_START override." }
    }
)

Assert-Equal -Name "WEB_HOST (settings vs .env.example)" -Expected "127.0.0.1" -Actual $checks[0].Settings
Assert-Equal -Name "WEB_HOST (.env.example)" -Expected "127.0.0.1" -Actual $checks[0].Example
Assert-Equal -Name "WEB_HOST (scripts/web.ps1)" -Expected "127.0.0.1" -Actual $checks[0].WebPs
Assert-Equal -Name "WEB_HOST (scripts/start_web_ps.ps1)" -Expected "127.0.0.1" -Actual $checks[0].StartWebPs
Assert-Equal -Name "WEB_HOST (_once.ps1)" -Expected "127.0.0.1" -Actual $checks[0].OncePs

Assert-Equal -Name "OPENAI_BASE_URL (settings)" -Expected "https://api.openai.com/v1" -Actual $checks[1].Settings
Assert-Equal -Name "OPENAI_BASE_URL (.env.example)" -Expected "https://api.openai.com/v1" -Actual $checks[1].Example

Assert-Equal -Name "OPENAI_BACKUP_BASE_URL (settings)" -Expected "" -Actual $checks[2].Settings
Assert-Equal -Name "OPENAI_BACKUP_BASE_URL (.env.example)" -Expected "" -Actual $checks[2].Example

Assert-Equal -Name "ANALYTICS_HISTORY_ENABLED (settings)" -Expected "False" -Actual $checks[3].Settings
Assert-Equal -Name "ANALYTICS_HISTORY_ENABLED (.env.example)" -Expected "false" -Actual $checks[3].Example.ToLowerInvariant()
Assert-Equal -Name "ANALYTICS_HISTORY_ENABLED (scripts/web.ps1)" -Expected "default off" -Actual $checks[3].WebPs

Assert-Equal -Name "ALLOW_PERSISTED_LIVE_MODE_START (settings)" -Expected "False" -Actual $checks[4].Settings
Assert-Equal -Name "ALLOW_PERSISTED_LIVE_MODE_START (.env.example)" -Expected "false" -Actual $checks[4].Example.ToLowerInvariant()
Assert-Equal -Name "ALLOW_PERSISTED_LIVE_MODE_START (scripts/web.ps1)" -Expected "supported" -Actual $checks[4].WebPs
Assert-Equal -Name "ALLOW_PERSISTED_LIVE_MODE_START (scripts/start_web_ps.ps1)" -Expected "supported" -Actual $checks[4].StartWebPs
Assert-Equal -Name "ALLOW_PERSISTED_LIVE_MODE_START (_once.ps1)" -Expected "managed-override" -Actual $checks[4].OncePs

Write-Host ""
Write-Host "Configuration contract checks passed." -ForegroundColor Green
