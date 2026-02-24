param(
    [Parameter(Mandatory = $false)]
    $LogRetentionDays = 7,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$retentionDays = 7
try {
    $retentionDays = [int]($LogRetentionDays | Select-Object -First 1)
}
catch {
    $retentionDays = 7
}

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Remove-PathSafely {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Target
    )
    if (-not (Test-Path -LiteralPath $Target)) {
        return
    }
    if ($DryRun) {
        Write-Host "[DRY-RUN] remove: $Target"
        return
    }
    Remove-Item -LiteralPath $Target -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "removed: $Target"
}

Write-Host "Cleaning repository artifacts..."

# Python/test caches
Get-ChildItem -Path . -Recurse -Force -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-PathSafely -Target $_.FullName }

Get-ChildItem -Path . -Recurse -Force -File -Include "*.pyc", "*.pyo" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-PathSafely -Target $_.FullName }

Remove-PathSafely -Target ".pytest_cache"
Remove-PathSafely -Target ".mypy_cache"

# Remove accidental Windows reserved-name file if present in listing
try {
    cmd /c "del /f /q \\?\$projectRoot\nul" | Out-Null
    if (-not $DryRun) {
        Write-Host "removed: nul (if existed)"
    }
}
catch {
    Write-Host "skip removing nul: $($_.Exception.Message)"
}

# Rotate old logs only
if (Test-Path -LiteralPath "logs") {
    $threshold = (Get-Date).AddDays(-[math]::Abs($retentionDays))
    Get-ChildItem -Path "logs" -File -Filter "*.log" -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $threshold } |
        ForEach-Object { Remove-PathSafely -Target $_.FullName }
}

Write-Host "Cleanup completed."
