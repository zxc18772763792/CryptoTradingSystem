param(
    [int]$LogRetentionDays = 7,
    [switch]$DryRun,
    [switch]$IncludeOutput,
    [switch]$IncludeNodeModules
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Resolve-ProjectPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Target
    )

    $candidate = if ([System.IO.Path]::IsPathRooted($Target)) {
        [System.IO.Path]::GetFullPath($Target)
    }
    else {
        [System.IO.Path]::GetFullPath((Join-Path $projectRoot $Target))
    }

    $rootWithSeparator = $projectRoot.TrimEnd('\') + '\'
    if ($candidate -ne $projectRoot -and -not $candidate.StartsWith($rootWithSeparator, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to touch path outside project root: $Target"
    }

    return $candidate
}

function Remove-PathSafely {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Target
    )

    $resolvedTarget = Resolve-ProjectPath -Target $Target
    if (-not (Test-Path -LiteralPath $resolvedTarget)) {
        return $false
    }

    if ($DryRun) {
        Write-Host "[DRY-RUN] remove: $resolvedTarget"
        return $true
    }

    Remove-Item -LiteralPath $resolvedTarget -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "removed: $resolvedTarget"
    return $true
}

function Remove-OldFiles {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string[]]$Patterns,
        [Parameter(Mandatory = $true)]
        [datetime]$Threshold
    )

    $resolvedPath = Resolve-ProjectPath -Target $Path
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    foreach ($pattern in $Patterns) {
        Get-ChildItem -Path $resolvedPath -File -Filter $pattern -ErrorAction SilentlyContinue |
            Where-Object { $_.LastWriteTime -lt $Threshold } |
            ForEach-Object { Remove-PathSafely -Target $_.FullName | Out-Null }
    }
}

function Remove-ZeroLengthLogs {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $resolvedPath = Resolve-ProjectPath -Target $Path
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    Get-ChildItem -Path $resolvedPath -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Length -eq 0 -and $_.Extension -in @('.log', '.out', '.err') } |
        ForEach-Object { Remove-PathSafely -Target $_.FullName | Out-Null }
}

function Remove-MatchingFiles {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string[]]$Patterns
    )

    $resolvedPath = Resolve-ProjectPath -Target $Path
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    foreach ($pattern in $Patterns) {
        Get-ChildItem -Path $resolvedPath -Force -File -Filter $pattern -ErrorAction SilentlyContinue |
            ForEach-Object { Remove-PathSafely -Target $_.FullName | Out-Null }
    }
}

function Remove-OldDirectories {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string[]]$Names,
        [Parameter(Mandatory = $true)]
        [datetime]$Threshold
    )

    $resolvedPath = Resolve-ProjectPath -Target $Path
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    Get-ChildItem -Path $resolvedPath -Force -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -in $Names -and $_.LastWriteTime -lt $Threshold } |
        ForEach-Object { Remove-PathSafely -Target $_.FullName | Out-Null }
}

Write-Host "Cleaning repository artifacts..."

# Python/test caches
Get-ChildItem -Path . -Recurse -Force -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-PathSafely -Target $_.FullName | Out-Null }

Get-ChildItem -Path . -Recurse -Force -File -Include "*.pyc", "*.pyo" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-PathSafely -Target $_.FullName | Out-Null }

@(
    ".pytest_cache",
    ".mypy_cache",
    ".pytest_tmp",
    ".playwright-cli",
    "MagicMock",
    "test-results",
    "tmp"
) | ForEach-Object { Remove-PathSafely -Target $_ | Out-Null }

Remove-MatchingFiles -Path "." -Patterns @(
    ".tmp_uvicorn_*.log",
    "web_start*.log",
    "playwright_eval_tmp.js",
    ".~lock*#"
)

# Remove accidental Windows reserved-name file if present in listing
try {
    if ($DryRun) {
        Write-Host "[DRY-RUN] remove reserved file: \\?\$projectRoot\nul (if present)"
    }
    else {
        cmd /c "del /f /q \\?\$projectRoot\nul >nul 2>&1" | Out-Null
        Write-Host "removed: nul (if existed)"
    }
}
catch {
    Write-Host "skip removing nul: $($_.Exception.Message)"
}

# Rotate old logs and clear empty log shells
$threshold = (Get-Date).AddDays(-[math]::Abs($LogRetentionDays))

@(
    @{ Path = "."; Patterns = @(".tmp_uvicorn_*.log", "web_start*.log") },
    @{ Path = "logs"; Patterns = @("*.log", "*.out", "*.err", "*.jsonl", "*.txt") },
    @{ Path = "runtime"; Patterns = @("*.log", "*.out", "*.err") }
) | ForEach-Object {
    Remove-OldFiles -Path $_.Path -Patterns $_.Patterns -Threshold $threshold
    Remove-ZeroLengthLogs -Path $_.Path
}

if ($IncludeOutput) {
    @(
        "output\.playwright-cli",
        "output\playwright"
    ) | ForEach-Object { Remove-PathSafely -Target $_ | Out-Null }

    Remove-OldFiles -Path "output" -Patterns @("*.log", "*.out", "*.err", "*.png", "*.yml") -Threshold $threshold
    Remove-ZeroLengthLogs -Path "output"
    Remove-OldDirectories -Path "output" -Names @(
        ".playwright-cli",
        "service",
        "exit_refactor",
        "exit_refactor_cci",
        "exit_refactor_cci_v2",
        "exit_refactor_chunk_13_20",
        "exit_refactor_chunk_13_20_v2",
        "exit_refactor_chunk_15_20",
        "exit_refactor_chunk_21_30",
        "exit_refactor_chunk_31_41",
        "exit_refactor_fama_smoke",
        "exit_refactor_final",
        "exit_refactor_full",
        "exit_refactor_single_ma_partial",
        "exit_refactor_smoke",
        "exit_refactor_stochrsi"
    ) -Threshold $threshold
}

if ($IncludeNodeModules) {
    if ((-not (Test-Path -LiteralPath "package.json")) -and (Test-Path -LiteralPath "node_modules")) {
        Remove-PathSafely -Target "node_modules" | Out-Null
    }
}

Write-Host "Cleanup completed."
