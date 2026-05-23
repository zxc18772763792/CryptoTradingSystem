param(
    [int]$LogRetentionDays = 30,
    [int]$MaxLogFileMB = 50,
    [int]$MaxRotatedLogFiles = 3,
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

    $removed = $false

    try {
        Remove-Item -LiteralPath $resolvedTarget -Recurse -Force -ErrorAction Stop
    }
    catch {
        Write-Warning "unable to remove: $resolvedTarget ($($_.Exception.Message))"
        return $false
    }

    if (-not (Test-Path -LiteralPath $resolvedTarget)) {
        $removed = $true
    }

    if ($removed) {
        Write-Host "removed: $resolvedTarget"
        return $true
    }

    Write-Warning "unable to remove: $resolvedTarget"
    return $false
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

function Rotate-LargeFiles {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string[]]$Patterns,
        [Parameter(Mandatory = $true)]
        [int]$MaxSizeMB,
        [Parameter(Mandatory = $true)]
        [int]$KeepCount
    )

    if ($MaxSizeMB -le 0) {
        return
    }

    $resolvedPath = Resolve-ProjectPath -Target $Path
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    $maxBytes = [int64]$MaxSizeMB * 1MB

    foreach ($pattern in $Patterns) {
        $largeFiles = Get-ChildItem -Path $resolvedPath -File -Filter $pattern -ErrorAction SilentlyContinue |
            Where-Object { $_.Length -gt $maxBytes -and $_.BaseName -notmatch "\.\d{8}T\d{6}$" }

        foreach ($file in $largeFiles) {
            $stamp = Get-Date -Format "yyyyMMddTHHmmss"
            $newName = "{0}.{1}{2}" -f $file.BaseName, $stamp, $file.Extension
            $newPath = Join-Path $file.DirectoryName $newName

            if (Test-Path -LiteralPath $newPath) {
                $newName = "{0}.{1}.{2}{3}" -f $file.BaseName, $stamp, (Get-Random), $file.Extension
                $newPath = Join-Path $file.DirectoryName $newName
            }

            if ($DryRun) {
                Write-Host ("[DRY-RUN] rotate: {0} -> {1} ({2:N2} MB)" -f $file.FullName, $newPath, ($file.Length / 1MB))
            }
            else {
                try {
                    Rename-Item -LiteralPath $file.FullName -NewName $newName -ErrorAction Stop
                    Write-Host ("rotated: {0} -> {1} ({2:N2} MB)" -f $file.FullName, $newPath, ($file.Length / 1MB))
                }
                catch {
                    Write-Warning "unable to rotate: $($file.FullName) ($($_.Exception.Message))"
                    continue
                }
            }

            if ($KeepCount -ge 0) {
                $rotatedNamePattern = "^{0}\.\d{{8}}T\d{{6}}(\.\d+)?{1}$" -f [regex]::Escape($file.BaseName), [regex]::Escape($file.Extension)
                $oldRotations = Get-ChildItem -Path $file.DirectoryName -File -ErrorAction SilentlyContinue |
                    Where-Object { $_.Name -match $rotatedNamePattern } |
                    Sort-Object LastWriteTime -Descending |
                    Select-Object -Skip $KeepCount

                foreach ($oldRotation in $oldRotations) {
                    Remove-PathSafely -Target $oldRotation.FullName | Out-Null
                }
            }
        }
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
    $reservedNamePath = "\\?\$projectRoot\nul"
    if ($DryRun) {
        Write-Host "[DRY-RUN] remove reserved file: $reservedNamePath (if present)"
    }
    elseif (Test-Path -LiteralPath $reservedNamePath) {
        Remove-Item -LiteralPath $reservedNamePath -Force -ErrorAction Stop
        Write-Host "removed: nul"
    }
    else {
        Write-Host "reserved file not present: nul"
    }
}
catch {
    Write-Host "skip removing nul: $($_.Exception.Message)"
}

# Rotate large logs, prune old logs, and clear empty log shells
$threshold = (Get-Date).AddDays(-[math]::Abs($LogRetentionDays))

@(
    @{ Path = "."; Patterns = @(".tmp_uvicorn_*.log", "web_start*.log") },
    @{ Path = "logs"; Patterns = @("*.log", "*.out", "*.err", "*.jsonl", "*.txt") },
    @{ Path = "runtime"; Patterns = @("*.log", "*.out", "*.err") }
) | ForEach-Object {
    Rotate-LargeFiles -Path $_.Path -Patterns $_.Patterns -MaxSizeMB $MaxLogFileMB -KeepCount $MaxRotatedLogFiles
    Remove-OldFiles -Path $_.Path -Patterns $_.Patterns -Threshold $threshold
    Remove-ZeroLengthLogs -Path $_.Path
}

if ($IncludeOutput) {
    @(
        "output\.playwright-cli",
        "output\playwright"
    ) | ForEach-Object { Remove-PathSafely -Target $_ | Out-Null }

    Remove-OldFiles -Path "output" -Patterns @("*.log", "*.out", "*.err", "*.png", "*.yml") -Threshold $threshold
    Rotate-LargeFiles -Path "output" -Patterns @("*.log", "*.out", "*.err") -MaxSizeMB $MaxLogFileMB -KeepCount $MaxRotatedLogFiles
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
