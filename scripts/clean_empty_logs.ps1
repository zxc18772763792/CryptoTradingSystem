param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"

$targets = @(
    $ProjectRoot,
    (Join-Path $ProjectRoot "logs"),
    (Join-Path $ProjectRoot "runtime")
) | Select-Object -Unique

$deleted = New-Object System.Collections.Generic.List[string]

foreach ($target in $targets) {
    if (-not (Test-Path $target)) { continue }
    $files = Get-ChildItem -Path $target -File -ErrorAction SilentlyContinue | Where-Object {
        $_.Length -eq 0 -and $_.Extension -in @(".log", ".out", ".err")
    }
    foreach ($file in $files) {
        Remove-Item -LiteralPath $file.FullName -Force -ErrorAction SilentlyContinue
        $deleted.Add($file.FullName) | Out-Null
    }
}

if ($deleted.Count -eq 0) {
    Write-Host "No empty log files found."
    exit 0
}

Write-Host ("Deleted {0} empty log files:" -f $deleted.Count)
$deleted | Sort-Object | ForEach-Object { Write-Host (" - " + $_) }
