$ErrorActionPreference = 'Stop'
$auditRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$auditFiles = @(& git -C $auditRoot ls-files -- '*.ps1') + @(& git -C $auditRoot ls-files --others --exclude-standard -- '*.ps1')
$auditResults = foreach ($auditName in ($auditFiles | Sort-Object -Unique)) {
    $auditPath = Join-Path $auditRoot $auditName
    if (-not (Test-Path -LiteralPath $auditPath -PathType Leaf)) { continue }
    $auditTokens = $null
    $auditErrors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile($auditPath, [ref]$auditTokens, [ref]$auditErrors)
    [pscustomobject]@{ file = $auditName; errors = @($auditErrors | ForEach-Object { [pscustomobject]@{ line = $_.Extent.StartLineNumber; message = $_.Message } }) }
}
$auditReport = [pscustomobject]@{ files = @($auditResults).Count; error_files = @($auditResults | Where-Object { $_.errors.Count -gt 0 }).Count; results = @($auditResults) }
$auditReport | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'powershell_syntax.json') -Encoding UTF8
Write-Output ('PowerShell parsed files: {0}, error files: {1}' -f $auditReport.files, $auditReport.error_files)
if ($auditReport.error_files -gt 0) { exit 1 }
