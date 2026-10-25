$ErrorActionPreference = 'Stop'
$auditOutput = $PSScriptRoot
$auditInventory = Get-Content -LiteralPath (Join-Path $auditOutput 'inventory.json') -Raw | ConvertFrom-Json
$auditSnapshot = Get-Content -LiteralPath (Join-Path $auditOutput 'snapshot.json') -Raw | ConvertFrom-Json
$auditResults = @()
foreach ($auditFile in $auditInventory.files) {
    if ($auditFile.suffix -ne '.ps1') { continue }
    $auditTokens = $null
    $auditErrors = $null
    $auditPath = Join-Path $auditSnapshot.source $auditFile.path
    $null = [System.Management.Automation.Language.Parser]::ParseFile($auditPath, [ref]$auditTokens, [ref]$auditErrors)
    $auditResults += [pscustomobject]@{
        file = $auditFile.path
        errors = @($auditErrors | ForEach-Object { [pscustomobject]@{line=$_.Extent.StartLineNumber; message=$_.Message; id=$_.ErrorId} })
    }
}
$auditReport = [pscustomobject]@{
    files = $auditResults.Count
    error_files = @($auditResults | Where-Object { $_.errors.Count -gt 0 }).Count
    results = $auditResults
}
$auditReport | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $auditOutput 'powershell_syntax.json') -Encoding UTF8
Write-Output ('PowerShell parsed files: {0}, error files: {1}' -f $auditReport.files, $auditReport.error_files)
