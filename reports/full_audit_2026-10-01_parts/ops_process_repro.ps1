# Extract only pure process-selection functions; every OS operation is mocked.
$ErrorActionPreference = 'Stop'
$projectAuditRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function Get-AuditFunction([string]$Path, [string]$Name) {
    $auditTokens = $null
    $auditErrors = $null
    $auditAst = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$auditTokens, [ref]$auditErrors)
    $auditNode = $auditAst.Find({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $Name }, $true)
    if (-not $auditNode) { throw "Missing function $Name" }
    return [scriptblock]::Create($auditNode.Extent.Text)
}
$fakeProcesses = @(
    [pscustomobject]@{Name='python.exe'; CommandLine='python.exe -m uvicorn web.main:app --port 8000'; ProcessId=11001; CreationDate=[datetime]'2026-09-01'; TestProject='main'},
    [pscustomobject]@{Name='python.exe'; CommandLine='python.exe -m uvicorn web.main:app --port 8001'; ProcessId=11002; CreationDate=[datetime]'2026-09-02'; TestProject='radar'},
    [pscustomobject]@{Name='python.exe'; CommandLine='python.exe -m core.news.service.worker'; ProcessId=12001; CreationDate=[datetime]'2026-09-01'; TestProject='main'},
    [pscustomobject]@{Name='python.exe'; CommandLine='python.exe -m core.news.service.worker'; ProcessId=12002; CreationDate=[datetime]'2026-09-02'; TestProject='radar'}
)
function Get-CimInstance { param($ClassName, $ErrorAction) return $fakeProcesses }
$stoppedFakeIds = [System.Collections.Generic.List[int]]::new()
function Stop-Process { param([int]$Id, [switch]$Force, $ErrorAction) $stoppedFakeIds.Add($Id) }
function Write-SupervisorLog { param($Message, $Level) }
function Write-SupervisorState { param($Status, $Component, $Detail) }
$workerMissingStreaks = @{}
. (Get-AuditFunction (Join-Path $projectAuditRoot 'scripts/web.ps1') 'Get-ManagedWebProcesses')
. (Get-AuditFunction (Join-Path $projectAuditRoot 'scripts/web.ps1') 'Get-ObservedWorkerProcesses')
. (Get-AuditFunction (Join-Path $projectAuditRoot 'scripts/supervise_web.ps1') 'Get-MatchingPythonProcesses')
. (Get-AuditFunction (Join-Path $projectAuditRoot 'scripts/supervise_web.ps1') 'Start-MissingWorker')
$selectedWeb = @(Get-ManagedWebProcesses)
$selectedWorkers = @(Get-ObservedWorkerProcesses 'core.news.service.worker')
$workerResult = Start-MissingWorker -Label 'News' -Module 'core.news.service.worker'
$result = [ordered]@{
    requested_port=8000
    selected_web_ports=@($selectedWeb | ForEach-Object { if ($_.ProcessId -eq 11001) {8000} else {8001} })
    selected_worker_projects=@($selectedWorkers.TestProject)
    duplicate_cleanup_stopped_fake_process_ids=@($stoppedFakeIds)
    all_os_process_operations_mocked=$true
}
if ($selectedWeb.Count -ne 2 -or $selectedWorkers.Count -ne 2 -or $stoppedFakeIds[0] -ne 12002) { throw 'Unexpected reproduction result' }
$result | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'ops_process_repro.json') -Encoding utf8
$result | ConvertTo-Json -Depth 5
