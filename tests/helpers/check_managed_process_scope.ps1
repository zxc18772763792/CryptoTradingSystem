$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Port = 8000
. (Join-Path $projectRoot 'scripts\managed_process.ps1')
if ((Get-ManagedInstanceName $projectRoot 8000) -eq (Get-ManagedInstanceName "$projectRoot-other" 8000)) { throw 'Project task/mutex identity collision' }
if ((Get-ManagedInstanceName $projectRoot 8000) -eq (Get-ManagedInstanceName $projectRoot 8001)) { throw 'Port task/mutex identity collision' }
function Import-FunctionsOnly($Path, $Names) {
    $tokens = $null; $errors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw 'PowerShell parse errors' }
    foreach ($node in $ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]}, $false)) {
        if ($node.Name -in $Names) {
            Set-Item -Path "Function:script:$($node.Name)" -Value ([scriptblock]::Create($node.Body.Extent.Text.TrimStart('{').TrimEnd('}')))
        }
    }
}
Import-FunctionsOnly (Join-Path $projectRoot 'scripts\web.ps1') @('Get-ObservedWorkerProcesses', 'Get-ManagedWebProcesses', 'Test-IsManagedWebProcess')
function Record($Root, $WebPort, $Module, $ProcessId) {
    [pscustomobject]@{Name='python.exe'; ProcessId=$ProcessId; CommandLine="python.exe `"$(Join-Path $Root scripts\managed_entry.py)`" --instance-port $WebPort --module $Module"}
}
$script:records = @(
    (Record $projectRoot 8000 'uvicorn' 101), (Record $projectRoot 8001 'uvicorn' 102),
    (Record "$projectRoot-other" 8000 'uvicorn' 103),
    (Record $projectRoot 8000 'core.news.service.worker' 201),
    (Record $projectRoot 8001 'core.news.service.worker' 202),
    (Record "$projectRoot-other" 8000 'core.news.service.worker' 203)
)
function Get-CimInstance { param($ClassName, $ErrorAction) return $script:records }
$web = @(Get-ManagedWebProcesses)
$workers = @(Get-ObservedWorkerProcesses 'core.news.service.worker')
if ($web.Count -ne 1 -or $web[0].ProcessId -ne 101) { throw 'Web ownership boundary failed' }
if ($workers.Count -ne 1 -or $workers[0].ProcessId -ne 201) { throw 'Worker ownership boundary failed' }
Import-FunctionsOnly (Join-Path $projectRoot 'scripts\supervise_web.ps1') @('Get-MatchingPythonProcesses', 'Get-WebProcesses')
if (@(Get-WebProcesses).Count -ne 1) { throw 'Supervisor Web scope failed' }
if (@(Get-MatchingPythonProcesses 'core.news.service.worker').Count -ne 1) { throw 'Supervisor worker scope failed' }
Write-Output 'Managed process project/port isolation PASS'
