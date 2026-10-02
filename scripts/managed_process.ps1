# Pure predicates only: dot-sourcing this file never enumerates or stops processes.
function Get-ManagedInstanceName {
    param([string]$ProjectRoot, [int]$InstancePort)
    $canonical = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\').ToLowerInvariant()
    $hasher = [Security.Cryptography.SHA256]::Create()
    try {
        $hash = [BitConverter]::ToString($hasher.ComputeHash([Text.Encoding]::UTF8.GetBytes($canonical))).Replace('-', '').Substring(0, 16)
    } finally { $hasher.Dispose() }
    return "CryptoTradingSystem_WebSupervisor_${hash}_$InstancePort"
}

function Test-ManagedPythonInstance {
    param($ProcessRecord, [string]$ProjectRoot, [int]$InstancePort, [string]$Module)
    if (-not $ProcessRecord) { return $false }
    if ([string]$ProcessRecord.Name -notin @("python.exe", "pythonw.exe")) { return $false }
    $cmd = [string]$ProcessRecord.CommandLine
    $entry = [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'scripts\managed_entry.py'))
    $entryPattern = '(?i)(?:^|\s)"?' + [regex]::Escape($entry) + '"?(?:\s|$)'
    $portPattern = '(?i)(?:^|\s)--instance-port\s+' + $InstancePort + '(?:\s|$)'
    $modulePattern = '(?i)(?:^|\s)--module\s+' + [regex]::Escape($Module) + '(?:\s|$)'
    return $cmd -match $entryPattern -and $cmd -match $portPattern -and $cmd -match $modulePattern
}

function Stop-ManagedPythonInstance {
    param($ProcessRecord, [string]$ProjectRoot, [int]$InstancePort, [string]$Module)
    if (-not $ProcessRecord) { return }
    $current = Get-CimInstance Win32_Process -Filter "ProcessId=$([int]$ProcessRecord.ProcessId)" -ErrorAction SilentlyContinue
    if (-not (Test-ManagedPythonInstance $current $ProjectRoot $InstancePort $Module)) { return }
    if ($ProcessRecord.CreationDate -and $current.CreationDate -ne $ProcessRecord.CreationDate) { return }
    Stop-Process -Id $current.ProcessId -Force -ErrorAction SilentlyContinue
}
