# Deprecated: this used to run an unmanaged `python main.py --mode web` bound to 0.0.0.0.
# Use the managed entry instead (127.0.0.1, supervised workers, no pid-file leaks).
$ErrorActionPreference = "Stop"
Write-Host "[DEPRECATED] scripts/dev_web.ps1 now forwards to: web.bat start" -ForegroundColor Yellow
Write-Host "             See STARTUP.md > 入口总览." -ForegroundColor Yellow
& (Join-Path (Split-Path -Parent $PSScriptRoot) "web.bat") start @args
exit $LASTEXITCODE
