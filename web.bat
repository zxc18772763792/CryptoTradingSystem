@echo off
setlocal

set "ROOT=%~dp0"
set "PS1=%ROOT%scripts\web.ps1"

if not exist "%PS1%" (
  echo [ERROR] Script not found: "%PS1%"
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "CODE=%ERRORLEVEL%"

endlocal & exit /b %CODE%
