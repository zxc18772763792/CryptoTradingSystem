@echo off
setlocal

set "ROOT=%~dp0"
set "PS1=%ROOT%scripts\web.ps1"

if not exist "%PS1%" (
  echo [ERROR] Script not found: "%PS1%"
  exit /b 1
)

if "%~1"=="" (
  echo [INFO] One-click startup: ".\web.bat"
  echo [INFO] Effective command: ".\web.bat start -OpenBrowser"
  echo [INFO] Managed profile: web + news worker + news LLM worker
  echo [INFO] Optional agent: ".\web.bat start -StartAutonomousAgent"
  echo [INFO] Help: ".\web.bat help"
  echo [INFO] Startup doc: "STARTUP.md"
  echo.
  powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" start -OpenBrowser
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
)
set "CODE=%ERRORLEVEL%"

endlocal & exit /b %CODE%
