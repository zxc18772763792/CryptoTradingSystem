@echo off
setlocal

set "ROOT=%~dp0"
set "PS1=%ROOT%scripts\start_web_ps.ps1"

if not exist "%PS1%" (
  echo [ERROR] Script not found: "%PS1%"
  exit /b 1
)

echo [INFO] Starting web service...
echo [INFO] Log file: "%ROOT%logs\web_ps.log"

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "CODE=%ERRORLEVEL%"

if not "%CODE%"=="0" (
  echo.
  echo [ERROR] Startup failed with exit code %CODE%.
  echo [HINT] Check log: "%ROOT%logs\web_ps.log"
  exit /b %CODE%
)

exit /b 0
