@echo off
setlocal

set "ROOT=%~dp0"
set "ENTRY=%ROOT%web.bat"

if not exist "%ENTRY%" (
  echo [ERROR] Script not found: "%ENTRY%"
  exit /b 1
)

echo [INFO] Preferred command: ".\web.bat start"
echo [INFO] Starting web service...
echo [INFO] Log file: "%ROOT%logs\web_ps.log"

call "%ENTRY%" start %*
set "CODE=%ERRORLEVEL%"

if not "%CODE%"=="0" (
  echo.
  echo [ERROR] Startup failed with exit code %CODE%.
  echo [HINT] Check log: "%ROOT%logs\web_ps.log"
  exit /b %CODE%
)

exit /b 0
