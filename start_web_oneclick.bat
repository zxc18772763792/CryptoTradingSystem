@echo off
setlocal

set "ROOT=%~dp0"
set "ENTRY=%ROOT%web.bat"

if not exist "%ENTRY%" (
  echo [ERROR] Script not found: "%ENTRY%"
  exit /b 1
)

echo [INFO] One-click launcher: ".\start_web_oneclick.bat"
echo [INFO] Canonical command family: ".\web.bat ..."
echo [INFO] Effective command: ".\web.bat start -OpenBrowser"
echo [INFO] Managed profile: web + news worker + news LLM worker
echo [INFO] Optional agent: add "-StartAutonomousAgent" when you explicitly want the AI autonomous agent started too
echo [INFO] Help: ".\web.bat help"
echo [INFO] Startup doc: "STARTUP.md"
echo [INFO] Log file: "%ROOT%logs\web_ps.log"

call "%ENTRY%" start -OpenBrowser %*
set "CODE=%ERRORLEVEL%"

if not "%CODE%"=="0" (
  echo.
  echo [ERROR] Startup failed with exit code %CODE%.
  echo [HINT] Check log: "%ROOT%logs\web_ps.log"
  exit /b %CODE%
)

echo.
echo [INFO] Dashboard: http://127.0.0.1:8000
echo [INFO] News:      http://127.0.0.1:8000/news
echo [INFO] Status:    .\web.bat status
echo [INFO] Stop:      .\web.bat stop -IncludeWorkers
echo [INFO] Agent:     .\web.bat start -StartAutonomousAgent
echo [INFO] Docs:      STARTUP.md

exit /b 0
