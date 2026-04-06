@echo off
setlocal

set "ROOT=%~dp0"
set "ENTRY=%ROOT%start_web_oneclick.bat"

if not exist "%ENTRY%" (
  echo [ERROR] Script not found: "%ENTRY%"
  exit /b 1
)

echo [WARN] start_once.bat is deprecated.
echo [INFO] Forwarding to ".\start_web_oneclick.bat" ...
call "%ENTRY%" %*
set "CODE=%ERRORLEVEL%"

if "%CODE%"=="0" (
  echo [INFO] Use ".\web.bat status" to verify mode and worker state.
)

endlocal & exit /b %CODE%
