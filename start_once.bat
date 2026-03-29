@echo off
setlocal

set "ROOT=%~dp0"
set "ENTRY=%ROOT%web.bat"

if not exist "%ENTRY%" (
  echo [ERROR] Script not found: "%ENTRY%"
  exit /b 1
)

echo [WARN] start_once.bat is deprecated.
echo [INFO] Forwarding to ".\web.bat start" ...
call "%ENTRY%" start %*
set "CODE=%ERRORLEVEL%"

endlocal & exit /b %CODE%
