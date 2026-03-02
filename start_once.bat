@echo off
setlocal
cd /d "%~dp0"

echo [CryptoTradingSystem] starting web service...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_once.ps1"
set EXITCODE=%ERRORLEVEL%

if not "%EXITCODE%"=="0" (
  echo.
  echo Startup failed with exit code %EXITCODE%.
  echo Check conda env "crypto_trading" and your .env config.
  pause
)

endlocal
