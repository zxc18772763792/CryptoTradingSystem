@echo off
setlocal
cd /d "%~dp0"

echo ============================================================
echo   CryptoTradingSystem - Web Service Launcher
echo ============================================================
echo.
echo   Data Sources Available:
echo     - Funding Rate (Binance, Bybit, OKX, Gate)
echo     - Fear ^& Greed Index
echo     - Order Book Level 2
echo     - Open Interest
echo.
echo   Tip: Set TEST_DATA_SOURCES=1 to test API connectivity
echo ============================================================
echo.

REM Check for test data sources flag
if "%TEST_DATA_SOURCES%"=="1" (
    echo [TEST MODE] Will test data sources after startup...
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_once.ps1" -TestDataSources $true
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_once.ps1"
)
set EXITCODE=%ERRORLEVEL%

if not "%EXITCODE%"=="0" (
  echo.
  echo Startup failed with exit code %EXITCODE%.
  echo Check conda env "crypto_trading" and your .env config.
  pause
)

endlocal
