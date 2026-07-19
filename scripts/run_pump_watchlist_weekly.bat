@echo off
rem Weekly pump-precursor watchlist refresh (registered as scheduled task
rem CryptoTradingSystem_PumpWatchlistWeekly, Mondays 08:30).
rem Logs to logs\pump_watchlist_weekly.log; Coinglass budget-paced (~15 min).
setlocal
cd /d F:\9_Crypto\crypto_trading_system
set PYEXE=F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe
if not exist logs mkdir logs
echo [%date% %time%] pump watchlist weekly run start >> logs\pump_watchlist_weekly.log
"%PYEXE%" -u scripts\generate_pump_watchlist.py >> logs\pump_watchlist_weekly.log 2>&1
echo [%date% %time%] pump watchlist weekly run exit %errorlevel% >> logs\pump_watchlist_weekly.log
endlocal
