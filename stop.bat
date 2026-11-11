@echo off
rem latbot stop: creates logs\stop.flag and waits for the bot to exit.
setlocal
cd /d "%~dp0"
if not exist logs mkdir logs
echo stop > logs\stop.flag
echo [latbot] stop requested (logs\stop.flag)
set /a N=0
:wait
if not exist logs\bot.pid goto stopped
set /a N+=1
if %N% geq 30 goto timeout
timeout /t 1 /nobreak >nul
goto wait
:timeout
echo [latbot] still running after 30 s, check the console window
goto end
:stopped
echo [latbot] stopped.
:end
endlocal
