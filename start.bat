@echo off
rem latbot launcher: runs the bot and restarts it after a crash.
rem Exit code 0 = requested stop (stop.bat, Ctrl+C, SECONDS elapsed),
rem exit code 3 = another instance is already running; both end the loop.
setlocal
cd /d "%~dp0"
title latbot
:loop
echo [latbot] starting...
python -u run.py
set RC=%errorlevel%
if "%RC%"=="0" goto done
if "%RC%"=="3" goto done
echo [latbot] exited with code %RC%, restart in 5 s (Ctrl+C to abort)
timeout /t 5 /nobreak >nul
goto loop
:done
echo [latbot] stopped.
endlocal
