@echo off
rem latbot restart: stop (if running) and start again.
setlocal
cd /d "%~dp0"
call stop.bat
call start.bat
endlocal
