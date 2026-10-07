@echo off
rem ============================================================
rem  latbot one-click launcher (Windows)
rem  First run: checks Python, installs dependencies, creates .env
rem  and opens it in Notepad (paste TELEGRAM_BOT_TOKEN), then starts
rem  the bot and opens the terminal at http://127.0.0.1:8787
rem  Later runs: just start (and auto-restart after a crash).
rem  Stop: stop.bat (or close this window).
rem ============================================================
setlocal
cd /d "%~dp0"
title latbot

rem ---- 1. find Python 3.11+ -----------------------------------
set PY=
py -3.13 -c "import sys" >nul 2>&1 && set PY=py -3.13
if "%PY%"=="" py -3.12 -c "import sys" >nul 2>&1 && set PY=py -3.12
if "%PY%"=="" py -3.11 -c "import sys" >nul 2>&1 && set PY=py -3.11
if "%PY%"=="" python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1 && set PY=python
if "%PY%"=="" py -3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1 && set PY=py -3
if "%PY%"=="" (
  echo [latbot] Python 3.11 or newer was not found.
  echo [latbot] Install it from https://www.python.org/downloads/windows/
  echo [latbot] IMPORTANT: tick "Add python.exe to PATH" in the installer, then run start.bat again.
  pause
  exit /b 1
)
echo [latbot] using: %PY%

rem ---- 2. dependencies (first run or when requirements.txt changed) ----
set NEED_PIP=0
if not exist .deps_ok set NEED_PIP=1
rem (to force a reinstall: delete the .deps_ok file)
if "%NEED_PIP%"=="1" (
  echo [latbot] installing dependencies, this takes a minute...
  %PY% -m pip install --upgrade pip >nul 2>&1
  %PY% -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [latbot] dependency installation failed. Check the internet connection and run start.bat again.
    pause
    exit /b 1
  )
  echo ok > .deps_ok
)

rem ---- 3. .env with the Telegram token --------------------------
if not exist .env (
  copy /y .env.example .env >nul
  echo.
  echo [latbot] Notepad will open the .env file now.
  echo [latbot] Paste your bot token after TELEGRAM_BOT_TOKEN=  (no spaces, no quotes),
  echo [latbot] optionally TELEGRAM_CHAT_ID=, then SAVE (Ctrl+S) and CLOSE Notepad.
  echo [latbot] Exchange API keys are NOT needed for PAPER mode.
  echo.
  notepad .env
)

rem ---- 4. open the terminal in the browser after the server is up ----
start "" /b cmd /c "timeout /t 8 /nobreak >nul && start http://127.0.0.1:8787"

rem ---- 5. run (restart after a crash) ----------------------------
:loop
echo [latbot] starting... terminal: http://127.0.0.1:8787   (stop: stop.bat)
%PY% -u run.py
set RC=%errorlevel%
if "%RC%"=="0" goto done
if "%RC%"=="3" (
  echo [latbot] another latbot instance is already running. Use stop.bat first.
  pause
  goto done
)
echo [latbot] exited with code %RC%, restart in 5 s (Ctrl+C to abort)
timeout /t 5 /nobreak >nul
goto loop
:done
echo [latbot] stopped.
endlocal
