@echo off
rem Creates a "Latency Bot" shortcut to start.bat on the Desktop.
setlocal
cd /d "%~dp0"
powershell -NoProfile -Command "$d=[Environment]::GetFolderPath('Desktop'); $s=(New-Object -ComObject WScript.Shell).CreateShortcut((Join-Path $d 'Latency Bot.lnk')); $s.TargetPath='%~dp0start.bat'; $s.WorkingDirectory='%~dp0'; $s.Description='latbot terminal'; $s.Save(); Write-Host 'shortcut created on the Desktop'"
pause
endlocal
