@echo off
set "DEMO_FOLDER=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$p = (wsl -d Ubuntu-24.04 --exec wslpath -a -u $env:DEMO_FOLDER).Trim(); if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; wsl -d Ubuntu-24.04 --cd $p --exec bash demo.sh; exit $LASTEXITCODE"
pause
