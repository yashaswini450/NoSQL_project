@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Start-Demo.ps1" -Action mapreduce -BatchSize 500000
pause
