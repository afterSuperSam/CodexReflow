@echo off
cd /d "%~dp0"
python -X utf8 "%~dp0monitor.py" stop
if errorlevel 1 pause
