@echo off
cd /d "%~dp0"
python -X utf8 "%~dp0monitor.py" start
if errorlevel 1 pause
