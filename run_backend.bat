@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo Python is not installed or not added to PATH.
  echo Install Python from https://www.python.org/downloads/ and tick "Add Python to PATH".
  pause
  exit /b 1
)

echo Starting Smart Resource Allocation backend...
echo Backend URL: http://127.0.0.1:8000
python server.py
pause
