@echo off
setlocal
set PYTHON_EXE=C:\Users\laoj0\AppData\Local\Programs\Python\Python314\python.exe
set SCRIPT=%~dp0tools\ga_backend_server.py
for /f %%I in ('netstat -ano ^| findstr /R /C:":8765 .*LISTENING"') do (
  echo GA backend port 8765 is already in use. Please close the existing backend first or verify /health.
  pause
  exit /b 1
)
if not exist "%PYTHON_EXE%" (
  echo Python not found: %PYTHON_EXE%
  pause
  exit /b 1
)
if not exist "%SCRIPT%" (
  echo Script not found: %SCRIPT%
  pause
  exit /b 1
)
echo Starting GA backend on http://127.0.0.1:8765
"%PYTHON_EXE%" "%SCRIPT%"
