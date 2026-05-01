@echo off
setlocal
title Whale Scheduler - Start GA Backend

set "PYTHON_EXE=C:\Users\laoj0\AppData\Local\Programs\Python\Python314\python.exe"
set "SCRIPT=%~dp0ga_backend_server.py"

echo ========================================
echo   Whale Scheduler - Start GA Backend
echo ========================================
echo.

if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python not found: %PYTHON_EXE%
  pause
  exit /b 1
)

if not exist "%SCRIPT%" (
  echo [ERROR] Backend script not found: %SCRIPT%
  pause
  exit /b 1
)

echo [1/3] Checking skopt (dynamic tuning dependency)...
"%PYTHON_EXE%" -c "import importlib.util,sys;sys.exit(0 if importlib.util.find_spec('skopt') else 2)"
if errorlevel 1 (
  echo [ERROR] skopt is missing. Dynamic tuning will not work.
  echo         Run: "%PYTHON_EXE%" -m pip install scikit-optimize
  pause
  exit /b 2
)
echo [OK] skopt is available.
echo.

echo [2/3] Checking port 8765...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr /R /C:":8765 .*LISTENING"') do (
  echo [ERROR] Port 8765 is already used by PID %%a
  echo         Close old backend first, or run restart_backend.bat
  pause
  exit /b 1
)
echo [OK] Port 8765 is free.
echo.

echo [3/3] Starting backend on http://127.0.0.1:8765 ...
"%PYTHON_EXE%" "%SCRIPT%"
