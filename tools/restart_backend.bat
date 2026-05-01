@echo off
setlocal
title Whale Scheduler - Restart Backend

set "SERVICE_SCRIPT=%~dp0start_services.ps1"

echo ========================================
echo   Whale Scheduler - Restart Backend
echo ========================================
echo.

if not exist "%SERVICE_SCRIPT%" (
  echo [ERROR] Service starter not found: %SERVICE_SCRIPT%
  pause
  exit /b 1
)

echo [1/3] Killing listeners on port 8765...
set "KILLED=0"
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8765 ^| findstr LISTENING') do (
  echo   - killing PID %%a
  taskkill /PID %%a /F >nul 2>&1
  set "KILLED=1"
)
if "%KILLED%"=="0" echo   - no existing listener
echo.

echo [2/3] Waiting for port release...
timeout /t 2 /nobreak >nul
echo.

echo [3/3] Starting services...
powershell -NoProfile -ExecutionPolicy Bypass -File "%SERVICE_SCRIPT%"
set "PS_EXIT=%ERRORLEVEL%"
echo.

if not "%PS_EXIT%"=="0" (
  echo [WARN] Service startup returned exit code %PS_EXIT%.
)

echo Restart command finished.
pause
