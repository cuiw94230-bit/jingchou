@echo off
setlocal
title 鲸筹调度求解器 - 同步店铺经纬度与详细地址

set "PYTHON_EXE=C:\Users\laoj0\AppData\Local\Programs\Python\Python314\python.exe"
set "SCRIPT=%~dp0sync_shop_geo_from_remote.py"

if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python not found: %PYTHON_EXE%
  pause
  exit /b 1
)
if not exist "%SCRIPT%" (
  echo [ERROR] Script not found: %SCRIPT%
  pause
  exit /b 1
)

echo Running sync...
"%PYTHON_EXE%" "%SCRIPT%"
echo.
pause
