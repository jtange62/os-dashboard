@echo off
cd /d "%~dp0"

set PY=
where py >nul 2>nul && set PY=py -3
if "%PY%"=="" where python >nul 2>nul && set PY=python
if "%PY%"=="" (
  echo Python 3 not found. Install from https://python.org/downloads/
  pause
  exit /b 1
)

%PY% -c "import psycopg" >nul 2>nul
if errorlevel 1 (
  echo Installing psycopg ...
  %PY% -m pip install --quiet "psycopg[binary]"
)

echo Starting Offerings Dashboard...
start "" http://localhost:8766/
%PY% -u server.py
