@echo off
setlocal

set "ROOT=%~dp0"
set "PY=%ROOT%.venv\Scripts\python.exe"

if not exist "%PY%" (
  echo [ERROR] No se encontro Python del entorno virtual en:
  echo         %PY%
  exit /b 1
)

"%PY%" -m diagramador_optimizado.pre_entrega_check --mode standard
exit /b %errorlevel%

