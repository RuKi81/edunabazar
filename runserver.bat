@echo off
setlocal

set PROJECT_DIR=C:\Users\kiva_\PyCharmMiscProject

cd /d "%PROJECT_DIR%"

set PY_EXE=
if exist "%PROJECT_DIR%\.venv\Scripts\python.exe" set PY_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
if exist "%PROJECT_DIR%\venv\Scripts\python.exe" set PY_EXE=%PROJECT_DIR%\venv\Scripts\python.exe
if exist "%PROJECT_DIR%\.venv313\Scripts\python.exe" set PY_EXE=%PROJECT_DIR%\.venv313\Scripts\python.exe
if "%PY_EXE%"=="" if exist "C:\Users\kiva_\AppData\Local\Python\pythoncore-3.14-64\python.exe" set PY_EXE=C:\Users\kiva_\AppData\Local\Python\pythoncore-3.14-64\python.exe
if "%PY_EXE%"=="" set PY_EXE=python

echo Starting Django dev server...
echo Project: %CD%
echo Python: %PY_EXE%
if "%PY_EXE%"=="python" (
  where python
)
echo URL: http://127.0.0.1:8000/
echo Press Ctrl+C to stop.

%PY_EXE% manage.py runserver

echo.
echo Django server process finished with exit code %ERRORLEVEL%.
if not "%ERRORLEVEL%"=="0" (
  echo.
  echo Tip: It looks like Django isn't installed in the Python you're using.
  echo Recommended: create a venv in the project folder and install dependencies.
  echo Example:
  echo   py -3.13 -m venv .venv
  echo   .venv\Scripts\pip install django pymysql
)
echo If the window closed too quickly before, the error message should be visible above now.
pause

endlocal
