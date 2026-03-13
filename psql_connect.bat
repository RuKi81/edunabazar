@echo off
setlocal

set PROJECT_DIR=C:\Users\kiva_\PyCharmMiscProject
cd /d "%PROJECT_DIR%"

echo Connecting to PostgreSQL using psql...

rem You can override these variables in the current console before запуск:
rem   set DB_HOST=127.0.0.1
rem   set DB_PORT=5432
rem   set DB_USER=postgres
rem   set DB_PASSWORD=...
rem   set DB_NAME=enb

if "%DB_HOST%"=="" set DB_HOST=127.0.0.1
if "%DB_PORT%"=="" set DB_PORT=5432
if "%DB_USER%"=="" set DB_USER=postgres
if "%DB_NAME%"=="" set DB_NAME=enb_DB

set PSQL_EXE=psql

where psql >nul 2>nul
if not "%ERRORLEVEL%"=="0" (
  if not "%PG_BIN%"=="" if exist "%PG_BIN%\psql.exe" set PSQL_EXE=%PG_BIN%\psql.exe

  if "%PSQL_EXE%"=="psql" if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" set PSQL_EXE=C:\Program Files\PostgreSQL\17\bin\psql.exe
  if "%PSQL_EXE%"=="psql" if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" set PSQL_EXE=C:\Program Files\PostgreSQL\16\bin\psql.exe
  if "%PSQL_EXE%"=="psql" if exist "C:\Program Files\PostgreSQL\15\bin\psql.exe" set PSQL_EXE=C:\Program Files\PostgreSQL\15\bin\psql.exe
  if "%PSQL_EXE%"=="psql" if exist "C:\OpenServer\modules\PostgreSQL-18\bin\psql.exe" set PSQL_EXE=C:\OpenServer\modules\PostgreSQL-18\bin\psql.exe
  if "%PSQL_EXE%"=="psql" if exist "C:\OpenServer\modules\PostgreSQL-17\bin\psql.exe" set PSQL_EXE=C:\OpenServer\modules\PostgreSQL-17\bin\psql.exe
  if "%PSQL_EXE%"=="psql" if exist "C:\OpenServer\modules\PostgreSQL-16\bin\psql.exe" set PSQL_EXE=C:\OpenServer\modules\PostgreSQL-16\bin\psql.exe
  if "%PSQL_EXE%"=="psql" if exist "C:\Program Files\QGIS 3.44.6\bin\psql.exe" set PSQL_EXE=C:\Program Files\QGIS 3.44.6\bin\psql.exe
)

if not exist "%PSQL_EXE%" (
  rem if PSQL_EXE is just 'psql', exist check will fail; handle separately
  where psql >nul 2>nul
  if not "%ERRORLEVEL%"=="0" (
    echo.
    echo ERROR: psql not found.
    echo - Add PostgreSQL bin folder to PATH, or set PG_BIN, e.g.:
    echo   set PG_BIN=C:\Program Files\PostgreSQL\16\bin
    echo.
    pause
    endlocal
    exit /b 1
  )
)

echo Host: %DB_HOST%
echo Port: %DB_PORT%
echo User: %DB_USER%
echo DB:   %DB_NAME%
echo.
echo Tip: If password is required, set DB_PASSWORD env var before run.
echo.

set PGPASSWORD=%DB_PASSWORD%

%PSQL_EXE% -h "%DB_HOST%" -p "%DB_PORT%" -U "%DB_USER%" -d "%DB_NAME%"

echo.
echo psql finished with exit code %ERRORLEVEL%.
pause

endlocal
