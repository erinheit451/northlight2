@echo off
REM Complete ETL Pipeline Runner
REM Runs extraction, loading, and analytics refresh

echo ========================================
echo Unified Northlight Complete ETL Pipeline
echo ========================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo ERROR: .env file not found
    echo Please create .env file with required credentials
    pause
    exit /b 1
)

REM Set working directory to script location
cd /d "%~dp0"

REM Get today's date in YYYY-MM-DD format
for /f "tokens=1-3 delims=/" %%a in ('date /t') do set today=%%c-%%a-%%b
REM Fix date format for different locale
for /f %%i in ('powershell -command "Get-Date -Format 'yyyy-MM-dd'"') do set today=%%i

REM Run the complete ETL pipeline
echo Starting complete ETL pipeline for date: %today%
python scripts/runners/run_etl_pipeline.py --date %today%

REM Capture exit code
set ETL_EXIT_CODE=%errorlevel%

echo.
echo ========================================
if %ETL_EXIT_CODE% equ 0 (
    echo SUCCESS: Complete ETL pipeline completed successfully
    echo - Data extracted from all sources
    echo - Data loaded into PostgreSQL database
    echo - Analytics views refreshed
    echo Check logs/etl.log for details
) else (
    echo PARTIAL/FAILED: ETL pipeline completed with errors
    echo Check logs/etl.log for details
)
echo ========================================

exit /b %ETL_EXIT_CODE%