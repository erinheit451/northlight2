@echo off
REM Unified Northlight Multi-Report Extractor
REM Downloads all 10 reports with comprehensive error handling

echo ========================================
echo Unified Northlight Multi-Report Extractor
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
    echo Please create .env file with required credentials:
    echo   CORP_PORTAL_USERNAME=your_username
    echo   CORP_PORTAL_PASSWORD=your_password
    echo   SF_USERNAME=your_sf_username
    echo   SF_PASSWORD=your_sf_password
    pause
    exit /b 1
)

REM Install required packages if needed
echo Checking dependencies...
python -c "import playwright, dotenv" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install playwright python-dotenv requests
    playwright install chromium
)

REM Set working directory to script location
cd /d "%~dp0"

REM Run the extraction pipeline
echo Starting extraction pipeline...
python run_all_extractors.py

REM Capture exit code
set EXTRACTION_EXIT_CODE=%errorlevel%

echo.
echo ========================================
if %EXTRACTION_EXIT_CODE% equ 0 (
    echo SUCCESS: All extractors completed successfully
    echo Check data/raw/ for downloaded files
) else if %EXTRACTION_EXIT_CODE% equ 1 (
    echo PARTIAL: Some extractors failed
    echo Check logs/etl.log for details
) else if %EXTRACTION_EXIT_CODE% equ 130 (
    echo INTERRUPTED: Process was interrupted by user
) else (
    echo FAILED: Extraction pipeline failed
    echo Check logs/etl.log and alerts/alerts.jsonl for details
)
echo ========================================

pause
exit /b %EXTRACTION_EXIT_CODE%