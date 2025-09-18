@echo off
echo =========================================================
echo UNIFIED NORTHLIGHT PLATFORM SETUP
echo =========================================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python not found. Please install Python 3.9+ first.
    echo Download from: https://python.org/downloads/
    pause
    exit /b 1
)

echo Step 1: Setting up Python virtual environment...
python -m venv venv
if %errorlevel% neq 0 (
    echo Error: Failed to create virtual environment
    pause
    exit /b 1
)

echo Step 2: Activating virtual environment...
call venv\Scripts\activate.bat

echo Step 3: Upgrading pip...
python -m pip install --upgrade pip

echo Step 4: Installing dependencies...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo Error: Failed to install dependencies
    pause
    exit /b 1
)

echo Step 5: Starting database services...
call scripts\start_services.bat

echo Step 6: Testing database connection...
python scripts\test_connection.py
if %errorlevel% neq 0 (
    echo Warning: Database connection test failed
    echo Please check Docker and database configuration
)

echo Step 7: Running data migration (if needed)...
python scripts\data_migration.py
if %errorlevel% neq 0 (
    echo Warning: Data migration had issues
    echo This is normal if no legacy data exists
)

echo.
echo =========================================================
echo SETUP COMPLETE
echo =========================================================
echo.
echo The Unified Northlight Platform is now ready!
echo.
echo Next steps:
echo 1. Start the application: python main.py
echo 2. Run ETL pipeline: python scripts\run_unified_etl.py
echo 3. View dashboard: http://localhost:8000/dashboard
echo 4. View API docs: http://localhost:8000/docs
echo.
echo For help:
echo - Check README.md for detailed documentation
echo - View logs in the logs/ directory
echo - Use --help flag with scripts for options
echo.
pause