@echo off
REM Sync Book Schema Data
REM Run this to manually sync performance data to book tables

cd C:\Users\Roci\unified-northlight
python scripts\run_book_sync.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Sync completed successfully!
) else (
    echo.
    echo Sync failed! Check the error messages above.
)

pause
