@echo off
REM Quick ETL Pipeline Runner
REM Always runs from the correct directory

REM Set working directory to script location
cd /d "%~dp0"

REM Run the ETL pipeline
python run_etl_pipeline.py

REM Pause to see results (comment out for scheduled tasks)
pause
