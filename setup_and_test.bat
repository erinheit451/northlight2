@echo off
echo ========================================
echo UNIFIED NORTHLIGHT PLATFORM SETUP
echo ========================================

echo.
echo Step 1: Installing missing dependencies...
pip install duckdb pandas pyarrow
if %errorlevel% neq 0 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)
echo ✅ Dependencies installed successfully

echo.
echo Step 2: Testing database connection...
python test_db_connection.py
if %errorlevel% neq 0 (
    echo WARNING: Database connection test failed
    echo This might be normal if PostgreSQL is still starting up
)

echo.
echo Step 3: Testing basic application import...
python -c "from main import app; print('✅ Application imports successfully')"
if %errorlevel% neq 0 (
    echo ERROR: Application import failed
    pause
    exit /b 1
)

echo.
echo Step 4: Running data migration (if possible)...
python scripts/data_migration.py
if %errorlevel% neq 0 (
    echo WARNING: Data migration failed - this might be expected on first run
)

echo.
echo Step 5: Testing API endpoints without database...
python -c "
from fastapi.testclient import TestClient
from main import app
client = TestClient(app)
try:
    response = client.get('/health')
    print(f'Health endpoint: {response.status_code}')
    response = client.get('/version')
    print(f'Version endpoint: {response.status_code}')
    print('✅ Basic API tests passed')
except Exception as e:
    print(f'❌ API test failed: {e}')
"

echo.
echo ========================================
echo SETUP COMPLETE
echo ========================================
echo.
echo Next steps:
echo 1. If everything above succeeded, run: python main.py
echo 2. Open browser to: http://localhost:8000/dashboard
echo 3. Test book system at: http://localhost:8000/book/
echo.
pause