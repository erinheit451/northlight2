@echo off
echo Starting Unified Northlight Platform Services...

REM Check if Docker is available
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker not found. Please install Docker Desktop for Windows.
    echo Download from: https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

REM Start services with Docker Compose
echo Starting PostgreSQL and Redis containers...
docker-compose up -d

REM Wait for services to start
echo Waiting for services to initialize...
timeout /t 10 /nobreak >nul

REM Check service status
echo Checking service status...
docker-compose ps

echo.
echo Services started successfully!
echo - PostgreSQL: localhost:5432
echo - Redis: localhost:6379
echo - Database: unified_northlight
echo - Username: app_user
echo.
echo To stop services: docker-compose down
echo To view logs: docker-compose logs -f
echo.
pause