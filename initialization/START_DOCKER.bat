@echo off
REM Data Visualization Platform - Docker Startup Script

echo.
echo ========================================
echo   Data Visualization Platform
echo   Docker Startup Script
echo ========================================
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not installed or not in PATH
    echo Please install Docker Desktop from: https://www.docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)

echo [✓] Docker found
echo.

REM Get the directory where this script is located
cd /d "%~dp0.."

echo Starting services...
echo.

REM Remove stale containers from previous runs
echo Cleaning up stale containers...
for %%C in (datavis_db datavis_api datavis_frontend) do (
    docker rm -f %%C >nul 2>&1
)
docker-compose down --remove-orphans >nul 2>&1
echo.

REM Start all services
docker-compose up -d

echo.
echo [✓] Waiting for services to initialize...
timeout /t 3 /nobreak

echo.
echo Services Status:
docker-compose ps

echo.
echo ========================================
echo   Services are starting up...
echo ========================================
echo.
echo Frontend:  http://localhost:3000
echo Backend:   http://localhost:8001
echo Database:  localhost:3306
echo.
echo Opening frontend in browser...
timeout /t 2 /nobreak

REM Try to open the frontend in default browser
start http://localhost:3000

echo.
echo To view logs, run:
echo   docker-compose logs -f
echo.
echo To stop services, run:
echo   docker-compose down
echo.
echo For more info, see DOCKER_SETUP.md
echo.
pause
