@echo off
setlocal enabledelayedexpansion

echo.
echo 🚀 Starting Neatly — Data Quality Monitoring SaaS
echo.

REM Check if .env exists
if not exist .env (
    echo ⚠️  .env file not found!
    echo Creating from .env.example...
    copy .env.example .env
    echo.
    echo Please edit .env and set ANTHROPIC_API_KEY, then run this script again.
    pause
    exit /b 1
)

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Error: Docker is not installed or not in PATH
    echo Please install Docker Desktop from: https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

echo 📦 Building Docker images...
docker-compose build

if errorlevel 1 (
    echo ❌ Build failed!
    pause
    exit /b 1
)

echo.
echo ✅ Starting services...
docker-compose up -d

if errorlevel 1 (
    echo ❌ Start failed!
    pause
    exit /b 1
)

echo.
echo 🎉 Services started!
echo.
echo 📊 Dashboard:  http://localhost:8501
echo 🔧 API:        http://localhost:8000
echo 📚 API Docs:   http://localhost:8000/docs
echo.
echo View logs with: docker-compose logs -f
echo Stop with: docker-compose down
echo.
pause
