#!/bin/bash

set -e

echo "🚀 Starting Neatly — Data Quality Monitoring SaaS"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo "Creating from .env.example..."
    cp .env.example .env
    echo "Please edit .env and set ANTHROPIC_API_KEY, then run this script again."
    exit 1
fi

# Check if ANTHROPIC_API_KEY is set
if ! grep -q "^ANTHROPIC_API_KEY=sk-ant-" .env; then
    echo "❌ Error: ANTHROPIC_API_KEY not set in .env"
    exit 1
fi

# Load environment
set -a
source .env
set +a

# Build and start with Docker Compose
echo "📦 Building Docker images..."
docker-compose build

echo ""
echo "✅ Starting services..."
docker-compose up -d

echo ""
echo "🎉 Services started!"
echo ""
echo "📊 Dashboard: http://localhost:8501"
echo "🔧 API: http://localhost:8000"
echo "📚 API Docs: http://localhost:8000/docs"
echo ""
echo "View logs with: docker-compose logs -f"
echo "Stop with: docker-compose down"
