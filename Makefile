.PHONY: help install test run-api run-dashboard dev docker-build docker-up docker-down docker-logs clean

help:
	@echo "Neatly — Data Quality Monitoring SaaS"
	@echo ""
	@echo "Local Development:"
	@echo "  make install      - Install Python dependencies"
	@echo "  make test         - Run all tests"
	@echo "  make run-api      - Start FastAPI server (port 8000)"
	@echo "  make run-dashboard- Start Streamlit dashboard (port 8501)"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build - Build Docker images"
	@echo "  make docker-up    - Start containers (docker-compose)"
	@echo "  make docker-down  - Stop containers"
	@echo "  make docker-logs  - View container logs"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean        - Remove cache files"

install:
	pip install -r requirements.txt
	@echo "Dependencies installed!"

test:
	pytest tests/ -v --tb=short

test-multitenancy:
	pytest tests/test_api_multitenancy.py -v

test-e2e:
	pytest tests/test_e2e_workflow.py -v

run-api:
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

run-dashboard:
	streamlit run src/dashboard/streamlit_app.py

dev:
	@echo "Starting development servers..."
	@echo ""
	@echo "Terminal 1: run 'make run-api'"
	@echo "Terminal 2: run 'make run-dashboard'"
	@echo ""
	@echo "API:       http://localhost:8000"
	@echo "Dashboard: http://localhost:8501"
	@echo "API Docs:  http://localhost:8000/docs"

docker-build:
	docker-compose build
	@echo "Docker images built!"

docker-up:
	docker-compose up -d
	@echo ""
	@echo "Containers started!"
	@echo "API:       http://localhost:8000"
	@echo "Dashboard: http://localhost:8501"
	@echo "API Docs:  http://localhost:8000/docs"
	@echo ""
	@echo "View logs with: make docker-logs"

docker-down:
	docker-compose down
	@echo "Containers stopped!"

docker-logs:
	docker-compose logs -f

docker-logs-api:
	docker-compose logs -f api

docker-logs-dashboard:
	docker-compose logs -f dashboard

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -f .pytest_cache
	@echo "Cache cleaned!"

.DEFAULT_GOAL := help
