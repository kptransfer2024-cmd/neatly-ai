# Neatly Deployment & Testing Guide

## Quick Start (Docker)

### 1. Setup environment
```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### 2. Start everything
```bash
# Option A: Using shell script (Linux/Mac)
bash start.sh

# Option B: Using Make
make docker-up

# Option C: Manual Docker Compose
docker-compose up -d
```

### 3. Access the app
- **Dashboard**: http://localhost:8501
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

### 4. Stop
```bash
docker-compose down
```

---

## Local Development (No Docker)

### 1. Install dependencies
```bash
make install
# or: pip install -r requirements.txt
```

### 2. Start API (Terminal 1)
```bash
make run-api
# API runs on http://localhost:8000
```

### 3. Start Dashboard (Terminal 2)
```bash
make run-dashboard
# Dashboard runs on http://localhost:8501
```

### 4. View API docs
Open http://localhost:8000/docs

---

## Testing

### Run all tests
```bash
make test
```

### Run specific test suites
```bash
make test-multitenancy    # Multi-tenancy enforcement (8 tests)
make test-e2e             # End-to-end workflow (3 tests)
```

### Run individual test
```bash
pytest tests/test_api_multitenancy.py::test_user_can_only_see_own_datasets -v
```

---

## Available Make Commands

```bash
make help              # Show all commands
make install           # Install dependencies
make test              # Run all tests
make run-api           # Start API server
make run-dashboard     # Start Streamlit dashboard
make dev               # Show development setup
make docker-build      # Build Docker images
make docker-up         # Start containers
make docker-down       # Stop containers
make docker-logs       # View all logs
make docker-logs-api   # View API logs only
make clean             # Remove cache files
```

---

## Docker Details

### Services
- **api**: FastAPI backend (port 8000)
- **dashboard**: Streamlit frontend (port 8501)

### Volumes
- Database persists in `neatly.db`
- Python cache cleaned between runs

### Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api
docker-compose logs -f dashboard
```

---

## Configuration

### Environment Variables (.env)
```
ANTHROPIC_API_KEY=sk-ant-...          # Required: Claude API key
SECRET_KEY=dev-secret-key             # For JWT tokens
DATABASE_URL=sqlite:///./neatly.db    # SQLite (default) or PostgreSQL
DEBUG=true                             # Debug mode
```

See `.env.example` for all options.

---

## Workflow

### Register & Login
1. Open http://localhost:8501
2. Register with email/password
3. Login with credentials

### Upload Data
1. Go to "Datasets" tab
2. Upload a CSV file
3. Wait for analysis (shows quality score)

### View Results
1. Go to "Runs" tab
2. Select dataset
3. View quality trend chart
4. Click "View Issues" to see problems detected

### Manage Account
1. Go to "Settings" tab
2. View session info
3. Logout

---

## Troubleshooting

### API won't start
```bash
# Check if port 8000 is in use
lsof -i :8000
# Kill process: kill -9 <PID>
```

### Dashboard won't connect to API
- Check API is running: `http://localhost:8000/health`
- Verify API URL in dashboard (should be `http://localhost:8000/api/v1`)

### Database errors
```bash
# Reset database
rm neatly.db
docker-compose restart api
```

### Clear cache
```bash
make clean
```

---

## Production Deployment

For production, consider:
1. Use PostgreSQL instead of SQLite
2. Set proper `SECRET_KEY` (random string)
3. Disable `DEBUG=false`
4. Use HTTPS with reverse proxy (nginx)
5. Add rate limiting
6. Set up monitoring/logging

See deployment docs for specifics.
