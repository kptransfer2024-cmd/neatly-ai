"""FastAPI application factory."""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.core.config import settings
from src.db.session import init_db
from src.api.routes import auth, datasets, diagnoses
from src.api.scheduler import create_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    # Startup: initialize database
    init_db()

    # Create and start scheduler
    scheduler = create_scheduler()
    scheduler.start()
    app.state.scheduler = scheduler

    # Load existing scheduled jobs from database
    # TODO: Load dataset schedules from DB on startup

    yield

    # Shutdown: stop scheduler
    if hasattr(app.state, "scheduler"):
        app.state.scheduler.shutdown()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS middleware for frontend integration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register route modules
    app.include_router(auth.router, prefix=f"{settings.API_V1_STR}/auth", tags=["auth"])
    app.include_router(datasets.router, prefix=f"{settings.API_V1_STR}", tags=["datasets"])
    app.include_router(diagnoses.router, prefix=f"{settings.API_V1_STR}", tags=["diagnoses"])

    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "ok"}

    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
