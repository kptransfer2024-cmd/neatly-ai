"""Application configuration and settings."""
import os
from pydantic_settings import BaseSettings

_DEV_SECRET = "dev-secret-key-change-in-production"


class Settings(BaseSettings):
    """Application settings from environment variables."""

    # API
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Neatly — Data Quality API"
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"

    # CORS — comma-separated list; "*" only acceptable in dev
    CORS_ORIGINS: list[str] = ["*"]

    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./neatly.db")
    SQLALCHEMY_ECHO: bool = os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"

    # Auth
    SECRET_KEY: str = os.getenv("SECRET_KEY", _DEV_SECRET)
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # Claude API
    CLAUDE_MODEL: str = "claude-sonnet-4-6"
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    # File Storage
    UPLOAD_DIR: str = os.getenv("UPLOAD_DIR", "./uploads")

    class Config:
        case_sensitive = True

    def __init__(self, **data):
        super().__init__(**data)
        if not self.DEBUG and self.SECRET_KEY == _DEV_SECRET:
            raise ValueError(
                "SECRET_KEY must be set to a random value in production. "
                "Set the SECRET_KEY environment variable."
            )


settings = Settings()
