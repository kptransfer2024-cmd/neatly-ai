"""FastAPI dependencies for database sessions and authentication."""
from typing import Generator
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from jwt import decode as jwt_decode
from jwt.exceptions import InvalidTokenError

from src.db.session import SessionLocal
from src.core.config import settings
from src.db.models import User

security = HTTPBearer()


def get_db() -> Generator[Session, None, None]:
    """Dependency to get a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(
    credentials = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    """Validate JWT token and return the current user."""
    token = credentials.credentials

    try:
        payload = jwt_decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        try:
            user_id = int(payload.get("sub"))
        except (TypeError, ValueError):
            raise InvalidTokenError("Invalid user ID in token")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user
