"""SQLAlchemy ORM models for the Neatly API."""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    """User account with authentication."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    plan_tier = Column(String, default="free")  # free, pro, business
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    datasets = relationship("Dataset", back_populates="owner")


class Dataset(Base):
    """A registered data source for continuous monitoring."""

    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    name = Column(String, index=True)
    source_type = Column(String)  # 'upload', 'postgres', 's3', etc.
    source_config = Column(JSON)  # {bucket: 'x', key: 'y'} or {dsn: '...'} etc.
    schedule_cron = Column(String, nullable=True)  # '0 * * * *' for hourly
    alert_threshold = Column(Float, default=80.0)  # quality_score below this → alert
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    owner = relationship("User", back_populates="datasets")
    runs = relationship("DiagnosisRun", back_populates="dataset")


class DiagnosisRun(Base):
    """A single data quality diagnosis run."""

    __tablename__ = "diagnosis_runs"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), index=True)
    started_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, default="pending")  # pending, running, success, failed
    quality_score = Column(Float, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    result_json = Column(JSON)  # full DiagnosisResult blob for historical replay

    dataset = relationship("Dataset", back_populates="runs")
    issues = relationship("Issue", back_populates="run")


class Issue(Base):
    """A data quality issue detected in a diagnosis run."""

    __tablename__ = "issues"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(Integer, ForeignKey("diagnosis_runs.id"), index=True)
    detector_name = Column(String)  # e.g., 'missing_value_detector'
    issue_type = Column(String)  # e.g., 'missing_value'
    column_name = Column(String, nullable=True)
    severity = Column(String)  # low, medium, high
    description = Column(String)  # summary of the issue
    explanation = Column(String)  # plain-English explanation from Claude
    resolved_at = Column(DateTime, nullable=True)

    run = relationship("DiagnosisRun", back_populates="issues")
