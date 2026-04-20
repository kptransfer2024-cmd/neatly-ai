"""Dataset management routes: CRUD for data sources."""
from typing import List
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from src.api.deps import get_db, get_current_user
from src.db.models import User, Dataset

router = APIRouter()


class DatasetCreate(BaseModel):
    """Create a new dataset."""

    name: str
    source_type: str  # 'upload', 'postgres', 's3', 'bigquery', etc.
    source_config: dict  # {'bucket': 'x', 'key': 'y'} or {'host': '...', 'database': '...'}
    schedule_cron: str | None = None  # '0 * * * *' for hourly, None for manual
    alert_threshold: float = 80.0


class DatasetUpdate(BaseModel):
    """Update a dataset."""

    name: str | None = None
    schedule_cron: str | None = None
    alert_threshold: float | None = None


class DatasetResponse(BaseModel):
    """Dataset response."""

    id: int
    name: str
    source_type: str
    schedule_cron: str | None
    alert_threshold: float
    created_at: str

    class Config:
        from_attributes = True


@router.get("/datasets", response_model=List[DatasetResponse])
def list_datasets(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all datasets for the current user."""
    datasets = db.query(Dataset).filter(Dataset.user_id == current_user.id).all()
    return datasets


@router.post("/datasets", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
def create_dataset(
    dataset_data: DatasetCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Register a new data source for monitoring."""
    # Check plan tier limits (free: 1 dataset, pro: 10, business: unlimited)
    if current_user.plan_tier == "free":
        existing_count = db.query(Dataset).filter(Dataset.user_id == current_user.id).count()
        if existing_count >= 1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Free tier limited to 1 dataset. Upgrade to Pro.",
            )
    elif current_user.plan_tier == "pro":
        existing_count = db.query(Dataset).filter(Dataset.user_id == current_user.id).count()
        if existing_count >= 10:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Pro tier limited to 10 datasets. Upgrade to Business.",
            )

    # Validate cron expression if provided
    if dataset_data.schedule_cron:
        from apscheduler.triggers.cron import CronTrigger
        try:
            CronTrigger.from_crontab(dataset_data.schedule_cron)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid cron expression: {str(e)}",
            )

    new_dataset = Dataset(
        user_id=current_user.id,
        name=dataset_data.name,
        source_type=dataset_data.source_type,
        source_config=dataset_data.source_config,
        schedule_cron=dataset_data.schedule_cron,
        alert_threshold=dataset_data.alert_threshold,
    )
    db.add(new_dataset)
    db.commit()
    db.refresh(new_dataset)
    return new_dataset


@router.get("/datasets/{dataset_id}", response_model=DatasetResponse)
def get_dataset(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get a specific dataset (owner only)."""
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == current_user.id,
    ).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )
    return dataset


@router.patch("/datasets/{dataset_id}", response_model=DatasetResponse)
def update_dataset(
    dataset_id: int,
    dataset_data: DatasetUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update a dataset (owner only)."""
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == current_user.id,
    ).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )

    # Update fields if provided
    if dataset_data.name is not None:
        dataset.name = dataset_data.name
    if dataset_data.schedule_cron is not None:
        dataset.schedule_cron = dataset_data.schedule_cron
    if dataset_data.alert_threshold is not None:
        dataset.alert_threshold = dataset_data.alert_threshold

    db.commit()
    db.refresh(dataset)
    return dataset


@router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Delete a dataset (owner only)."""
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == current_user.id,
    ).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )

    db.delete(dataset)
    db.commit()
