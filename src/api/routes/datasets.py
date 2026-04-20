"""Dataset management routes: CRUD for data sources."""
from typing import List
from datetime import datetime
from pydantic import BaseModel, ConfigDict, field_serializer
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session

from src.api.deps import get_db, get_current_user
from src.db.models import User, Dataset
from src.api.scheduler import add_dataset_schedule

router = APIRouter()


class DatasetCreate(BaseModel):
    """Create a new dataset."""

    name: str
    source_type: str  # 'upload', 'postgres', 's3', 'bigquery', etc.
    source_config: dict  # {'bucket': 'x', 'key': 'y'} or {'host': '...', 'database': '...'}
    schedule_cron: str | None = None  # '0 * * * *' for hourly, None for manual
    alert_threshold: float = 80.0
    alert_webhook_url: str | None = None  # Optional webhook for alerts


class DatasetUpdate(BaseModel):
    """Update a dataset."""

    name: str | None = None
    schedule_cron: str | None = None
    alert_threshold: float | None = None
    alert_webhook_url: str | None = None


class DatasetResponse(BaseModel):
    """Dataset response."""

    id: int
    name: str
    source_type: str
    schedule_cron: str | None
    alert_threshold: float
    alert_webhook_url: str | None = None
    created_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)

    @field_serializer('created_at')
    def serialize_created_at(self, value: datetime | None) -> str | None:
        """Serialize datetime to ISO string."""
        return value.isoformat() if value else None


@router.get("/datasets", response_model=List[DatasetResponse])
def list_datasets(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all datasets for the current user."""
    datasets = db.query(Dataset).filter(Dataset.user_id == current_user.id).all()
    return datasets


@router.post("/datasets", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    request: Request,
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
        alert_webhook_url=dataset_data.alert_webhook_url,
    )
    db.add(new_dataset)
    db.commit()
    db.refresh(new_dataset)

    # Register schedule with APScheduler if cron is provided
    if new_dataset.schedule_cron:
        scheduler = request.app.state.scheduler
        await add_dataset_schedule(scheduler, new_dataset.id, new_dataset.schedule_cron)

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
async def update_dataset(
    request: Request,
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

    # Validate cron expression if provided
    if dataset_data.schedule_cron is not None:
        from apscheduler.triggers.cron import CronTrigger
        try:
            CronTrigger.from_crontab(dataset_data.schedule_cron)
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid cron expression: {str(e)}",
            )

    # Update fields if provided
    if dataset_data.name is not None:
        dataset.name = dataset_data.name
    if dataset_data.schedule_cron is not None:
        dataset.schedule_cron = dataset_data.schedule_cron
    if dataset_data.alert_threshold is not None:
        dataset.alert_threshold = dataset_data.alert_threshold
    if dataset_data.alert_webhook_url is not None:
        dataset.alert_webhook_url = dataset_data.alert_webhook_url

    db.commit()
    db.refresh(dataset)

    # Update schedule with APScheduler if cron was changed
    if dataset_data.schedule_cron is not None:
        scheduler = request.app.state.scheduler
        await add_dataset_schedule(scheduler, dataset.id, dataset.schedule_cron)

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
