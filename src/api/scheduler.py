"""APScheduler integration for scheduled diagnosis runs."""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.base import JobLookupError
from apscheduler.executors.asyncio import AsyncIOExecutor

from src.core.config import settings
from src.db.session import engine

logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure APScheduler."""
    jobstores = {
        "default": SQLAlchemyJobStore(engine=engine)
    }
    executors = {
        "default": AsyncIOExecutor()
    }
    job_defaults = {
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 3600,  # Skip jobs > 1 hour late
    }

    scheduler = AsyncIOScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
    )
    return scheduler


async def add_dataset_schedule(
    scheduler: AsyncIOScheduler,
    dataset_id: int,
    cron_expr: str | None,
) -> None:
    """Add or update a scheduled diagnosis for a dataset.

    Args:
        scheduler: The AsyncIOScheduler instance
        dataset_id: The dataset ID to schedule
        cron_expr: Cron expression like '0 * * * *' for hourly, or None to remove
    """
    job_id = f"dataset-{dataset_id}"

    # Remove existing job if present (no-op if it doesn't exist yet)
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Removed existing job for dataset {dataset_id}")
    except JobLookupError:
        pass

    if cron_expr is None:
        logger.info(f"Disabled schedule for dataset {dataset_id}")
        return

    # Add new job
    try:
        from src.api.scheduler_tasks import run_scheduled_diagnosis

        scheduler.add_job(
            run_scheduled_diagnosis,
            trigger="cron",
            args=[dataset_id],
            id=job_id,
            name=f"Diagnose dataset {dataset_id}",
            replace_existing=True,
            **_parse_cron(cron_expr),
        )
        logger.info(f"Scheduled dataset {dataset_id} with cron: {cron_expr}")
    except Exception as e:
        logger.error(f"Failed to schedule dataset {dataset_id}: {e}")
        raise


def _parse_cron(cron_expr: str) -> dict:
    """Parse cron expression into apscheduler kwargs.

    Supports standard 5-field cron: minute hour day month day_of_week
    """
    parts = cron_expr.split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expr}")

    minute, hour, day, month, day_of_week = parts
    return {
        "minute": minute,
        "hour": hour,
        "day": day,
        "month": month,
        "day_of_week": day_of_week,
    }
