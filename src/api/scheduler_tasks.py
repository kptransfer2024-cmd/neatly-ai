"""Scheduled background tasks for diagnosis runs."""
import logging
from datetime import datetime
from sqlalchemy.orm import Session

from src.db.session import SessionLocal
from src.db.models import Dataset, DiagnosisRun, Issue
from src.orchestrator import run_diagnosis
from src.core.connectors import get_connector

logger = logging.getLogger(__name__)


async def run_scheduled_diagnosis(dataset_id: int) -> None:
    """Run a scheduled diagnosis for a dataset.

    Args:
        dataset_id: The dataset ID to diagnose
    """
    db = SessionLocal()
    try:
        # Fetch dataset
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if not dataset:
            logger.error(f"Dataset {dataset_id} not found")
            return

        logger.info(f"Starting scheduled diagnosis for dataset {dataset_id} ({dataset.name})")

        # Fetch data from source
        try:
            connector = get_connector(dataset.source_type, dataset.source_config)
            df = await connector.fetch()
        except Exception as e:
            logger.error(f"Failed to fetch data for dataset {dataset_id}: {e}")
            return

        # Run diagnosis
        try:
            diagnosis_result = run_diagnosis(df)
        except Exception as e:
            logger.error(f"Diagnosis failed for dataset {dataset_id}: {e}")
            return

        # Save run to database
        run = DiagnosisRun(
            dataset_id=dataset_id,
            finished_at=datetime.utcnow(),
            status="success",
            quality_score=diagnosis_result.get("quality_score"),
            row_count=diagnosis_result.get("row_count"),
            column_count=diagnosis_result.get("column_count"),
            result_json=diagnosis_result,
        )
        db.add(run)
        db.flush()

        # Create Issue records
        for issue_dict in diagnosis_result.get("issues", []):
            issue = Issue(
                run_id=run.id,
                detector_name=issue_dict.get("detector", "unknown"),
                issue_type=issue_dict.get("type", "unknown"),
                column_name=issue_dict.get("column"),
                severity=issue_dict.get("severity", "medium"),
                description=issue_dict.get("summary", ""),
                explanation=issue_dict.get("summary", ""),
            )
            db.add(issue)

        db.commit()
        logger.info(
            f"Completed diagnosis for dataset {dataset_id}: "
            f"quality_score={run.quality_score:.1f}, "
            f"issues={len(diagnosis_result.get('issues', []))}"
        )

        # Check alert threshold
        if run.quality_score is not None and run.quality_score < dataset.alert_threshold:
            logger.warning(
                f"Quality score {run.quality_score:.1f} below threshold "
                f"{dataset.alert_threshold} for dataset {dataset_id}"
            )
            # TODO: Send alert (Phase 2.3)

    except Exception as e:
        logger.error(f"Unexpected error in scheduled diagnosis for dataset {dataset_id}: {e}")
    finally:
        db.close()
