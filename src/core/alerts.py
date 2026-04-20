"""Alert system for quality threshold violations."""
import logging
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import httpx

from src.db.models import Dataset, DiagnosisRun

logger = logging.getLogger(__name__)


async def check_and_send_alerts(
    db: Session,
    dataset: Dataset,
    run: DiagnosisRun,
) -> None:
    """Check if quality score triggered an alert and send notifications.

    Args:
        db: Database session
        dataset: The dataset that was diagnosed
        run: The completed diagnosis run
    """
    if run.quality_score is None:
        return

    # Check if threshold was exceeded
    if run.quality_score >= dataset.alert_threshold:
        logger.info(
            f"Quality score {run.quality_score:.1f} above threshold "
            f"{dataset.alert_threshold} for dataset {dataset.id}"
        )
        return

    # Quality score below threshold - send alerts
    logger.warning(
        f"Quality score {run.quality_score:.1f} below threshold "
        f"{dataset.alert_threshold} for dataset {dataset.id}"
    )

    # TODO: Check if alert was already sent in the last 24 hours

    # Send email alert
    try:
        await send_email_alert(dataset, run)
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")

    # Send webhook alert
    try:
        await send_webhook_alert(dataset, run)
    except Exception as e:
        logger.error(f"Failed to send webhook alert: {e}")


async def send_email_alert(dataset: Dataset, run: DiagnosisRun) -> None:
    """Send email alert to dataset owner.

    Args:
        dataset: The dataset that triggered the alert
        run: The diagnosis run with poor quality score
    """
    import aiosmtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    # Get owner email from dataset.owner
    if not dataset.owner or not dataset.owner.email:
        logger.warning(f"No email address for dataset owner {dataset.id}")
        return

    to_addr = dataset.owner.email
    subject = f"Data Quality Alert: {dataset.name}"
    html_body = f"""
    <h2>Data Quality Alert</h2>
    <p>Your dataset <strong>{dataset.name}</strong> has a low quality score.</p>
    <ul>
        <li><strong>Quality Score:</strong> {run.quality_score:.1f}%</li>
        <li><strong>Threshold:</strong> {dataset.alert_threshold}%</li>
        <li><strong>Rows:</strong> {run.row_count}</li>
        <li><strong>Columns:</strong> {run.column_count}</li>
        <li><strong>Issues Found:</strong> {len(run.issues)}</li>
        <li><strong>Time:</strong> {run.finished_at}</li>
    </ul>
    <p>
        <a href="https://neatly.app/datasets/{dataset.id}/runs/{run.id}">
            View Details
        </a>
    </p>
    """

    # TODO: Configure SMTP settings in .env
    # For now, just log it
    logger.info(f"Would send email alert to {to_addr}: {subject}")


async def send_webhook_alert(dataset: Dataset, run: DiagnosisRun) -> None:
    """Send webhook POST to user-configured URL.

    Args:
        dataset: The dataset that triggered the alert
        run: The diagnosis run with poor quality score
    """
    # TODO: Store webhook_url in user or dataset settings
    webhook_url = None  # dataset.alert_webhook_url

    if not webhook_url:
        logger.debug(f"No webhook configured for dataset {dataset.id}")
        return

    payload = {
        "event": "quality_alert",
        "dataset_id": dataset.id,
        "dataset_name": dataset.name,
        "quality_score": run.quality_score,
        "threshold": dataset.alert_threshold,
        "row_count": run.row_count,
        "column_count": run.column_count,
        "issues_count": len(run.issues),
        "timestamp": run.finished_at.isoformat(),
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(webhook_url, json=payload)
            response.raise_for_status()
            logger.info(f"Webhook alert sent to {webhook_url} for dataset {dataset.id}")
    except httpx.HTTPError as e:
        logger.error(f"Failed to send webhook to {webhook_url}: {e}")
