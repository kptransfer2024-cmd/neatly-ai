"""Alert system for quality threshold violations."""
import logging
import os
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
import httpx

from src.db.models import Dataset, DiagnosisRun, AlertLog

logger = logging.getLogger(__name__)

_ALERT_DEDUP_HOURS = 24  # Don't send duplicate alerts within 24h


async def check_and_send_alerts(
    db: Session,
    dataset: Dataset,
    run: DiagnosisRun,
    issue_count: int = 0,
) -> None:
    """Check if quality score triggered an alert and send notifications.

    Args:
        db: Database session
        dataset: The dataset that was diagnosed
        run: The completed diagnosis run
        issue_count: Number of issues detected (passed to avoid DetachedInstanceError)
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

    # Quality score below threshold - check for duplicate alerts
    logger.warning(
        f"Quality score {run.quality_score:.1f} below threshold "
        f"{dataset.alert_threshold} for dataset {dataset.id}"
    )

    if _should_skip_alert_due_to_recent(db, dataset.id):
        logger.info(f"Skipping alert for dataset {dataset.id} (sent recently)")
        return

    # Send email alert
    email_sent = False
    try:
        await send_email_alert(db, dataset, run, issue_count)
        email_sent = True
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")

    # Send webhook alert
    webhook_sent = False
    try:
        await send_webhook_alert(db, dataset, run, issue_count)
        webhook_sent = True
    except Exception as e:
        logger.error(f"Failed to send webhook alert: {e}")

    # Log the alert if at least one was sent successfully
    if email_sent or webhook_sent:
        for alert_type in ['email', 'webhook']:
            if (alert_type == 'email' and email_sent) or (alert_type == 'webhook' and webhook_sent):
                log = AlertLog(
                    dataset_id=dataset.id,
                    run_id=run.id,
                    alert_type=alert_type,
                    quality_score=run.quality_score,
                )
                db.add(log)
        try:
            db.commit()
        except Exception as e:
            logger.error(f"Failed to log alert: {e}")


def _should_skip_alert_due_to_recent(db: Session, dataset_id: int) -> bool:
    """Check if an alert was sent for this dataset within the last 24 hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_ALERT_DEDUP_HOURS)
    recent_alert = db.query(AlertLog).filter(
        AlertLog.dataset_id == dataset_id,
        AlertLog.sent_at >= cutoff,
    ).first()
    return recent_alert is not None


async def send_email_alert(
    db: Session,
    dataset: Dataset,
    run: DiagnosisRun,
    issue_count: int = 0,
) -> None:
    """Send email alert to dataset owner.

    Args:
        db: Database session
        dataset: The dataset that triggered the alert
        run: The diagnosis run with poor quality score
        issue_count: Number of issues detected
    """
    if not dataset.owner or not dataset.owner.email:
        logger.warning(f"No email address for dataset owner {dataset.id}")
        return

    smtp_host = os.getenv("SMTP_HOST")
    if not smtp_host:
        logger.debug("SMTP not configured; skipping email alert")
        return

    to_addr = dataset.owner.email
    subject = f"🚨 Data Quality Alert: {dataset.name}"
    finished_at_str = run.finished_at.isoformat() if run.finished_at else "N/A"

    html_body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <h2 style="color: #d32f2f;">⚠️ Data Quality Alert</h2>
                <p>Your dataset <strong>{dataset.name}</strong> has a low quality score.</p>
                <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                    <tr style="background: #f5f5f5;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Quality Score</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{run.quality_score:.1f}%</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Threshold</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{dataset.alert_threshold}%</td>
                    </tr>
                    <tr style="background: #f5f5f5;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Rows</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{run.row_count}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Columns</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{run.column_count}</td>
                    </tr>
                    <tr style="background: #f5f5f5;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Issues Found</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{issue_count}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Diagnosed At</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{finished_at_str}</td>
                    </tr>
                </table>
                <p>
                    <a href="https://neatly.app/datasets/{dataset.id}/runs/{run.id}"
                       style="display: inline-block; padding: 10px 20px; background: #7c3aed; color: white;
                              text-decoration: none; border-radius: 4px;">
                        View Details
                    </a>
                </p>
            </div>
        </body>
    </html>
    """

    try:
        import aiosmtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = os.getenv("SMTP_FROM", "neatly@example.com")
        msg["To"] = to_addr

        msg.attach(MIMEText(html_body, "html"))

        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASSWORD")

        async with aiosmtplib.SMTP(hostname=smtp_host, port=smtp_port) as smtp:
            if smtp_user and smtp_pass:
                await smtp.login(smtp_user, smtp_pass)
            await smtp.send_message(msg)

        logger.info(f"Email alert sent to {to_addr} for dataset {dataset.id}")
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        raise


async def send_webhook_alert(
    db: Session,
    dataset: Dataset,
    run: DiagnosisRun,
    issue_count: int = 0,
) -> None:
    """Send webhook POST to user-configured URL.

    Args:
        db: Database session
        dataset: The dataset that triggered the alert
        run: The diagnosis run with poor quality score
        issue_count: Number of issues detected
    """
    webhook_url = dataset.alert_webhook_url

    if not webhook_url:
        logger.debug(f"No webhook configured for dataset {dataset.id}")
        return

    timestamp_str = run.finished_at.isoformat() if run.finished_at else None
    payload = {
        "event": "quality_alert",
        "dataset_id": dataset.id,
        "dataset_name": dataset.name,
        "quality_score": run.quality_score,
        "threshold": dataset.alert_threshold,
        "row_count": run.row_count,
        "column_count": run.column_count,
        "issues_count": issue_count,
        "timestamp": timestamp_str,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(webhook_url, json=payload)
            response.raise_for_status()
            logger.info(f"Webhook alert sent to {webhook_url} for dataset {dataset.id}")
    except httpx.HTTPError as e:
        logger.error(f"Failed to send webhook to {webhook_url}: {e}")
        raise
