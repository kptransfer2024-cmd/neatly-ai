"""Diagnosis and file upload routes."""
import pathlib
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import List
from pydantic import BaseModel, ConfigDict
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.orm import Session

from src.api.deps import get_db, get_current_user
from src.db.models import User, Dataset, DiagnosisRun, Issue
from src.utils.file_ingestion import parse_uploaded_file
from src.orchestrator import run_diagnosis
from src.core.config import settings

router = APIRouter()


def _build_issue_row(run_id: int, issue: dict) -> Issue:
    """Build an Issue ORM object from a diagnosis issue dict."""
    return Issue(
        run_id=run_id,
        detector_name=issue.get("detector", "unknown"),
        issue_type=issue.get("type", "unknown"),
        column_name=(issue.get("columns") or [None])[0],
        severity=issue.get("severity", "medium"),
        description=issue.get("summary", ""),
        explanation=issue.get("summary", ""),
    )


class IssueResponse(BaseModel):
    """Issue details."""

    id: int
    detector_name: str
    issue_type: str
    column_name: str | None
    severity: str
    description: str
    explanation: str
    resolved_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class DiagnosisRunResponse(BaseModel):
    """Diagnosis run details."""

    id: int
    dataset_id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    quality_score: float | None
    row_count: int | None
    column_count: int | None
    issues: List[IssueResponse]

    model_config = ConfigDict(from_attributes=True)


@router.post("/datasets/{dataset_id}/diagnose", response_model=DiagnosisRunResponse)
async def trigger_diagnosis(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Trigger an immediate data quality diagnosis run on a registered dataset."""
    # Verify dataset ownership
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == current_user.id,
    ).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )

    started_at = datetime.now(timezone.utc)

    # Create a "running" diagnosis run immediately
    run = DiagnosisRun(
        dataset_id=dataset_id,
        status="running",
        started_at=started_at,
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        # Load data from the configured source
        from src.core.connectors import get_connector
        connector = get_connector(dataset.source_type, dataset.source_config)
        df = await connector.fetch()
    except Exception as e:
        run.status = "failed"
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load data from source: {str(e)}",
        )

    try:
        # Run the diagnosis
        diagnosis_result = run_diagnosis(df)
    except Exception as e:
        run.status = "failed"
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Diagnosis failed: {str(e)}",
        )

    # Persist successful diagnosis results
    run.status = "success"
    run.finished_at = datetime.now(timezone.utc)
    run.quality_score = diagnosis_result.get("quality_score")
    run.row_count = diagnosis_result.get("row_count")
    run.column_count = diagnosis_result.get("column_count")
    run.result_json = diagnosis_result

    # Create Issue records for each issue
    for issue_dict in diagnosis_result.get("issues", []):
        db.add(_build_issue_row(run.id, issue_dict))

    db.commit()
    db.refresh(run)
    return run


@router.get("/datasets/{dataset_id}/runs", response_model=List[DiagnosisRunResponse])
def list_diagnosis_runs(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get diagnosis run history for a dataset."""
    # Verify dataset ownership
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == current_user.id,
    ).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )

    runs = db.query(DiagnosisRun).filter(
        DiagnosisRun.dataset_id == dataset_id
    ).order_by(DiagnosisRun.started_at.desc()).all()
    return runs


@router.get("/runs/{run_id}", response_model=DiagnosisRunResponse)
def get_diagnosis_run(
    run_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get details of a specific diagnosis run."""
    run = db.query(DiagnosisRun).filter(DiagnosisRun.id == run_id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnosis run not found",
        )

    # Verify ownership through dataset
    dataset = run.dataset
    if dataset.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )

    return run


@router.post("/upload", response_model=DiagnosisRunResponse)
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Upload a file and run diagnosis immediately."""
    # Enforce plan tier limits (free: 1 dataset, pro: 10, business: unlimited)
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

    # Read file bytes once — stream can only be consumed once
    try:
        raw = await file.read()
        buf = BytesIO(raw)
        buf.name = file.filename  # parse_uploaded_file needs .name
        df = parse_uploaded_file(buf)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse file: {str(e)}",
        )

    # Save the same raw bytes to disk for future re-diagnosis
    try:
        upload_dir = pathlib.Path(settings.UPLOAD_DIR)
        upload_dir.mkdir(parents=True, exist_ok=True)
        suffix = pathlib.Path(file.filename).suffix
        saved_path = upload_dir / f"{uuid.uuid4().hex}{suffix}"
        saved_path.write_bytes(raw)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save file: {str(e)}",
        )

    # Create a temporary dataset for this upload
    temp_dataset = Dataset(
        user_id=current_user.id,
        name=f"Upload: {file.filename}",
        source_type="upload",
        source_config={"path": str(saved_path), "filename": file.filename},
    )
    db.add(temp_dataset)
    db.flush()  # Get the dataset ID without committing yet

    # Run diagnosis using the orchestrator
    try:
        diagnosis_result = run_diagnosis(df)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Diagnosis failed: {str(e)}",
        )

    # Create a DiagnosisRun record
    run = DiagnosisRun(
        dataset_id=temp_dataset.id,
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        status="success",
        quality_score=diagnosis_result.get("quality_score"),
        row_count=diagnosis_result.get("row_count"),
        column_count=diagnosis_result.get("column_count"),
        result_json=diagnosis_result,
    )
    db.add(run)
    db.flush()

    # Create Issue records for each issue
    for issue_dict in diagnosis_result.get("issues", []):
        db.add(_build_issue_row(run.id, issue_dict))

    db.commit()
    db.refresh(run)
    return run


@router.get("/runs/{run_id}/issues", response_model=List[IssueResponse])
def list_issues(
    run_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get issues from a diagnosis run (owner only)."""
    run = db.query(DiagnosisRun).filter(DiagnosisRun.id == run_id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnosis run not found",
        )

    # Verify ownership through dataset
    dataset = run.dataset
    if dataset.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )

    issues = db.query(Issue).filter(Issue.run_id == run_id).all()
    return issues


@router.post("/runs/{run_id}/issues/{issue_id}/resolve", status_code=status.HTTP_200_OK)
def resolve_issue(
    run_id: int,
    issue_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Mark an issue as resolved (owner only)."""
    run = db.query(DiagnosisRun).filter(DiagnosisRun.id == run_id).first()
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Diagnosis run not found",
        )

    # Verify ownership through dataset
    dataset = run.dataset
    if dataset.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied",
        )

    issue = db.query(Issue).filter(
        Issue.id == issue_id,
        Issue.run_id == run_id,
    ).first()
    if not issue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Issue not found",
        )

    issue.resolved_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(issue)
    return {"message": "Issue resolved"}
