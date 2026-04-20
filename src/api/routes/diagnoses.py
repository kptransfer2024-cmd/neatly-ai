"""Diagnosis and file upload routes."""
from datetime import datetime
from typing import List
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.orm import Session

from src.api.deps import get_db, get_current_user
from src.db.models import User, Dataset, DiagnosisRun, Issue
from src.utils.file_ingestion import parse_uploaded_file
from src.orchestrator import run_diagnosis

router = APIRouter()


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

    class Config:
        from_attributes = True


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

    class Config:
        from_attributes = True


@router.post("/datasets/{dataset_id}/diagnose", response_model=DiagnosisRunResponse)
def trigger_diagnosis(
    dataset_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Trigger an immediate data quality diagnosis run."""
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

    # TODO: Load data from source (for MVP, assume test data)
    # For now, this is a placeholder that requires file upload first
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Diagnose from registered source coming in Phase 2. Use /upload for now.",
    )


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
    try:
        # Parse the uploaded file
        df = parse_uploaded_file(file)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse file: {str(e)}",
        )

    # Create a temporary dataset for this upload
    temp_dataset = Dataset(
        user_id=current_user.id,
        name=f"Upload: {file.filename}",
        source_type="upload",
        source_config={"filename": file.filename},
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
        finished_at=datetime.utcnow(),
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
    db.refresh(run)
    return run
