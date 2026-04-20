"""Tests for API multi-tenancy enforcement."""
import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from unittest.mock import patch, MagicMock

from src.api.main import create_app
from src.db.models import User, Dataset, DiagnosisRun, Issue
from src.db.session import SessionLocal, Base, engine


@pytest.fixture(scope="function")
def db():
    """Create a fresh database for each test."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def user1(db):
    """Create user 1."""
    user = User(
        email="user1@example.com",
        hashed_password="hashed_pwd1",
        plan_tier="free",
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@pytest.fixture
def user2(db):
    """Create user 2."""
    user = User(
        email="user2@example.com",
        hashed_password="hashed_pwd2",
        plan_tier="pro",
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@pytest.fixture(scope="function")
def client(db):
    """Create a test client with mocked get_current_user."""
    app = create_app()

    def override_get_db():
        try:
            yield db
        finally:
            pass

    from src.api.deps import get_db, get_current_user

    app.dependency_overrides[get_db] = override_get_db

    # Create a store for the current user in the test
    _current_user = {}

    def set_current_user_for_request(user: User):
        """Helper to set current user for next request."""
        _current_user["user"] = user

    def override_get_current_user():
        """Override get_current_user to use test user store."""
        if "user" not in _current_user:
            raise Exception("No user set - call client.set_current_user() first")
        return _current_user["user"]

    app.dependency_overrides[get_current_user] = override_get_current_user
    client = TestClient(app)
    client.set_current_user = set_current_user_for_request
    return client


def test_user_can_only_see_own_datasets(client, user1, user2, db):
    """User 1 should not see User 2's datasets."""
    # User 1 creates a dataset
    client.set_current_user(user1)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 1 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    assert response.status_code == 201
    dataset1_id = response.json()["id"]

    # User 2 creates a dataset
    client.set_current_user(user2)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 2 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    assert response.status_code == 201
    dataset2_id = response.json()["id"]

    # User 1 lists their datasets (should see only their own)
    client.set_current_user(user1)
    response = client.get("/api/v1/datasets")
    assert response.status_code == 200
    datasets = response.json()
    assert len(datasets) == 1
    assert datasets[0]["name"] == "User 1 Dataset"

    # User 1 cannot access User 2's dataset
    response = client.get(f"/api/v1/datasets/{dataset2_id}")
    assert response.status_code == 404


def test_user_cannot_delete_other_users_dataset(client, user1, user2, db):
    """User 1 should not be able to delete User 2's dataset."""
    # User 2 creates a dataset
    client.set_current_user(user2)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 2 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    assert response.status_code == 201
    dataset2_id = response.json()["id"]

    # User 1 tries to delete User 2's dataset
    client.set_current_user(user1)
    response = client.delete(f"/api/v1/datasets/{dataset2_id}")
    assert response.status_code == 404

    # Dataset still exists for User 2
    client.set_current_user(user2)
    response = client.get(f"/api/v1/datasets/{dataset2_id}")
    assert response.status_code == 200


def test_free_tier_limited_to_one_dataset(client, user1, db):
    """Free tier user should not be able to create more than 1 dataset."""
    client.set_current_user(user1)

    # Create first dataset (should succeed)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "Dataset 1",
            "source_type": "upload",
            "source_config": {},
        },
    )
    assert response.status_code == 201

    # Try to create second dataset (should fail)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "Dataset 2",
            "source_type": "upload",
            "source_config": {},
        },
    )
    assert response.status_code == 403
    assert "Free tier" in response.json()["detail"]


def test_pro_tier_limited_to_ten_datasets(client, user2, db):
    """Pro tier user should not be able to create more than 10 datasets."""
    client.set_current_user(user2)

    # Create 10 datasets
    for i in range(10):
        response = client.post(
            "/api/v1/datasets",
            json={
                "name": f"Dataset {i+1}",
                "source_type": "upload",
                "source_config": {},
            },
        )
        assert response.status_code == 201

    # Try to create 11th dataset (should fail)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "Dataset 11",
            "source_type": "upload",
            "source_config": {},
        },
    )
    assert response.status_code == 403
    assert "Pro tier" in response.json()["detail"]


def test_user_cannot_see_other_users_diagnosis_runs(client, user1, user2, db):
    """User 1 should not be able to see User 2's diagnosis runs."""
    # User 1 creates dataset
    client.set_current_user(user1)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 1 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    dataset1_id = response.json()["id"]

    # User 2 creates dataset
    client.set_current_user(user2)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 2 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    dataset2_id = response.json()["id"]

    # Create a diagnosis run for User 2's dataset
    run = DiagnosisRun(
        dataset_id=dataset2_id,
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        status="success",
        quality_score=95.0,
        row_count=100,
        column_count=5,
        result_json={},
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # User 1 tries to access User 2's run
    client.set_current_user(user1)
    response = client.get(f"/api/v1/runs/{run.id}")
    assert response.status_code == 403


def test_user_can_see_own_diagnosis_runs(client, user1, db):
    """User should be able to see their own diagnosis runs."""
    client.set_current_user(user1)

    # Create dataset
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    dataset_id = response.json()["id"]

    # Create a diagnosis run
    run = DiagnosisRun(
        dataset_id=dataset_id,
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        status="success",
        quality_score=95.0,
        row_count=100,
        column_count=5,
        result_json={},
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # User retrieves their run
    response = client.get(f"/api/v1/runs/{run.id}")
    assert response.status_code == 200
    assert response.json()["id"] == run.id


def test_user_cannot_see_other_users_issues(client, user1, user2, db):
    """User 1 should not be able to see User 2's issues."""
    # User 2 creates dataset and run
    client.set_current_user(user2)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 2 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    dataset2_id = response.json()["id"]

    run = DiagnosisRun(
        dataset_id=dataset2_id,
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        status="success",
        quality_score=95.0,
        row_count=100,
        column_count=5,
        result_json={},
    )
    db.add(run)
    db.flush()

    issue = Issue(
        run_id=run.id,
        detector_name="test",
        issue_type="test",
        severity="medium",
        description="Test issue",
        explanation="Test issue",
    )
    db.add(issue)
    db.commit()
    db.refresh(run)

    # User 1 tries to access User 2's issues
    client.set_current_user(user1)
    response = client.get(f"/api/v1/runs/{run.id}/issues")
    assert response.status_code == 403


def test_user_cannot_resolve_other_users_issues(client, user1, user2, db):
    """User 1 should not be able to resolve User 2's issues."""
    # User 2 creates dataset, run, and issue
    client.set_current_user(user2)
    response = client.post(
        "/api/v1/datasets",
        json={
            "name": "User 2 Dataset",
            "source_type": "upload",
            "source_config": {},
        },
    )
    dataset2_id = response.json()["id"]

    run = DiagnosisRun(
        dataset_id=dataset2_id,
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        status="success",
        quality_score=95.0,
        row_count=100,
        column_count=5,
        result_json={},
    )
    db.add(run)
    db.flush()

    issue = Issue(
        run_id=run.id,
        detector_name="test",
        issue_type="test",
        severity="medium",
        description="Test issue",
        explanation="Test issue",
    )
    db.add(issue)
    db.commit()
    db.refresh(run)
    db.refresh(issue)

    # User 1 tries to resolve User 2's issue
    client.set_current_user(user1)
    response = client.post(f"/api/v1/runs/{run.id}/issues/{issue.id}/resolve")
    assert response.status_code == 403
