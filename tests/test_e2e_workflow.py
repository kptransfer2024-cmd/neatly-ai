"""End-to-end workflow test: registration → upload → diagnosis → view results."""
import pytest
import pandas as pd
import io
from datetime import datetime
from sqlalchemy.orm import Session

from fastapi.testclient import TestClient
from src.api.main import create_app
from src.db.models import User
from src.db.session import SessionLocal, Base, engine


@pytest.fixture(scope="function")
def db():
    """Create fresh database for test."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def client(db):
    """Create test client with mocked dependencies."""
    app = create_app()

    def override_get_db():
        try:
            yield db
        finally:
            pass

    from src.api.deps import get_db, get_current_user

    app.dependency_overrides[get_db] = override_get_db

    _current_user = {}

    def override_get_current_user():
        if "user" not in _current_user:
            raise Exception("No user set")
        return _current_user["user"]

    app.dependency_overrides[get_current_user] = override_get_current_user
    test_client = TestClient(app)
    test_client.set_current_user = lambda user: _current_user.update({"user": user})
    return test_client


def create_test_csv():
    """Create a sample CSV with data quality issues."""
    data = {
        "id": [1, 2, 3, 4, 5, None],
        "email": ["user1@example.com", "user2@example.com", "invalid-email", "user4@example.com", "user5@example.com", "user6@example.com"],
        "age": [25, 30, 150, 28, 32, 45],  # age 150 is an outlier
        "name": ["Alice", "Bob", "Charlie", "", "Eve", "Frank"],  # empty name
    }
    df = pd.DataFrame(data)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    return csv_buffer.getvalue()


def test_full_user_workflow(client, db):
    """
    Test complete workflow:
    1. User registration
    2. User login
    3. Create dataset via file upload
    4. View diagnosis results
    5. View issues from run
    """
    from unittest.mock import patch

    # Step 1: Register user (mock password hashing)
    with patch('src.api.routes.auth.hash_password') as mock_hash:
        mock_hash.return_value = "hashed_pass"
        register_response = client.post(
            "/api/v1/auth/register",
            json={"email": "testuser@example.com", "password": "testpass123"},
        )
    assert register_response.status_code == 200
    token = register_response.json()["access_token"]
    assert token is not None

    # Get user from database to use in client
    user = db.query(User).filter_by(email="testuser@example.com").first()
    assert user is not None
    client.set_current_user(user)

    # Step 2: Verify login works (simulated)
    assert token is not None

    # Step 3: List datasets (should be empty)
    list_response = client.get("/api/v1/datasets")
    assert list_response.status_code == 200
    datasets = list_response.json()
    assert len(datasets) == 0

    # Step 4: Upload file and run diagnosis
    csv_content = create_test_csv()
    upload_response = client.post(
        "/api/v1/upload",
        files={"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")},
    )
    assert upload_response.status_code == 200
    run_result = upload_response.json()
    run_id = run_result["id"]
    quality_score = run_result.get("quality_score")

    # Verify quality score is in valid range
    assert quality_score is not None
    assert 0.0 <= quality_score <= 100.0

    # Step 5: Verify dataset was created
    list_response = client.get("/api/v1/datasets")
    assert list_response.status_code == 200
    datasets = list_response.json()
    assert len(datasets) == 1
    dataset_id = datasets[0]["id"]

    # Step 6: Get diagnosis run details
    run_response = client.get(f"/api/v1/runs/{run_id}")
    assert run_response.status_code == 200
    run_data = run_response.json()
    assert run_data["status"] == "success"
    assert run_data["quality_score"] == quality_score

    # Step 7: Get run issues
    issues_response = client.get(f"/api/v1/runs/{run_id}/issues")
    assert issues_response.status_code == 200
    issues = issues_response.json()
    assert len(issues) > 0  # Should detect issues in test data

    # Step 8: Verify issue structure
    for issue in issues:
        assert "id" in issue
        assert "detector_name" in issue
        assert "severity" in issue
        assert issue["severity"] in ["low", "medium", "high"]

    # Step 9: List runs for dataset
    dataset_runs_response = client.get(f"/api/v1/datasets/{dataset_id}/runs")
    assert dataset_runs_response.status_code == 200
    dataset_runs = dataset_runs_response.json()
    assert len(dataset_runs) >= 1
    assert any(r["id"] == run_id for r in dataset_runs)


def test_plan_tier_enforcement_in_workflow(client, db):
    """
    Test that plan tier limits are enforced during workflow.
    """
    from unittest.mock import patch

    # Register free tier user
    with patch('src.api.routes.auth.hash_password') as mock_hash:
        mock_hash.return_value = "hashed_pass"
        register_response = client.post(
            "/api/v1/auth/register",
            json={"email": "freeuser@example.com", "password": "testpass123"},
        )
    assert register_response.status_code == 200
    token = register_response.json()["access_token"]

    user = db.query(User).filter_by(email="freeuser@example.com").first()
    user.plan_tier = "free"
    db.commit()
    client.set_current_user(user)

    # First upload should succeed
    csv_content = create_test_csv()
    upload_response = client.post(
        "/api/v1/upload",
        files={"file": ("test1.csv", io.BytesIO(csv_content.encode()), "text/csv")},
    )
    assert upload_response.status_code == 200

    # Second upload should fail (free tier limited to 1 dataset)
    upload_response = client.post(
        "/api/v1/upload",
        files={"file": ("test2.csv", io.BytesIO(csv_content.encode()), "text/csv")},
    )
    assert upload_response.status_code == 403
    assert "Free tier" in upload_response.json()["detail"]


def test_multi_user_isolation_in_workflow(client, db):
    """
    Test that data from two different users remains isolated.
    """
    from unittest.mock import patch

    # Register first user
    with patch('src.api.routes.auth.hash_password') as mock_hash:
        mock_hash.return_value = "hashed_pass"
        client.post(
            "/api/v1/auth/register",
            json={"email": "user1@example.com", "password": "pass1"},
        )
    user1 = db.query(User).filter_by(email="user1@example.com").first()
    client.set_current_user(user1)

    # User 1 uploads file
    csv_content = create_test_csv()
    upload1 = client.post(
        "/api/v1/upload",
        files={"file": ("user1.csv", io.BytesIO(csv_content.encode()), "text/csv")},
    )
    assert upload1.status_code == 200
    run1_id = upload1.json()["id"]
    dataset1_id = upload1.json()["dataset_id"]

    # Register second user
    with patch('src.api.routes.auth.hash_password') as mock_hash:
        mock_hash.return_value = "hashed_pass"
        client.post(
            "/api/v1/auth/register",
            json={"email": "user2@example.com", "password": "pass2"},
        )
    user2 = db.query(User).filter_by(email="user2@example.com").first()
    client.set_current_user(user2)

    # User 2 uploads file
    upload2 = client.post(
        "/api/v1/upload",
        files={"file": ("user2.csv", io.BytesIO(csv_content.encode()), "text/csv")},
    )
    assert upload2.status_code == 200
    run2_id = upload2.json()["id"]
    dataset2_id = upload2.json()["dataset_id"]

    # User 2 tries to access User 1's data (should fail)
    run1_response = client.get(f"/api/v1/runs/{run1_id}")
    assert run1_response.status_code == 403

    dataset1_response = client.get(f"/api/v1/datasets/{dataset1_id}")
    assert dataset1_response.status_code == 404

    # User 2 can access their own data
    run2_response = client.get(f"/api/v1/runs/{run2_id}")
    assert run2_response.status_code == 200

    dataset2_response = client.get(f"/api/v1/datasets/{dataset2_id}")
    assert dataset2_response.status_code == 200

    # Switch back to User 1 and verify they can't see User 2's data
    client.set_current_user(user1)
    run2_response = client.get(f"/api/v1/runs/{run2_id}")
    assert run2_response.status_code == 403
