"""Neatly Dashboard - Streamlit app for monitoring data quality."""
import streamlit as st
import httpx
import json
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Neatly — Data Quality Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state
if "api_token" not in st.session_state:
    st.session_state.api_token = None
if "user_email" not in st.session_state:
    st.session_state.user_email = None
if "api_url" not in st.session_state:
    st.session_state.api_url = "http://localhost:8000/api/v1"

API_URL = st.session_state.api_url


def get_headers():
    """Get headers with JWT token for API requests."""
    if not st.session_state.api_token:
        return {}
    return {"Authorization": f"Bearer {st.session_state.api_token}"}


def render_auth_page():
    """Render login/register page."""
    st.title("Neatly — Data Quality Monitoring")
    st.write("Monitor data quality continuously across your data sources.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Register")
        email = st.text_input("Email", key="reg_email")
        password = st.text_input("Password", type="password", key="reg_password")
        if st.button("Sign Up", key="reg_btn"):
            try:
                with httpx.Client() as client:
                    response = client.post(
                        f"{API_URL}/auth/register",
                        json={"email": email, "password": password},
                    )
                    if response.status_code == 200:
                        data = response.json()
                        st.session_state.api_token = data["access_token"]
                        st.session_state.user_email = email
                        st.success("Registered! Reloading...")
                        st.rerun()
                    else:
                        st.error(f"Registration failed: {response.json().get('detail')}")
            except Exception as e:
                st.error(f"Error: {e}")

    with col2:
        st.subheader("Login")
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")
        if st.button("Sign In", key="login_btn"):
            try:
                with httpx.Client() as client:
                    response = client.post(
                        f"{API_URL}/auth/login",
                        json={"email": email, "password": password},
                    )
                    if response.status_code == 200:
                        data = response.json()
                        st.session_state.api_token = data["access_token"]
                        st.session_state.user_email = email
                        st.success("Logged in! Reloading...")
                        st.rerun()
                    else:
                        st.error(f"Login failed: {response.json().get('detail')}")
            except Exception as e:
                st.error(f"Error: {e}")


def render_main_app():
    """Render main dashboard after authentication."""
    st.sidebar.title("Neatly")

    # User menu
    col1, col2 = st.sidebar.columns([1, 1])
    with col1:
        st.sidebar.write(f"👤 {st.session_state.user_email}")
    with col2:
        if st.sidebar.button("Logout"):
            st.session_state.api_token = None
            st.session_state.user_email = None
            st.rerun()

    st.sidebar.divider()

    # Navigation
    page = st.sidebar.radio(
        "Pages",
        ["Home", "Datasets", "Runs", "Settings"],
        icons=["🏠", "📁", "📊", "⚙️"],
    )

    st.sidebar.divider()
    st.sidebar.caption("Neatly v0.1.0")

    # Render pages
    if page == "Home":
        render_home()
    elif page == "Datasets":
        render_datasets()
    elif page == "Runs":
        render_runs()
    elif page == "Settings":
        render_settings()


def render_home():
    """Home page with overview metrics."""
    st.title("📊 Dashboard")

    try:
        headers = get_headers()
        with httpx.Client() as client:
            response = client.get(
                f"{API_URL}/datasets",
                headers=headers,
            )
            if response.status_code == 401:
                st.error("Session expired. Please log in again.")
                st.session_state.api_token = None
                st.rerun()

            datasets = response.json()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Datasets", len(datasets))
        col2.metric("Total Diagnoses", "N/A")
        col3.metric("Avg Quality", "N/A")
        col4.metric("Active Alerts", "0")

        st.divider()

        if datasets:
            st.subheader("Recent Datasets")
            for dataset in datasets[:5]:
                with st.container(border=True):
                    col1, col2, col3 = st.columns([2, 1, 1])
                    col1.write(f"**{dataset['name']}**")
                    col2.write(f"Source: {dataset['source_type']}")
                    if col3.button("View", key=f"view_{dataset['id']}"):
                        st.session_state.page = "Datasets"
                        st.rerun()
        else:
            st.info("No datasets yet. Create one to get started!")

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")


def render_datasets():
    """Datasets list and management page."""
    st.title("📁 Datasets")
    st.write("Manage your data sources and monitoring schedules.")

    # Upload new dataset
    st.subheader("Upload CSV for Analysis")
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
    if uploaded_file:
        if st.button("Analyze File"):
            try:
                with st.spinner("Analyzing data quality..."):
                    headers = get_headers()
                    with httpx.Client() as client:
                        files = {"file": (uploaded_file.name, uploaded_file, "text/csv")}
                        response = client.post(
                            f"{API_URL}/upload",
                            files=files,
                            headers=headers,
                        )
                    if response.status_code == 200:
                        run_data = response.json()
                        st.success(f"Analysis complete! Quality Score: {run_data.get('quality_score', 'N/A'):.1f}%")
                        st.json(run_data)
                    else:
                        st.error(f"Upload failed: {response.json().get('detail')}")
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    st.subheader("Your Datasets")

    try:
        with st.spinner("Loading datasets..."):
            headers = get_headers()
            with httpx.Client() as client:
                response = client.get(
                    f"{API_URL}/datasets",
                    headers=headers,
                )
            if response.status_code == 401:
                st.error("Session expired. Please log in again.")
                st.session_state.api_token = None
                st.rerun()

            datasets = response.json()

        if datasets:
            for dataset in datasets:
                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                    col1.write(f"**{dataset['name']}**")
                    col2.write(f"Source: {dataset['source_type']}")
                    col3.write(f"Created: {dataset.get('created_at', 'N/A')[:10]}")
                    if col4.button("View Runs", key=f"view_runs_{dataset['id']}"):
                        st.session_state.selected_dataset_id = dataset["id"]
                        st.rerun()
        else:
            st.info("No datasets yet. Upload a file above to get started!")

    except Exception as e:
        st.error(f"Error loading datasets: {e}")


def render_runs():
    """Diagnosis runs history page."""
    st.title("📊 Diagnosis Runs")
    st.write("View historical diagnosis runs and quality trends.")

    try:
        headers = get_headers()
        with httpx.Client() as client:
            response = client.get(
                f"{API_URL}/datasets",
                headers=headers,
            )
        if response.status_code == 401:
            st.error("Session expired. Please log in again.")
            st.session_state.api_token = None
            st.rerun()

        datasets = response.json()

        if not datasets:
            st.info("No datasets yet. Create one to view runs.")
            return

        # Select dataset to view runs for
        dataset_names = {d["id"]: d["name"] for d in datasets}
        selected_dataset_name = st.selectbox(
            "Select Dataset",
            options=list(dataset_names.values()),
        )
        selected_dataset_id = [k for k, v in dataset_names.items() if v == selected_dataset_name][0]

        # Fetch runs for selected dataset
        with httpx.Client() as client:
            response = client.get(
                f"{API_URL}/datasets/{selected_dataset_id}/runs",
                headers=headers,
            )
        if response.status_code == 200:
            runs = response.json()

            if runs:
                st.subheader(f"Runs for {selected_dataset_name}")

                # Quality trend chart
                if len(runs) > 0:
                    try:
                        import altair as alt
                        import pandas as pd

                        run_data = []
                        for run in sorted(runs, key=lambda r: r.get("started_at", "")):
                            run_data.append({
                                "Date": run.get("started_at", "").split("T")[0],
                                "Quality Score": run.get("quality_score") or 0,
                                "Status": run.get("status", "unknown"),
                            })

                        if run_data:
                            df = pd.DataFrame(run_data)
                            chart = alt.Chart(df).mark_line(point=True).encode(
                                x="Date:T",
                                y="Quality Score:Q",
                                color="Status:N",
                                tooltip=["Date:T", "Quality Score:Q", "Status:N"],
                            ).interactive()
                            st.altair_chart(chart, use_container_width=True)
                    except Exception as e:
                        st.warning(f"Could not render chart: {e}")

                st.divider()

                # Run details
                for run in runs:
                    with st.container(border=True):
                        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                        col1.write(f"**Run {run['id']}**")
                        col2.metric("Quality", f"{run.get('quality_score', 'N/A')}%")
                        col3.write(f"Status: {run.get('status', 'unknown')}")
                        col4.write(f"Run: {run.get('started_at', 'N/A')[:10]}")

                        if st.button("View Issues", key=f"view_issues_{run['id']}"):
                            st.session_state.selected_run_id = run["id"]

                        # Show issues inline if selected
                        if st.session_state.get("selected_run_id") == run["id"]:
                            with st.spinner("Loading issues..."):
                                try:
                                    with httpx.Client() as client:
                                        issues_response = client.get(
                                            f"{API_URL}/runs/{run['id']}/issues",
                                            headers=headers,
                                        )
                                    if issues_response.status_code == 200:
                                        issues = issues_response.json()
                                        if issues:
                                            st.write(f"**Issues ({len(issues)})**")
                                            for issue in issues:
                                                st.write(f"- {issue.get('detector_name')}: {issue.get('description')}")
                                        else:
                                            st.success("No issues found!")
                                except Exception as e:
                                    st.error(f"Error loading issues: {e}")
            else:
                st.info("No diagnosis runs yet for this dataset.")

    except Exception as e:
        st.error(f"Error loading runs: {e}")


def render_settings():
    """User settings page."""
    st.title("⚙️ Settings")
    st.write("Manage your account and preferences.")

    st.subheader("Account Information")
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Email", value=st.session_state.user_email, disabled=True)
    with col2:
        st.text_input("API URL", value=st.session_state.api_url, disabled=True)

    st.divider()

    st.subheader("Session Management")
    col1, col2 = st.columns(2)

    with col1:
        st.write("**Current Session**")
        st.info(f"Logged in as: {st.session_state.user_email}")
        if st.button("Logout", type="primary"):
            st.session_state.api_token = None
            st.session_state.user_email = None
            st.success("Logged out!")
            st.rerun()

    with col2:
        st.write("**API Token**")
        st.caption("Your session token (for API access)")
        if st.button("Show Token"):
            st.session_state.show_token = not st.session_state.get("show_token", False)

        if st.session_state.get("show_token"):
            st.code(st.session_state.api_token, language="text")

    st.divider()

    st.subheader("Plan Information")
    st.info("""
    Your current plan determines:
    - **Free**: 1 dataset, manual runs only
    - **Pro**: 10 datasets, hourly scheduling
    - **Business**: Unlimited datasets, 5-minute scheduling

    Contact support to upgrade your plan.
    """)

    st.divider()

    st.subheader("About")
    st.write("""
    **Neatly** - Data Quality Monitoring SaaS

    Version: 0.1.0

    Continuously monitor your data quality with intelligent detectors
    and natural language explanations powered by Claude AI.
    """)


# Main app logic
if st.session_state.api_token:
    render_main_app()
else:
    render_auth_page()
