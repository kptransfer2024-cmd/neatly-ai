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
                async with httpx.AsyncClient() as client:
                    response = await client.post(
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
                async with httpx.AsyncClient() as client:
                    response = await client.post(
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


async def render_home():
    """Home page with overview metrics."""
    st.title("📊 Dashboard")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{API_URL}/datasets",
                headers=get_headers(),
            )
            if response.status_code == 401:
                st.error("Session expired. Please log in again.")
                st.session_state.api_token = None
                st.rerun()

            datasets = response.json()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Datasets", len(datasets))
        col2.metric("Total Diagnoses", "N/A")  # TODO: aggregate from runs
        col3.metric("Avg Quality", "N/A")  # TODO: compute from latest runs
        col4.metric("Active Alerts", "0")  # TODO: count recent alerts

        st.divider()

        if datasets:
            st.subheader("Recent Datasets")
            for dataset in datasets[:5]:
                with st.container(border=True):
                    col1, col2, col3 = st.columns([2, 1, 1])
                    col1.write(f"**{dataset['name']}**")
                    col2.write(f"Source: {dataset['source_type']}")
                    if col3.button("View", key=f"view_{dataset['id']}"):
                        st.switch_page(f"pages/01_datasets.py")
        else:
            st.info("No datasets yet. Create one to get started!")

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")


def render_datasets():
    """Datasets list and management page."""
    st.title("📁 Datasets")
    st.write("Manage your data sources and monitoring schedules.")
    # TODO: Implement datasets page


def render_runs():
    """Diagnosis runs history page."""
    st.title("📊 Runs")
    st.write("View historical diagnosis runs and quality trends.")
    # TODO: Implement runs page


def render_settings():
    """User settings page."""
    st.title("⚙️ Settings")
    st.write("Manage your account and preferences.")
    # TODO: Implement settings page


# Main app logic
if st.session_state.api_token:
    import asyncio

    asyncio.run(render_main_app())
else:
    render_auth_page()
