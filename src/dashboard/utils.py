"""Dashboard utilities for API calls and data management."""
import httpx
import streamlit as st
from typing import Any, Dict, Optional


class APIClient:
    """Helper class for making API calls to the backend."""

    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url
        self.token = token

    def get_headers(self) -> Dict[str, str]:
        """Get headers with JWT token."""
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def get(self, endpoint: str) -> Any:
        """Make a GET request."""
        url = f"{self.base_url}{endpoint}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()

    async def post(self, endpoint: str, json: Dict[str, Any]) -> Any:
        """Make a POST request."""
        url = f"{self.base_url}{endpoint}"
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=json, headers=self.get_headers())
            response.raise_for_status()
            return response.json()

    async def patch(self, endpoint: str, json: Dict[str, Any]) -> Any:
        """Make a PATCH request."""
        url = f"{self.base_url}{endpoint}"
        async with httpx.AsyncClient() as client:
            response = await client.patch(url, json=json, headers=self.get_headers())
            response.raise_for_status()
            return response.json()

    async def delete(self, endpoint: str) -> None:
        """Make a DELETE request."""
        url = f"{self.base_url}{endpoint}"
        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=self.get_headers())
            response.raise_for_status()


def get_api_client() -> APIClient:
    """Get an API client with the current session token."""
    token = st.session_state.get("api_token")
    api_url = st.session_state.get("api_url", "http://localhost:8000/api/v1")
    return APIClient(api_url, token)


def format_quality_score(score: Optional[float]) -> str:
    """Format quality score as a percentage."""
    if score is None:
        return "N/A"
    return f"{score:.1f}%"


def get_quality_color(score: Optional[float]) -> str:
    """Get color for quality score (red/yellow/green)."""
    if score is None:
        return "gray"
    if score >= 80:
        return "green"
    elif score >= 60:
        return "orange"
    else:
        return "red"


def render_quality_badge(score: Optional[float]) -> None:
    """Render a quality score badge."""
    color = get_quality_color(score)
    formatted = format_quality_score(score)

    if color == "green":
        st.success(formatted)
    elif color == "orange":
        st.warning(formatted)
    else:
        st.error(formatted)
