"""Pytest configuration for Neatly AI tests.

Adds the src directory to sys.path so tests can import modules using simple names.
"""
import sys
from pathlib import Path

# Add parent directory (src) to Python path
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))
