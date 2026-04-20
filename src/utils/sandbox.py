"""Codespace sandbox utilities: package management and safe execution."""

import importlib.metadata
import re
import subprocess
import sys
import time
from typing import Optional

# ---------------------------------------------------------------------------
# Package name validation
# ---------------------------------------------------------------------------

_PKG_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$')

_BLOCKED_PACKAGES = frozenset({
    'os', 'sys', 'subprocess', 'socket', 'shutil', 'ctypes',
    'multiprocessing', 'threading', 'signal', 'pty', 'gc',
    'pickle', 'marshal', 'shelve', 'tempfile',
    'ast', 'dis', 'code', 'codeop', 'compileall', 'py_compile',
    'importlib', 'builtins', 'linecache', 'tokenize',
})


def validate_package_name(name: str) -> tuple[bool, str]:
    name = name.strip()
    if not name:
        return False, 'Package name cannot be empty.'
    if not _PKG_RE.match(name):
        return False, f'Invalid name {name!r}. Use letters, digits, hyphens, underscores, dots only.'
    if name.lower() in _BLOCKED_PACKAGES:
        return False, f'{name!r} is not allowed for security reasons.'
    return True, ''


# ---------------------------------------------------------------------------
# Package installer
# ---------------------------------------------------------------------------

def install_package(name: str) -> tuple[bool, str]:
    """pip install `name` in the current interpreter. Returns (success, message)."""
    valid, err = validate_package_name(name)
    if not valid:
        return False, err
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', name.strip(), '--quiet', '--no-warn-script-location'],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            ver = get_package_version(name.strip())
            suffix = f' ({ver})' if ver else ''
            return True, f'Installed {name}{suffix}'
        stderr = (result.stderr or result.stdout or 'unknown error').strip()
        last_line = stderr.splitlines()[-1] if stderr else 'pip failed'
        return False, last_line
    except subprocess.TimeoutExpired:
        return False, 'Installation timed out (120 s).'
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def get_package_version(name: str) -> Optional[str]:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


# Canonical packages shown in the sidebar
_SIDEBAR_PACKAGES = [
    ('pandas',       'pd'),
    ('numpy',        'np'),
    ('scipy',        'sp'),
    ('matplotlib',   'plt'),
    ('seaborn',      'sns'),
    ('plotly',       'px'),
    ('scikit-learn', 'sklearn'),
    ('statsmodels',  'sm'),
    ('polars',       'pl'),
    ('pyarrow',      'pa'),
    ('nltk',         'nltk'),
    ('rapidfuzz',    'fuzz'),
]


def get_sidebar_package_status() -> list[dict]:
    return [
        {
            'name':      pkg,
            'alias':     alias,
            'version':   get_package_version(pkg),
            'installed': get_package_version(pkg) is not None,
        }
        for pkg, alias in _SIDEBAR_PACKAGES
    ]


# ---------------------------------------------------------------------------
# Safe execution
# ---------------------------------------------------------------------------

def run_code(code: str, df_in, extra_ns: dict | None = None) -> dict:
    """Execute `code` with df, pd, np, and optionally imported packages in scope.

    Returns a result dict:
      ok        bool
      df        DataFrame | None
      stdout    str
      error     str
      variables dict   (non-df names created by the code)
      elapsed   float  (seconds)
    """
    import io
    import traceback
    from contextlib import redirect_stdout
    import pandas as pd
    import numpy as np

    ns: dict = {
        '__builtins__': __builtins__,
        'df': df_in.copy(),
        'pd': pd,
        'np': np,
    }
    if extra_ns:
        ns.update(extra_ns)

    buf = io.StringIO()
    t0 = time.perf_counter()
    try:
        with redirect_stdout(buf):
            exec(code, ns)  # noqa: S102
        elapsed = time.perf_counter() - t0
        result_df = ns.get('df')
        if not isinstance(result_df, pd.DataFrame):
            return {
                'ok': False,
                'df': None,
                'stdout': buf.getvalue(),
                'error': f'`df` must remain a DataFrame after execution — got {type(result_df).__name__}.',
                'variables': {},
                'elapsed': elapsed,
            }
        variables = {
            k: v for k, v in ns.items()
            if not k.startswith('_') and k not in ('df', 'pd', 'np', '__builtins__')
            and not callable(v)
        }
        return {
            'ok': True,
            'df': result_df,
            'stdout': buf.getvalue(),
            'error': '',
            'variables': variables,
            'elapsed': elapsed,
        }
    except Exception:
        elapsed = time.perf_counter() - t0
        tb_lines = traceback.format_exc().strip().split('\n')
        user_line = next(
            (ln.strip() for ln in reversed(tb_lines)
             if ln.strip().startswith('File') and '<string>' in ln),
            None,
        )
        error_msg = tb_lines[-1]
        clean = f'{user_line}\n{error_msg}' if user_line else error_msg
        return {
            'ok': False,
            'df': None,
            'stdout': buf.getvalue(),
            'error': clean,
            'variables': {},
            'elapsed': elapsed,
        }
