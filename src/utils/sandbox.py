"""Codespace sandbox utilities: package management and safe execution."""

import builtins as _builtins_mod
import importlib.metadata
import io
import re
import subprocess
import sys
import time
import traceback
from contextlib import redirect_stdout
from typing import Optional

# ---------------------------------------------------------------------------
# Safe builtins — allow imports & all data-science operations; block shell
# ---------------------------------------------------------------------------

_SAFE_BUILTIN_NAMES = {
    'abs', 'all', 'any', 'ascii', 'bin', 'bool', 'bytearray', 'bytes',
    'callable', 'chr', 'complex', 'delattr', 'dict', 'dir', 'divmod',
    'enumerate', 'filter', 'float', 'format', 'frozenset', 'getattr',
    'globals', 'hasattr', 'hash', 'hex', 'id', 'int', 'isinstance',
    'issubclass', 'iter', 'len', 'list', 'locals', 'map', 'max', 'min',
    'next', 'object', 'oct', 'ord', 'pow', 'print', 'property', 'range',
    'repr', 'reversed', 'round', 'set', 'setattr', 'slice', 'sorted',
    'staticmethod', 'str', 'sum', 'super', 'tuple', 'type', 'vars', 'zip',
    # exceptions
    'ArithmeticError', 'AttributeError', 'EOFError', 'Exception',
    'FileNotFoundError', 'FloatingPointError', 'GeneratorExit', 'IOError',
    'ImportError', 'IndexError', 'KeyError', 'KeyboardInterrupt',
    'ModuleNotFoundError', 'NameError', 'NotImplementedError', 'OSError',
    'OverflowError', 'RecursionError', 'RuntimeError', 'StopIteration',
    'SyntaxError', 'TypeError', 'UnicodeError', 'ValueError',
    'ZeroDivisionError',
    # constants
    'Ellipsis', 'False', 'None', 'NotImplemented', 'True',
    # needed for `import X` statements inside exec
    '__import__', '__name__', '__doc__', '__package__', '__spec__',
    '__loader__', '__build_class__',
}

_SAFE_BUILTINS: dict = {k: getattr(_builtins_mod, k)
                        for k in _SAFE_BUILTIN_NAMES if hasattr(_builtins_mod, k)}


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

def run_code(
    code: str,
    df_in,
    extra_ns: dict | None = None,
    kernel_ns: dict | None = None,
) -> dict:
    """Execute `code` in a sandboxed namespace.

    Uses a curated builtins set so `import` works but shell access is blocked.
    If `kernel_ns` is provided, variables from previous runs are available and
    new variables are merged back into it after execution (persistent kernel).

    Returns:
      ok        bool
      df        DataFrame | None
      stdout    str
      error     str          (empty string on success)
      lineno    int | None   (line number of error, if parseable)
      variables dict         (non-df names created or updated by the code)
      elapsed   float        (seconds)
    """
    import pandas as pd
    import numpy as np

    ns: dict = {
        '__builtins__': _SAFE_BUILTINS,
        'df': df_in.copy(),
        'pd': pd,
        'np': np,
    }
    # Merge kernel state so previous variables are still in scope
    if kernel_ns:
        for k, v in kernel_ns.items():
            if k not in ns:
                ns[k] = v
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
                'ok': False, 'df': None,
                'stdout': buf.getvalue(),
                'error': f'`df` must remain a DataFrame — got {type(result_df).__name__}.',
                'lineno': None, 'variables': {}, 'elapsed': elapsed,
            }
        variables = {
            k: v for k, v in ns.items()
            if not k.startswith('_')
            and k not in ('df', 'pd', 'np', '__builtins__')
            and not callable(v)
        }
        # Persist new variables back into kernel_ns
        if kernel_ns is not None:
            kernel_ns.update(variables)
        return {
            'ok': True, 'df': result_df,
            'stdout': buf.getvalue(),
            'error': '', 'lineno': None,
            'variables': variables, 'elapsed': elapsed,
        }
    except Exception:
        elapsed = time.perf_counter() - t0
        tb_lines = traceback.format_exc().strip().split('\n')
        user_frame = next(
            (ln.strip() for ln in reversed(tb_lines)
             if ln.strip().startswith('File') and '<string>' in ln),
            None,
        )
        error_msg = tb_lines[-1]
        clean = f'{user_frame}\n{error_msg}' if user_frame else error_msg
        # Try to extract line number from the traceback
        lineno = None
        if user_frame:
            m = re.search(r'line (\d+)', user_frame)
            if m:
                lineno = int(m.group(1))
        return {
            'ok': False, 'df': None,
            'stdout': buf.getvalue(),
            'error': clean, 'lineno': lineno,
            'variables': {}, 'elapsed': elapsed,
        }


def repr_variable(v) -> str:
    """One-line human-readable summary of a variable value."""
    import pandas as pd
    import numpy as np
    if isinstance(v, pd.DataFrame):
        return f'DataFrame {v.shape[0]:,}×{v.shape[1]}'
    if isinstance(v, pd.Series):
        return f'Series len={len(v):,}'
    if isinstance(v, np.ndarray):
        return f'ndarray {v.shape}'
    if isinstance(v, (list, tuple, set, frozenset)):
        return f'{type(v).__name__} len={len(v):,}'
    if isinstance(v, dict):
        return f'dict {len(v)} keys'
    s = repr(v)
    return s[:60] + '…' if len(s) > 60 else s
