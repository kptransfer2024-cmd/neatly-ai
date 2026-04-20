Scaffold a new detector module. The argument is the detector name in snake_case (e.g. `duplicate_detector`).

Steps:
1. Create `detectors/<name>.py` with this exact structure:
   - Module docstring: one sentence describing what it detects
   - `detect(df: pd.DataFrame) -> list[dict]` function that raises `NotImplementedError`
   - Each returned issue dict must follow: `{"type": str, "column": str, "detail": str, "count": int}`

2. Create `tests/test_<name>.py` with:
   - One passing smoke test: `test_returns_list` — calls `detect()` on a minimal DataFrame and asserts the return is a list
   - One placeholder test marked `@pytest.mark.skip(reason="not implemented")` for the real logic

3. Run `pytest tests/test_<name>.py -v` to confirm the smoke test passes.

4. Report what was created and the test result.
