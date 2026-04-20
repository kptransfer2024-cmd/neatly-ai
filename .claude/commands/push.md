Test, commit, and push the current branch to GitHub.

Steps:
1. Run `pytest tests/ -v`. If any tests fail, STOP and report the failures — do not commit.
2. Run `git status` to see what's staged and unstaged.
3. Stage only files in: `detectors/`, `tests/`, `app.py`, `orchestrator.py`, `explanation_layer.py`, `transformation_executor.py`, `requirements.txt`, `CLAUDE.md`. Never stage `.env` or secrets.
4. Commit with message format `feat: [what]` or `fix: [what]`. Ask the user for the message if unclear.
5. Run `git push`.
6. Report the commit hash and confirm the push succeeded.
