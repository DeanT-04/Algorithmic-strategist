---
description: Mandatory end-of-task checklist — test, tidy, clean
---

# Finish Task Checklist

Run this checklist at the **end of every task**, no exceptions. The goal is to keep professional standards and move fast.

## 1. Test

// turbo
```bash
uv run pytest -v
```

// turbo
```bash
uv run ruff check src/ tests/
```

// turbo
```bash
uv run mypy src/
```

- Fix any failures before moving on.

## 2. Tidy

// turbo
```bash
uv run ruff format src/ tests/
```

- Ensure imports are sorted, code is formatted, no dead code remains.
- Remove any temporary files, debug prints, or commented-out blocks added during the task.

## 3. Clean

- Verify the project structure is still organised — no orphaned files, no misplaced modules.
- Confirm `pyproject.toml` dependencies are up to date if any were added.
- Run `uv sync` if dependencies changed.

Only mark the task as complete once all three steps pass cleanly.
