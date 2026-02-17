# Algorithmic Strategist

Algorithmic trading strategy research, backtesting, and analysis toolkit.

## Quick Start

```bash
# Create virtual environment & install all dependencies (including dev tools)
uv sync --all-extras

# Run the CLI
uv run strategist

# Run directly
uv run python -m algorithmic_strategist.main
```

## Development

```bash
# Lint & format
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# Type-check
uv run mypy src/

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=algorithmic_strategist
```

## Project Structure

```
.
├── src/
│   └── algorithmic_strategist/
│       ├── __init__.py
│       ├── main.py
│       └── py.typed
├── tests/
│   ├── __init__.py
│   └── test_main.py
├── pyproject.toml
├── .python-version
├── .gitignore
└── README.md
```

## License

MIT
