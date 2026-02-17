"""Smoke tests for the main entry point."""

from __future__ import annotations

from algorithmic_strategist.main import main


def test_main_returns_zero() -> None:
    """Entry point should exit cleanly."""
    assert main() == 0
