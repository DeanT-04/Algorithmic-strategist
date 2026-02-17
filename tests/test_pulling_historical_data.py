"""Tests for the historical data pulling module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from algorithmic_strategist.pulling_historical_data import clean_dataframe, load_symbols


def _make_ohlcv_df(data: list[dict]) -> pd.DataFrame:
    """Helper to build a timestamped OHLCV DataFrame."""
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp")
    return df


class TestCleanDataframe:
    """Verify defensive cleaning logic."""

    def test_removes_duplicate_timestamps(self) -> None:
        df = _make_ohlcv_df(
            [
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.15,
                    "volume": 100,
                },
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.15,
                    "volume": 100,
                },
                {
                    "timestamp": "2025-01-01 01:00",
                    "open": 1.2,
                    "high": 1.3,
                    "low": 1.1,
                    "close": 1.25,
                    "volume": 200,
                },
            ]
        )
        result = clean_dataframe(df)
        assert len(result) == 2

    def test_removes_nan_rows(self) -> None:
        df = _make_ohlcv_df(
            [
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.15,
                    "volume": 100,
                },
                {
                    "timestamp": "2025-01-01 01:00",
                    "open": None,
                    "high": 1.3,
                    "low": 1.1,
                    "close": 1.25,
                    "volume": 200,
                },
            ]
        )
        result = clean_dataframe(df)
        assert len(result) == 1

    def test_removes_corrupt_bars(self) -> None:
        df = _make_ohlcv_df(
            [
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 0.9,
                    "low": 1.2,
                    "close": 1.15,
                    "volume": 100,
                },
                {
                    "timestamp": "2025-01-01 01:00",
                    "open": 1.2,
                    "high": 1.3,
                    "low": 1.1,
                    "close": 1.25,
                    "volume": 200,
                },
            ]
        )
        result = clean_dataframe(df)
        assert len(result) == 1

    def test_removes_zero_volume(self) -> None:
        df = _make_ohlcv_df(
            [
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.15,
                    "volume": 0,
                },
                {
                    "timestamp": "2025-01-01 01:00",
                    "open": 1.2,
                    "high": 1.3,
                    "low": 1.1,
                    "close": 1.25,
                    "volume": 200,
                },
            ]
        )
        result = clean_dataframe(df)
        assert len(result) == 1

    def test_clean_data_passes_through(self) -> None:
        df = _make_ohlcv_df(
            [
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.15,
                    "volume": 100,
                },
                {
                    "timestamp": "2025-01-01 01:00",
                    "open": 1.2,
                    "high": 1.3,
                    "low": 1.1,
                    "close": 1.25,
                    "volume": 200,
                },
                {
                    "timestamp": "2025-01-01 02:00",
                    "open": 1.3,
                    "high": 1.4,
                    "low": 1.2,
                    "close": 1.35,
                    "volume": 300,
                },
            ]
        )
        result = clean_dataframe(df)
        assert len(result) == 3

    def test_sorted_output(self) -> None:
        df = _make_ohlcv_df(
            [
                {
                    "timestamp": "2025-01-01 02:00",
                    "open": 1.3,
                    "high": 1.4,
                    "low": 1.2,
                    "close": 1.35,
                    "volume": 300,
                },
                {
                    "timestamp": "2025-01-01 00:00",
                    "open": 1.1,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.15,
                    "volume": 100,
                },
                {
                    "timestamp": "2025-01-01 01:00",
                    "open": 1.2,
                    "high": 1.3,
                    "low": 1.1,
                    "close": 1.25,
                    "volume": 200,
                },
            ]
        )
        result = clean_dataframe(df)
        assert result.index.is_monotonic_increasing


class TestLoadSymbols:
    """Verify symbol loading."""

    def test_loads_symbols_from_config(self, tmp_path: Path) -> None:
        config = {"symbols": [{"instrument": "EUR/USD", "label": "EURUSD"}]}
        config_file = tmp_path / "symbols.json"
        config_file.write_text(json.dumps(config))

        with patch("algorithmic_strategist.pulling_historical_data.CONFIG_PATH", config_file):
            symbols = load_symbols()

        assert len(symbols) == 1
        assert symbols[0]["instrument"] == "EUR/USD"
        assert symbols[0]["label"] == "EURUSD"
