"""SMA Crossover Strategy – smoke-test for the backtesting pipeline.

Uses EURUSD 1-hour data to verify that:
  1. Historical Parquet data loads correctly.
  2. The `backtesting` library runs end-to-end.
  3. Stats and an HTML report are produced.

Strategy logic
--------------
- **Buy** when the fast SMA crosses ABOVE the slow SMA.
- **Sell** when the fast SMA crosses BELOW the slow SMA.
- Default parameters: fast = 10, slow = 30.

Usage
-----
    uv run python strategies/sma_crossover.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA


# ── Configuration ────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT_DIR / "historical_data" / "EURUSD" / "1hr" / "EURUSD_1hr.parquet"
REPORTS_DIR = ROOT_DIR / "reports" / "backtests"


# ── Data loader ──────────────────────────────────────────────────────────
def load_data(path: Path) -> pd.DataFrame:
    """Load a Parquet OHLCV file and format it for backtesting.py.

    backtesting.py expects:
      - Capitalised column names: Open, High, Low, Close, Volume
      - A DatetimeIndex (timezone-naive)
    """
    df = pd.read_parquet(path)

    # Capitalise columns to match backtesting.py convention
    df.columns = [c.capitalize() for c in df.columns]

    # Strip timezone info – backtesting.py works with tz-naive datetimes
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    return df


# ── Strategy definition ─────────────────────────────────────────────────
class SmaCrossover(Strategy):
    """Simple Moving Average crossover strategy.

    Parameters
    ----------
    fast_period : int
        Lookback window for the fast SMA (default 10).
    slow_period : int
        Lookback window for the slow SMA (default 30).
    """

    fast_period = 10
    slow_period = 30

    def init(self) -> None:
        close = self.data.Close
        self.fast_sma = self.I(SMA, close, self.fast_period)
        self.slow_sma = self.I(SMA, close, self.slow_period)

    def next(self) -> None:
        if crossover(self.fast_sma, self.slow_sma):
            self.buy()
        elif crossover(self.slow_sma, self.fast_sma):
            self.sell()


# ── Main ─────────────────────────────────────────────────────────────────
def main() -> None:
    """Run the SMA crossover backtest and print results."""
    print("=" * 60)
    print("SMA Crossover Backtest  –  EURUSD 1hr")
    print("=" * 60)

    # Load data
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Data file not found: {DATA_FILE}\n"
            "Run `uv run pull-data` first to download historical data."
        )

    df = load_data(DATA_FILE)
    print(f"\nData loaded: {len(df):,} bars  |  "
          f"{df.index.min()} → {df.index.max()}")

    # Run backtest
    bt = Backtest(
        df,
        SmaCrossover,
        cash=10_000,
        commission=0.00007,   # ~0.7 pip spread for EURUSD
        exclusive_orders=True,
    )

    stats = bt.run()

    # Print key statistics
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)
    print(stats)

    # Save HTML report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / "sma_crossover_eurusd_1hr.html"
    bt.plot(filename=str(report_path), open_browser=False)
    print(f"\nInteractive report saved → {report_path}")


if __name__ == "__main__":
    main()
