"""Pull historical OHLCV data from Dukascopy and store as Parquet files.

Reads symbols from config/symbols.json and downloads candle data for each
symbol across multiple timeframes, organised into:

    historical_data/<SYMBOL>/<TIMEFRAME>/<SYMBOL>_<TIMEFRAME>.parquet

Timeframe windows
-----------------
- 6 months  : 1min, 5min
- 1 year    : 15min, 30min, 1hr
- 5 years   : 4hr, 1day

Data quality
------------
Dukascopy is an institutional-grade data provider. Their aggregated candle data
is derived from pre-filtered tick data and is generally clean. This script
applies additional defensive cleaning:
  - Drop duplicate timestamps
  - Drop rows with any NaN in OHLCV columns
  - Drop rows where high < low (corrupt bars)
  - Drop rows with non-positive volume
  - Sort by timestamp (ascending)
  - Verify monotonically increasing index
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import dukascopy_python as dp
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "config" / "symbols.json"
DATA_DIR = ROOT_DIR / "historical_data"

# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Timeframe configuration ─────────────────────────────────────────────
# Each entry: (dukascopy interval constant, folder label, lookback in days)
SIX_MONTHS_DAYS = 183
ONE_YEAR_DAYS = 365
FIVE_YEARS_DAYS = 1826

TIMEFRAME_CONFIG: list[tuple[str, str, int]] = [
    # 6-month lookback
    (dp.INTERVAL_MIN_1, "1min", SIX_MONTHS_DAYS),
    (dp.INTERVAL_MIN_5, "5min", SIX_MONTHS_DAYS),
    # 1-year lookback
    (dp.INTERVAL_MIN_15, "15min", ONE_YEAR_DAYS),
    (dp.INTERVAL_MIN_30, "30min", ONE_YEAR_DAYS),
    (dp.INTERVAL_HOUR_1, "1hr", ONE_YEAR_DAYS),
    # 5-year lookback
    (dp.INTERVAL_HOUR_4, "4hr", FIVE_YEARS_DAYS),
    (dp.INTERVAL_DAY_1, "1day", FIVE_YEARS_DAYS),
]


# ── Helpers ──────────────────────────────────────────────────────────────
def load_symbols() -> list[dict[str, str]]:
    """Load symbol definitions from config/symbols.json."""
    if not CONFIG_PATH.exists():
        log.error("Symbol config not found: %s", CONFIG_PATH)
        sys.exit(1)

    with CONFIG_PATH.open() as f:
        data = json.load(f)

    symbols: list[dict[str, str]] = data["symbols"]
    log.info("Loaded %d symbols from config", len(symbols))
    return symbols


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply defensive data-quality filters to an OHLCV DataFrame.

    Dukascopy data is already institutional-grade, but we guard against:
    - Duplicate timestamps
    - NaN / null values in OHLCV columns
    - Corrupt bars where high < low
    - Non-positive volume
    """
    original_len = len(df)

    # Ensure sorted by timestamp
    df = df.sort_index()

    # Drop duplicate timestamps
    df = df[~df.index.duplicated(keep="first")]

    # Drop rows with any NaN in OHLCV columns
    ohlcv_cols = ["open", "high", "low", "close", "volume"]
    df = df.dropna(subset=ohlcv_cols)

    # Drop corrupt bars: high must be >= low
    df = df[df["high"] >= df["low"]]

    # Drop rows with non-positive volume
    df = df[df["volume"] > 0]

    dropped = original_len - len(df)
    if dropped > 0:
        log.warning("  Cleaned %d bad rows (%.2f%%)", dropped, dropped / original_len * 100)

    # Final integrity check
    if not df.index.is_monotonic_increasing:
        log.error("  Index is NOT monotonically increasing after cleaning!")

    return df


def download_symbol_timeframe(
    instrument: str,
    label: str,
    interval: str,
    tf_label: str,
    lookback_days: int,
) -> None:
    """Download, clean, and save a single symbol + timeframe combination."""
    now = datetime.now(tz=UTC)
    start = now - timedelta(days=lookback_days)

    # Build output path
    out_dir = DATA_DIR / label / tf_label
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{label}_{tf_label}.parquet"

    log.info(
        "  [%s] %s | %s -> %s",
        label,
        tf_label,
        start.strftime("%Y-%m-%d"),
        now.strftime("%Y-%m-%d"),
    )

    try:
        df = dp.fetch(
            instrument=instrument,
            interval=interval,
            offer_side=dp.OFFER_SIDE_BID,
            start=start,
            end=now,
        )
    except Exception:
        log.exception("  FAILED to download %s %s", label, tf_label)
        return

    if df.empty:
        log.warning("  No data returned for %s %s", label, tf_label)
        return

    # Clean
    df = clean_dataframe(df)

    if df.empty:
        log.warning("  All data dropped after cleaning for %s %s", label, tf_label)
        return

    # Save as Parquet (efficient, typed, compressed)
    df.to_parquet(out_file, engine="pyarrow")

    log.info(
        "  Saved %s rows | %s to %s | %s",
        f"{len(df):,}",
        df.index.min().strftime("%Y-%m-%d %H:%M"),
        df.index.max().strftime("%Y-%m-%d %H:%M"),
        out_file.name,
    )


# ── Main ─────────────────────────────────────────────────────────────────
def main() -> int:
    """Entry point - pull all historical data sequentially."""
    log.info("=" * 60)
    log.info("Dukascopy Historical Data Downloader")
    log.info("=" * 60)

    symbols = load_symbols()
    total = len(symbols) * len(TIMEFRAME_CONFIG)
    completed = 0

    for sym in symbols:
        instrument = sym["instrument"]
        label = sym["label"]
        log.info("-" * 40)
        log.info("Symbol: %s (%s)", label, instrument)
        log.info("-" * 40)

        for interval, tf_label, lookback_days in TIMEFRAME_CONFIG:
            completed += 1
            log.info("[%d/%d] Downloading...", completed, total)
            download_symbol_timeframe(instrument, label, interval, tf_label, lookback_days)

    log.info("=" * 60)
    log.info("Download complete. %d/%d tasks finished.", completed, total)
    log.info("Data saved to: %s", DATA_DIR)
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
