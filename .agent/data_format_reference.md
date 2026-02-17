# Data Format Reference – Parquet ↔ backtesting.py

> Quick-reference so we never waste tokens rediscovering this.

## Parquet Files (from Dukascopy)

**Location:** `historical_data/<SYMBOL>/<TIMEFRAME>/<SYMBOL>_<TIMEFRAME>.parquet`

**Columns (lowercase):**
| Column   | Type    |
|----------|---------|
| `open`   | float64 |
| `high`   | float64 |
| `low`    | float64 |
| `close`  | float64 |
| `volume` | float64 |

**Index:** `DatetimeIndex`, timezone-aware (`UTC`)  
**Index name:** `timestamp`

## backtesting.py Requirements

backtesting.py (`from backtesting import Backtest, Strategy`) expects:

| Column   | Type    |
|----------|---------|
| `Open`   | float64 |
| `High`   | float64 |
| `Low`    | float64 |
| `Close`  | float64 |
| `Volume` | float64 |

**Index:** `DatetimeIndex`, **timezone-naive** (no tz info)

## Conversion (copy-paste ready)

```python
df = pd.read_parquet(path)
df.columns = [c.capitalize() for c in df.columns]
if df.index.tz is not None:
    df.index = df.index.tz_localize(None)
```

## Available Symbols

EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD, EURGBP, EURJPY, GBPJPY, XAUUSD, XAGUSD

## Available Timeframes

| Folder  | Period    | Lookback   |
|---------|-----------|------------|
| `1min`  | 1 minute  | 6 months   |
| `5min`  | 5 minutes | 6 months   |
| `15min` | 15 min    | 1 year     |
| `30min` | 30 min    | 1 year     |
| `1hr`   | 1 hour    | 1 year     |
| `4hr`   | 4 hours   | 5 years    |
| `1day`  | 1 day     | 5 years    |

## Strategies Location

All strategies live in `strategies/` at project root.
