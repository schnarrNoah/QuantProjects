from pathlib import Path

# Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# Marketdata
PARQUET_FILE = PROJECT_ROOT / "Cryptocurrencies" / "BTCUSD" / "btc_1min.parquet"


# BACKTEST TIMEFRAME & SESSIONS
START_DATE = "2025-01-01T00:00:00"
END_DATE   = "2025-12-31T23:59:59"
START_TIME = "14:30"
END_TIME   = "16:30"
SESSION    = "ny"

# STRATEGIES
ACTIVE_STRATEGIES = [
    {
        "active": True,
        "class_name": "SMC_NY_Strategy",
        "display_name": "SMC_Aggressive_RRR_3",
        "params": {"rrr": 3.0}
    },
    {
        "active": False,
        "class_name": "SMC_NY_Strategy",
        "display_name": "SMC_Standard_RRR_1",
        "params": {"rrr": 1.0}
    }
]


# DOWNLOAD CONFIGURATION
DOWNLOAD_TICKERS = ["X:BTCUSD"]
DOWNLOAD_FRAME = "minute"
DOWNLOAD_START_FALLBACK = "2025-01-01"