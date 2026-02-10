from config.config import POLYGON_API_KEY, SESSION
import pandas as pd
import requests
import time
import numpy as np


# -------------------------------
# API
# -------------------------------
def get_data(tickers, start, end):
    multiplier = 1
    timespan = "minute"
    d = {} #datadict

    for pair in tickers:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{pair}/range/"
            f"{multiplier}/{timespan}/{start}/{end}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
        )

        r = requests.get(url)
        print(pair, r.status_code)

        if r.status_code != 200:
            print(r.text)
            continue

        data = r.json()
        d[pair] = pd.DataFrame(data.get("results", []))
        time.sleep(12)

    return d


# -------------------------------
# Formatting
# -------------------------------
def format_data(d):
    for pair, df in d.items():
        if df.empty:
            continue
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        d[pair] = df.set_index("t")
    return d


# -------------------------------
# Session Filter
# -------------------------------
def filter_session(df, session=SESSION):
    #Sessions in UTC
    sessions = {
        "london": (8, 16),
        "newyork": (13, 21),
        "overlap": (13, 16),
    }
    start, end = sessions[session]
    return df.between_time(f"{start}:00", f"{end}:00")


# -------------------------------
# Returns
# -------------------------------
def compute_log_returns(df, price_col="c"):
    returns = np.log(df[price_col]).diff()
    returns.name = "log_returns"
    return returns.dropna()
