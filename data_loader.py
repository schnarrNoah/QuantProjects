import pandas as pd
import requests
import time

POLYGON_API_KEY = ""
pg_tickers = ["C:EURUSD", "X:BTCUSD"]

# -------------------------------
# API
# -------------------------------
def get_fx_pairs_data(pg_tickers, start, end):
    multiplier = 1
    timespan = "minute"
    fx_pairs_dict = {}

    for pair in pg_tickers:
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
        fx_pairs_dict[pair] = pd.DataFrame(data.get("results", []))
        time.sleep(12)

    return fx_pairs_dict


# -------------------------------
# Formatting
# -------------------------------
def format_fx_pairs(fx_pairs_dict):
    for pair, df in fx_pairs_dict.items():
        if df.empty:
            continue
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        fx_pairs_dict[pair] = df.set_index("t")
    return fx_pairs_dict


# -------------------------------
# Session Filter
# -------------------------------
def filter_session(df, session="london"):
    """
    Sessions in UTC
    London: 08–16
    New York: 13–21
    Overlap: 13–16
    """
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
    return pd.Series(
        pd.np.log(df[price_col]).diff(),
        index=df.index,
        name="returns",
    )
