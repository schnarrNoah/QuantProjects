import pandas as pd
import requests
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np


class API:
    def __init__(self, api_key, tickers, session, frame, start, end, save_dir):
        self.apikey = api_key
        self.tickers = tickers
        self.session = session
        self.frame = frame
        self.start = start
        self.end = end
        self.save_dir = save_dir

    def run_api(self):
        all_returns = {}

        for ticker in self.tickers:
            df = self.download_ticker_data(
                ticker,
                datetime.strptime(self.start, "%Y-%m-%d"),
                datetime.strptime(self.end, "%Y-%m-%d")
            )

            # keine CSV laden, direkt weiter
            all_returns[ticker] = self.compute_log_returns(df)  # optional: Session-Filter weglassen

            # Session filtern
            df_sess = self.filter_session(df, self.session)
            all_returns[ticker] = self.compute_log_returns(df_sess)

    def fetch_chunk(self, ticker, start, end, multiplier=1, retries=2):
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/"
            f"{multiplier}/{self.frame}/{start}/{end}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={self.apikey}"
        )

        for attempt in range(retries):
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                return pd.DataFrame(data.get("results", []))
            print(f"Retry {attempt+1} failed for {ticker} {self.start} → {end}: {r.text}")
            time.sleep(5)
        return pd.DataFrame()


    def download_ticker_data(self, ticker, start_date, end_date):
        current = start_date
        all_chunks = []

        while current < end_date:
            chunk_end = min(current + relativedelta(months=1), end_date)
            start_str = current.strftime("%Y-%m-%d")
            end_str = chunk_end.strftime("%Y-%m-%d")
            print(f"Fetching {ticker}: {start_str} → {end_str}")

            df = self.fetch_chunk(ticker, start_str, end_str, self.frame)
            if not df.empty:
                df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
                df.set_index("t", inplace=True)
                all_chunks.append(df)

            current = chunk_end
            time.sleep(13)  # Rate-Limit-Schutz

        if not all_chunks:
            print(f"No data for {ticker}")
            return pd.DataFrame()

        df = pd.concat(all_chunks)
        df = df[~df.index.duplicated(keep="last")].sort_index()

        # Neuer Dateiname mit Zeitraum
        start_str_file = start_date.strftime("%Y-%m-%d")
        end_str_file = end_date.strftime("%Y-%m-%d")
        file_path = SAVE_DIR / f"{ticker.replace(':','_')}_{start_str_file}_{end_str_file}_{self.frame}.csv"

        df.to_csv(file_path, index_label="t")
        print(f"Saved → {file_path}")
        return df


    def filter_session(self, df):
        """Filtert Daten nach Session. Wenn session leer, wird nicht gefiltert."""
        if not self.session:
            return df

        sessions = {
            "london": (8, 16),
            "newyork": (13, 21),
            "overlap": (13, 16),
        }
        if self.session not in sessions:
            raise ValueError(f"Unknown session: {self.session}")

        start, end = sessions[self.session]
        return df.between_time(f"{start}:00", f"{end}:00")

    def compute_log_returns(df, price_col="c"):
        """Berechnet Log-Returns."""
        returns = np.log(df[price_col]).diff()
        returns.name = "log_returns"
        return returns.dropna()