import time
import requests
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path


class API:
    def __init__(self, api_key, tickers, frame, parquet_file, start_fallback="2024-01-01"):
        self.apikey = api_key
        self.tickers = tickers
        self.frame = frame
        self.parquet_file = Path(parquet_file)
        self.start_fallback = start_fallback

    def get_last_timestamp(self):
        """Liest den letzten Zeitstempel aus der Parquet-Datei, ohne sie ganz zu laden."""
        if not self.parquet_file.exists():
            return None
        try:
            # Nutzt Lazy-Loading für maximale Geschwindigkeit
            last_ts = pl.scan_parquet(self.parquet_file).select(pl.col("t").tail(1)).collect().item()
            return last_ts
        except Exception as e:
            print(f"Hinweis: Konnte letzten Zeitstempel nicht lesen ({e}).")
            return None

    def run(self):
        """Hauptmethode: Berechnet Startzeit automatisch und startet Download."""
        # 1. Startzeit bestimmen
        last_ts = self.get_last_timestamp()

        if last_ts:
            # Wir starten 1 Minute nach dem letzten Eintrag
            start_dt = last_ts + timedelta(minutes=1)
            start_str = start_dt.strftime("%Y-%m-%d")
            print(f"Fortsetzung gefunden: Letzter Datenpunkt {last_ts}. Starte ab {start_str}")
        else:
            start_str = self.start_fallback
            print(f"Kein Bestand gefunden. Initialer Download ab {start_str}")

        # 2. Endzeit ist immer 'heute'
        end_str = datetime.now().strftime("%Y-%m-%d")

        # 3. Download für jeden Ticker
        for ticker in self.tickers:
            print(f"\n--- Syncing {ticker} ---")
            df_pandas = self.download_ticker_data(
                ticker,
                datetime.strptime(start_str, "%Y-%m-%d"),
                datetime.strptime(end_str, "%Y-%m-%d")
            )

            if not df_pandas.empty:
                self.append_to_parquet(df_pandas)
            else:
                print(f"Keine neuen Daten für {ticker} verfügbar.")

    def fetch_chunk(self, ticker, start, end, multiplier=1, retries=3):
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/"
            f"{multiplier}/{self.frame}/{start}/{end}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={self.apikey}"
        )
        for attempt in range(retries):
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    return pd.DataFrame(data.get("results", []))
                elif r.status_code == 429:
                    print("Rate Limit (429). Warte 60s...")
                    time.sleep(60)
                else:
                    print(f"Fehler {r.status_code}: {r.text}")
            except Exception as e:
                print(f"Verbindungsfehler: {e}")
            time.sleep(5)
        return pd.DataFrame()

    def download_ticker_data(self, ticker, start_date, end_date):
        current = start_date
        all_chunks = []
        # Falls start_date heute ist, überspringen
        if start_date.date() >= end_date.date():
            return pd.DataFrame()

        while current <= end_date:
            chunk_end = min(current + relativedelta(months=1), end_date)
            df = self.fetch_chunk(ticker, current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))
            if not df.empty:
                df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
                all_chunks.append(df)
            current = chunk_end + relativedelta(days=1)
            time.sleep(12)  # Polygon Free Tier

        return pd.concat(all_chunks).drop_duplicates(subset=["t"]).sort_values("t") if all_chunks else pd.DataFrame()

    def append_to_parquet(self, df_pandas):
        new_df = pl.from_pandas(df_pandas).with_columns([
            pl.col("t").dt.replace_time_zone(None).dt.cast_time_unit("us"),
            pl.col(["o", "h", "l", "c", "v", "vw"]).cast(pl.Float32)
        ]).select(["t", "o", "h", "l", "c", "v", "vw"])

        if self.parquet_file.exists():
            existing_df = pl.read_parquet(self.parquet_file).with_columns(pl.col("t").dt.cast_time_unit("us"))
            combined_df = pl.concat([existing_df, new_df])
        else:
            combined_df = new_df

        combined_df = (
            combined_df.unique(subset="t").sort("t")
            .upsample(time_column="t", every="1m")
            .with_columns([
                pl.col(["o", "h", "l", "c", "vw"]).forward_fill(),
                pl.col("v").fill_null(0)
            ])
        )
        self.parquet_file.parent.mkdir(parents=True, exist_ok=True)
        combined_df.write_parquet(self.parquet_file, compression="snappy")
        print(f"Update Erfolg: {combined_df.height:,} Zeilen insgesamt.")