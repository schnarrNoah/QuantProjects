import polars as pl
from pathlib import Path
import datetime as dt


class ParquetReader:
    def __init__(self, parquet_path):
        self.parquet_path = Path(parquet_path)
        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Datei {self.parquet_path} nicht gefunden!")

    def load_range(self, start, end):
        # 1. Konvertierung der Zeitstempel
        start_dt = dt.datetime.fromisoformat(str(start)).replace(tzinfo=None)
        end_dt = dt.datetime.fromisoformat(str(end)).replace(tzinfo=None)

        # 2. Lazy Scan starten
        q = pl.scan_parquet(self.parquet_path)

        # 3. Effiziente Pipeline (Lazy)
        return (
            q.filter(
                pl.col("t").is_between(start_dt, end_dt)
            )
            .with_columns([
                # --- SPEICHER-OPTIMIERUNG (Punkt 2) ---
                # Preisdaten auf Float32 casten (reicht völlig und spart 50% RAM)
                pl.col("o", "h", "l", "c").cast(pl.Float32),

                # --- SESSION-HELPER ---
                (pl.col("t").dt.hour().cast(pl.Int32) * 3600 +
                 pl.col("t").dt.minute().cast(pl.Int32) * 60 +
                 pl.col("t").dt.second().cast(pl.Int32)).alias("t_int")
            ])
            # --- ROBUSTHEIT ---
            .drop_nulls(subset=["o", "h", "l", "c"])  # Kaputte Kerzen direkt aussortieren
            .unique(subset=["t"])  # Doppelte Zeitstempel verhindern
            .sort("t")
            .collect()  # Erst hier wird die Arbeit wirklich ausgeführt
        )