import polars as pl
from pathlib import Path
import datetime as dt

class ParquetReader:
    def __init__(self, parquet_path):
        """
        Initialisiert den Reader mit dem Pfad zur master.parquet Datei.
        """
        self.parquet_path = Path(parquet_path)
        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Die Datei {self.parquet_path} wurde nicht gefunden!")

    def load_range(self, start, end):
        # 1. Konvertierung der Eingabe-Strings
        start_dt = dt.datetime.fromisoformat(str(start)).replace(tzinfo=None)
        end_dt = dt.datetime.fromisoformat(str(end)).replace(tzinfo=None)

        # 2. Lazy Scan
        q = pl.scan_parquet(self.parquet_path)

        # 3. TYP-Korrektur und Filterung
        return (
            q.with_columns([
                # Falls 't' ein Int64 ist, konvertiere es in Datetime
                # Falls es schon Datetime ist, schadet dieser Cast nicht
                pl.col("t").cast(pl.Datetime("ms"))
            ])
            .filter(
                pl.col("t").is_between(start_dt, end_dt)
            )
            .with_columns([
                # t_int Berechnung für Session-Filter
                (pl.col("t").dt.hour().cast(pl.Int32) * 3600 +
                 pl.col("t").dt.minute().cast(pl.Int32) * 60 +
                 pl.col("t").dt.second().cast(pl.Int32)).alias("t_int")
            ])
            .sort("t")  # Sicherstellen, dass die Zeitachse stimmt
            .collect()
        )