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
        # Konvertierung zu datetime (ohne Zeitzone, da wir sie im Converter entfernt haben)
        start_dt = dt.datetime.fromisoformat(str(start)).replace(tzinfo=None)
        end_dt = dt.datetime.fromisoformat(str(end)).replace(tzinfo=None)

        # DER ULTIMATIVE LAZY FLOW
        # Polars scannt hier nur die Metadaten der Parquet-Datei.
        # Er weiß sofort, wo die Daten für deinen Zeitbereich liegen.
        # In deiner load_range Methode im ParquetReader:
        return (
            pl.scan_parquet(self.parquet_path)
            .filter(pl.col("t").is_between(start_dt, end_dt))
            .with_columns(
                # Wir berechnen t_int absolut sicher:
                (pl.col("t").dt.hour().cast(pl.Int32) * 3600 +
                 pl.col("t").dt.minute().cast(pl.Int32) * 60 +
                 pl.col("t").dt.second().cast(pl.Int32)).alias("t_int")
            )
            .select(["t", "o", "h", "l", "c", "v", "vw", "t_int"])
            .collect()
        )