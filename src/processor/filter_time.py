import polars as pl
import datetime
from src.processor.session import SessionProcessor  # Dein Manager


def timezone(df):
    """Filtert die raw_data in eine trading_data View."""
    if df.raw_data is not None:
        print(f"--- Filtering Timezone: {df.start_time} - {df.end_time} ---")

        t_start = datetime.time.fromisoformat(df.start_time)
        t_end = datetime.time.fromisoformat(df.end_time)

        start_int = t_start.hour * 3600 + t_start.minute * 60 + t_start.second
        end_int = t_end.hour * 3600 + t_end.minute * 60 + t_end.second

        df.trading_data = df.raw_data.filter(
            pl.col("t_int").is_between(start_int, end_int)
        )
    return df


def session(lf: pl.LazyFrame, session_name: str) -> pl.LazyFrame:
    if session_name is None:
        return lf

    # Wir ziehen uns die Zeiten direkt aus dem SessionProcessor
    # Damit musst du sie NIE WIEDER an zwei Stellen ändern.
    times = SessionProcessor.SESSIONS.get(session_name.lower())

    if not times:
        print(f"Warnung: Session {session_name} nicht im SessionProcessor gefunden!")
        return lf

    start_int, end_int = times

    # Falls die Session über Mitternacht geht (start > end)
    if start_int > end_int:
        return lf.filter((pl.col("t_int") >= start_int) | (pl.col("t_int") <= end_int))

    return lf.filter(pl.col("t_int").is_between(start_int, end_int))