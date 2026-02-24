import polars as pl
import datetime


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


import polars as pl

def session(df, session_name: str):
    if session_name is None:
        print("--- No Session Filter: Monitoring 24/7 ---")
        return df

    sessions = {
        "asia": (0, 21600),
        "frankfurt": (25200, 54000),
        "london": (28800, 57600),
        "ny": (46800, 75600),
        "full": (0, 86399)
    }

    session_name = session_name.lower()

    if session_name not in sessions:
        print(f"Warning: Session '{session_name}' not found. No filtering applied.")
        return df

    start_int, end_int = sessions[session_name]

    #debugging
    sample_ints = df.select("t_int").head(5).to_series().to_list()
    print(f"DEBUG: Session: {session_name} | Bereich {start_int} bis {end_int}")

    result = df.filter(
        pl.col("t_int").is_between(start_int, end_int)
    )

    return result