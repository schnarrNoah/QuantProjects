import polars as pl

def fvg_1min(df):
    return df.with_columns([
        # Bullish FVG
        (pl.col("l") > pl.col("h").shift(2)).alias("bull_fvg"),
        pl.col("l").alias("bull_fvg_top"),
        pl.col("h").shift(2).alias("bull_fvg_bottom"),

        # Bearish FVG
        (pl.col("h") < pl.col("l").shift(2)).alias("bear_fvg"),
        pl.col("l").shift(2).alias("bear_fvg_top"),
        pl.col("h").alias("bear_fvg_bottom")
    ]).with_columns([
        # Nulls (durch shift) sauber zu False konvertieren
        pl.col("bull_fvg").fill_null(False),
        pl.col("bear_fvg").fill_null(False)
    ])

def fvg_5min(df):
    # In Polars nutzen wir rolling_min/max für die Fenster-Logik
    step = 5
    return df.with_columns([
        pl.col("l").rolling_min(window_size=step).alias("curr_low"),
        pl.col("h").rolling_max(window_size=step).alias("curr_high"),
        pl.col("h").rolling_max(window_size=step).shift(2 * step).alias("past_high"),
        pl.col("l").rolling_min(window_size=step).shift(2 * step).alias("past_low")
    ]).with_columns([
        (pl.col("curr_low") > pl.col("past_high")).alias("bull_fvg"),
        pl.col("curr_low").alias("bull_fvg_top"),
        pl.col("past_high").alias("bull_fvg_bottom"),

        (pl.col("curr_high") < pl.col("past_low")).alias("bear_fvg"),
        pl.col("past_low").alias("bear_fvg_top"),
        pl.col("curr_high").alias("bear_fvg_bottom")
    ]).with_columns([
        pl.col("bull_fvg").fill_null(False),
        pl.col("bear_fvg").fill_null(False)
    ]).drop(["curr_low", "curr_high", "past_high", "past_low"]) # Hilfsspalten aufräumen