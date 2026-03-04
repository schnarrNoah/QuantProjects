import polars as pl

class ExitManager:
    @staticmethod
    def fixed_rrr(lf: pl.LazyFrame, entry_long: str, entry_short: str, rrr: float, lookback: int) -> pl.LazyFrame:
        """Berechnet SL über Swing High/Low und TP über festes RRR."""
        return lf.with_columns([
            pl.col("l").rolling_min(lookback).alias("sl_long"),
            pl.col("h").rolling_max(lookback).alias("sl_short")
        ]).with_columns([
            (pl.col(entry_long) + (pl.col(entry_long) - pl.col("sl_long")) * rrr).alias("tp_long"),
            (pl.col(entry_short) - (pl.col("sl_short") - pl.col(entry_short)) * rrr).alias("tp_short")
        ]).fill_null(strategy="backward")

    @staticmethod
    def session_level_exit(lf: pl.LazyFrame, session_prefix: str, rrr: float) -> pl.LazyFrame:
        """Beispiel: SL unter das Tief der Asia-Session setzen."""
        return lf.with_columns([
            pl.col(f"{session_prefix}_low").alias("sl_long"),
            pl.col(f"{session_prefix}_high").alias("sl_short")
        ]).with_columns([
            (pl.col("c") + (pl.col("c") - pl.col("sl_long")) * rrr).alias("tp_long"),
            (pl.col("c") - (pl.col("sl_short") - pl.col("c")) * rrr).alias("tp_short")
        ])