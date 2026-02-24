from src.strategy.base import BaseStrategy
from src.process.trader import jit_simulator
import polars as pl


class SMCStrategy(BaseStrategy):
    def __init__(self, name="SMC_FVG", initial_balance=10000, rrr=2.0, risk_percent=0.01):
        super().__init__(name, initial_balance, risk_percent)
        self.rrr = rrr

    def prepare_features(self, df):
        print(f"--- Calculating Features: {self.name} ---")
        return df.with_columns([
            (pl.col("h").shift(2) < pl.col("l")).alias("bull_fvg"),
            (pl.col("l").shift(2) > pl.col("h")).alias("bear_fvg"),
            pl.col("h").shift(2).alias("fvg_top"),
            pl.col("l").shift(2).alias("fvg_bot")
        ]).fill_null(False)

    def run_logic(self, df):
        print(f"--- Running Execution: {self.name} ---")
        df = df.with_columns([
            ((pl.col("bull_fvg").cum_sum() > 0) & (pl.col("l") <= pl.col("fvg_top"))).alias("long_sig"),
            ((pl.col("bear_fvg").cum_sum() > 0) & (pl.col("h") >= pl.col("fvg_bot"))).alias("short_sig"),
            pl.col("l").rolling_min(10).alias("sl_long"),
            pl.col("h").rolling_max(10).alias("sl_short")
        ]).fill_null(strategy="backward")

        pnl_curve = jit_simulator(
            df['o'].to_numpy(), df['h'].to_numpy(),
            df['l'].to_numpy(), df['c'].to_numpy(),
            df['long_sig'].to_numpy(), df['short_sig'].to_numpy(),
            df['fvg_top'].to_numpy(),
            df['sl_long'].to_numpy(),
            (df['fvg_top'] + (df['fvg_top'] - df['sl_long']) * self.rrr).to_numpy(),
            self.initial_balance, self.risk_percent
        )

        # WICHTIG: Hier nutzen wir den dynamischen Namen!
        return df.with_columns(pl.Series(self.pnl_col, pnl_curve))