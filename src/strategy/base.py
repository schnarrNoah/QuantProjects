import polars as pl
from src.process.trader import jit_simulator


class BaseStrategy:
    def __init__(self, name, initial_balance=10000, risk_percent=0.01, rrr=2.0):
        self.name = name
        self.initial_balance = initial_balance
        self.risk_percent = risk_percent
        self.rrr = rrr
        self.pnl_col = f"pnl_{self.name.lower()}"

    def set_signals(self, df, long, short):
        return df.with_columns([
            # .shift(1) sorgt dafür, dass ein Signal von Kerze 10 erst bei Kerze 11 gehandelt wird
            long.shift(1).fill_null(False).alias("long_sig"),
            short.shift(1).fill_null(False).alias("short_sig")
        ])

    def set_exit(self, df, entry_long, entry_short, lookback=10):
        return df.with_columns([
            pl.col("l").rolling_min(lookback).alias("sl_long"),
            pl.col("h").rolling_max(lookback).alias("sl_short")
        ]).with_columns([
            (pl.col(entry_long) + (pl.col(entry_long) - pl.col("sl_long")) * self.rrr).alias("tp_long"),
            (pl.col(entry_short) - (pl.col("sl_short") - pl.col(entry_short)) * self.rrr).alias("tp_short")
        ]).fill_null(strategy="backward")

    def execute(self, df, entry_long, entry_short):
        # Hier schieben wir jetzt alle Arrays (Long und Short) getrennt in den Motor!
        pnl_curve = jit_simulator(
            df['o'].to_numpy(), df['h'].to_numpy(), df['l'].to_numpy(), df['c'].to_numpy(),
            df['long_sig'].to_numpy(), df['short_sig'].to_numpy(),
            df[entry_long].to_numpy(), df[entry_short].to_numpy(),
            df['sl_long'].to_numpy(), df['sl_short'].to_numpy(),
            df['tp_long'].to_numpy(), df['tp_short'].to_numpy(),
            self.initial_balance, self.risk_percent
        )
        return df.with_columns(pl.Series(self.pnl_col, pnl_curve))

    def run(self, df):
        raise NotImplementedError("Jede Strategie muss eine 'run' Methode haben!")