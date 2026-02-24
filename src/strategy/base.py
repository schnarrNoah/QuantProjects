import polars as pl

class BaseStrategy:
    def __init__(self, name, initial_balance=10000, risk_percent=0.01):
        self.name = name
        self.initial_balance = initial_balance
        self.risk_percent = risk_percent
        # Jede Strategie bekommt dynamisch einen eigenen Spaltennamen!
        self.pnl_col = f"pnl_{self.name.lower().replace(' ', '_')}"

    def prepare_features(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError

    def run_logic(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError