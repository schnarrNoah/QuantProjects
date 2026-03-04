# src/strategy/base.py
import polars as pl
from src.config.config import BALANCE, RISK_PERCENT
from src.execution.trader import jit_simulator
from src.processor.filter_time import session

class BaseStrategy:
    def __init__(self, name, initial_balance, risk_percent, rrr=None):
        self.name = name
        self.initial_balance = initial_balance
        self.risk_percent = risk_percent
        self.rrr = rrr
        self.pnl_col = f"pnl_{self.name.lower()}"

    def run_strategy(self, df: pl.DataFrame, settings: dict):
        """
        Diese Methode wird NIEMALS in der Unterklasse überschrieben.
        Sie ist das Gesetz, wie eine Strategie abläuft.
        """
        # 1. Start Lazy Mode
        lf = df.lazy()

        # 2. Schritt: Features (Wird von Unterklasse gefüllt)
        lf = self.prepare_features(lf)

        # 3. Schritt: Globaler Zeitfilter (Session wie NY, London etc.)
        lf = session(lf, settings.get("session"))

        # 4. Schritt: Logik & Exits (Wird von Unterklasse gefüllt)
        lf = self.logic(lf)

        # 5. RECHENPOWER: Alles zusammenführen
        final_df = lf.collect()

        if final_df.height == 0:
            return final_df

        # 6. Backtest Ausführung
        return self.run_backtest(final_df)

    # --- Diese Methoden MÜSSEN in der Unterklasse definiert werden ---

    def prepare_features(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Hier rufst du z.B. SessionProcessor.apply() auf."""
        raise NotImplementedError("Subklasse muss prepare_features definieren!")

    def logic(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Hier definierst du Signale und nutzt den ExitManager."""
        raise NotImplementedError("Subklasse muss logic definieren!")

    # --- Vererbbare Hilfsfunktion für den technischen Part ---

    def run_backtest(self, df: pl.DataFrame):
        """Übergang zu Numpy und JIT-Simulator."""
        # Wir gehen davon aus, dass 'long_sig', 'sl_long', 'tp_long' etc.
        # durch die logic() Methode erstellt wurden.
        pnl_curve = jit_simulator(
            df['o'].to_numpy(), df['h'].to_numpy(), df['l'].to_numpy(), df['c'].to_numpy(),
            df['long_sig'].to_numpy(), df['short_sig'].to_numpy(),
            df['c'].to_numpy(), df['c'].to_numpy(), # Entry zum Close
            df['sl_long'].to_numpy(), df['sl_short'].to_numpy(),
            df['tp_long'].to_numpy(), df['tp_short'].to_numpy(),
            self.initial_balance, self.risk_percent
        )
        return df.with_columns(pl.Series(self.pnl_col, pnl_curve))