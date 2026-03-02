from src.strategy.base import BaseStrategy
import polars as pl

class RSIStrategy(BaseStrategy):
    def run(self, df):
        # 1. Indikator berechnen (Features)
        df = df.with_columns(pl.col("c").rsi().alias("rsi"))

        # 2. LOGIK (Hier definierst DU die Strategie)
        l_cond = pl.col("rsi") < 30  # Kauf bei RSI unter 30
        s_cond = pl.col("rsi") > 70  # Verkauf bei RSI über 70

        # 3. Den Assistenten (BaseStrategy) die Arbeit machen lassen
        df = self.set_signals(df, long=l_cond, short=s_cond)
        df = self.set_exit(df, entry_long="c", entry_short="c") # Entry ist hier einfach der Close
        return self.execute(df, entry_long="c", entry_short="c")