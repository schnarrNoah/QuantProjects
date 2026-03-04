from src.strategy.base import BaseStrategy
from src.processor.session import SessionProcessor
from src.execution.exit import ExitManager
import polars as pl

class SMC_NY_Strategy(BaseStrategy):
    def prepare_features(self, lf):
        # Nutzt den externen Prozessor (Lazy)
        return SessionProcessor.extrema(lf, ["as", "lo"])

    def logic(self, lf):
        # 1. Signale definieren
        long_cond = (pl.col("c") > pl.col("lo_high"))
        short_cond = (pl.col("c") < pl.col("lo_low"))

        lf = lf.with_columns([
            long_cond.shift(1).fill_null(False).alias("long_sig"),
            short_cond.shift(1).fill_null(False).alias("short_sig")
        ])

        # 2. Exit Logik extern aufrufen (Lazy)
        # Hier kannst du jetzt wählen: fixed_rrr oder session_level_exit
        lf = ExitManager.fixed_rrr(lf, entry_long="c", entry_short="c", rrr=self.rrr, lookback=120)

        return lf