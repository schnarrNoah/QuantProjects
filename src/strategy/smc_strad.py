from src.strategy.base import BaseStrategy
import src.process.filter_fvg as ind
import polars as pl


class SMCStrategy(BaseStrategy):
    def run(self, df):
        # 1. Berechne die Gaps auf den aktuellen Daten
        df = ind.fvg_1min(df)

        # 2. Erstelle geshiftete "Handels-Levels" (Look-Ahead Schutz)
        # Wir definieren hier genau, was wir zum Zeitpunkt des Handelns wissen durften
        df = df.with_columns([
            pl.col("bull_fvg").shift(1).fill_null(False).alias("can_trade_bull"),
            pl.col("bear_fvg").shift(1).fill_null(False).alias("can_trade_bear"),
            pl.col("bull_fvg_top").shift(1).alias("long_entry_p"),
            pl.col("bear_fvg_bottom").shift(1).alias("short_entry_p")
        ])

        # 3. Trigger-Bedingungen (Die Korrektur)
        # Long: Wir wussten vom Gap UND das aktuelle Low berührt das alte Top
        l_cond = (pl.col("can_trade_bull")) & (pl.col("l") <= pl.col("long_entry_p"))

        # Short: Wir wussten vom Gap UND das aktuelle High berührt die alte Unterkante
        # HIER war der Fehler: High ('h') muss das Level von unten testen!
        s_cond = (pl.col("can_trade_bear")) & (pl.col("h") >= pl.col("short_entry_p"))

        # 4. Signale setzen (set_signals nutzt intern fill_null)
        df = self.set_signals(df, long=l_cond, short=s_cond)

        # 5. Stop-Loss & Take-Profit basierend auf den geshifteten Einstiegspreisen
        df = self.set_exit(df, entry_long="long_entry_p", entry_short="short_entry_p")

        # 6. Ausführung: Nutze NUR die geshifteten Preis-Spalten!
        return self.execute(df, entry_long="long_entry_p", entry_short="short_entry_p")