from src.utils.parquet_reader import ParquetReader
from src.process.filter_time import session


class Pipeline:
    def __init__(self, data_path, start_date, end_date, start_time, end_time, session, strategies):
        self.data_path = data_path
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.session = session

        # Sicherstellen, dass strategies eine Liste ist
        self.strategies = strategies if isinstance(strategies, list) else [strategies]

        self.raw_data = None
        self.trading_data = None

    def load_data(self):
        reader = ParquetReader(self.data_path)
        self.raw_data = reader.load_range(start=self.start_date, end=self.end_date)
        return self

    def filter(self):
        if self.raw_data is not None:
            print(f"--- Filtering: Time ---")
            self.trading_data = session(self.raw_data, self.session)
            print(f"Session geladen: {len(self.trading_data)} Zeilen.")
        return self

    # NEUE METHODE: Iteriert über alle übergebenen Strategien
    def execute_strategies(self):
        if self.trading_data is None or self.trading_data.height == 0:
            print("Keine Daten zum Traden vorhanden.")
            return self

        print("--- Executing Strategies ---")
        for strat in self.strategies:
            print(f"-> Processing: {strat.name}")
            # 1. Indikatoren für diese Strategie berechnen
            self.trading_data = strat.prepare_features(self.trading_data)
            # 2. JIT Simulator für diese Strategie ausführen
            self.trading_data = strat.run_logic(self.trading_data)

            final_pnl = self.trading_data[strat.pnl_col][-1]
            print(f"   Backtest {strat.name} beendet. Final Balance: {final_pnl:.2f} USD")

        return self

    def visualize(self):
        if self.trading_data is not None:
            print("--- Bereite Dashboard vor ---")

            pnl_cols = [col for col in self.trading_data.columns if col.startswith("pnl_")]

            if not pnl_cols:
                print("!!! Fehler: Keine PnL-Spalten gefunden. Hat die Strategie Spalten angelegt?")
                return self

            cols_to_select = ["t", "o", "h", "l", "c"] + pnl_cols
            df_subset = self.trading_data.select(cols_to_select)

            main_pnl_col = pnl_cols[0]
            df_plot_data = df_subset.rename({main_pnl_col: "pnl_curve"})

            # --- Ab hier wie bisher ---
            total_candles = df_plot_data.height
            df_chart = df_plot_data.tail(5000).to_pandas()

            step = max(1, total_candles // 5000)
            df_perf = df_plot_data.gather_every(step).to_pandas()

            start_bal = df_plot_data["pnl_curve"].item(0)
            end_bal = df_plot_data["pnl_curve"].item(-1)

            from src.view.TabbedViewer import plot_modern_backtest
            plot_modern_backtest(df_chart, df_perf, total_candles, start_bal, end_bal, "BTC/USD")

        return self

    def run(self):
        (self.load_data()
         .filter()
         .execute_strategies()  # Die neue Schleife!
         .visualize())